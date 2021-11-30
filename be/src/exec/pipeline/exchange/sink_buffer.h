// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <algorithm>
#include <future>
#include <list>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "column/chunk.h"
#include "gen_cpp/BackendService.h"
#include "runtime/current_thread.h"
#include "util/blocking_queue.hpp"
#include "util/brpc_stub_cache.h"
#include "util/callback_closure.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::pipeline {

using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;

struct TransmitChunkInfo {
    // For BUCKET_SHFFULE_HASH_PARTITIONED, multiple channels may be related to
    // a same exchange source fragment instance, so we should use fragment_instance_id
    // of the destination as the key of destination instead of channel_id.
    TUniqueId fragment_instance_id;
    doris::PBackendService_Stub* brpc_stub;
    PTransmitChunkParamsPtr params;
    butil::IOBuf attachment;
};

class SinkBuffer {
public:
    SinkBuffer(RuntimeState* state, const std::vector<TPlanFragmentDestination>& destinations, size_t num_sinkers)
            : _mem_tracker(state->instance_mem_tracker()),
              _brpc_timeout_ms(std::min(3600, state->query_options().query_timeout) * 1000),
              _num_remaining_sinkers(num_sinkers) {
        _threads = ExecEnv::GetInstance()->pipeline_exchange_sink_thread_pool();
        for (const auto& dest : destinations) {
            const auto& dest_instance_id = dest.fragment_instance_id;

            auto it = _num_sinkers_per_dest_instance.find(dest_instance_id.lo);
            if (it != _num_sinkers_per_dest_instance.end()) {
                it->second += num_sinkers;
            } else {
                _num_sinkers_per_dest_instance[dest_instance_id.lo] = num_sinkers;

                // This dest_instance_id first occurs, so create other variable for this dest instance.
                auto* closure = new CallBackClosure<PTransmitChunkResult, TUniqueId>();
                closure->ref();
                closure->addFailedHandler([this]() noexcept {
                    {
                        std::lock_guard<std::mutex> l(_mutex);
                        _is_finishing = true;
                    }
                    LOG(WARNING) << " transmit chunk rpc failed";
                    _in_flight_rpc_num.fetch_sub(1, std::memory_order_release);
                });
                closure->addSuccessHandler([this, closure](const PTransmitChunkResult& result) noexcept {
                    Status status(result.status());
                    if (!status.ok()) {
                        {
                            std::lock_guard<std::mutex> l(_mutex);
                            _is_finishing = true;
                        }
                        LOG(WARNING) << " transmit chunk rpc failed, " << status.message();
                    } else {
                        _is_closure_finishing[closure->context().lo]->store(true, std::memory_order_release);
                        _try_to_trigger_rpc_task(closure->context());
                    }
                    _in_flight_rpc_num.fetch_sub(1, std::memory_order_release);
                });
                _closures[dest_instance_id.lo] = closure;

                _is_closure_finishing[dest_instance_id.lo] = std::make_unique<std::atomic_bool>(false);

                _request_seqs[dest_instance_id.lo] = 0;
                _buffers[dest_instance_id.lo] = std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>();
                _instance_mutexes[dest_instance_id.lo] = std::make_unique<std::mutex>();

                PUniqueId finst_id;
                finst_id.set_hi(dest_instance_id.hi);
                finst_id.set_lo(dest_instance_id.lo);
                _instance_id2finst_id[dest_instance_id.lo] = std::move(finst_id);
            }

            _expected_eos = 0;
            for (auto& [_, num] : _num_sinkers_per_dest_instance) {
                _expected_eos += num;
            }
        }
    }

    ~SinkBuffer() {
        DCHECK(is_finished());

        // Relase resources
        for (auto& [_, closure] : _closures) {
            if (closure->unref()) {
                delete closure;
            }
        }

        for (auto& [_, buffer] : _buffers) {
            while (!buffer.empty()) {
                auto& request = buffer.front();
                request.params->release_finst_id();
                buffer.pop();
            }
        }
    }

    void add_request(const TransmitChunkInfo& request) {
        DCHECK(_expected_eos > 0);
        if (_is_finishing) {
            request.params->release_finst_id();
            return;
        }
        {
            std::lock_guard<std::mutex> l(*_instance_mutexes[request.fragment_instance_id.lo]);
            _buffers[request.fragment_instance_id.lo].push(request);
        }

        _try_to_trigger_rpc_task(request.fragment_instance_id);
    }

    bool is_full() const {
        // TODO(hcf) if one channel is congested, it may cause all other channel unwritable
        // std::queue' read is concurrent safe without mutex
        return std::any_of(_buffers.begin(), _buffers.end(),
                           [](const auto& entry) { return entry.second.size() > config::pipeline_io_buffer_size; });
    }

    bool is_finishing() const { return _is_finishing; }

    bool is_finished() const {
        if (!_is_finishing) {
            return false;
        }

        // Here is the guarantee that
        // 1. No new rpc task will be created after finishing stage
        // 2. No new brpc process will be triggered after finishing stage
        // Therefore, we just wait for existed rpc task and bprc process to be finished
        return !_is_rpc_task_active && _in_flight_rpc_num.load(std::memory_order_acquire) == 0;
    }

    void decrease_running_sinkers() {
        if (--_num_remaining_sinkers == 0) {
            std::lock_guard<std::mutex> l(_mutex);
            _is_finishing = true;
        }
    }

private:
    void _try_to_trigger_rpc_task(const TUniqueId& instance_id) {
        // Guarantee that no new rpc task will be created after finishing stage
        std::lock_guard<std::mutex> l(_mutex);
        if (_is_finishing) {
            return;
        }

        _events.push(instance_id);
        if (_is_rpc_task_active) {
            return;
        }

        PriorityThreadPool::Task task;
        task.work_function = [this]() { _process(); };
        // TODO(by satanson): set a proper priority
        task.priority = 20;

        _is_rpc_task_active = true;
        if (!_threads->offer(task)) {
            LOG(WARNING) << "SinkBuffer failed to offer rpc task due to thread pool shutdown";
            _is_finishing = true;
            _is_rpc_task_active = false;
        }
    }

    void _process() {
        try {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);

            for (;;) {
                TUniqueId instance_id;
                {
                    std::lock_guard<std::mutex> l(_mutex);
                    if (_is_finishing || _events.empty()) {
                        _is_rpc_task_active = false;
                        break;
                    }
                    instance_id = _events.front();
                    _events.pop();
                }

                auto* closure = _closures[instance_id.lo];
                auto& buffer = _buffers[instance_id.lo];
                {
                    for (;;) {
                        {
                            std::lock_guard<std::mutex> l(*_instance_mutexes[instance_id.lo]);
                            if (buffer.empty()) {
                                break;
                            }
                        }
                        if (closure->has_in_flight_rpc()) {
                            // This closure is bound to finish for a short while
                            // So block wait for the closure to be finished
                            if (_is_closure_finishing[instance_id.lo]->load(std::memory_order_acquire)) {
                                brpc::Join(closure->cntl.call_id());
                                _is_closure_finishing[instance_id.lo]->store(false, std::memory_order_release);
                            } else {
                                break;
                            }
                        }
                        TransmitChunkInfo request = buffer.front();
                        _send_rpc(request);
                        request.params->release_finst_id();
                        {
                            std::lock_guard<std::mutex> l(*_instance_mutexes[instance_id.lo]);
                            buffer.pop();
                        }
                    }
                }
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] sink_buffer::process: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] sink_buffer::process: UNKNOWN";
        }
    }

    void _send_rpc(TransmitChunkInfo& request) {
        // Guarantee that no new brpc process will be triggered after finishing stage
        std::lock_guard<std::mutex> l(_mutex);
        if (_is_finishing) {
            return;
        }

        if (request.params->eos()) {
            if (--_expected_eos == 0) {
                _is_finishing = true;
            }
            // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
            // eos is the last packet to send to finish the input stream of the corresponding of
            // ExchangeSourceOperator and eos is sent exactly-once.
            if (--_num_sinkers_per_dest_instance[request.fragment_instance_id.lo] > 0) {
                if (request.params->chunks_size() == 0) {
                    return;
                } else {
                    request.params->set_eos(false);
                }
            }
        }

        request.params->set_allocated_finst_id(&_instance_id2finst_id[request.fragment_instance_id.lo]);
        request.params->set_sequence(_request_seqs[request.fragment_instance_id.lo]++);

        auto* closure = _closures[request.fragment_instance_id.lo];
        DCHECK(closure != nullptr);
        DCHECK(!closure->has_in_flight_rpc());
        closure->ref();
        closure->cntl.Reset();
        closure->cntl.set_timeout_ms(_brpc_timeout_ms);
        closure->cntl.request_attachment().append(request.attachment);
        closure->set_context(request.fragment_instance_id);
        _in_flight_rpc_num.fetch_add(1, std::memory_order_release);
        request.brpc_stub->transmit_chunk(&closure->cntl, request.params.get(), &closure->result, closure);
    }

    PriorityThreadPool* _threads = nullptr;
    MemTracker* _mem_tracker = nullptr;
    const int32_t _brpc_timeout_ms;

    // Taking into account of efficiency, all the following maps
    // use int64_t as key, which is the field type of TUniqueId::lo
    // because TUniqueId::hi is exactly the same in one query

    phmap::flat_hash_map<int64_t, size_t> _num_sinkers_per_dest_instance;
    phmap::flat_hash_map<int64_t, int64_t> _request_seqs;
    std::atomic<int32_t> _in_flight_rpc_num = 0;

    // The request needs the reference to the allocated finst id,
    // so cache finst id for each dest fragment instance.
    phmap::flat_hash_map<int64_t, PUniqueId> _instance_id2finst_id;

    phmap::flat_hash_map<int64_t, CallBackClosure<PTransmitChunkResult, TUniqueId>*> _closures;
    // Closure becomes idle only when handle finished execution
    // Rpc task may be created in success handler of closure, if this task being executed immediately
    // and judge whether closure is idle by method closure->has_in_flight_rpc(), and this method may
    // return true if success handler hasn't finished its execution, which lead to this event(channel ready)
    // being swallowed
    phmap::flat_hash_map<int64_t, std::unique_ptr<std::atomic_bool>> _is_closure_finishing;
    phmap::flat_hash_map<int64_t, std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>> _buffers;
    phmap::flat_hash_map<int64_t, std::unique_ptr<std::mutex>> _instance_mutexes;

    std::mutex _mutex;
    // Events include 'new request' and 'channel ready'
    std::queue<TUniqueId, std::list<TUniqueId>> _events;

    // _is_rpc_task_active and _is_finishing are used in _try_to_trigger_rpc_task and _send_rpc.
    // - Write and read them with lock, to protect critical region between _try_to_trigger_rpc_task and _send_rpc.
    // - Read them without lock in is_finished() and ~SinkBuffer(), whose visibility is guaranteed by atomic.
    std::atomic<bool> _is_rpc_task_active = false;
    // True means that SinkBuffer needn't input chunk and send chunk anymore,
    // but there may be still RPC task or in-flight RPC running.
    // It becomes true, when all sinkers have sent EOS, or been set_finished/cancelled, or RPC has returned error.
    std::atomic<bool> _is_finishing = false;

    int32_t _expected_eos = 0;

    std::atomic<int> _num_remaining_sinkers;
};

} // namespace starrocks::pipeline
