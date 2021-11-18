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
    SinkBuffer(MemTracker* mem_tracker, const std::vector<TPlanFragmentDestination>& destinations, size_t num_sinkers)
            : _mem_tracker(mem_tracker) {
        _threads = ExecEnv::GetInstance()->pipeline_exchange_sink_thread_pool();
        for (const auto& dest : destinations) {
            const auto& dest_instance_id = dest.fragment_instance_id;

            auto it = _num_sinkers_per_dest_instance.find(dest_instance_id);
            if (it != _num_sinkers_per_dest_instance.end()) {
                it->second += num_sinkers;
            } else {
                _num_sinkers_per_dest_instance[dest_instance_id] = num_sinkers;

                // This dest_instance_id first occurs, so create closure and buffer for it.
                auto* closure = new CallBackClosure<PTransmitChunkResult>();
                closure->ref();
                closure->addFailedHandler([this]() noexcept {
                    _in_flight_rpc_num.fetch_sub(1, std::memory_order_release);
                    {
                        std::lock_guard<std::mutex> l(_mutex);
                        _is_finishing = true;
                    }
                    LOG(WARNING) << " transmit chunk rpc failed";
                });
                closure->addSuccessHandler([this](const PTransmitChunkResult& result) noexcept {
                    _in_flight_rpc_num.fetch_sub(1, std::memory_order_release);
                    Status status(result.status());
                    if (!status.ok()) {
                        {
                            std::lock_guard<std::mutex> l(_mutex);
                            _is_finishing = true;
                        }
                        LOG(WARNING) << " transmit chunk rpc failed, " << status.message();
                    }
                });
                _closures[dest_instance_id] = closure;
                _buffers[dest_instance_id] = std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>();
                _request_seqs[dest_instance_id] = 0;
            }

            _expected_eos = 0;
            for (auto& [_, num] : _num_sinkers_per_dest_instance) {
                _expected_eos += num;
            }
        }
    }

    ~SinkBuffer() {
        // Handle abnormal exit of SinkBuffer
        // TODO(hcf) It may be solved by adding state 'PENDING_FINISH' for ExchangeSinkOperator
        bool abnormal_exit = false;
        if (!is_finished()) {
            std::lock_guard<std::mutex> l(_mutex);
            abnormal_exit = true;
            _is_finishing = true;
        }
        if (abnormal_exit) {
            for (auto& [_, closure] : _closures) {
                auto cntl = &closure->cntl;
                brpc::Join(cntl->call_id());
            }
        }

        // Wait for the rpc task to exit to avoid
        // DeferOp accessing illegal memory
        std::promise<void>* exit_promise = nullptr;
        {
            std::lock_guard<std::mutex> l(_mutex);
            exit_promise = _rpc_task_exit_promise.get();
        }
        if (exit_promise != nullptr) {
            exit_promise->get_future().get();
        }

        // Relase resources
        for (auto& [_, closure] : _closures) {
            if (closure->unref()) {
                delete closure;
            }
        }
        for (auto& [_, buffer] : _buffers) {
            while (!buffer.empty()) {
                auto& info = buffer.front();
                info.params->release_finst_id();
                buffer.pop();
            }
        }
    }

    void add_request(const TransmitChunkInfo& request) {
        DCHECK(_expected_eos > 0);
        {
            // Guarantee that no request will be added to buffer after finishing stage
            std::lock_guard<std::mutex> l(_mutex);
            if (_is_finishing) {
                request.params->release_finst_id();
                return;
            } else {
                _buffers[request.fragment_instance_id].push(request);
            }
        }

        _try_to_trigger_rpc_task();
    }

    bool is_full() const {
        // TODO(hcf) if one channel is congested, it may cause all other channel unwritable
        // std::queue' read is concurrent safe without mutex
        return std::any_of(_buffers.begin(), _buffers.end(),
                           [](const auto& entry) { return entry.second.size() > config::pipeline_io_buffer_size; });
    }

    bool is_finished() {
        if (!_is_finishing) {
            return false;
        }

        // Here is the guarantee that
        // 1. No new rpc task will be created after finishing stage
        // 2. No new brpc process will be triggered after finishing stage
        // So we just wait for existed rpc task and bprc process to be finished
        return !_is_rpc_task_active && _in_flight_rpc_num.load(std::memory_order_acquire) == 0;
    }

private:
    void _try_to_trigger_rpc_task() {
        // Guarantee that no new rpc task will be created after finishing stage
        std::lock_guard<std::mutex> l(_mutex);
        if (_is_finishing) {
            return;
        }

        // Tell rpc task scan buffer one more time
        if (_is_rpc_task_active) {
            _need_one_more_check = true;
            return;
        }

        // Waiting for rpc task to exit, it won't take too long
        if (_rpc_task_exit_promise != nullptr) {
            _rpc_task_exit_promise->get_future().get();
        }
        _rpc_task_exit_promise = std::make_unique<std::promise<void>>();

        PriorityThreadPool::Task task;
        task.work_function = [this]() { _process(); };
        // TODO(by satanson): set a proper priority
        task.priority = 20;

        _is_rpc_task_active = true;
        if (!_threads->offer(task)) {
            LOG(WARNING) << "SinkBuffer failed to offer rpc task due to thread pool shutdown";
            _is_finishing = true;
            _is_rpc_task_active = false;
            _rpc_task_exit_promise = nullptr;
        }
    }

    void _process() {
        try {
            MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(_mem_tracker);
            DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

            for (;;) {
                bool buffer_empty = true;
                bool send_any = false;
                for (auto& [fragment_instance_id, buffer] : _buffers) {
                    auto* closure = _closures[fragment_instance_id];
                    DCHECK(closure != nullptr);
                    if (buffer.empty()) {
                        continue;
                    }
                    buffer_empty = false;
                    if (closure->has_in_flight_rpc()) {
                        continue;
                    }
                    send_any = true;

                    TransmitChunkInfo info = buffer.front();
                    _send_rpc(info);
                    info.params->release_finst_id();
                    if (info.params->eos()) {
                        std::lock_guard<std::mutex> l(_mutex);
                        if (--_expected_eos == 0) {
                            _is_finishing = true;
                        }
                    }
                    {
                        std::lock_guard<std::mutex> l(_mutex);
                        buffer.pop();
                    }
                }

                if (buffer_empty) {
                    // When traversing the buffer, a new request may be inserted
                    // so the value of buffer_empty may not be that accurate, we
                    // need to communicate with add_request through _need_one_more_check
                    // in order to avoid exiting rpc task with non-empty buffer
                    std::lock_guard<std::mutex> l(_mutex);
                    if (_need_one_more_check) {
                        _need_one_more_check = false;
                        continue;
                    }
                    _need_one_more_check = false;
                    _is_rpc_task_active = false;
                    break;
                } else if (!send_any) {
#ifdef __x86_64__
                    _mm_pause();
#else
                    // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
                    sched_yield();
#endif
                }
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] sink_buffer::process: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] sink_buffer::process: UNKNOWN";
        }
        _rpc_task_exit_promise->set_value();
    }

    void _send_rpc(TransmitChunkInfo& request) {
        // Guarantee that no new brpc process will be triggered after finishing stage
        std::lock_guard<std::mutex> l(_mutex);
        if (_is_finishing) {
            return;
        }

        if (request.params->eos()) {
            // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
            // eos is the last packet to send to finish the input stream of the corresponding of
            // ExchangeSourceOperator and eos is sent exactly-once.
            if (--_num_sinkers_per_dest_instance[request.fragment_instance_id] > 0) {
                if (request.params->chunks_size() == 0) {
                    return;
                } else {
                    request.params->set_eos(false);
                }
            }
        }
        request.params->set_sequence(_request_seqs[request.fragment_instance_id]++);
        auto* closure = _closures[request.fragment_instance_id];
        DCHECK(closure != nullptr);
        DCHECK(!closure->has_in_flight_rpc());
        closure->ref();
        closure->cntl.Reset();
        closure->cntl.set_timeout_ms(500);
        closure->cntl.request_attachment().append(request.attachment);
        _in_flight_rpc_num.fetch_add(1, std::memory_order_release);
        request.brpc_stub->transmit_chunk(&closure->cntl, request.params.get(), &closure->result, closure);
    }

    PriorityThreadPool* _threads = nullptr;
    MemTracker* _mem_tracker = nullptr;

    std::unordered_map<TUniqueId, size_t> _num_sinkers_per_dest_instance;
    std::unordered_map<TUniqueId, int64_t> _request_seqs;
    std::atomic<int32_t> _in_flight_rpc_num = 0;

    std::unordered_map<TUniqueId, CallBackClosure<PTransmitChunkResult>*> _closures;
    std::unordered_map<TUniqueId, std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>> _buffers;

    std::mutex _mutex;
    bool _is_finishing = false;
    bool _need_one_more_check = false;
    bool _is_rpc_task_active = false;
    int32_t _expected_eos = 0;

    // This field is used to help properly handle special situation
    // that SinkBuffer may destruct when method is_finished() return false
    std::unique_ptr<std::promise<void>> _rpc_task_exit_promise = nullptr;
};

} // namespace starrocks::pipeline
