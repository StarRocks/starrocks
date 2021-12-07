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
#include "runtime/runtime_state.h"
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
        for (const auto& dest : destinations) {
            const auto& dest_instance_id = dest.fragment_instance_id;

            auto it = _num_sinkers.find(dest_instance_id.lo);
            if (it != _num_sinkers.end()) {
                it->second += num_sinkers;
            } else {
                _num_sinkers[dest_instance_id.lo] = num_sinkers;

                // This dest_instance_id first occurs, so create other variable for this dest instance.
                auto* closure = new CallBackClosure<PTransmitChunkResult, TUniqueId>(dest_instance_id);
                closure->addFailedHandler([this, closure]() noexcept {
                    closure->give_back();
                    _is_finishing = true;
                    --_num_in_flight_rpc;
                    LOG(WARNING) << " transmit chunk rpc failed";
                });
                closure->addSuccessHandler(
                        [this, closure](const TUniqueId& instance_id, const PTransmitChunkResult& result) noexcept {
                            closure->give_back();
                            Status status(result.status());
                            if (!status.ok()) {
                                _is_finishing = true;
                                LOG(WARNING) << " transmit chunk rpc failed, " << status.message();
                            } else {
                                std::lock_guard<std::mutex> l(*_mutexes[instance_id.lo]);
                                _try_to_send_rpc(instance_id);
                            }
                            --_num_in_flight_rpc;
                        });
                _closures[dest_instance_id.lo] = closure;

                _request_seqs[dest_instance_id.lo] = 0;
                _buffers[dest_instance_id.lo] = std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>();
                _mutexes[dest_instance_id.lo] = std::make_unique<std::mutex>();

                PUniqueId finst_id;
                finst_id.set_hi(dest_instance_id.hi);
                finst_id.set_lo(dest_instance_id.lo);
                _instance_id2finst_id[dest_instance_id.lo] = std::move(finst_id);
            }

            _num_remaining_eos = 0;
            for (auto& [_, num] : _num_sinkers) {
                _num_remaining_eos += num;
            }
        }
    }

    ~SinkBuffer() {
        DCHECK(is_finished());

        // Relase resources
        for (auto& [_, closure] : _closures) {
            delete closure;
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
        DCHECK(_num_remaining_eos > 0);
        if (_is_finishing) {
            request.params->release_finst_id();
            return;
        }
        {
            auto& instance_id = request.fragment_instance_id;
            std::lock_guard<std::mutex> l(*_mutexes[instance_id.lo]);
            _buffers[instance_id.lo].push(request);
            _try_to_send_rpc(instance_id);
        }
    }

    bool is_full() const {
        // If one channel is congested, it may cause all other channel unwritable
        // std::queue' read is concurrent safe without mutex
        return std::any_of(_buffers.begin(), _buffers.end(),
                           [](const auto& entry) { return entry.second.size() > config::pipeline_io_buffer_size; });
    }

    bool is_finishing() const { return _is_finishing; }

    bool is_finished() const {
        if (!_is_finishing) {
            return false;
        }

        return _num_sending_rpc == 0 && _num_in_flight_rpc == 0;
    }

    void decrease_running_sinkers() {
        if (--_num_remaining_sinkers == 0) {
            _is_finishing = true;
        }
    }

private:
    void _try_to_send_rpc(const TUniqueId& instance_id) {
        DeferOp defer([this]() { --_num_sending_rpc; });
        ++_num_sending_rpc;

        if (_is_finishing) {
            return;
        }

        for (;;) {
            auto& buffer = _buffers[instance_id.lo];
            auto* closure = _closures[instance_id.lo];
            if (buffer.empty() || !closure->is_idle()) {
                return;
            }

            TransmitChunkInfo request = buffer.front();
            buffer.pop();

            if (request.params->eos()) {
                if (--_num_remaining_eos == 0) {
                    _is_finishing = true;
                }
                // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
                // eos is the last packet to send to finish the input stream of the corresponding of
                // ExchangeSourceOperator and eos is sent exactly-once.
                if (--_num_sinkers[instance_id.lo] > 0) {
                    if (request.params->chunks_size() == 0) {
                        request.params->release_finst_id();
                        continue;
                    } else {
                        request.params->set_eos(false);
                    }
                }
            }

            request.params->set_allocated_finst_id(&_instance_id2finst_id[instance_id.lo]);
            request.params->set_sequence(_request_seqs[instance_id.lo]++);

            ++_num_in_flight_rpc;

            closure->borrow();
            closure->cntl.Reset();
            closure->cntl.set_timeout_ms(_brpc_timeout_ms);
            closure->cntl.request_attachment().append(request.attachment);
            request.brpc_stub->transmit_chunk(&closure->cntl, request.params.get(), &closure->result, closure);

            request.params->release_finst_id();
            return;
        }
    }

    MemTracker* _mem_tracker = nullptr;
    const int32_t _brpc_timeout_ms;

    // Taking into account of efficiency, all the following maps
    // use int64_t as key, which is the field type of TUniqueId::lo
    // because TUniqueId::hi is exactly the same in one query

    phmap::flat_hash_map<int64_t, size_t> _num_sinkers;
    phmap::flat_hash_map<int64_t, int64_t> _request_seqs;
    std::atomic<int32_t> _num_in_flight_rpc = 0;
    std::atomic<int32_t> _num_remaining_sinkers;
    std::atomic<int32_t> _num_remaining_eos = 0;

    // The request needs the reference to the allocated finst id,
    // so cache finst id for each dest fragment instance.
    phmap::flat_hash_map<int64_t, PUniqueId> _instance_id2finst_id;
    phmap::flat_hash_map<int64_t, CallBackClosure<PTransmitChunkResult, TUniqueId>*> _closures;
    phmap::flat_hash_map<int64_t, std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>> _buffers;
    phmap::flat_hash_map<int64_t, std::unique_ptr<std::mutex>> _mutexes;

    // True means that SinkBuffer needn't input chunk and send chunk anymore,
    // but there may be still in-flight RPC running.
    // It becomes true, when all sinkers have sent EOS, or been set_finished/cancelled, or RPC has returned error.
    // Unfortunately, _is_finishing itself cannot guarantee that no brpc process will trigger if entering finishing stage,
    // considering the following situations(events order by time)
    //      time1(thread A): _try_to_send_rpc check _is_finishing which returns false
    //      time2(thread B): set _is_finishing to true
    //      time3(thread A): _try_to_send_rpc trigger brpc process
    // So _num_sending_rpc is introduced to solve this problem by providing extra information
    // of how many threads are calling _try_to_send_rpc
    std::atomic<bool> _is_finishing = false;
    std::atomic<int32_t> _num_sending_rpc = 0;

}; // namespace starrocks::pipeline

} // namespace starrocks::pipeline
