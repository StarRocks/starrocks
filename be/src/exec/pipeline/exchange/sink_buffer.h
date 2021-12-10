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
#include "util/defer_op.h"
#include "util/disposable_closure.h"
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

// TODO(hcf) how to export brpc error
class SinkBuffer {
public:
    SinkBuffer(RuntimeState* state, const std::vector<TPlanFragmentDestination>& destinations, size_t num_sinkers)
            : _mem_tracker(state->instance_mem_tracker()),
              _brpc_timeout_ms(std::min(3600, state->query_options().query_timeout) * 1000),
              _num_uncancelled_sinkers(num_sinkers) {
        for (const auto& dest : destinations) {
            const auto& dest_instance_id = dest.fragment_instance_id;

            auto it = _num_sinkers.find(dest_instance_id.lo);
            if (it != _num_sinkers.end()) {
                it->second += num_sinkers;
            } else {
                _num_sinkers[dest_instance_id.lo] = num_sinkers;

                _request_seqs[dest_instance_id.lo] = 0;
                _buffers[dest_instance_id.lo] = std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>();
                _num_finished_rpcs[dest_instance_id.lo] = 0;
                _num_in_flight_rpcs[dest_instance_id.lo] = 0;
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
        // std::queue' read is concurrent safe without mutex
        // Judgement may not that accurate because we do not known in advance which
        // instance the data to be sent corresponds to
        size_t max_buffer_size = config::pipeline_sink_buffer_size * _buffers.size();
        size_t buffer_size = 0;
        for (auto& [_, buffer] : _buffers) {
            buffer_size += buffer.size();
        }
        return buffer_size > max_buffer_size;
    }

    bool is_finished() const {
        if (!_is_finishing) {
            return false;
        }

        return _num_sending_rpc == 0 && _total_in_flight_rpc == 0;
    }

    // When all the ExchangeSinkOperator shared this SinkBuffer are cancelled,
    // the rest chunk request and EOS request needn't be sent anymore.
    void cancel_one_sinker() {
        if (--_num_uncancelled_sinkers == 0) {
            _is_finishing = true;
        }
    }

private:
    void _try_to_send_rpc(const TUniqueId& instance_id) {
        DeferOp decrease_defer([this]() { --_num_sending_rpc; });
        ++_num_sending_rpc;

        for (;;) {
            if (_is_finishing) {
                return;
            }

            auto& buffer = _buffers[instance_id.lo];
            if (buffer.empty() || _num_in_flight_rpcs[instance_id.lo] >= config::pipeline_sink_brpc_dop) {
                return;
            }

            TransmitChunkInfo request = buffer.front();
            bool need_wait = false;
            DeferOp pop_defer([&need_wait, &buffer]() {
                if (need_wait) {
                    return;
                }
                buffer.pop();
            });

            // The order of data transmiting in IO level may not be strictly the same as
            // the order of submitting data packets
            // But we must guarantee that first packet must be received first
            if (_num_finished_rpcs[instance_id.lo] == 0 && _num_in_flight_rpcs[instance_id.lo] > 0) {
                need_wait = true;
                return;
            }
            if (request.params->eos()) {
                DeferOp eos_defer([this, &instance_id, &need_wait]() {
                    if (need_wait) {
                        return;
                    }
                    if (--_num_remaining_eos == 0) {
                        _is_finishing = true;
                    }
                    --_num_sinkers[instance_id.lo];
                });
                // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
                // eos is the last packet to send to finish the input stream of the corresponding of
                // ExchangeSourceOperator and eos is sent exactly-once.
                if (_num_sinkers[instance_id.lo] > 1) {
                    if (request.params->chunks_size() == 0) {
                        request.params->release_finst_id();
                        continue;
                    } else {
                        request.params->set_eos(false);
                    }
                } else {
                    // The order of data transmiting in IO level may not be strictly the same as
                    // the order of submitting data packets
                    // But we must guarantee that eos packent must be the last packet
                    if (_num_in_flight_rpcs[instance_id.lo] > 0) {
                        need_wait = true;
                        return;
                    }
                }
            }

            auto* closure = new DisposableClosure<PTransmitChunkResult, TUniqueId>(instance_id);
            closure->addFailedHandler([this, closure](const TUniqueId& instance_id) noexcept {
                _is_finishing = true;
                {
                    std::lock_guard<std::mutex> l(*_mutexes[instance_id.lo]);
                    ++_num_finished_rpcs[instance_id.lo];
                    --_num_in_flight_rpcs[instance_id.lo];
                }
                --_total_in_flight_rpc;
                LOG(WARNING) << " transmit chunk rpc failed";
            });
            closure->addSuccessHandler(
                    [this, closure](const TUniqueId& instance_id, const PTransmitChunkResult& result) noexcept {
                        Status status(result.status());
                        {
                            std::lock_guard<std::mutex> l(*_mutexes[instance_id.lo]);
                            ++_num_finished_rpcs[instance_id.lo];
                            --_num_in_flight_rpcs[instance_id.lo];
                        }
                        if (!status.ok()) {
                            _is_finishing = true;
                            LOG(WARNING) << " transmit chunk rpc failed, " << status.message();
                        } else {
                            std::lock_guard<std::mutex> l(*_mutexes[instance_id.lo]);
                            _try_to_send_rpc(instance_id);
                        }
                        --_total_in_flight_rpc;
                    });

            request.params->set_allocated_finst_id(&_instance_id2finst_id[instance_id.lo]);
            request.params->set_sequence(_request_seqs[instance_id.lo]++);

            ++_total_in_flight_rpc;
            ++_num_in_flight_rpcs[instance_id.lo];

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

    phmap::flat_hash_map<int64_t, int64_t> _num_sinkers;
    phmap::flat_hash_map<int64_t, int64_t> _request_seqs;
    std::atomic<int32_t> _total_in_flight_rpc = 0;
    std::atomic<int32_t> _num_uncancelled_sinkers;
    std::atomic<int32_t> _num_remaining_eos = 0;

    // The request needs the reference to the allocated finst id,
    // so cache finst id for each dest fragment instance.
    phmap::flat_hash_map<int64_t, PUniqueId> _instance_id2finst_id;
    phmap::flat_hash_map<int64_t, std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>> _buffers;
    phmap::flat_hash_map<int64_t, int32_t> _num_finished_rpcs;
    phmap::flat_hash_map<int64_t, int32_t> _num_in_flight_rpcs;
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
