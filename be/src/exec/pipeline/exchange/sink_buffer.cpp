// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/sink_buffer.h"

namespace starrocks::pipeline {

SinkBuffer::SinkBuffer(RuntimeState* state, const std::vector<TPlanFragmentDestination>& destinations,
                       size_t num_sinkers)
        : _mem_tracker(state->instance_mem_tracker()),
          _brpc_timeout_ms(std::min(3600, state->query_options().query_timeout) * 1000),
          _num_uncancelled_sinkers(num_sinkers) {
    for (const auto& dest : destinations) {
        const auto& instance_id = dest.fragment_instance_id;

        auto it = _num_sinkers.find(instance_id.lo);
        if (it != _num_sinkers.end()) {
            it->second += num_sinkers;
        } else {
            _num_sinkers[instance_id.lo] = num_sinkers;

            _request_sequences[instance_id.lo] = 0;
            _max_processed_sequences[instance_id.lo] = -1;
            _unprocessed_sequences[instance_id.lo] = std::unordered_set<int64_t>();
            _buffers[instance_id.lo] = std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>>();
            _num_finished_rpcs[instance_id.lo] = 0;
            _num_in_flight_rpcs[instance_id.lo] = 0;
            _mutexes[instance_id.lo] = std::make_unique<std::mutex>();

            PUniqueId finst_id;
            finst_id.set_hi(instance_id.hi);
            finst_id.set_lo(instance_id.lo);
            _instance_id2finst_id[instance_id.lo] = std::move(finst_id);
        }

        _num_remaining_eos = 0;
        for (auto& [_, num] : _num_sinkers) {
            _num_remaining_eos += num;
        }
    }
}

SinkBuffer::~SinkBuffer() {
    DCHECK(is_finished());

    for (auto& [_, buffer] : _buffers) {
        while (!buffer.empty()) {
            auto& request = buffer.front();
            request.params->release_finst_id();
            buffer.pop();
        }
    }
}

void SinkBuffer::add_request(const TransmitChunkInfo& request) {
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

bool SinkBuffer::is_full() const {
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

bool SinkBuffer::is_finished() const {
    if (!_is_finishing) {
        return false;
    }

    return _num_sending_rpc == 0 && _total_in_flight_rpc == 0;
}

// When all the ExchangeSinkOperator shared this SinkBuffer are cancelled,
// the rest chunk request and EOS request needn't be sent anymore.
void SinkBuffer::cancel_one_sinker() {
    if (--_num_uncancelled_sinkers == 0) {
        _is_finishing = true;
    }
}

void SinkBuffer::_process_send_window(const TUniqueId& instance_id, const int64_t sequence) {
    auto& sequences = _unprocessed_sequences[instance_id.lo];
    sequences.insert(sequence);
    auto& max_processed_sequence = _max_processed_sequences[instance_id.lo];
    std::unordered_set<int64_t>::iterator it;
    while ((it = sequences.find(max_processed_sequence + 1)) != sequences.end()) {
        sequences.erase(it);
        ++max_processed_sequence;
    }
}

void SinkBuffer::_try_to_send_rpc(const TUniqueId& instance_id) {
    DeferOp decrease_defer([this]() { --_num_sending_rpc; });
    ++_num_sending_rpc;

    for (;;) {
        if (_is_finishing) {
            return;
        }

        auto& buffer = _buffers[instance_id.lo];
        int64_t unprocessed_window_size = _request_sequences[instance_id.lo] - _max_processed_sequences[instance_id.lo];
        if (buffer.empty() || unprocessed_window_size >= config::pipeline_sink_brpc_dop) {
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

        request.params->set_allocated_finst_id(&_instance_id2finst_id[instance_id.lo]);
        request.params->set_sequence(_request_sequences[instance_id.lo]++);

        auto* closure =
                new DisposableClosure<PTransmitChunkResult, ClosureContext>({instance_id, request.params->sequence()});
        closure->addFailedHandler([this, closure](const ClosureContext& ctx) noexcept {
            _is_finishing = true;
            {
                std::lock_guard<std::mutex> l(*_mutexes[ctx.instance_id.lo]);
                ++_num_finished_rpcs[ctx.instance_id.lo];
                --_num_in_flight_rpcs[ctx.instance_id.lo];
            }
            --_total_in_flight_rpc;
            LOG(WARNING) << " transmit chunk rpc failed";
        });
        closure->addSuccessHandler(
                [this, closure](const ClosureContext& ctx, const PTransmitChunkResult& result) noexcept {
                    Status status(result.status());
                    {
                        std::lock_guard<std::mutex> l(*_mutexes[ctx.instance_id.lo]);
                        ++_num_finished_rpcs[ctx.instance_id.lo];
                        --_num_in_flight_rpcs[ctx.instance_id.lo];
                    }
                    if (!status.ok()) {
                        _is_finishing = true;
                        LOG(WARNING) << " transmit chunk rpc failed, " << status.message();
                    } else {
                        std::lock_guard<std::mutex> l(*_mutexes[ctx.instance_id.lo]);
                        _process_send_window(ctx.instance_id, ctx.sequence);
                        _try_to_send_rpc(ctx.instance_id);
                    }
                    --_total_in_flight_rpc;
                });

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
} // namespace starrocks::pipeline
