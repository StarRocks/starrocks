// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "gen_cpp/BackendService.h"
#include "util/blocking_queue.hpp"
#include "util/brpc_stub_cache.h"
#include "util/callback_closure.h"

namespace starrocks::pipeline {

struct TransmitChunkInfo {
    size_t channel_id;
    PTransmitChunkParams params;
    doris::PBackendService_Stub* brpc_stub;

    // for protocol header.
    butil::IOBuf attachment;
};

class SinkBuffer {
public:
    SinkBuffer(size_t channel_number, size_t num_sinkers)
            : _closure_size(channel_number), _num_sinkers_per_channel(channel_number, num_sinkers) {
        for (size_t i = 0; i < channel_number; ++i) {
            auto _chunk_closure = new CallBackClosure<PTransmitChunkResult>();
            _chunk_closure->ref();
            _chunk_closure->addFailedHandler([this]() noexcept {
                _in_flight_rpc_num--;
                _is_cancelled = true;
                LOG(WARNING) << " transmit chunk rpc failed, ";
            });

            _chunk_closure->addSuccessHandler([this](const PTransmitChunkResult& result) noexcept {
                _in_flight_rpc_num--;
                Status status(result.status());
                if (!status.ok()) {
                    _is_cancelled = true;
                    LOG(WARNING) << " transmit chunk rpc failed, ";
                }
            });
            _closures.push_back(_chunk_closure);
        }
        try {
            _thread = std::thread{&SinkBuffer::process, this};
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] create thread: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] create thread: unknown";
        }
    }

    ~SinkBuffer() {
        _pending_chunks.shutdown();
        _thread.join();
        for (auto* closure : _closures) {
            if (closure->unref()) {
                delete closure;
            }
        }
    }

    void add_request(const TransmitChunkInfo& request) {
        _in_flight_rpc_num++;
        _pending_chunks.put(request);
    }

    void process() {
        try {
            while (true) {
                // If the head closure has flight rpc, means all closures are busy
                if (_closures.front()->has_in_flight_rpc()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }

                TransmitChunkInfo info;
                if (!_pending_chunks.blocking_get(&info)) {
                    break;
                }
                _send_rpc(info);

                // The original design is bad, we must release_finst_id here!
                info.params.release_finst_id();
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] sink_buffer::process: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] sink_buffer::process: UNKNOWN";
        }
    }

    bool is_full() const { return _in_flight_rpc_num >= _closure_size; }

    bool is_finished() const { return _in_flight_rpc_num == 0 || _is_cancelled; }

    bool is_cancelled() const { return _is_cancelled; }

private:
    void _send_rpc(TransmitChunkInfo& request) {
        if (request.params.eos()) {
            // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
            // eos is the last packet to send to finish the input stream of the corresponding of
            // ExchangeSourceOperator and eos is sent exactly-once.
            if (--_num_sinkers_per_channel[request.channel_id] > 0) {
                if (request.params.chunks_size() == 0) {
                    _in_flight_rpc_num--;
                    return;
                } else {
                    request.params.set_eos(false);
                }
            }
        }
        request.params.set_sequence(_request_seq);
        auto* closure = _closures.front();
        DCHECK(!closure->has_in_flight_rpc());
        // Move the closure has flight rpc to tail
        _closures.pop_front();
        _closures.push_back(closure);
        closure->ref();
        closure->cntl.Reset();
        closure->cntl.set_timeout_ms(500);
        closure->cntl.request_attachment().append(request.attachment);
        request.brpc_stub->transmit_chunk(&closure->cntl, &request.params, &closure->result, closure);
        _request_seq++;
    }

    // To avoid lock
    const int32_t _closure_size;
    vector<size_t> _num_sinkers_per_channel;
    int64_t _request_seq = 0;
    std::atomic<int32_t> _in_flight_rpc_num = 0;
    std::atomic<bool> _is_cancelled{false};
    std::deque<CallBackClosure<PTransmitChunkResult>*> _closures;
    std::thread _thread;
    UnboundedBlockingQueue<TransmitChunkInfo> _pending_chunks;
};

} // namespace starrocks::pipeline
