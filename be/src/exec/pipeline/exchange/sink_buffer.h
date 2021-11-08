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

// TODO(hcf) There is a potential optimization point that different channels can be parallel
class SinkBuffer {
public:
    SinkBuffer(size_t channel_number, size_t num_sinkers) : _num_sinkers_per_channel(channel_number, num_sinkers) {
        _closure = new CallBackClosure<PTransmitChunkResult>();
        _closure->ref();
        _closure->addFailedHandler([this]() noexcept {
            _is_cancelled = true;
            LOG(WARNING) << " transmit chunk rpc failed";
        });

        _closure->addSuccessHandler([this](const PTransmitChunkResult& result) noexcept {
            Status status(result.status());
            if (!status.ok()) {
                _is_cancelled = true;
                LOG(WARNING) << " transmit chunk rpc failed, " << status.message();
            }
        });
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
        if (_closure->unref()) {
            delete _closure;
        }
    }

    void add_request(const TransmitChunkInfo& request) { _pending_chunks.put(request); }

    void process() {
        try {
            while (true) {
                if (_closure->has_in_flight_rpc()) {
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

    bool is_full() const { return _pending_chunks.get_size() >= config::pipeline_io_cache_size; }

    bool is_finished() const { return (_pending_chunks.empty() && !_closure->has_in_flight_rpc()) || _is_cancelled; }

    bool is_cancelled() const { return _is_cancelled; }

private:
    void _send_rpc(TransmitChunkInfo& request) {
        if (request.params.eos()) {
            // Only the last eos is sent to ExchangeSourceOperator. it must be guaranteed that
            // eos is the last packet to send to finish the input stream of the corresponding of
            // ExchangeSourceOperator and eos is sent exactly-once.
            if (--_num_sinkers_per_channel[request.channel_id] > 0) {
                if (request.params.chunks_size() == 0) {
                    return;
                } else {
                    request.params.set_eos(false);
                }
            }
        }
        request.params.set_sequence(_request_seq);
        DCHECK(!_closure->has_in_flight_rpc());
        // Move the closure has flight rpc to tail
        _closure->ref();
        _closure->cntl.Reset();
        _closure->cntl.set_timeout_ms(500);
        _closure->cntl.request_attachment().append(request.attachment);
        request.brpc_stub->transmit_chunk(&_closure->cntl, &request.params, &_closure->result, _closure);
        _request_seq++;
    }

    // To avoid lock
    vector<size_t> _num_sinkers_per_channel;
    int64_t _request_seq = 0;
    std::atomic<bool> _is_cancelled{false};
    CallBackClosure<PTransmitChunkResult>* _closure;
    std::thread _thread;
    UnboundedBlockingQueue<TransmitChunkInfo> _pending_chunks;
};

} // namespace starrocks::pipeline
