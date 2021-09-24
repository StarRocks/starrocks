// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "gen_cpp/BackendService.h"
#include "util/blocking_queue.hpp"
#include "util/brpc_stub_cache.h"
#include "util/callback_closure.h"

namespace starrocks::pipeline {

struct TransmitChunkInfo {
    PTransmitChunkParams params;
    PBackendService_Stub* brpc_stub;
};

class SinkBuffer {
public:
    SinkBuffer(size_t channel_number) : _closure_size(channel_number) {
        for (size_t i = 0; i < channel_number; ++i) {
            auto _chunk_closure = new CallBackClosure<PTransmitChunkResult>();
            _chunk_closure->ref();
            _chunk_closure->addFailedHandler([this]() {
                _in_flight_rpc_num--;
                _is_cancelled = true;
                LOG(WARNING) << " transmit chunk rpc failed, ";
            });

            _chunk_closure->addSuccessHandler([this](const PTransmitChunkResult& result) {
                _in_flight_rpc_num--;
                Status status(result.status());
                if (!status.ok()) {
                    _is_cancelled = true;
                    LOG(WARNING) << " transmit chunk rpc failed, ";
                }
            });
            _closures.push_back(_chunk_closure);
        }

        _thread = std::thread{&SinkBuffer::process, this};
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
    }

    bool is_full() const { return _in_flight_rpc_num >= _closure_size; }

    bool is_finished() const { return _in_flight_rpc_num == 0 || _is_cancelled; }

    bool is_cancelled() const { return _is_cancelled; }

    void set_sinker_number(int64_t sinker_number) { _sinker_number = sinker_number; }

private:
    void _send_rpc(TransmitChunkInfo& request) {
        if (request.params.eos()) {
            // Only send eos for last sinker, because we could only send eos once
            if (--_sinker_number > 0) {
                _in_flight_rpc_num--;
                return;
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
        request.brpc_stub->transmit_chunk(&closure->cntl, &request.params, &closure->result, closure);
        _request_seq++;
    }

    // To avoid lock
    const int32_t _closure_size;
    int64_t _request_seq = 0;
    int64_t _sinker_number = 0;
    std::atomic<int32_t> _in_flight_rpc_num = 0;
    std::atomic<bool> _is_cancelled{false};
    std::deque<CallBackClosure<PTransmitChunkResult>*> _closures;
    std::thread _thread;
    UnboundedBlockingQueue<TransmitChunkInfo> _pending_chunks;
};

} // namespace starrocks::pipeline