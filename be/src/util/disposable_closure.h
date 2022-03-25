// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <utility>

#include "service/brpc.h"

namespace starrocks {

// Disposable call back, it must be created on the heap.
// It will destroy itself after call back
template <typename T, typename C = void>
class DisposableClosure : public google::protobuf::Closure {
public:
    DisposableClosure(const C& ctx) : _ctx(ctx) {
        _send_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
    }
    ~DisposableClosure() override = default;

    // Disallow copy and assignment.
    DisposableClosure(const DisposableClosure& other) = delete;
    DisposableClosure& operator=(const DisposableClosure& other) = delete;

    void addFailedHandler(std::function<void(const C&)> fn) { _failed_handler = std::move(fn); }
    void addSuccessHandler(std::function<void(const C&, const T&, const int64_t send_timestamp)> fn) {
        _success_handler = fn;
    }

    void Run() noexcept override {
        try {
            if (cntl.Failed()) {
                LOG(WARNING) << "brpc failed, error=" << berror(cntl.ErrorCode())
                             << ", error_text=" << cntl.ErrorText();
                _failed_handler(_ctx);
            } else {
                _success_handler(_ctx, result, _send_timestamp);
            }
            delete this;
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] Callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] Callback error: Unknown";
        }
    }

    brpc::Controller cntl;
    T result;

private:
    const C _ctx;
    int64_t _send_timestamp;
    std::function<void(const C&)> _failed_handler;
    std::function<void(const C&, const T&, const int64_t send_timestamp)> _success_handler;
};
} // namespace starrocks
