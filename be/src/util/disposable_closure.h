// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <utility>

#include "service/brpc.h"

namespace starrocks {

class MemTracker;

// Disposable call back, it must be created on the heap.
// It will destroy itself after call back
template <typename T, typename C = void>
class DisposableClosure : public google::protobuf::Closure {
public:
    using FailedFunc = std::function<void(const C&)>;
    using SuccessFunc = std::function<void(const C&, const T&)>;

    DisposableClosure(const C& ctx) : _ctx(ctx) {}
    ~DisposableClosure() override = default;
    // Disallow copy and assignment.
    DisposableClosure(const DisposableClosure& other) = delete;
    DisposableClosure& operator=(const DisposableClosure& other) = delete;

    void addFailedHandler(FailedFunc fn) { _failed_handler = std::move(fn); }
    void addSuccessHandler(SuccessFunc fn) { _success_handler = fn; }

    void Run() noexcept override {
        std::unique_ptr<DisposableClosure> self_guard(this);

        try {
            if (cntl.Failed()) {
                LOG(WARNING) << "brpc failed, error=" << berror(cntl.ErrorCode())
                             << ", error_text=" << cntl.ErrorText();
                _failed_handler(_ctx);
            } else {
                _success_handler(_ctx, result);
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] Callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] Callback error: Unknown";
        }
    }

public:
    brpc::Controller cntl;
    T result;

private:
    const C _ctx;
    FailedFunc _failed_handler;
    SuccessFunc _success_handler;
};
} // namespace starrocks
