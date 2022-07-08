// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
    DisposableClosure(const C& ctx, MemTracker* const mem_tracker) : _ctx(ctx), _mem_tracker(mem_tracker) {}
    ~DisposableClosure() override = default;

    // Disallow copy and assignment.
    DisposableClosure(const DisposableClosure& other) = delete;
    DisposableClosure& operator=(const DisposableClosure& other) = delete;

    void addFailedHandler(std::function<void(const C&)> fn) { _failed_handler = std::move(fn); }
    void addSuccessHandler(std::function<void(const C&, const T&)> fn) { _success_handler = fn; }

    void Run() noexcept override {
        try {
            if (cntl.Failed()) {
                LOG(WARNING) << "brpc failed, error=" << berror(cntl.ErrorCode())
                             << ", error_text=" << cntl.ErrorText();
                _failed_handler(_ctx);
            } else {
                _success_handler(_ctx, result);
            }
            {
                // The request memory is acquired by ExchangeSinkOperator,
                // so use the instance_mem_tracker passed from ExchangeSinkOperator to release memory.
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
                delete this;
            }
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
    std::function<void(const C&)> _failed_handler;
    std::function<void(const C&, const T&)> _success_handler;
    MemTracker* const _mem_tracker = nullptr;
};
} // namespace starrocks
