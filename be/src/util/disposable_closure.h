// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <string_view>
#include <utility>

#include "service/brpc.h"

namespace starrocks {

class MemTracker;

// Disposable call back, it must be created on the heap.
// It will destroy itself after call back
template <typename T, typename C = void>
class DisposableClosure : public google::protobuf::Closure {
public:
    using FailedFunc = std::function<void(const C&, std::string_view)>;
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
                auto rpc_err =
                        fmt::format("brpc failed, error={}, error_text={}", berror(cntl.ErrorCode()), cntl.ErrorText());
                _failed_handler(_ctx, rpc_err);
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
