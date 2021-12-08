// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/callback_closure.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <utility>

#include "service/brpc.h"

namespace starrocks {

// RefCountClosure with call back
template <typename T, typename C = void>
class CallBackClosure : public google::protobuf::Closure {
public:
    CallBackClosure(const C& ctx) : _ctx(ctx) {}
    ~CallBackClosure() override = default;

    bool is_in_flight() { return _refs > 0; }

    void ref() {
        DCHECK_EQ(_refs, 0);
        ++_refs;
    }

    void unref() {
        DCHECK_EQ(_refs, 1);
        --_refs;
    }

    // Disallow copy and assignment.
    CallBackClosure(const CallBackClosure& other) = delete;
    CallBackClosure& operator=(const CallBackClosure& other) = delete;

    void addFailedHandler(std::function<void()> fn) { _failed_handler = std::move(fn); }
    void addSuccessHandler(std::function<void(const C&, const T&)> fn) { _success_handler = fn; }

    void Run() noexcept override {
        try {
            if (cntl.Failed()) {
                LOG(WARNING) << "brpc failed, error=" << berror(cntl.ErrorCode())
                             << ", error_text=" << cntl.ErrorText();
                _failed_handler();
            } else {
                _success_handler(_ctx, result);
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
    std::atomic<int> _refs = 0;
    std::function<void()> _failed_handler;
    std::function<void(const C&, const T&)> _success_handler;
};
} // namespace starrocks
