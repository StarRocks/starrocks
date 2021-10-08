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
template <typename T>
class CallBackClosure : public google::protobuf::Closure {
public:
    CallBackClosure() : _refs(0) {}
    ~CallBackClosure() override = default;

    // Disallow copy and assignment.
    CallBackClosure(const CallBackClosure& other) = delete;
    CallBackClosure& operator=(const CallBackClosure& other) = delete;

    void ref() { _refs.fetch_add(1); }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    bool has_in_flight_rpc() { return _refs > 1; }

    void addFailedHandler(std::function<void()> fn) { _failed_handler = std::move(fn); }
    void addSuccessHandler(std::function<void(const T&)> fn) { _success_handler = fn; }

    void Run() override {
        if (cntl.Failed()) {
            LOG(WARNING) << "brpc failed, error=" << berror(cntl.ErrorCode()) << ", error_text=" << cntl.ErrorText();
            _failed_handler();
        } else {
            _success_handler(result);
        }
        if (unref()) {
            delete this;
        }
    }

    brpc::Controller cntl;
    T result;

private:
    std::atomic<int> _refs;
    std::function<void()> _failed_handler;
    std::function<void(const T&)> _success_handler;
};
} // namespace starrocks