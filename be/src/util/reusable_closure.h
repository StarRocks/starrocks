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

#include "service/brpc.h"
#include "util/time.h"

namespace starrocks {

template <typename T>
class ReusableClosure : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID), _refs(0) {}
    ~ReusableClosure() override = default;

    int count() { return _refs.load(); }

    void ref() {
        _start_timestamp = MonotonicNanos();
        _refs.fetch_add(1);
    }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    void Run() override {
        _latency = MonotonicNanos() - _start_timestamp;
        if (unref()) {
            delete this;
        }
    }

    bool join() {
        if (cid != INVALID_BTHREAD_ID) {
            brpc::Join(cid);
            cid = INVALID_BTHREAD_ID;
            return true;
        } else {
            return false;
        }
    }

    void cancel() {
        if (cid != INVALID_BTHREAD_ID) {
            brpc::StartCancel(cid);
        }
    }

    void reset() {
        cntl.Reset();
        cid = cntl.call_id();
    }

    int64_t latency() { return _latency; }

    brpc::Controller cntl;
    T result;
    int64_t request_size;

private:
    brpc::CallId cid;
    std::atomic<int> _refs;
    int64_t _start_timestamp;
    int64_t _latency;
};

} // namespace starrocks
