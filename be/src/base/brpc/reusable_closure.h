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

#include <bvar/bvar.h>
#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <cerrno>

#include "base/brpc/brpc.h"
#include "base/time/time.h"

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
        if (_inflight != nullptr) {
            *_inflight << -1;
        }
        // Recorded here rather than by whoever joins the call, so a call the owner never joins (it gave
        // up after another call failed) still counts once it completes. A call the owner cancelled is
        // not an outcome of the rpc layer and is left out.
        if (cntl.ErrorCode() != ECANCELED) {
            if (_latency_us != nullptr) {
                *_latency_us << _latency / 1000;
            }
            if (_failed != nullptr && cntl.Failed()) {
                *_failed << 1;
            }
        }
        if (unref()) {
            delete this;
        }
    }

    // The bvars the calls issued through this closure report to; set once before the first call.
    // inflight counts calls brpc has not completed yet: the owner calls issued() right before handing
    // the closure to the stub and Run() takes the count back, so a call that is cancelled or never
    // joined still leaves it balanced. latency_us gets one sample and failed one increment per
    // completed call, from Run().
    void set_rpc_metrics(bvar::Adder<int64_t>* inflight, bvar::LatencyRecorder* latency_us,
                         bvar::Adder<int64_t>* failed) {
        _inflight = inflight;
        _latency_us = latency_us;
        _failed = failed;
    }

    void issued() {
        if (_inflight != nullptr) {
            *_inflight << 1;
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
    bvar::Adder<int64_t>* _inflight = nullptr;
    bvar::LatencyRecorder* _latency_us = nullptr;
    bvar::Adder<int64_t>* _failed = nullptr;
};

} // namespace starrocks
