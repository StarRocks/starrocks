// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <google/protobuf/stubs/common.h>

#include <atomic>

#include "service/brpc.h"

namespace starrocks {

template <typename T>
class ReusableClosure : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID), _refs(0) {}
    ~ReusableClosure() override = default;

    int count() { return _refs.load(); }

    void ref() { _refs.fetch_add(1); }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    void Run() override {
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

    brpc::Controller cntl;
    T result;

private:
    brpc::CallId cid;
    std::atomic<int> _refs;
};

} // namespace starrocks
