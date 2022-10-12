// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <google/protobuf/service.h>

#include "runtime/current_thread.h"

namespace starrocks {

// RAII: Call Run() of the closure on destruction.
// Just like brpc::ClosureGuard, but before calling Run(),
// the thread-local memory tracker will be reset to NULL.
class ClosureGuard {
public:
    ClosureGuard() : _done(NULL) {}

    // Constructed with a closure which will be Run() inside dtor.
    explicit ClosureGuard(google::protobuf::Closure* done) : _done(done) {}

    // Run internal closure if it's not NULL.
    ~ClosureGuard() {
        if (_done) {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            _done->Run();
        }
    }

    // Copying this object makes no sense.
    ClosureGuard(const ClosureGuard&) = delete;
    void operator=(const ClosureGuard&) = delete;
    ClosureGuard(ClosureGuard&&) = delete;
    void operator=(ClosureGuard&&) = delete;

    // Run internal closure if it's not NULL and set it to `done'.
    void reset(google::protobuf::Closure* done) {
        if (_done) {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            _done->Run();
        }
        _done = done;
    }

    // Return and set internal closure to NULL.
    google::protobuf::Closure* release() {
        google::protobuf::Closure* const prev_done = _done;
        _done = NULL;
        return prev_done;
    }

    // True if no closure inside.
    bool empty() const { return _done == NULL; }

    // Exchange closure with another guard.
    void swap(ClosureGuard& other) { std::swap(_done, other._done); }

private:
    google::protobuf::Closure* _done;
};

} // namespace starrocks
