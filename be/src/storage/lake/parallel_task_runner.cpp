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

#include "storage/lake/parallel_task_runner.h"

#include <memory>
#include <utility>

#include "base/debug/trace.h"
#include "common/thread/threadpool.h"

namespace starrocks::lake {

ParallelTaskRunner::ParallelTaskRunner(ThreadPoolToken* token) : _token(token), _trace(Trace::CurrentTrace()) {}

ParallelTaskRunner::~ParallelTaskRunner() {
    // A task still in flight would write into _mutex / _status as they are destroyed.
    (void)join();
}

void ParallelTaskRunner::run(std::function<Status()> task) {
    if (_token == nullptr) {
        // Inline: the caller's trace is already current, so nothing to adopt.
        record(task());
        return;
    }
    // Held by shared_ptr rather than moved into the lambda so the submit-failure path below can still
    // call it.
    auto shared = std::make_shared<std::function<Status()>>(std::move(task));
    auto st = _token->submit_func([this, shared]() {
        // Pool workers do not inherit the caller's thread-local trace, so re-adopt it or every
        // TRACE_COUNTER_* inside the task is silently dropped.
        ADOPT_TRACE(_trace);
        record((*shared)());
    });
    if (!st.ok()) {
        // A token only refuses while it is shutting down, and the work still has to happen, so run it
        // here. Safe for the same reason the task was safe on a worker: it shares nothing with the
        // caller thread beyond what it already captured.
        record((*shared)());
    }
}

Status ParallelTaskRunner::join() {
    if (_token != nullptr) {
        // Cheap on an already-idle token, so no need to track whether we have joined before; that also
        // keeps a runner reusable across two phases sharing one token.
        _token->wait();
    }
    std::lock_guard<std::mutex> l(_mutex);
    return _status;
}

void ParallelTaskRunner::record(const Status& st) {
    if (st.ok()) {
        return;
    }
    std::lock_guard<std::mutex> l(_mutex);
    // update() keeps the first non-OK status, matching what the hand-rolled copies did.
    _status.update(st);
}

} // namespace starrocks::lake
