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

#include "storage/lake/segment_task_runner.h"

#include <utility>

#include "common/thread/threadpool.h"

namespace starrocks::lake {

SegmentTaskRunner::SegmentTaskRunner(ThreadPool* pool, int /*max_concurrency*/) {
    // The `max_concurrency` parameter is informational — at runtime the
    // outer pool is already sized to
    //   alter_tablet_worker_count * lake_schema_change_per_tablet_parallelism
    // so a single job naturally observes at most that many concurrent
    // slots without any per-token cap inside the runner.
    if (pool != nullptr) {
        _token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    }
}

SegmentTaskRunner::~SegmentTaskRunner() {
    // Belt-and-suspenders: callers must call wait(); if they didn't, the
    // token destructor will Shutdown() and wait for in-flight tasks. We
    // also explicitly shutdown to drop queued tasks immediately on abort.
    if (_token != nullptr) {
        _token->shutdown();
    }
}

Status SegmentTaskRunner::submit(std::function<Status()> task) {
    if (_token == nullptr) {
        // No pool: run inline. Preserves correctness when the inner pool is
        // not configured (e.g. compute node) at the cost of no parallelism.
        if (_failed.load(std::memory_order_acquire)) {
            return Status::OK();
        }
        Status s = task();
        if (!s.ok()) {
            record_error(s);
        }
        return Status::OK();
    }
    auto wrapped = [this, t = std::move(task)]() {
        if (_failed.load(std::memory_order_acquire)) {
            // Fast-fail: another task already errored, skip this one to
            // drain the queue quickly.
            return;
        }
        Status s = t();
        if (!s.ok()) {
            record_error(s);
        }
    };
    return _token->submit_func(std::move(wrapped));
}

Status SegmentTaskRunner::wait() {
    if (_token != nullptr) {
        _token->wait();
    }
    std::lock_guard<std::mutex> lg(_err_mtx);
    return _first_error;
}

void SegmentTaskRunner::record_error(const Status& s) {
    bool expected = false;
    if (_failed.compare_exchange_strong(expected, true)) {
        std::lock_guard<std::mutex> lg(_err_mtx);
        _first_error = s;
    }
}

} // namespace starrocks::lake
