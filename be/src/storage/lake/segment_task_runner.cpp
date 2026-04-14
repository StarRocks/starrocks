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

#include <algorithm>
#include <utility>

#include "common/thread/threadpool.h"

namespace starrocks::lake {

SegmentTaskRunner::SegmentTaskRunner(ThreadPool* pool, int max_concurrency)
        : _max_concurrency(std::max(1, max_concurrency)) {
    if (pool != nullptr) {
        // CONCURRENT mode: tasks run in parallel up to the pool's max_threads.
        // The per-job concurrency cap is enforced by the caller (it submits
        // up to _max_concurrency tasks at a time, or relies on the pool's
        // global capacity). We do not introduce a per-token concurrency
        // counter here because the pool is already sized to
        //   alter_tablet_worker_count * lake_schema_change_per_tablet_parallelism
        // so a single job will naturally see at most _max_concurrency slots.
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
