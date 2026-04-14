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

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

#include "common/status.h"

namespace starrocks {
class ThreadPool;
class ThreadPoolToken;
} // namespace starrocks

namespace starrocks::lake {

// Per-job parallel submission helper for lake schema-change inner sub-tasks.
//
// Currently used only by the ADD INDEX fast path (AddIndexSchemaChange) to
// build per-segment .idx files in parallel. Other schema-change paths
// (LinkedSchemaChange, DirectSchemaChange, SortedSchemaChange) and the DROP
// INDEX fast path remain single-threaded and do not use this runner.
//
// The runner wraps a CONCURRENT ThreadPoolToken bound to the dedicated
// _thread_pool_lake_schema_change pool, with a per-job concurrency cap from
// `config::lake_schema_change_per_tablet_parallelism`. It collects the first
// failure across all sub-tasks (fail-fast: subsequent tasks short-circuit
// once a failure is observed) and exposes that error from `wait()`.
//
// Lifetime: construct one per job; submit() returns immediately on enqueue
// failure (e.g. token shutdown). Always call wait() before destruction so
// that no in-flight task references stack-captured state from the caller.
class SegmentTaskRunner {
public:
    // `pool` must outlive this runner. `max_concurrency` should normally be
    // `config::lake_schema_change_per_tablet_parallelism`; values <= 0 are
    // clamped to 1.
    SegmentTaskRunner(ThreadPool* pool, int max_concurrency);
    ~SegmentTaskRunner();

    SegmentTaskRunner(const SegmentTaskRunner&) = delete;
    SegmentTaskRunner& operator=(const SegmentTaskRunner&) = delete;

    // Enqueue one sub-task. The task is skipped (Status::OK returned by
    // submit()) if a previous task has already failed. Submit returns the
    // pool's enqueue status; the actual task error is surfaced via wait().
    Status submit(std::function<Status()> task);

    // Block until all submitted tasks complete (success or failure). Returns
    // the first task error, or OK if all succeeded. Safe to call once.
    Status wait();

private:
    void record_error(const Status& s);

    int _max_concurrency;
    std::unique_ptr<ThreadPoolToken> _token;
    std::atomic<bool> _failed{false};
    std::mutex _err_mtx;
    Status _first_error;
};

} // namespace starrocks::lake
