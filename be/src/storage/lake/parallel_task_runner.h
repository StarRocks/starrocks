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

#include <functional>
#include <mutex>

#include "common/status.h"

namespace starrocks {
class ThreadPoolToken;
class Trace;
} // namespace starrocks

namespace starrocks::lake {

// One fan-out/join for a loop that spreads its iterations over a thread-pool token.
//
// Every parallel step in the primary-key publish path had the same shape written out by hand:
// iterate the work items on the caller thread, run each one either on a token or inline, join, then
// check a single aggregated status. Nine copies of that shape had drifted apart in two ways that
// this class settles:
//
//   * Submit failure. Two copies ran the task inline instead; the rest recorded the submit error and
//     failed the publish. Running inline is strictly more robust -- the task is going to run either
//     way, and a token only refuses while it is shutting down -- so that is what happens here.
//   * Trace propagation. TRACE_COUNTER_* reads a thread-local current trace that pool workers do not
//     inherit, so a counter incremented inside a submitted task is silently dropped (the macro is a
//     no-op with no current trace). Only two of the nine copies adopted the caller's trace. This
//     always does, which is why counters such as `multi_get_us` now survive a parallel PK-index read.
//
// `token == nullptr` means run everything inline on the caller thread. That is how every caller
// degrades when `enable_pk_index_parallel_execution` is off, or when the work is too small to be
// worth a pool round-trip, so it is a first-class mode rather than an error.
//
// A task that fails does not cancel the tasks after it: several callers depend on side effects --
// releasing per-segment state, accumulating IO stats -- that must still happen on a publish that is
// going to fail. The first error is kept and returned from join().
//
// Not thread-safe: run() and join() belong to the single thread driving the loop. The runner must
// outlive its tasks, so join() before letting it go out of scope -- the destructor does that anyway,
// because a task that outlived the runner would write into a destroyed mutex.
class ParallelTaskRunner {
public:
    // `token` may be null (inline mode). When non-null it must outlive this runner.
    explicit ParallelTaskRunner(ThreadPoolToken* token);

    // Joins any still-running task. Prefer calling join() explicitly so the error is observed.
    ~ParallelTaskRunner();

    ParallelTaskRunner(const ParallelTaskRunner&) = delete;
    ParallelTaskRunner& operator=(const ParallelTaskRunner&) = delete;

    // Run one task, on the token when there is one, inline otherwise. Captured state must stay alive
    // until join() returns.
    void run(std::function<Status()> task);

    // Wait for every task submitted so far and return the first error, or OK. Idempotent.
    Status join();

private:
    void record(const Status& st);

    ThreadPoolToken* _token;
    // The caller thread's trace, re-adopted inside each submitted task. Null when the caller is not
    // tracing, which ScopedAdoptTrace handles.
    Trace* _trace;
    std::mutex _mutex;
    Status _status;
};

} // namespace starrocks::lake
