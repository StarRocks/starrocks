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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

#include "base/testutil/assert.h"
#include "common/thread/threadpool.h"

namespace starrocks::lake {

// Helper: build a CONCURRENT thread pool with `n` workers, suitable for
// SegmentTaskRunner tests that need real parallelism.
static std::unique_ptr<ThreadPool> make_pool(int n) {
    std::unique_ptr<ThreadPool> p;
    EXPECT_TRUE(ThreadPoolBuilder("seg_tr_test").set_min_threads(0).set_max_threads(n).build(&p).ok());
    return p;
}

// Inline path: with a null pool, submit() runs the task on the calling
// thread and wait() returns its status (no internal token).
TEST(SegmentTaskRunnerTest, run_inline_when_pool_is_null) {
    SegmentTaskRunner runner(/*pool=*/nullptr, /*max_concurrency=*/4);
    int counter = 0;
    ASSERT_OK(runner.submit([&] {
        ++counter;
        return Status::OK();
    }));
    EXPECT_EQ(1, counter);
    ASSERT_OK(runner.wait());
}

// Inline path failure semantics: first error is recorded and stored;
// subsequent submits short-circuit (the closure must not run).
TEST(SegmentTaskRunnerTest, inline_records_first_error_and_short_circuits) {
    SegmentTaskRunner runner(/*pool=*/nullptr, /*max_concurrency=*/2);
    ASSERT_OK(runner.submit([] { return Status::IOError("boom-1"); }));
    bool ran_after = false;
    ASSERT_OK(runner.submit([&] {
        ran_after = true;
        return Status::IOError("boom-2");
    }));
    EXPECT_FALSE(ran_after);
    Status st = runner.wait();
    EXPECT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("boom-1"));
}

// Async path: all submitted tasks run, even when the pool has fewer threads
// than tasks. Tests that the per-token CONCURRENT mode actually drains the
// queue.
TEST(SegmentTaskRunnerTest, async_pool_executes_all_tasks_in_parallel) {
    auto pool = make_pool(4);
    SegmentTaskRunner runner(pool.get(), /*max_concurrency=*/4);
    constexpr int kN = 16;
    std::atomic<int> ran{0};
    for (int i = 0; i < kN; ++i) {
        ASSERT_OK(runner.submit([&] {
            ran.fetch_add(1, std::memory_order_relaxed);
            return Status::OK();
        }));
    }
    ASSERT_OK(runner.wait());
    EXPECT_EQ(kN, ran.load());
}

// Async path failure: the first observed error from any worker is the one
// returned by wait(). Other tasks may still complete; we only assert the
// returned status carries an error.
TEST(SegmentTaskRunnerTest, async_pool_returns_first_error_after_wait) {
    auto pool = make_pool(2);
    SegmentTaskRunner runner(pool.get(), /*max_concurrency=*/2);
    ASSERT_OK(runner.submit([] { return Status::OK(); }));
    ASSERT_OK(runner.submit([] { return Status::IOError("first"); }));
    ASSERT_OK(runner.submit([] { return Status::OK(); }));
    Status st = runner.wait();
    EXPECT_FALSE(st.ok());
}

// Async path: once a worker has recorded a failure, subsequent submitted
// tasks observe the _failed flag and skip their body. We block the only
// worker until after the failing task has recorded so the ordering is
// deterministic.
TEST(SegmentTaskRunnerTest, async_pool_skips_remaining_after_first_failure) {
    auto pool = make_pool(1); // serialize so failure precedes later tasks
    SegmentTaskRunner runner(pool.get(), /*max_concurrency=*/1);
    ASSERT_OK(runner.submit([] { return Status::IOError("first"); }));
    std::atomic<int> later_ran{0};
    for (int i = 0; i < 5; ++i) {
        ASSERT_OK(runner.submit([&] {
            later_ran.fetch_add(1, std::memory_order_relaxed);
            return Status::OK();
        }));
    }
    Status st = runner.wait();
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(0, later_ran.load());
}

// wait() with no submits is a no-op and returns OK regardless of pool/no-pool.
TEST(SegmentTaskRunnerTest, wait_with_no_submitted_tasks_returns_ok) {
    SegmentTaskRunner inline_runner(/*pool=*/nullptr, /*max_concurrency=*/1);
    EXPECT_OK(inline_runner.wait());

    auto pool = make_pool(2);
    SegmentTaskRunner async_runner(pool.get(), /*max_concurrency=*/2);
    EXPECT_OK(async_runner.wait());
}

// Destructor must not crash if the caller forgets to call wait(): the token
// dtor should drain in-flight tasks and the runner should not dereference
// freed state. Smoke test only.
TEST(SegmentTaskRunnerTest, destructor_handles_pending_tasks_safely) {
    auto pool = make_pool(2);
    {
        SegmentTaskRunner runner(pool.get(), /*max_concurrency=*/2);
        for (int i = 0; i < 8; ++i) {
            ASSERT_OK(runner.submit([] {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                return Status::OK();
            }));
        }
        // Intentionally do NOT call wait(): exercise the dtor's shutdown path.
    }
    SUCCEED();
}

// max_concurrency <= 0 is clamped to 1; the runner must still accept work.
TEST(SegmentTaskRunnerTest, max_concurrency_zero_is_clamped_to_one) {
    auto pool = make_pool(1);
    SegmentTaskRunner runner(pool.get(), /*max_concurrency=*/0);
    std::atomic<int> ran{0};
    ASSERT_OK(runner.submit([&] {
        ran.fetch_add(1);
        return Status::OK();
    }));
    ASSERT_OK(runner.wait());
    EXPECT_EQ(1, ran.load());
}

} // namespace starrocks::lake
