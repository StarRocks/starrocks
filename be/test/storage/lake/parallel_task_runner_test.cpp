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

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>

#include "base/debug/trace.h"
#include "base/testutil/assert.h"
#include "common/thread/threadpool.h"

namespace starrocks::lake {

static std::unique_ptr<ThreadPool> make_pool(int max_threads) {
    std::unique_ptr<ThreadPool> pool;
    CHECK_OK(ThreadPoolBuilder("ptr_test").set_min_threads(1).set_max_threads(max_threads).build(&pool));
    return pool;
}

TEST(ParallelTaskRunnerTest, inline_mode_runs_every_task) {
    ParallelTaskRunner runner(nullptr);
    std::atomic<int> ran{0};
    for (int i = 0; i < 5; ++i) {
        runner.run([&ran]() {
            ran++;
            return Status::OK();
        });
    }
    ASSERT_OK(runner.join());
    ASSERT_EQ(5, ran.load());
}

TEST(ParallelTaskRunnerTest, inline_mode_keeps_first_error_and_still_runs_the_rest) {
    ParallelTaskRunner runner(nullptr);
    std::atomic<int> ran{0};
    runner.run([&ran]() {
        ran++;
        return Status::InternalError("first");
    });
    runner.run([&ran]() {
        ran++;
        return Status::InternalError("second");
    });
    runner.run([&ran]() {
        ran++;
        return Status::OK();
    });
    auto st = runner.join();
    ASSERT_FALSE(st.ok());
    // update() keeps the first non-OK status; the later tasks still ran, because callers depend on
    // their side effects even on a failing publish.
    ASSERT_NE(std::string::npos, st.to_string().find("first"));
    ASSERT_EQ(3, ran.load());
}

TEST(ParallelTaskRunnerTest, async_mode_runs_every_task) {
    auto pool = make_pool(4);
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::atomic<int> ran{0};
    {
        ParallelTaskRunner runner(token.get());
        for (int i = 0; i < 32; ++i) {
            runner.run([&ran]() {
                ran++;
                return Status::OK();
            });
        }
        ASSERT_OK(runner.join());
    }
    ASSERT_EQ(32, ran.load());
}

TEST(ParallelTaskRunnerTest, async_mode_reports_error_after_join) {
    auto pool = make_pool(2);
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    ParallelTaskRunner runner(token.get());
    std::atomic<int> ran{0};
    for (int i = 0; i < 8; ++i) {
        runner.run([&ran, i]() {
            ran++;
            return i == 3 ? Status::InternalError("boom") : Status::OK();
        });
    }
    auto st = runner.join();
    ASSERT_FALSE(st.ok());
    ASSERT_EQ(8, ran.load());
}

TEST(ParallelTaskRunnerTest, join_with_no_tasks_is_ok) {
    ParallelTaskRunner inline_runner(nullptr);
    ASSERT_OK(inline_runner.join());

    auto pool = make_pool(2);
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    ParallelTaskRunner async_runner(token.get());
    ASSERT_OK(async_runner.join());
}

TEST(ParallelTaskRunnerTest, join_is_repeatable_and_keeps_the_error) {
    auto pool = make_pool(2);
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    ParallelTaskRunner runner(token.get());
    runner.run([]() { return Status::InternalError("boom"); });
    auto first = runner.join();
    ASSERT_FALSE(first.ok());
    // A second phase may share the same runner and token, so join() must stay usable.
    std::atomic<int> ran{0};
    runner.run([&ran]() {
        ran++;
        return Status::OK();
    });
    ASSERT_FALSE(runner.join().ok());
    ASSERT_EQ(1, ran.load());
}

// The whole point of the runner adopting the caller's trace: TRACE_COUNTER_* inside a task must land
// in the caller's trace instead of being silently dropped on a pool worker.
TEST(ParallelTaskRunnerTest, async_task_counters_reach_the_callers_trace) {
    auto pool = make_pool(4);
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    scoped_refptr<Trace> trace(new Trace);
    {
        ADOPT_TRACE(trace.get());
        ParallelTaskRunner runner(token.get());
        for (int i = 0; i < 4; ++i) {
            runner.run([]() {
                TRACE_COUNTER_INCREMENT("runner_test_cnt", 1);
                return Status::OK();
            });
        }
        ASSERT_OK(runner.join());
    }
    // GetMetric() compares the interned name pointer-wise, so look the counter up by value instead.
    int64_t counted = 0;
    for (const auto& [name, value] : trace->metrics()->Get()) {
        if (std::string(name) == "runner_test_cnt") {
            counted = value;
        }
    }
    ASSERT_EQ(4, counted);
}

TEST(ParallelTaskRunnerTest, destructor_joins_in_flight_tasks) {
    auto pool = make_pool(2);
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::atomic<int> ran{0};
    {
        ParallelTaskRunner runner(token.get());
        for (int i = 0; i < 16; ++i) {
            runner.run([&ran]() {
                ran++;
                return Status::OK();
            });
        }
        // No explicit join(); the destructor must wait, otherwise a task would write into a destroyed
        // mutex.
    }
    ASSERT_EQ(16, ran.load());
}

} // namespace starrocks::lake
