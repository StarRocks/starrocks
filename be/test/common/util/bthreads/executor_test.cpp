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

#include "common/util/bthreads/executor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "common/config_diagnostic_fwd.h"
#include "common/thread/threadpool.h"

namespace starrocks::bthreads {

class ThreadPoolExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_fail_fast = config::enable_load_fail_fast_when_disk_write_hang;
        _saved_hang_second = config::be_exit_after_disk_write_hang_second;
    }
    void TearDown() override {
        config::enable_load_fail_fast_when_disk_write_hang = _saved_fail_fast;
        config::be_exit_after_disk_write_hang_second = _saved_hang_second;
    }

    // Builds a pool with a single worker and a single queue slot, then blocks the worker
    // and fills the queue so the pool is saturated (mirrors the "40960/40960 queued" state).
    // A task is run to completion first so last_active_timestamp() is recent rather than the
    // far-past default value, keeping the "not hung yet" assertions independent of uptime.
    std::unique_ptr<ThreadPool> build_saturated_pool(const std::string& name, std::atomic<bool>* release) {
        std::unique_ptr<ThreadPool> pool;
        CHECK(ThreadPoolBuilder(name).set_min_threads(1).set_max_threads(1).set_max_queue_size(1).build(&pool).ok());

        // Stamp last_active_timestamp() to ~now by running one task to completion.
        MonoTime before = MonoTime::Now();
        CHECK(pool->submit_func([]() {}).ok());
        while (pool->last_active_timestamp().GetDeltaSince(before).ToNanoseconds() < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Occupy the single worker so queued tasks cannot make progress.
        auto started = std::make_shared<std::atomic<bool>>(false);
        CHECK(pool->submit_func([started, release]() {
                      started->store(true);
                      while (!release->load()) {
                          std::this_thread::sleep_for(std::chrono::milliseconds(1));
                      }
                  }).ok());
        while (!started->load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // Fill the only queue slot; the blocked worker can never drain it.
        CHECK(pool->submit_func([]() {}).ok());
        CHECK_EQ(pool->max_queue_size(), pool->num_queued_tasks());
        return pool;
    }

    bool _saved_fail_fast{};
    int32_t _saved_hang_second{};
};

TEST_F(ThreadPoolExecutorTest, is_overloaded) {
    std::atomic<bool> release{false};
    auto pool = build_saturated_pool("test_overload", &release);
    ThreadPoolExecutor executor(pool.get(), kDontTakeOwnership);

    config::enable_load_fail_fast_when_disk_write_hang = true;
    config::be_exit_after_disk_write_hang_second = 0;

    // Full queue and no task completing for >= the (zero) timeout => overloaded.
    ASSERT_TRUE(executor.is_overloaded());

    // The feature flag gates the behavior entirely.
    config::enable_load_fail_fast_when_disk_write_hang = false;
    ASSERT_FALSE(executor.is_overloaded());
    config::enable_load_fail_fast_when_disk_write_hang = true;

    // A large hang threshold means the pool is just busy, not hung yet.
    config::be_exit_after_disk_write_hang_second = 100000;
    ASSERT_FALSE(executor.is_overloaded());

    release.store(true);
    pool->shutdown();
}

TEST_F(ThreadPoolExecutorTest, is_not_overloaded_when_pool_has_capacity) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("test_idle")
                        .set_min_threads(1)
                        .set_max_threads(2)
                        .set_max_queue_size(16)
                        .build(&pool)
                        .ok());
    ThreadPoolExecutor executor(pool.get(), kDontTakeOwnership);

    config::enable_load_fail_fast_when_disk_write_hang = true;
    // Even with a zero timeout, an idle/non-full pool must never be treated as hung.
    config::be_exit_after_disk_write_hang_second = 0;
    ASSERT_FALSE(executor.is_overloaded());

    pool->shutdown();
}

TEST_F(ThreadPoolExecutorTest, submit_runs_inline_instead_of_crashing) {
    std::atomic<bool> release{false};
    auto pool = build_saturated_pool("test_inline", &release);
    ThreadPoolExecutor executor(pool.get(), kDontTakeOwnership);

    config::enable_load_fail_fast_when_disk_write_hang = true;
    config::be_exit_after_disk_write_hang_second = 0;

    // The pool is saturated and "hung". submit() must run the task inline and report
    // success instead of taking the whole process down with LOG(FATAL).
    std::atomic<int> ran{0};
    auto fn = [](void* arg) -> void* {
        static_cast<std::atomic<int>*>(arg)->fetch_add(1);
        return nullptr;
    };
    int rc = executor.submit(fn, &ran);
    ASSERT_EQ(0, rc);
    ASSERT_EQ(1, ran.load());

    release.store(true);
    pool->shutdown();
}

} // namespace starrocks::bthreads
