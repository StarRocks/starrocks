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

#include "compute_env/arrow_io_thread_pool.h"

#include <arrow/io/interfaces.h>
#include <arrow/util/future.h>
#include <arrow/util/thread_pool.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"

namespace starrocks {
namespace {

using namespace std::chrono_literals;

// Tasks on Arrow's IO pool that block until release(). None finishes before that, so the number
// that started is the number running at once.
class BlockingTasks {
public:
    ~BlockingTasks() {
        release();
        wait_finished();
    }

    Status submit(int n) {
        auto* executor = arrow::io::default_io_context().executor();
        for (int i = 0; i < n; ++i) {
            auto future = executor->Submit([this] { _run(); });
            if (!future.ok()) {
                return Status::InternalError(future.status().ToString());
            }
            _futures.push_back(std::move(future).ValueUnsafe());
        }
        return Status::OK();
    }

    bool wait_started(int n) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, 10s, [&] { return _started >= n; });
    }

    int started() {
        std::lock_guard lock(_mutex);
        return _started;
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _released = true;
        }
        _cv.notify_all();
    }

    void wait_finished() {
        for (auto& future : _futures) {
            future.Wait();
        }
    }

private:
    void _run() {
        std::unique_lock lock(_mutex);
        ++_started;
        _cv.notify_all();
        _cv.wait(lock, [this] { return _released; });
    }

    std::mutex _mutex;
    std::condition_variable _cv;
    int _started = 0;
    bool _released = false;
    std::vector<arrow::Future<>> _futures;
};

class ScopedDeathTestStyle {
public:
    ScopedDeathTestStyle() : _previous(::testing::FLAGS_gtest_death_test_style) {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
    ~ScopedDeathTestStyle() { ::testing::FLAGS_gtest_death_test_style = _previous; }

private:
    std::string _previous;
};

// Runs in a death-test child started with ARROW_IO_THREADS=13. Exits 0 when the pool follows the
// variable for size 0 and the config for any other size.
[[noreturn]] void check_arrow_io_threads_env_in_child() {
    auto expect_capacity = [](const char* step, int expected) {
        const int actual = arrow::io::GetIOThreadPoolCapacity();
        if (actual != expected) {
            std::fprintf(stderr, "%s: expected capacity %d, got %d\n", step, expected, actual);
            std::_Exit(1);
        }
    };
    auto apply = [](int32_t size) {
        if (Status st = apply_arrow_io_thread_pool_size(size); !st.ok()) {
            std::fprintf(stderr, "apply %d: %s\n", size, st.to_string().c_str());
            std::_Exit(2);
        }
    };

    if (arrow_io_thread_pool_default_capacity() != 13) {
        std::fprintf(stderr, "default capacity: expected 13, got %d\n", arrow_io_thread_pool_default_capacity());
        std::_Exit(3);
    }
    apply(0);
    expect_capacity("size 0 at startup", 13);
    apply(20);
    expect_capacity("size 20 overrides ARROW_IO_THREADS", 20);
    apply(0);
    expect_capacity("size 0 after an override", 13);
    std::_Exit(0);
}

} // namespace

TEST(ArrowIOThreadPoolTest, rejects_out_of_range_size_without_touching_pool) {
    const int capacity = arrow::io::GetIOThreadPoolCapacity();

    EXPECT_TRUE(apply_arrow_io_thread_pool_size(-1).is_invalid_argument());
    EXPECT_TRUE(apply_arrow_io_thread_pool_size(kMaxArrowIOThreadPoolSize + 1).is_invalid_argument());
    EXPECT_EQ(capacity, arrow::io::GetIOThreadPoolCapacity());
}

TEST(ArrowIOThreadPoolTest, zero_restores_arrow_default) {
    DeferOp restore_pool([] { ASSERT_OK(apply_arrow_io_thread_pool_size(0)); });
    const int default_capacity = arrow_io_thread_pool_default_capacity();
    if (std::getenv("ARROW_IO_THREADS") == nullptr) {
        EXPECT_EQ(8, default_capacity);
    }

    ASSERT_OK(apply_arrow_io_thread_pool_size(default_capacity + 3));
    EXPECT_EQ(default_capacity + 3, arrow::io::GetIOThreadPoolCapacity());
    ASSERT_OK(apply_arrow_io_thread_pool_size(kMaxArrowIOThreadPoolSize));
    EXPECT_EQ(kMaxArrowIOThreadPoolSize, arrow::io::GetIOThreadPoolCapacity());
    // Resizing does not move the default.
    EXPECT_EQ(default_capacity, arrow_io_thread_pool_default_capacity());

    ASSERT_OK(apply_arrow_io_thread_pool_size(0));
    EXPECT_EQ(default_capacity, arrow::io::GetIOThreadPoolCapacity());
}

TEST(ArrowIOThreadPoolTest, running_tasks_follow_capacity) {
    DeferOp restore_pool([] { ASSERT_OK(apply_arrow_io_thread_pool_size(0)); });

    ASSERT_OK(apply_arrow_io_thread_pool_size(2));
    {
        BlockingTasks tasks;
        ASSERT_OK(tasks.submit(4));
        ASSERT_TRUE(tasks.wait_started(2));
        std::this_thread::sleep_for(200ms);
        EXPECT_EQ(2, tasks.started());

        // Growing starts workers for the queued tasks right away.
        ASSERT_OK(apply_arrow_io_thread_pool_size(4));
        ASSERT_TRUE(tasks.wait_started(4));
    }

    // Shrinking makes the workers above the new capacity exit instead of taking another task.
    ASSERT_OK(apply_arrow_io_thread_pool_size(1));
    {
        BlockingTasks tasks;
        ASSERT_OK(tasks.submit(2));
        ASSERT_TRUE(tasks.wait_started(1));
        std::this_thread::sleep_for(200ms);
        EXPECT_EQ(1, tasks.started());

        tasks.release();
        tasks.wait_finished();
        EXPECT_EQ(2, tasks.started());
    }
}

TEST(ArrowIOThreadPoolTest, zero_follows_arrow_io_threads_env) {
    ScopedDeathTestStyle death_test_style;
    const char* saved = std::getenv("ARROW_IO_THREADS");
    const std::optional<std::string> saved_value = saved != nullptr ? std::optional<std::string>(saved) : std::nullopt;
    DeferOp restore_env([&] {
        if (saved_value.has_value()) {
            setenv("ARROW_IO_THREADS", saved_value->c_str(), 1);
        } else {
            unsetenv("ARROW_IO_THREADS");
        }
    });
    ASSERT_EQ(0, setenv("ARROW_IO_THREADS", "13", 1));

    // Arrow reads ARROW_IO_THREADS once, when it creates the pool, so check it in a fresh process.
    EXPECT_EXIT(check_arrow_io_threads_env_in_child(), testing::ExitedWithCode(0), "");
}

} // namespace starrocks
