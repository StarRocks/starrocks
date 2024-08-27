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

#include "util/bthreads/single_flight.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/bthreads/util.h"
#include "util/defer_op.h"

namespace starrocks::bthreads {

PARALLEL_TEST(SingleFlightTest, test_single_threaded) {
    int n = 0;
    singleflight::Group<int, int> test_group;
    for (int i = 0; i < 5; i++) {
        auto r = test_group.Do(0, [&]() { return n++; });
        EXPECT_EQ(i, r);
    }
    EXPECT_EQ(5, n);
}

PARALLEL_TEST(SingleFlightTest, test_wait01) {
    auto do_func = [&](std::atomic<int>* count) { return count->fetch_add(1, std::memory_order_relaxed); };

    const std::string KEY = "test_key";
    std::atomic<int> cnt{0};
    singleflight::Group<std::string, int> test_group;

    SyncPoint::GetInstance()->LoadDependency({{"singleflight::Group::Do:1", "singleflight::Group::Do:2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&]() { EXPECT_EQ(0, test_group.Do(KEY, do_func, &cnt)); }));
    EXPECT_EQ(0, test_group.Do(KEY, do_func, &cnt));
    EXPECT_EQ(0, bthread_join(tid, nullptr));
    EXPECT_EQ(1, cnt.load(std::memory_order_relaxed));
}

// Like above but use std::thread
PARALLEL_TEST(SingleFlightTest, test_wait02) {
    auto do_func = [&](std::atomic<int>* count) { return count->fetch_add(1, std::memory_order_relaxed); };

    const std::string KEY = "test_key";
    std::atomic<int> cnt{0};
    singleflight::Group<std::string, int> test_group;

    SyncPoint::GetInstance()->LoadDependency({{"singleflight::Group::Do:1", "singleflight::Group::Do:2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    auto t = std::thread([&]() { EXPECT_EQ(0, test_group.Do(KEY, do_func, &cnt)); });
    EXPECT_EQ(0, test_group.Do(KEY, do_func, &cnt));
    t.join();
    EXPECT_EQ(1, cnt.load(std::memory_order_relaxed));
}

PARALLEL_TEST(SingleFlightTest, test_exception) {
    std::atomic<int> counter{0};
    auto do_func = [&]() {
        counter.fetch_add(1, std::memory_order_relaxed);
        throw std::logic_error("injected error");
        return 0;
    };
    singleflight::Group<int, int> test_group;
    EXPECT_THROW({ test_group.Do(1, do_func); }, std::logic_error);

    SyncPoint::GetInstance()->LoadDependency({{"singleflight::Group::Do:1", "singleflight::Group::Do:2"}});
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    ASSIGN_OR_ABORT(auto tid,
                    bthreads::start_bthread([&]() { EXPECT_THROW({ test_group.Do(1, do_func); }, std::logic_error); }));
    EXPECT_THROW({ test_group.Do(1, do_func); }, std::logic_error);
    EXPECT_EQ(0, bthread_join(tid, nullptr));
    EXPECT_EQ(2, counter.load(std::memory_order_relaxed));
}

PARALLEL_TEST(SingleFlightTest, test_forget) {
    std::atomic<int> counter{0};
    auto func = [](std::atomic<int>* cnt) { return cnt->fetch_add(1, std::memory_order_relaxed); };
    constexpr int KEY = 1;
    singleflight::Group<int, int> test_group;
    SyncPoint::GetInstance()->LoadDependency({
            {"singleflight::Group::Do:2", "singleflight::Group::Forget:1"},
            {"singleflight::Group::Forget:2", "singleflight::Group::Do:3"},
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() { SyncPoint::GetInstance()->DisableProcessing(); });

    ASSIGN_OR_ABORT(auto tid, bthreads::start_bthread([&]() { (void)test_group.Do(KEY, func, &counter); }));
    test_group.Forget(KEY);
    (void)test_group.Do(KEY, func, &counter);
    EXPECT_EQ(0, bthread_join(tid, nullptr));
    EXPECT_EQ(2, counter.load(std::memory_order_relaxed));
}

} // namespace starrocks::bthreads
