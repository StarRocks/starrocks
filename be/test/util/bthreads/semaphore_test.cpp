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

#include "util/bthreads/semaphore.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <thread>

#include "testutil/assert.h"
#include "util/bthreads/util.h"

namespace starrocks::bthreads {

TEST(CountingSemaphoreTest, test_constructor) {
    CountingSemaphore semaphore1(2);
    CountingSemaphore semaphore2(0);
    EXPECT_TRUE(semaphore1.try_acquire());
    EXPECT_TRUE(semaphore1.try_acquire_for(std::chrono::microseconds(5)));
    EXPECT_FALSE(semaphore1.try_acquire());
    EXPECT_FALSE(semaphore2.try_acquire());
    EXPECT_FALSE(semaphore2.try_acquire_for(std::chrono::milliseconds(10)));
}

TEST(CountingSemaphoreTest, test_try_acquire_for) {
    CountingSemaphore sem(0);
    auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(sem.try_acquire_for(std::chrono::milliseconds(100)));
    auto t1 = std::chrono::steady_clock::now();
    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    EXPECT_GE(cost, 100);
    EXPECT_LT(cost, 150);
}

TEST(CountingSemaphoreTest, test01) {
    constexpr int kMaxValue = 10;
    constexpr int kMaxThreads = 20;
    CountingSemaphore semaphore(kMaxValue);
    std::atomic<int> concurrency{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kMaxThreads; i++) {
        threads.emplace_back([&]() {
            for (int loop = 0; loop < 100; loop++) {
                semaphore.acquire();
                int value = concurrency.fetch_add(1, std::memory_order_relaxed);
                ASSERT_LE(value + 1, kMaxValue);
                concurrency.fetch_sub(1, std::memory_order_relaxed);
                semaphore.release();
            }
        });
    }

    for (auto&& t : threads) {
        t.join();
    }
    EXPECT_EQ(0, concurrency.load(std::memory_order_relaxed));
}

TEST(CountingSemaphoreTest, test02) {
    constexpr int kMaxValue = 10;
    constexpr int kMaxThreads = 20;
    CountingSemaphore semaphore(kMaxValue);
    std::atomic<int> concurrency{0};
    std::vector<bthread_t> threads;
    for (int i = 0; i < kMaxThreads; i++) {
        ASSIGN_OR_ABORT(auto bid, start_bthread([&]() {
                            for (int loop = 0; loop < 100; loop++) {
                                semaphore.acquire();
                                int value = concurrency.fetch_add(1, std::memory_order_relaxed);
                                ASSERT_LE(value + 1, kMaxValue);
                                concurrency.fetch_sub(1, std::memory_order_relaxed);
                                semaphore.release();
                            }
                        }));
        threads.emplace_back(bid);
    }

    for (auto&& t : threads) {
        ASSERT_EQ(0, bthread_join(t, nullptr));
    }
    EXPECT_EQ(0, concurrency.load(std::memory_order_relaxed));
}
} // namespace starrocks::bthreads
