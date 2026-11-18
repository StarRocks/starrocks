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

#include "base/concurrency/spinlock.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

#include "common/object_pool.h"

namespace starrocks {

TEST(SpinLockTest, LayoutAndConstContract) {
    static_assert(sizeof(SpinLock) == 1, "SpinLock must remain 1 byte");
    static_assert(alignof(SpinLock) == 1, "SpinLock must remain 1-byte aligned");
    static_assert(std::is_trivially_copyable_v<SpinLock>);
    static_assert(std::is_trivially_move_constructible_v<SpinLock>);
    static_assert(std::is_trivially_move_assignable_v<SpinLock>);

    // Fails on legacy SpinLock where dcheck_locked() is non-const.
    EXPECT_TRUE((std::is_invocable_v<decltype(&SpinLock::dcheck_locked), const SpinLock&>));
}

TEST(SpinLockTest, BasicLockTryLockUnlock) {
    SpinLock lock;
    EXPECT_TRUE(lock.try_lock());
    EXPECT_FALSE(lock.try_lock());
    lock.dcheck_locked();
    const SpinLock& const_lock = lock;
    const_lock.dcheck_locked();
    lock.unlock();

    {
        std::lock_guard<SpinLock> guard(lock);
        lock.dcheck_locked();
        const_lock.dcheck_locked();
        EXPECT_FALSE(lock.try_lock());
    }
    {
        std::unique_lock<SpinLock> ulock(lock);
        EXPECT_TRUE(ulock.owns_lock());
        const_lock.dcheck_locked();
        EXPECT_FALSE(lock.try_lock());
    }
    EXPECT_TRUE(lock.try_lock());
    lock.unlock();
}

TEST(SpinLockTest, ObjectPoolMoveCompatibility) {
    ObjectPool pool1;
    int* val = pool1.add(new int(42));
    ASSERT_NE(val, nullptr);
    EXPECT_EQ(*val, 42);

    ObjectPool pool2(std::move(pool1));
    EXPECT_EQ(*val, 42);

    ObjectPool pool3;
    pool3 = std::move(pool2);
    EXPECT_EQ(*val, 42);
}

TEST(SpinLockTest, TryLockContendedDoesNotStealLock) {
    SpinLock lock;
    std::atomic<bool> holder_locked{false};
    std::atomic<bool> stop{false};
    std::atomic<int> unexpected_acquires{0};

    std::thread holder([&]() {
        lock.lock();
        holder_locked.store(true, std::memory_order_release);
        while (!stop.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        lock.dcheck_locked();
        lock.unlock();
    });

    while (!holder_locked.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::vector<std::thread> contenders;
    for (int t = 0; t < 4; ++t) {
        contenders.emplace_back([&]() {
            for (int i = 0; i < 10000; ++i) {
                if (lock.try_lock()) {
                    unexpected_acquires.fetch_add(1, std::memory_order_relaxed);
                    lock.unlock();
                }
            }
        });
    }
    for (auto& th : contenders) {
        th.join();
    }
    stop.store(true, std::memory_order_release);
    holder.join();

    EXPECT_EQ(unexpected_acquires.load(), 0);
}

TEST(SpinLockTest, AcquireReleaseMultiWordVisibility) {
    SpinLock lock;
    constexpr int kNumThreads = 16;
    constexpr int kIterationsPerThread = 20000;
    struct alignas(64) CacheLineWord {
        uint64_t value = 0;
    };
    CacheLineWord word_a;
    CacheLineWord word_b;
    CacheLineWord word_sum;

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kIterationsPerThread; ++i) {
                std::lock_guard<SpinLock> guard(lock);
                ASSERT_EQ(word_a.value + word_b.value, word_sum.value);
                word_a.value += 1;
                word_b.value += 2;
                word_sum.value = word_a.value + word_b.value;
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }

    const uint64_t expected_total = static_cast<uint64_t>(kNumThreads) * kIterationsPerThread;
    EXPECT_EQ(word_a.value, expected_total);
    EXPECT_EQ(word_b.value, expected_total * 2);
    EXPECT_EQ(word_sum.value, expected_total * 3);
}

TEST(SpinLockTest, HighContentionOversubscribed64Threads) {
    SpinLock lock;
    constexpr int kNumThreads = 64;
    constexpr int kIterationsPerThread = 5000;
    int64_t counter = 0;

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kIterationsPerThread; ++i) {
                std::lock_guard<SpinLock> guard(lock);
                ++counter;
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(counter, static_cast<int64_t>(kNumThreads) * kIterationsPerThread);
}

} // namespace starrocks
