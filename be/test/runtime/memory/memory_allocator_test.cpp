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

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/tracked_allocator.h"

namespace starrocks::memory {

TEST(JemallocAllocatorTest, ClearAndAlignment) {
    JemallocAllocator<true> allocator;
    auto* ptr = static_cast<char*>(allocator.alloc(16));
    ASSERT_NE(ptr, nullptr);
    for (int i = 0; i < 16; ++i) {
        EXPECT_EQ(ptr[i], 0);
    }

    std::memset(ptr, 0x5A, 16);
    auto* grown = static_cast<char*>(allocator.realloc(ptr, 16, 32));
    ASSERT_NE(grown, nullptr);
    for (int i = 0; i < 16; ++i) {
        EXPECT_EQ(grown[i], 0x5A);
    }
    for (int i = 16; i < 32; ++i) {
        EXPECT_EQ(grown[i], 0);
    }

    void* aligned = allocator.alloc(32, 64);
    ASSERT_NE(aligned, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(aligned) % 64, 0);

    allocator.free(aligned, 32);
    allocator.free(grown, 32);
}

TEST(JemallocAllocatorTest, ReallocWithLargeAlignment) {
    JemallocAllocator<true> allocator;
    constexpr size_t alignment = 64;
    auto* ptr = static_cast<char*>(allocator.alloc(32, alignment));
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0);

    std::memset(ptr, 0x5A, 32);
    auto* grown = static_cast<char*>(allocator.realloc(ptr, 32, 96, alignment));
    ASSERT_NE(grown, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(grown) % alignment, 0);
    for (int i = 0; i < 32; ++i) {
        EXPECT_EQ(grown[i], 0x5A);
    }
    for (int i = 32; i < 96; ++i) {
        EXPECT_EQ(grown[i], 0);
    }

    allocator.free(grown, 96);
}

TEST(TrackedAllocatorTest, TracksMemTrackerConsumption) {
    struct RestoreMemTrackerSource {
        ~RestoreMemTrackerSource() { starrocks::CurrentThread::set_mem_tracker_source(nullptr, nullptr); }
    } restore_guard;
    starrocks::CurrentThread::set_mem_tracker_source([]() { return true; },
                                                     []() { return (starrocks::MemTracker*)nullptr; });

    TrackedAllocator<JemallocAllocator<false>> allocator;
    starrocks::MemTracker tracker(-1, "jemalloc-ut");
    starrocks::CurrentThreadMemTrackerSetter setter(&tracker);

    constexpr size_t size1 = 64;
    constexpr size_t size2 = 128;
    int64_t alloc1 = allocator.nallox(size1, 0);
    int64_t alloc2 = allocator.nallox(size2, 0);

    void* ptr = allocator.alloc(size1);
    CurrentThread::current().mem_tracker_ctx_shift();
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(tracker.consumption(), alloc1);

    ptr = allocator.realloc(ptr, size1, size2);
    CurrentThread::current().mem_tracker_ctx_shift();
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(tracker.consumption(), alloc2);

    allocator.free(ptr, size2);
    CurrentThread::current().mem_tracker_ctx_shift();
    ASSERT_EQ(tracker.consumption(), 0);
}

template <typename Counter>
void AssertCountingBehavior(size_t size1, size_t size2) {
    CountingAllocator<JemallocAllocator<false>, Counter> allocator;

    void* ptr = allocator.alloc(size1);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(allocator.memory_usage(), static_cast<int64_t>(size1));

    ptr = allocator.realloc(ptr, size1, size2);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(allocator.memory_usage(), static_cast<int64_t>(size2));

    allocator.free(ptr, size2);
    ASSERT_EQ(allocator.memory_usage(), 0);
}

TEST(CountingAllocatorTest, CountsWithIntCounter) {
    AssertCountingBehavior<IntCounter>(64, 128);
}

TEST(CountingAllocatorTest, CountsWithAtomicIntCounter) {
    AssertCountingBehavior<AtomicIntCounter>(256, 64);
}

} // namespace starrocks::memory
