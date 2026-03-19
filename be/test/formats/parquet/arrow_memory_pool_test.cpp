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

#include "formats/parquet/arrow_memory_pool.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
namespace {

// Verify allocation is tracked on pool tracker AND propagates to parent,
// with exactly single-counting (no double-count, no miss).
TEST(ArrowMemoryPoolTest, TrackerHierarchyAllocateAndFree) {
    auto process_tracker = std::make_unique<MemTracker>(-1, "process");
    auto query_pool_tracker = std::make_unique<MemTracker>(-1, "query_pool", process_tracker.get());
    ArrowMemoryPool pool(query_pool_tracker.get());

    uint8_t* buf = nullptr;
    constexpr int64_t alloc_size = 4096;
    ASSERT_TRUE(pool.Allocate(alloc_size, 64, &buf).ok());
    ASSERT_NE(buf, nullptr);

    // Both query_pool and process see exactly alloc_size (single-counting).
    EXPECT_EQ(query_pool_tracker->consumption(), alloc_size);
    EXPECT_EQ(process_tracker->consumption(), alloc_size);

    pool.Free(buf, alloc_size, 64);

    // Both return to zero after free.
    EXPECT_EQ(query_pool_tracker->consumption(), 0);
    EXPECT_EQ(process_tracker->consumption(), 0);
}

// Verify that the outer TLS tracker is NOT affected by pool operations
// (hook is redirected to a sink tracker, preventing double-counting).
TEST(ArrowMemoryPoolTest, NoDoubleCountingWithOuterTracker) {
    auto pool_tracker = std::make_unique<MemTracker>(-1, "pool_tracker");
    auto outer_tracker = std::make_unique<MemTracker>(-1, "outer_tracker");
    ArrowMemoryPool pool(pool_tracker.get());

    // Simulate a scanner thread that already has a TLS tracker.
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(outer_tracker.get());

    uint8_t* buf = nullptr;
    constexpr int64_t alloc_size = 4096;
    ASSERT_TRUE(pool.Allocate(alloc_size, 64, &buf).ok());
    ASSERT_NE(buf, nullptr);

    // Pool tracker sees the allocation.
    EXPECT_EQ(pool_tracker->consumption(), alloc_size);
    // Outer tracker is unchanged — hook was redirected to the sink tracker.
    EXPECT_EQ(outer_tracker->consumption(), 0);

    pool.Free(buf, alloc_size, 64);

    // Pool tracker back to zero.
    EXPECT_EQ(pool_tracker->consumption(), 0);
    // Outer tracker still unchanged.
    EXPECT_EQ(outer_tracker->consumption(), 0);
}

// Verify Reallocate tracks correctly: old freed, new allocated, net = new_size.
TEST(ArrowMemoryPoolTest, ReallocateTracking) {
    auto tracker = std::make_unique<MemTracker>(-1, "realloc_tracker");
    ArrowMemoryPool pool(tracker.get());

    uint8_t* buf = nullptr;
    ASSERT_TRUE(pool.Allocate(1024, 64, &buf).ok());
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(tracker->consumption(), 1024);

    memset(buf, 0xAB, 1024);

    ASSERT_TRUE(pool.Reallocate(1024, 4096, 64, &buf).ok());
    ASSERT_NE(buf, nullptr);

    // Data preserved.
    for (int i = 0; i < 1024; i++) {
        ASSERT_EQ(buf[i], 0xAB) << "index " << i;
    }

    // Net consumption = new_size (old allocation was freed internally).
    EXPECT_EQ(tracker->consumption(), 4096);
    EXPECT_EQ(pool.bytes_allocated(), 4096);

    pool.Free(buf, 4096, 64);
    EXPECT_EQ(tracker->consumption(), 0);
}

// Verify pool works without a tracker (nullptr).
TEST(ArrowMemoryPoolTest, WithoutTracker) {
    ArrowMemoryPool pool;

    uint8_t* buf = nullptr;
    ASSERT_TRUE(pool.Allocate(1024, 64, &buf).ok());
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(pool.bytes_allocated(), 1024);

    pool.Free(buf, 1024, 64);
    EXPECT_EQ(pool.bytes_allocated(), 0);
}

// Verify zero-size allocation does not affect tracker.
TEST(ArrowMemoryPoolTest, ZeroSizeAllocation) {
    auto tracker = std::make_unique<MemTracker>(-1, "zero_tracker");
    ArrowMemoryPool pool(tracker.get());

    uint8_t* buf = nullptr;
    ASSERT_TRUE(pool.Allocate(0, 64, &buf).ok());
    EXPECT_EQ(tracker->consumption(), 0);

    pool.Free(buf, 0, 64);
    EXPECT_EQ(tracker->consumption(), 0);
}

} // namespace
} // namespace starrocks
