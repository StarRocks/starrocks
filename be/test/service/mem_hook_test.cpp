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

#include "service/mem_hook.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

namespace starrocks {

TEST(MemhookTest, test_should_report_large_memory_alloc) {
    // A threshold of 0 or below disables the report. This is also the value of the config global
    // before config::init() applies the declared default, so nothing may be reported then.
    EXPECT_FALSE(should_report_large_memory_alloc(0, 0));
    EXPECT_FALSE(should_report_large_memory_alloc(1, 0));
    EXPECT_FALSE(should_report_large_memory_alloc(std::numeric_limits<size_t>::max(), 0));
    EXPECT_FALSE(should_report_large_memory_alloc(std::numeric_limits<size_t>::max(), -1));

    // Strictly greater than the threshold, matching the historical 1GB constant.
    constexpr int64_t kThreshold = 1073741824;
    EXPECT_FALSE(should_report_large_memory_alloc(kThreshold - 1, kThreshold));
    EXPECT_FALSE(should_report_large_memory_alloc(kThreshold, kThreshold));
    EXPECT_TRUE(should_report_large_memory_alloc(kThreshold + 1, kThreshold));
}

} // namespace starrocks

// The remaining mem hook behavior is only testable when the production hook is enabled.
#if STARROCKS_ENABLE_JEMALLOC_MEM_HOOK

#include <atomic>
#include <vector>

#ifdef BE_TEST
// The test hook counts allocations globally; production uses query trackers.
extern std::atomic<int64_t> g_mem_usage;
#endif

#include "geo/geo_buffer.h"
#include "geo/geo_overlay.h"
#include "runtime/current_thread.h"

namespace starrocks {

TEST(MemhookTest, polygonOverlayPreparedAllocationsAreAccountedAndReleased) {
    WkbGeometry polygon;
    ASSERT_TRUE(
            WkbCodec::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0))", &polygon, WkbCoordinateSemantics::GEOMETRY_CARTESIAN)
                    .ok());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    // Warm up Boost before measuring the lifetime of one prepared model.
    auto warmup = PreparedGeoPolygon::prepare(Slice(wkb));
    ASSERT_TRUE(warmup.ok());
    warmup.value().reset();
#ifdef BE_TEST
    auto consumed = [] { return ::g_mem_usage.load(); };
#else
    MemTracker tracker(-1, "polygon overlay");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&tracker);
    auto consumed = [] { return tls_thread_status.get_consumed_bytes(); };
#endif
    const auto before = consumed();
    auto prepared = PreparedGeoPolygon::prepare(Slice(wkb));
    ASSERT_TRUE(prepared.ok()) << prepared.status();
    EXPECT_GT(consumed(), before);
    prepared.value().reset();
    EXPECT_EQ(before, consumed());
}

TEST(MemhookTest, bufferPreparedAndScratchAllocationsAreAccountedAndReleased) {
    WkbGeometry polygon;
    ASSERT_TRUE(
            WkbCodec::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0))", &polygon, WkbCoordinateSemantics::GEOMETRY_CARTESIAN)
                    .ok());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    {
        auto warmup = PreparedGeoBuffer::prepare(Slice(wkb));
        ASSERT_TRUE(warmup.ok());
        ASSERT_TRUE(warmup.value()->buffer(1).ok());
    }
#ifdef BE_TEST
    auto consumed = [] { return ::g_mem_usage.load(); };
#else
    MemTracker tracker(-1, "buffer");
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&tracker);
    auto consumed = [] { return tls_thread_status.get_consumed_bytes(); };
#endif
    const auto before = consumed();
    {
        auto input = PreparedGeoBuffer::prepare(Slice(wkb));
        ASSERT_TRUE(input.ok()) << input.status();
        EXPECT_GT(consumed(), before);
        for (double distance : {1.0, -1.0, 0.0}) {
            auto result = input.value()->buffer(distance);
            ASSERT_TRUE(result.ok()) << result.status();
        }
    }
    EXPECT_EQ(before, consumed());
}

static void try_malloc_memory(size_t size) {
    std::vector<int8_t> arr;
    arr.reserve(size);
    EXPECT_EQ(size, arr.capacity());
}

static void try_calloc_memory(size_t n, size_t size) {
    void* ptr = calloc(n, size);
    if (ptr == nullptr) {
        throw std::bad_alloc();
    }
    free(ptr);
}

TEST(MemhookTest, test_malloc_mem_hook_block_without_try_catch) {
    set_large_memory_alloc_failure_threshold(0);
    int64_t blocking_size = 1024 * 1024;

    set_large_memory_alloc_failure_threshold(blocking_size);
    // successful because blocking_size <= g_large_memory_alloc_failure_threshold
    EXPECT_NO_THROW(try_malloc_memory(blocking_size));

    set_large_memory_alloc_failure_threshold(blocking_size - 1);
    // fail with std::bad_alloc for the same size
    EXPECT_THROW(try_malloc_memory(blocking_size), std::bad_alloc);

    // reset to no limit
    set_large_memory_alloc_failure_threshold(0);
}

TEST(MemhookTest, test_malloc_mem_hook_block_with_try_catch) {
    // IS_BAD_ALLOC_CATCHED is always `false` when builds with -DBE_TEST.
    // There is no easy way to simulate the mem_tracker limit check here.
    // Leave the test body empty as intended for now.
}

TEST(MemhookTest, test_calloc_mem_hook_block_without_try_catch) {
    int64_t blocking_size = 1024 * 1024;

    set_large_memory_alloc_failure_threshold(blocking_size);
    // successful because blocking_size <= g_large_memory_alloc_failure_threshold
    EXPECT_NO_THROW(try_calloc_memory(blocking_size / sizeof(int), sizeof(int)));

    set_large_memory_alloc_failure_threshold(blocking_size - 1);
    // fail with std::bad_alloc for the same size
    EXPECT_THROW(try_calloc_memory(blocking_size / sizeof(int), sizeof(int)), std::bad_alloc);

    // reset to no limit
    set_large_memory_alloc_failure_threshold(0);
}
} // namespace starrocks
#endif // STARROCKS_ENABLE_JEMALLOC_MEM_HOOK
