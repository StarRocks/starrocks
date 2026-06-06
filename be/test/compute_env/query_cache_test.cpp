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

#include "column/fixed_length_column.h"
#include "compute_env/query_cache/cache_manager.h"
#include "compute_env/query_cache/lane_arbiter.h"

namespace starrocks {

TEST(ComputeEnvQueryCacheTest, cache_manager_populates_probes_and_invalidates) {
    query_cache::CacheManager cache_mgr(1024 * 1024);

    auto chunk = std::make_shared<Chunk>();
    auto column = Int8Column::create();
    column->append(1);
    chunk->append_column(std::move(column), 1);

    query_cache::CacheValue value(100, 7, {chunk});
    cache_mgr.populate("cache-key", value);

    auto probe_result = cache_mgr.probe("cache-key");
    ASSERT_TRUE(probe_result.ok());
    EXPECT_EQ(7, probe_result.value().version);
    EXPECT_EQ(1, probe_result.value().result.size());
    EXPECT_GT(cache_mgr.memory_usage(), 0);
    EXPECT_EQ(1, cache_mgr.lookup_count());
    EXPECT_EQ(1, cache_mgr.hit_count());

    cache_mgr.invalidate_all();
    EXPECT_FALSE(cache_mgr.probe("cache-key").ok());
}

TEST(ComputeEnvQueryCacheTest, lane_arbiter_assigns_releases_and_skips_processed_owners) {
    query_cache::LaneArbiter lane_arbiter(2);

    EXPECT_EQ(query_cache::AR_PROBE, lane_arbiter.try_acquire_lane(10));
    EXPECT_EQ(query_cache::AR_IO, lane_arbiter.try_acquire_lane(10));
    EXPECT_EQ(query_cache::AR_PROBE, lane_arbiter.try_acquire_lane(20));
    EXPECT_EQ(query_cache::AR_BUSY, lane_arbiter.try_acquire_lane(30));

    lane_arbiter.mark_processed(20);
    EXPECT_EQ(query_cache::AR_SKIP, lane_arbiter.try_acquire_lane(20));

    lane_arbiter.release_lane(10);
    EXPECT_EQ(query_cache::AR_PROBE, lane_arbiter.try_acquire_lane(30));

    lane_arbiter.enable_passthrough_mode();
    EXPECT_TRUE(lane_arbiter.in_passthrough_mode());
    EXPECT_EQ(query_cache::AR_IO, lane_arbiter.try_acquire_lane(40));
}

} // namespace starrocks
