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

#include "common/runtime_profile.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
TEST(HashVariantResolverTest, unary_assert) {
#define TYPE_RESULT(phase, type, nullable) \
    HashVariantResolver<AggHashMapVariant>::instance().get_unary_type(phase, type, nullable)

    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_TINYINT, true), AggHashMapVariant::Type::phase1_null_int8);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_SMALLINT, true), AggHashMapVariant::Type::phase1_null_int16);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_INT, true), AggHashMapVariant::Type::phase1_null_int32);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_BIGINT, true), AggHashMapVariant::Type::phase1_null_int64);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_LARGEINT, true), AggHashMapVariant::Type::phase1_null_int128);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_VARCHAR, true), AggHashMapVariant::Type::phase1_null_string);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DATE, true), AggHashMapVariant::Type::phase1_null_date);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DATETIME, true), AggHashMapVariant::Type::phase1_null_timestamp);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DECIMAL32, true), AggHashMapVariant::Type::phase1_null_decimal32);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DECIMAL64, true), AggHashMapVariant::Type::phase1_null_decimal64);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_DECIMAL128, true), AggHashMapVariant::Type::phase1_null_decimal128);

    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_TINYINT, false), AggHashMapVariant::Type::phase1_int8);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_SMALLINT, false), AggHashMapVariant::Type::phase1_int16);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_INT, false), AggHashMapVariant::Type::phase1_int32);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_BIGINT, false), AggHashMapVariant::Type::phase1_int64);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_LARGEINT, false), AggHashMapVariant::Type::phase1_int128);
    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_VARCHAR, false), AggHashMapVariant::Type::phase1_string);

    EXPECT_EQ(TYPE_RESULT(AggrPhase1, TYPE_FLOAT, true), AggHashMapVariant::Type::phase1_slice);
    EXPECT_EQ(TYPE_RESULT(AggrPhase2, TYPE_FLOAT, true), AggHashMapVariant::Type::phase2_slice);
}

// ----------------------------------------------------------------------------
// Reserve / supports_reserve / reserve_bytes_estimate (FE-NDV reserve support)
// ----------------------------------------------------------------------------

// reserve(n) on a flat numeric map grows capacity to cover n keys (>= n at 7/8 load).
TEST(AggHashMapReserveTest, reserve_grows_capacity) {
    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);

    AggHashMapVariant variant;
    variant.init(&dummy, AggHashMapVariant::Type::phase2_int64, &stat);
    const size_t before = variant.capacity();
    const size_t n = 1u << 20; // 1M
    variant.reserve(n);
    EXPECT_GE(variant.capacity(), n);
    EXPECT_GT(variant.capacity(), before);
}

// reserve on a fixed-size map (uint8/int8/int16 -> SmallFixedSizeHashMap) is a no-op:
// these never rehash, so the capacity must not move.
TEST(AggHashMapReserveTest, fixed_map_reserve_is_noop) {
    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);

    AggHashMapVariant variant;
    variant.init(&dummy, AggHashMapVariant::Type::phase2_int8, &stat);
    const size_t before = variant.capacity();
    variant.reserve(1u << 20);
    EXPECT_EQ(variant.capacity(), before);
}

// supports_reserve mirrors reserve(): every phmap-backed map is reservable; only the
// SmallFixedSizeHashMap variants (uint8/int8/int16, hard-coded full keyspace) are not.
// In particular the compressed-key (cx) and fixed-size-slice (fx) variants the optimizer
// picks for single numeric / multi-column keys once min-max stats are known are reservable
// -- exactly the case that engages the FE NDV estimate.
TEST(AggHashMapReserveTest, supports_reserve_covers_all_non_fixed) {
    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);
    auto supports = [&](AggHashMapVariant::Type type) {
        AggHashMapVariant variant;
        variant.init(&dummy, type, &stat);
        return variant.supports_reserve();
    };

    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_int32));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_int64));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_null_int64));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_decimal128));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_slice));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_string));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_null_string));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_slice_two_level));
    // compressed-key and fixed-size-slice variants (stats-driven) are reservable too.
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_slice_cx4));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase2_slice_fx8));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase1_slice_cx8));
    EXPECT_TRUE(supports(AggHashMapVariant::Type::phase1_slice_fx16));

    // Only the small fixed-size-array maps cannot reserve.
    EXPECT_FALSE(supports(AggHashMapVariant::Type::phase2_uint8));
    EXPECT_FALSE(supports(AggHashMapVariant::Type::phase2_int8));
    EXPECT_FALSE(supports(AggHashMapVariant::Type::phase2_int16));
}

// reserve() actually grows the bucket array for the compressed-key / fixed-size-slice
// maps -- the variants the optimizer picks for a single numeric key once min-max stats
// are known, i.e. exactly the case that carries the FE NDV estimate.
TEST(AggHashMapReserveTest, reserve_grows_compressed_and_fixed_slice) {
    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);
    const size_t n = 1u << 20; // 1M
    for (auto type : {AggHashMapVariant::Type::phase2_slice_cx4, AggHashMapVariant::Type::phase2_slice_fx8,
                      AggHashMapVariant::Type::phase1_slice_cx8, AggHashMapVariant::Type::phase1_slice_fx16}) {
        AggHashMapVariant variant;
        variant.init(&dummy, type, &stat);
        const size_t before = variant.capacity();
        variant.reserve(n);
        EXPECT_GE(variant.capacity(), n);
        EXPECT_GT(variant.capacity(), before);
    }
}

// parallel (two-level) reserve(n) is a TOTAL target across submaps, not per-submap.
TEST(AggHashMapReserveTest, two_level_reserve_is_total) {
    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);

    AggHashMapVariant variant;
    variant.init(&dummy, AggHashMapVariant::Type::phase2_slice_two_level, &stat);
    const size_t n = 1u << 20;
    variant.reserve(n);
    EXPECT_GE(variant.capacity(), n);
}

// reserve_bytes_estimate is monotonic in count for reservable maps and 0 for fixed.
TEST(AggHashMapReserveTest, reserve_bytes_estimate) {
    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics stat(&profile);

    AggHashMapVariant variant;
    variant.init(&dummy, AggHashMapVariant::Type::phase2_int64, &stat);
    EXPECT_GT(variant.reserve_bytes_estimate(1000), variant.reserve_bytes_estimate(100));

    AggHashMapVariant fixed;
    fixed.init(&dummy, AggHashMapVariant::Type::phase2_int8, &stat);
    EXPECT_EQ(fixed.reserve_bytes_estimate(1000), 0u);
}

} // namespace starrocks