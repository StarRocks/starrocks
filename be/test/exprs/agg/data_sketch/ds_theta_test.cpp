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
#include "exprs/agg/data_sketch/ds_theta.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <memory>

#include "base/hash/unaligned_access.h"
#include "base/string/slice.h"
#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/util/thrift_util.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "runtime/mem_pool.h"
#include "testutil/function_utils.h"
#include "types/bitmap_value.h"
#include "types/time_types.h"

namespace starrocks {

class DataSketchsThetaTest : public testing::Test {
public:
    DataSketchsThetaTest() = default;

    void SetUp() override {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }
    void TearDown() override {
        delete utils;
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
    }

private:
    FunctionUtils* utils{};
    FunctionContext* ctx{};
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

TEST_F(DataSketchsThetaTest, TestSerializeDeserialize1) {
    int64_t memory_usage = 0;
    DataSketchesTheta theta(Slice(), &memory_usage);
    for (int i = 0; i < 100; i++) {
        theta.update(i);
    }
    uint8_t dst[1024];
    size_t size = theta.serialize(dst);
    ASSERT_EQ(size, theta.serialize_size());
    ASSERT_EQ(theta.estimate_cardinality(), 100);

    DataSketchesTheta theta2(Slice(dst, size), &memory_usage);
    ASSERT_EQ(theta2.serialize_size(), size);
    ASSERT_EQ(theta2.estimate_cardinality(), 100);
}

TEST_F(DataSketchsThetaTest, TestSerializeDeserialize2) {
    int64_t memory_usage = 0;
    DataSketchesTheta theta1(&memory_usage);
    DataSketchesTheta theta2(&memory_usage);
    for (int i = 0; i < 100; i++) {
        theta1.update(i);
        theta2.update(i);
    }
    DataSketchesTheta theta3(&memory_usage);
    theta3.merge(theta1);
    theta3.merge(theta2);

    uint8_t dst[1024];
    size_t size = theta3.serialize(dst);
    ASSERT_EQ(size, theta3.serialize_size());
    ASSERT_EQ(theta3.estimate_cardinality(), 100);

    // deserialize
    {
        DataSketchesTheta theta4(&memory_usage);
        theta4.deserialize(Slice(dst, size));
        ASSERT_EQ(theta4.serialize_size(), size);
        ASSERT_EQ(theta4.estimate_cardinality(), 100);
    }
    {
        DataSketchesTheta theta4(Slice(dst, size), &memory_usage);
        ASSERT_EQ(theta4.serialize_size(), size);
        ASSERT_EQ(theta4.estimate_cardinality(), 100);
    }
}

// The serialized payload must start with the 2-byte StarRocks header
// [0xFE][lg_k] so that lg_k survives serialize -> deserialize round-trips.
TEST_F(DataSketchsThetaTest, HeaderRoundTripPreservesLgK) {
    int64_t memory_usage = 0;
    constexpr uint8_t kCustomLgK = 14;
    DataSketchesTheta theta(kCustomLgK, &memory_usage);
    for (int i = 0; i < 500; ++i) {
        theta.update(i);
    }

    uint8_t dst[8192];
    size_t size = theta.serialize(dst);
    ASSERT_GE(size, DataSketchesTheta::kHeaderSize);
    EXPECT_EQ(dst[0], DataSketchesTheta::kMagicByte);
    EXPECT_EQ(dst[1], kCustomLgK);

    DataSketchesTheta restored(&memory_usage);
    ASSERT_TRUE(restored.deserialize(Slice(dst, size)));
    EXPECT_EQ(restored.get_lg_config_k(), kCustomLgK);
    // The estimate should still match (within theta error bounds, but with
    // lg_k=14 and 500 distinct values it is essentially exact).
    EXPECT_EQ(restored.estimate_cardinality(), 500);
}

// Header-less payloads written by older StarRocks versions must still
// deserialize. Compact-theta's first byte is pre_longs in {1,2,3}, which can
// never collide with the magic byte 0xFE.
TEST_F(DataSketchsThetaTest, LegacyBytesWithoutHeaderDeserialize) {
    int64_t memory_usage = 0;
    DataSketchesTheta source(&memory_usage);
    for (int i = 0; i < 100; ++i) {
        source.update(i);
    }
    uint8_t framed[1024];
    size_t framed_size = source.serialize(framed);
    ASSERT_GT(framed_size, DataSketchesTheta::kHeaderSize);

    // Drop the 2-byte StarRocks header to mimic a pre-magic-byte payload.
    const uint8_t* legacy = framed + DataSketchesTheta::kHeaderSize;
    size_t legacy_size = framed_size - DataSketchesTheta::kHeaderSize;
    ASSERT_NE(legacy[0], DataSketchesTheta::kMagicByte);

    DataSketchesTheta restored(&memory_usage);
    ASSERT_TRUE(restored.deserialize(Slice(legacy, legacy_size)));
    EXPECT_EQ(restored.estimate_cardinality(), 100);
    // Header-less payloads carry no lg_k, so deserialization keeps the default.
    EXPECT_EQ(restored.get_lg_config_k(), DEFAULT_THETA_LOG_K);
}

// During merge() in the aggregate path, an empty state adopts the lg_k carried
// in the wire format of the incoming sketch. This keeps the configured
// precision intact across shuffle boundaries.
TEST_F(DataSketchsThetaTest, DeserializeAdoptsIncomingLgK) {
    int64_t memory_usage = 0;
    constexpr uint8_t kIncomingLgK = 14;
    DataSketchesTheta incoming(kIncomingLgK, &memory_usage);
    for (int i = 0; i < 100; ++i) {
        incoming.update(i);
    }

    uint8_t buf[2048];
    size_t size = incoming.serialize(buf);

    DataSketchesTheta parsed(Slice(buf, size), &memory_usage);
    EXPECT_EQ(parsed.get_lg_config_k(), kIncomingLgK);

    // Mirror what ThetaSketchAggregateFunction::merge() does when the state is
    // still empty: construct using the incoming sketch's lg_k, then merge.
    DataSketchesTheta state(parsed.get_lg_config_k(), &memory_usage);
    state.merge(parsed);
    EXPECT_EQ(state.get_lg_config_k(), kIncomingLgK);
    EXPECT_EQ(state.estimate_cardinality(), 100);
}

// A corrupted or malicious lg_k byte must not poison the state: we keep the
// default and still parse the body (the theta-union update will fail safely
// only on truly malformed bytes).
TEST_F(DataSketchsThetaTest, OutOfRangeLgKInHeaderFallsBackToDefault) {
    int64_t memory_usage = 0;
    DataSketchesTheta source(&memory_usage);
    for (int i = 0; i < 50; ++i) {
        source.update(i);
    }
    uint8_t framed[1024];
    size_t size = source.serialize(framed);

    // 0xFF is outside [MIN_LG_K=5, MAX_LG_K=26].
    framed[1] = 0xFF;

    DataSketchesTheta restored(&memory_usage);
    ASSERT_TRUE(restored.deserialize(Slice(framed, size)));
    EXPECT_EQ(restored.get_lg_config_k(), DEFAULT_THETA_LOG_K);
    EXPECT_EQ(restored.estimate_cardinality(), 50);
}

// A state that never saw update() or merge() serializes to just the header.
// Round-tripping that 2-byte payload must not throw and must keep the
// estimate at zero.
TEST_F(DataSketchsThetaTest, EmptyStateSerializeRoundTrip) {
    int64_t memory_usage = 0;
    DataSketchesTheta empty(&memory_usage);
    EXPECT_EQ(empty.estimate_cardinality(), 0);

    uint8_t dst[16];
    size_t size = empty.serialize(dst);
    EXPECT_EQ(size, DataSketchesTheta::kHeaderSize);
    EXPECT_EQ(dst[0], DataSketchesTheta::kMagicByte);

    DataSketchesTheta restored(&memory_usage);
    ASSERT_TRUE(restored.deserialize(Slice(dst, size)));
    EXPECT_EQ(restored.estimate_cardinality(), 0);
}
} // namespace starrocks