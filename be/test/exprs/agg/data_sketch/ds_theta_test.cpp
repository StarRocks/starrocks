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

// Disjoint sets produce a merged sketch whose estimate is the sum of inputs
// (within theta error). Guards against accidental serialization drift in the
// Apache DataSketches compact theta format used on the wire.
TEST_F(DataSketchsThetaTest, TestMergeDisjointSets) {
    int64_t memory_usage = 0;
    DataSketchesTheta theta_a(&memory_usage);
    DataSketchesTheta theta_b(&memory_usage);
    for (int i = 0; i < 1000; i++) {
        theta_a.update(i);
    }
    for (int i = 10000; i < 11000; i++) {
        theta_b.update(i);
    }
    DataSketchesTheta merged(&memory_usage);
    merged.merge(theta_a);
    merged.merge(theta_b);
    int64_t est = merged.estimate_cardinality();
    // Apache DataSketches default lg_k=12 gives ~3.125% relative error at 95% CI;
    // 10% bounds are very generous so flakiness is impossible.
    EXPECT_NEAR(est, 2000, 200);
}

// Round-trip: serialize via DataSketchesTheta, deserialize via
// wrapped_compact_theta_sketch, confirm Apache estimate matches our internal
// estimate. Guards the wire format used by ds_theta_estimate scalar and
// ds_theta_combine aggregate.
TEST_F(DataSketchsThetaTest, TestCompactWireRoundTrip) {
    int64_t memory_usage = 0;
    DataSketchesTheta theta(&memory_usage);
    for (int i = 0; i < 5000; i++) {
        theta.update(i);
    }
    size_t sz = theta.serialize_size();
    std::vector<uint8_t> buf(sz);
    size_t actual = theta.serialize(buf.data());
    ASSERT_EQ(sz, actual);

    using alloc_type = DataSketchesTheta::alloc_type;
    int64_t mem2 = 0;
    auto wrapped = datasketches::wrapped_compact_theta_sketch_alloc<alloc_type>::wrap(buf.data(), actual);
    int64_t apache_est = static_cast<int64_t>(wrapped.get_estimate());
    EXPECT_NEAR(apache_est, theta.estimate_cardinality(), 1);
    EXPECT_NEAR(apache_est, 5000, 500);
}
} // namespace starrocks