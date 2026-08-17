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
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/util/thrift_util.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/base_aggregate_test.h"
#include "exprs/ds_theta_functions.h"
#include "gutil/casts.h"
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
    auto wrapped = datasketches::wrapped_compact_theta_sketch_alloc<alloc_type>::wrap(buf.data(), actual);
    int64_t apache_est = static_cast<int64_t>(wrapped.get_estimate());
    EXPECT_NEAR(apache_est, theta.estimate_cardinality(), 1);
    EXPECT_NEAR(apache_est, 5000, 500);
}
// ---- Helpers ----------------------------------------------------------------

// Serialize 'count' unique integers starting from 'start' into a compact theta sketch.
static std::vector<uint8_t> make_sketch_bytes(int start, int count) {
    int64_t mem = 0;
    DataSketchesTheta theta(&mem);
    for (int i = start; i < start + count; i++) theta.update(static_cast<uint64_t>(i));
    std::vector<uint8_t> buf(theta.serialize_size());
    size_t sz = theta.serialize(buf.data());
    buf.resize(sz);
    return buf;
}

// Deserialize a compact theta sketch and return its cardinality estimate, or -1 on error.
static double estimate_from_slice(Slice s) {
    int64_t mem = 0;
    DataSketchesTheta theta(&mem);
    if (!theta.deserialize(s)) return -1.0;
    return static_cast<double>(theta.estimate_cardinality());
}

static ColumnPtr make_binary_col(Slice s) {
    auto col = BinaryColumn::create();
    col->append(s);
    return col;
}

// ---- Bug fix 1: zero-length operands to scalar set-op functions ---------

// Before the fix, wrap() threw "at least 8 bytes expected, actual 0" on any
// zero-length slice, turning queries into InternalError.  After the fix the
// set-op semantics (∅∩X=∅, ∅\X=∅, X\∅=X) must hold without error.

TEST_F(DataSketchsThetaTest, TestIntersectBothEmpty) {
    Columns cols{make_binary_col(Slice()), make_binary_col(Slice())};
    auto result = DsThetaFunctions::ds_theta_intersect(ctx, cols);
    ASSERT_TRUE(result.ok()) << result.status().message();
    auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
    ASSERT_GT(slice.size, 0u);
    EXPECT_NEAR(estimate_from_slice(slice), 0.0, 0.01);
}

TEST_F(DataSketchsThetaTest, TestIntersectOneEmpty) {
    auto sketch = make_sketch_bytes(0, 500);
    Slice sk_slice(reinterpret_cast<const char*>(sketch.data()), sketch.size());

    // non-empty lhs, empty rhs → empty
    {
        Columns cols{make_binary_col(sk_slice), make_binary_col(Slice())};
        auto result = DsThetaFunctions::ds_theta_intersect(ctx, cols);
        ASSERT_TRUE(result.ok());
        auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
        EXPECT_NEAR(estimate_from_slice(slice), 0.0, 0.01);
    }
    // empty lhs, non-empty rhs → empty
    {
        Columns cols{make_binary_col(Slice()), make_binary_col(sk_slice)};
        auto result = DsThetaFunctions::ds_theta_intersect(ctx, cols);
        ASSERT_TRUE(result.ok());
        auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
        EXPECT_NEAR(estimate_from_slice(slice), 0.0, 0.01);
    }
}

TEST_F(DataSketchsThetaTest, TestANotBEmptyLhs) {
    auto sketch = make_sketch_bytes(0, 500);
    Slice sk_slice(reinterpret_cast<const char*>(sketch.data()), sketch.size());

    Columns cols{make_binary_col(Slice()), make_binary_col(sk_slice)};
    auto result = DsThetaFunctions::ds_theta_a_not_b(ctx, cols);
    ASSERT_TRUE(result.ok());
    auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
    EXPECT_NEAR(estimate_from_slice(slice), 0.0, 0.01);
}

TEST_F(DataSketchsThetaTest, TestANotBEmptyRhs) {
    // X \ ∅ = X: result estimate must match the input sketch's estimate
    auto sketch = make_sketch_bytes(0, 500);
    Slice sk_slice(reinterpret_cast<const char*>(sketch.data()), sketch.size());

    Columns cols{make_binary_col(sk_slice), make_binary_col(Slice())};
    auto result = DsThetaFunctions::ds_theta_a_not_b(ctx, cols);
    ASSERT_TRUE(result.ok());
    auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
    EXPECT_NEAR(estimate_from_slice(slice), 500.0, 50.0);
}

// ---- Bug fix 2: malformed sketches must surface as errors, not empty results ----

// Before the fix, corrupt input was silently swallowed: ds_theta_combine
// returned a valid empty sketch for "garbage_not_a_sketch", making the
// corruption completely invisible.  After the fix ctx->has_error() must be true.

TEST_F(DataSketchsThetaTest, TestCombineRejectsMalformedInput) {
    std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_VARBINARY)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_VARBINARY);
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* func = get_aggregate_function("ds_theta_combine", TYPE_VARBINARY, TYPE_VARBINARY, false);
    ASSERT_NE(nullptr, func);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    auto data_col = BinaryColumn::create();
    data_col->append(Slice("not_a_valid_sketch"));
    const Column* raw[] = {data_col.get()};
    func->update_batch_single_state(local_ctx.get(), 1, raw, state->state());

    EXPECT_TRUE(local_ctx->has_error());
}

TEST_F(DataSketchsThetaTest, TestCombineAcceptsValidInput) {
    // Regression: valid input must not trip the error path.
    std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_VARBINARY)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_VARBINARY);
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* func = get_aggregate_function("ds_theta_combine", TYPE_VARBINARY, TYPE_VARBINARY, false);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    auto sketch = make_sketch_bytes(0, 200);
    auto data_col = BinaryColumn::create();
    data_col->append(Slice(reinterpret_cast<const char*>(sketch.data()), sketch.size()));
    const Column* raw[] = {data_col.get()};
    func->update_batch_single_state(local_ctx.get(), 1, raw, state->state());

    ASSERT_FALSE(local_ctx->has_error()) << local_ctx->error_msg();
    auto result_col = BinaryColumn::create();
    func->finalize_to_column(local_ctx.get(), state->state(), result_col.get());
    EXPECT_GT(result_col->get_slice(0).size, 0u);
}

TEST_F(DataSketchsThetaTest, TestIntersectCondAggRejectsMalformedInput) {
    std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_VARBINARY),
                                             TypeDescriptor::from_logical_type(TYPE_INT)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    const AggregateFunction* func =
            get_aggregate_function("ds_theta_intersect_cond_agg", TYPE_VARBINARY, TYPE_DOUBLE, false);
    ASSERT_NE(nullptr, func);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    auto sketch_col = BinaryColumn::create();
    sketch_col->append(Slice("garbage_not_a_sketch"));
    auto flag_col = FixedLengthColumn<int32_t>::create();
    flag_col->append(1); // is_anchor = 1
    const Column* raw[] = {sketch_col.get(), flag_col.get()};
    func->update_batch_single_state(local_ctx.get(), 1, raw, state->state());

    EXPECT_TRUE(local_ctx->has_error());
}

// ---- Happy-path tests: scalar set-op functions ----------------------------

TEST_F(DataSketchsThetaTest, TestScalarEstimate) {
    auto sketch = make_sketch_bytes(0, 1000);
    Slice sk(reinterpret_cast<const char*>(sketch.data()), sketch.size());
    Columns cols{make_binary_col(sk)};
    auto result = DsThetaFunctions::ds_theta_estimate(ctx, cols);
    ASSERT_TRUE(result.ok());
    auto* out = down_cast<const DoubleColumn*>(result.value().get());
    EXPECT_NEAR(out->get_data()[0], 1000.0, 100.0);
}

TEST_F(DataSketchsThetaTest, TestScalarUnionDisjoint) {
    auto a = make_sketch_bytes(0, 500);
    auto b = make_sketch_bytes(1000, 500);
    Columns cols{make_binary_col(Slice(reinterpret_cast<const char*>(a.data()), a.size())),
                 make_binary_col(Slice(reinterpret_cast<const char*>(b.data()), b.size()))};
    auto result = DsThetaFunctions::ds_theta_union(ctx, cols);
    ASSERT_TRUE(result.ok());
    auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
    EXPECT_NEAR(estimate_from_slice(slice), 1000.0, 100.0);
}

TEST_F(DataSketchsThetaTest, TestScalarIntersectOverlapping) {
    // [0,1000) ∩ [500,1500) = [500,1000): ~500
    auto a = make_sketch_bytes(0, 1000);
    auto b = make_sketch_bytes(500, 1000);
    Columns cols{make_binary_col(Slice(reinterpret_cast<const char*>(a.data()), a.size())),
                 make_binary_col(Slice(reinterpret_cast<const char*>(b.data()), b.size()))};
    auto result = DsThetaFunctions::ds_theta_intersect(ctx, cols);
    ASSERT_TRUE(result.ok());
    auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
    EXPECT_NEAR(estimate_from_slice(slice), 500.0, 100.0);
}

TEST_F(DataSketchsThetaTest, TestScalarANotBOverlapping) {
    // [0,1000) \ [500,1500) = [0,500): ~500
    auto a = make_sketch_bytes(0, 1000);
    auto b = make_sketch_bytes(500, 1000);
    Columns cols{make_binary_col(Slice(reinterpret_cast<const char*>(a.data()), a.size())),
                 make_binary_col(Slice(reinterpret_cast<const char*>(b.data()), b.size()))};
    auto result = DsThetaFunctions::ds_theta_a_not_b(ctx, cols);
    ASSERT_TRUE(result.ok());
    auto slice = down_cast<const BinaryColumn*>(result.value().get())->get_slice(0);
    EXPECT_NEAR(estimate_from_slice(slice), 500.0, 100.0);
}

// ---- Happy-path tests: ds_theta_intersect_cond_agg ------------------------

// Helper: build a FunctionContext and aggregate function for ds_theta_intersect_cond_agg.
static std::unique_ptr<FunctionContext> make_cond_agg_ctx() {
    std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_VARBINARY),
                                             TypeDescriptor::from_logical_type(TYPE_INT)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context(std::move(arg_types), return_type));
}

// Feed rows into ds_theta_intersect_cond_agg one at a time.
static void feed_row(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state,
                     const std::vector<uint8_t>& sketch_bytes, int32_t is_anchor) {
    auto sketch_col = BinaryColumn::create();
    sketch_col->append(Slice(reinterpret_cast<const char*>(sketch_bytes.data()), sketch_bytes.size()));
    auto flag_col = FixedLengthColumn<int32_t>::create();
    flag_col->append(is_anchor);
    const Column* raw[] = {sketch_col.get(), flag_col.get()};
    func->update_batch_single_state(ctx, 1, raw, state);
}

TEST_F(DataSketchsThetaTest, TestIntersectCondAggHappyPath) {
    // anchor = [0,1000), window = [500,1500) → intersection ~500
    auto local_ctx = make_cond_agg_ctx();
    const AggregateFunction* func =
            get_aggregate_function("ds_theta_intersect_cond_agg", TYPE_VARBINARY, TYPE_DOUBLE, false);
    ASSERT_NE(nullptr, func);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(0, 1000), 1);   // anchor
    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(500, 1000), 0); // window

    ASSERT_FALSE(local_ctx->has_error()) << local_ctx->error_msg();
    auto result_col = DoubleColumn::create();
    func->finalize_to_column(local_ctx.get(), state->state(), result_col.get());
    EXPECT_NEAR(result_col->get_data()[0], 500.0, 100.0);
}

TEST_F(DataSketchsThetaTest, TestIntersectCondAggDisjointSets) {
    // anchor = [0,500), window = [1000,1500) → intersection ~0
    auto local_ctx = make_cond_agg_ctx();
    const AggregateFunction* func =
            get_aggregate_function("ds_theta_intersect_cond_agg", TYPE_VARBINARY, TYPE_DOUBLE, false);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(0, 500), 1);
    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(1000, 500), 0);

    auto result_col = DoubleColumn::create();
    func->finalize_to_column(local_ctx.get(), state->state(), result_col.get());
    EXPECT_NEAR(result_col->get_data()[0], 0.0, 50.0);
}

TEST_F(DataSketchsThetaTest, TestIntersectCondAggMultipleRowsPerGroup) {
    // Feed two anchor sketches and two window sketches; their unions are intersected.
    // anchor = [0,500) ∪ [500,1000) = [0,1000)
    // window = [500,1000) ∪ [1000,1500) = [500,1500)
    // intersection = [500,1000) ~500
    auto local_ctx = make_cond_agg_ctx();
    const AggregateFunction* func =
            get_aggregate_function("ds_theta_intersect_cond_agg", TYPE_VARBINARY, TYPE_DOUBLE, false);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(0, 500), 1);
    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(500, 500), 1);
    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(500, 500), 0);
    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(1000, 500), 0);

    ASSERT_FALSE(local_ctx->has_error()) << local_ctx->error_msg();
    auto result_col = DoubleColumn::create();
    func->finalize_to_column(local_ctx.get(), state->state(), result_col.get());
    EXPECT_NEAR(result_col->get_data()[0], 500.0, 100.0);
}

TEST_F(DataSketchsThetaTest, TestIntersectCondAggSerializeDeserializeRoundTrip) {
    // Verify serialize → merge → finalize produces the same result as a direct finalize.
    auto local_ctx = make_cond_agg_ctx();
    const AggregateFunction* func =
            get_aggregate_function("ds_theta_intersect_cond_agg", TYPE_VARBINARY, TYPE_DOUBLE, false);

    // State 1: anchor rows
    auto state1 = ManagedAggrState::create(local_ctx.get(), func);
    feed_row(local_ctx.get(), func, state1->state(), make_sketch_bytes(0, 1000), 1);

    // State 2: window rows
    auto state2 = ManagedAggrState::create(local_ctx.get(), func);
    feed_row(local_ctx.get(), func, state2->state(), make_sketch_bytes(500, 1000), 0);

    // Serialize both partial states
    MutableColumnPtr serde1 = BinaryColumn::create();
    MutableColumnPtr serde2 = BinaryColumn::create();
    func->serialize_to_column(local_ctx.get(), state1->state(), serde1.get());
    func->serialize_to_column(local_ctx.get(), state2->state(), serde2.get());

    // Merge into a third state
    auto state3 = ManagedAggrState::create(local_ctx.get(), func);
    func->merge(local_ctx.get(), serde1.get(), state3->state(), 0);
    func->merge(local_ctx.get(), serde2.get(), state3->state(), 0);

    ASSERT_FALSE(local_ctx->has_error()) << local_ctx->error_msg();
    auto result_col = DoubleColumn::create();
    func->finalize_to_column(local_ctx.get(), state3->state(), result_col.get());
    EXPECT_NEAR(result_col->get_data()[0], 500.0, 100.0);
}

TEST_F(DataSketchsThetaTest, TestIntersectCondAggReturnsZeroWithMissingGroup) {
    // If only anchor rows are fed (no window), finalize must return 0 without error.
    auto local_ctx = make_cond_agg_ctx();
    const AggregateFunction* func =
            get_aggregate_function("ds_theta_intersect_cond_agg", TYPE_VARBINARY, TYPE_DOUBLE, false);
    auto state = ManagedAggrState::create(local_ctx.get(), func);

    feed_row(local_ctx.get(), func, state->state(), make_sketch_bytes(0, 500), 1);

    ASSERT_FALSE(local_ctx->has_error());
    auto result_col = DoubleColumn::create();
    func->finalize_to_column(local_ctx.get(), state->state(), result_col.get());
    EXPECT_EQ(result_col->get_data()[0], 0.0);
}

} // namespace starrocks