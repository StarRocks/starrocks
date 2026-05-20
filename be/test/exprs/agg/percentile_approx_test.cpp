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

#include "exprs/agg/percentile_approx.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <memory>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/function_context.h"
#include "testutil/function_utils.h"
#include "types/logical_type.h"

namespace starrocks {

class PercentileApproxAggTest : public ::testing::Test {
public:
    void SetUp() override {
        _utils = std::make_unique<FunctionUtils>();
        _ctx = _utils->get_fn_ctx();
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }
    void TearDown() override {
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
        _utils.reset();
    }

protected:
    std::unique_ptr<FunctionUtils> _utils;
    FunctionContext* _ctx{};
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

namespace {

// Build a FunctionContext that carries the const columns the aggregate
// expects via ctx->get_constant_column(i). Used for the quantile and
// (optionally) compression parameters.
std::unique_ptr<FunctionContext> make_ctx(std::vector<TypeDescriptor> arg_types, TypeDescriptor return_type,
                                          Columns const_columns) {
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context(std::move(arg_types), std::move(return_type)));
    ctx->set_constant_columns(std::move(const_columns));
    return ctx;
}

struct ManagedState {
    ManagedState(FunctionContext* ctx, const AggregateFunction* func) : _ctx(ctx), _func(func) {
        _state = _mem_pool.allocate_aligned(func->size(), func->alignof_size());
        _func->create(_ctx, _state);
    }
    ~ManagedState() { _func->destroy(_ctx, _state); }
    AggDataPtr state() { return _state; }

    FunctionContext* _ctx;
    const AggregateFunction* _func;
    MemPool _mem_pool;
    AggDataPtr _state;
};

} // namespace

// percentile_approx_weighted with a row whose weight is negative.
// The bug: only weight == 0 was filtered, so the negative-weight row
// entered the digest and produced an incorrect median.
TEST_F(PercentileApproxAggTest, weighted_negative_weight_is_skipped) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx_weighted", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_BIGINT),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1),
                          ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1), quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = DoubleColumn::create();
    auto weights = Int64Column::create();
    for (auto v : {10.0, 20.0, 30.0}) {
        values->append(v);
        weights->append(1);
    }
    values->append(100.0);
    weights->append(-10);

    std::vector<const Column*> raw{values.get(), weights.get(), quantile_const.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    EXPECT_DOUBLE_EQ(20.0, result->get_data()[0]);
}

// percentile_approx with non-finite values in the input.
// Bug: TDigest::add() rejected NaN but accepted +Inf / -Inf, which
// distorted the digest and the quantile.
TEST_F(PercentileApproxAggTest, scalar_inf_values_are_skipped) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = DoubleColumn::create();
    for (auto v : {10.0, 20.0, 30.0}) {
        values->append(v);
    }
    values->append(std::numeric_limits<double>::infinity());
    values->append(-std::numeric_limits<double>::infinity());
    values->append(std::numeric_limits<double>::quiet_NaN());

    std::vector<const Column*> raw{values.get(), quantile_const.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    const double median = result->get_data()[0];
    EXPECT_TRUE(std::isfinite(median));
    // T-Digest is approximate; the median of {10, 20, 30} should be near 20.
    EXPECT_GE(median, 10.0);
    EXPECT_LE(median, 30.0);
}

// Zero-total-weight state must finalize to SQL NULL via the nullable
// wrapper, not NaN. Reproduces only on the is_nullable=true mapping,
// which is what ALWAYS_NULLABLE_RESULT_AGG_FUNCS forces at runtime.
TEST_F(PercentileApproxAggTest, weighted_empty_state_finalizes_to_null) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx_weighted", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/true);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_BIGINT),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto weight_const = ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), weight_const, quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    auto* values_data = down_cast<DoubleColumn*>(values->data_column().get());
    for (auto v : {10.0, 20.0, 30.0}) {
        values_data->append(v);
        values->null_column_data().push_back(0);
    }

    auto weights = NullableColumn::create(weight_const->clone_shared(), NullColumn::create(values->size(), 0));

    std::vector<const Column*> raw{values.get(), weights.get(), quantile_const.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    EXPECT_TRUE(result->is_null(0));
}

// Non-finite compression must not propagate to TDigest sizing math.
// FE blocks the SQL path, so we drive it directly via FunctionContext to
// exercise the BE-side guard.
TEST_F(PercentileApproxAggTest, non_finite_compression_falls_back_to_default) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    auto bad_compression = ColumnHelper::create_const_column<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN(), 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const, bad_compression};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = DoubleColumn::create();
    for (auto v : {10.0, 20.0, 30.0}) {
        values->append(v);
    }
    std::vector<const Column*> raw{values.get(), quantile_const.get(), bad_compression.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    const double median = result->get_data()[0];
    EXPECT_TRUE(std::isfinite(median));
    EXPECT_GE(median, 10.0);
    EXPECT_LE(median, 30.0);
}

} // namespace starrocks
