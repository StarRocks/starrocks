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

#include "exec/pipeline/nljoin/nljoin_runtime_filter.h"

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "exec/runtime_filter_compat/runtime_filter_serde.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_factory.h"
#include "types/decimalv3.h"

namespace starrocks {
namespace {

ColumnPtr make_int_column(const std::vector<int32_t>& values) {
    auto column = Int32Column::create();
    for (int32_t value : values) {
        column->append(value);
    }
    return column;
}

} // namespace

TEST(NLJoinRuntimeFilterTest, ComputeMinMaxBoundaryWithNullsAndMultipleColumns) {
    auto values1 = ColumnHelper::cast_to_nullable_column(make_int_column({5, 3}));
    values1->append_nulls(1);
    auto values2 = ColumnHelper::cast_to_nullable_column(make_int_column({9, 1}));
    values2->append_nulls(1);

    auto min_boundary = pipeline::compute_min_max_boundary(TYPE_INT, TExprOpcode::GT, {values1, values2});
    ASSERT_NE(min_boundary, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_INT>(min_boundary).value(0), 1);

    auto max_boundary = pipeline::compute_min_max_boundary(TYPE_INT, TExprOpcode::LT, {values1, values2});
    ASSERT_NE(max_boundary, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_INT>(max_boundary).value(0), 9);

    auto all_null = ColumnHelper::create_const_null_column(3);
    EXPECT_EQ(pipeline::compute_min_max_boundary(TYPE_INT, TExprOpcode::GT, {all_null}), nullptr);
    EXPECT_EQ(pipeline::compute_min_max_boundary(TYPE_INT, TExprOpcode::LT, {all_null}), nullptr);
}

TEST(NLJoinRuntimeFilterTest, ComputeMinMaxBoundaryWithVarcharAndDecimal) {
    auto strings1 = BinaryColumn::create();
    strings1->append_string("mango");
    strings1->append_string("apple");
    auto strings2 = BinaryColumn::create();
    strings2->append_string("pear");
    strings2->append_string("banana");

    auto string_min = pipeline::compute_min_max_boundary(TYPE_VARCHAR, TExprOpcode::GT, {strings1, strings2});
    ASSERT_NE(string_min, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_VARCHAR>(string_min).value(0).to_string(), "apple");

    auto string_max = pipeline::compute_min_max_boundary(TYPE_VARCHAR, TExprOpcode::LT, {strings1, strings2});
    ASSERT_NE(string_max, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_VARCHAR>(string_max).value(0).to_string(), "pear");

    auto decimals1 = Decimal32Column::create(9, 2);
    int32_t decimal_1234 = 0;
    int32_t decimal_minus_500 = 0;
    DecimalV3Cast::from_string<int32_t>(&decimal_1234, 9, 2, "12.34", 5);
    DecimalV3Cast::from_string<int32_t>(&decimal_minus_500, 9, 2, "-5.00", 5);
    decimals1->append(decimal_1234);
    decimals1->append(decimal_minus_500);

    auto decimals2 = Decimal32Column::create(9, 2);
    int32_t decimal_9999 = 0;
    int32_t decimal_001 = 0;
    DecimalV3Cast::from_string<int32_t>(&decimal_9999, 9, 2, "99.99", 5);
    DecimalV3Cast::from_string<int32_t>(&decimal_001, 9, 2, "0.01", 4);
    decimals2->append(decimal_9999);
    decimals2->append(decimal_001);

    auto decimal_min = pipeline::compute_min_max_boundary(TYPE_DECIMAL32, TExprOpcode::GT, {decimals1, decimals2});
    ASSERT_NE(decimal_min, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_DECIMAL32>(decimal_min).value(0), decimal_minus_500);

    auto decimal_max = pipeline::compute_min_max_boundary(TYPE_DECIMAL32, TExprOpcode::LT, {decimals1, decimals2});
    ASSERT_NE(decimal_max, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_DECIMAL32>(decimal_max).value(0), decimal_9999);

    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto values = DoubleColumn::create();
    values->append(nan);
    values->append(3.0);
    values->append(-2.0);

    auto double_min = pipeline::compute_min_max_boundary(TYPE_DOUBLE, TExprOpcode::GT, {values});
    ASSERT_NE(double_min, nullptr);
    EXPECT_DOUBLE_EQ(ColumnViewer<TYPE_DOUBLE>(double_min).value(0), -2.0);

    auto double_max = pipeline::compute_min_max_boundary(TYPE_DOUBLE, TExprOpcode::LT, {values});
    ASSERT_NE(double_max, nullptr);
    EXPECT_DOUBLE_EQ(ColumnViewer<TYPE_DOUBLE>(double_max).value(0), 3.0);

    auto nan_values = DoubleColumn::create();
    nan_values->append(nan);
    EXPECT_EQ(pipeline::compute_min_max_boundary(TYPE_DOUBLE, TExprOpcode::GT, {nan_values}), nullptr);
}

TEST(NLJoinRuntimeFilterTest, ComputeMinMaxBoundaryFoldsSequentially) {
    auto strings1 = BinaryColumn::create();
    strings1->append_string("banana");
    auto first = pipeline::compute_min_max_boundary(TYPE_VARCHAR, TExprOpcode::GT, {strings1});
    ASSERT_NE(first, nullptr);

    // the previous one-row boundary joins the next chunk's fold and loses
    auto strings2 = BinaryColumn::create();
    strings2->append_string("apple");
    auto second = pipeline::compute_min_max_boundary(TYPE_VARCHAR, TExprOpcode::GT, {strings2, first});
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(ColumnViewer<TYPE_VARCHAR>(second).value(0).to_string(), "apple");

    // the boundary wins this round and must own its bytes on the way out
    auto strings3 = BinaryColumn::create();
    strings3->append_string("cherry");
    auto third = pipeline::compute_min_max_boundary(TYPE_VARCHAR, TExprOpcode::GT, {strings3, second});
    ASSERT_NE(third, nullptr);
    second.reset();
    EXPECT_EQ(ColumnViewer<TYPE_VARCHAR>(third).value(0).to_string(), "apple");
}

namespace {

void expect_filter_eq(const Filter& actual, const std::vector<uint8_t>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]);
    }
}

} // namespace

TEST(NLJoinRuntimeFilterTest, CreateMinMaxFilterCarriesBoundary) {
    ObjectPool pool;
    auto probe = make_int_column({5, 10, 15});
    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;

    auto* greater = RuntimeFilterFactory::create_min_max_filter(&pool, TYPE_INT, true, true, make_int_column({10}),
                                                                TRuntimeFilterBuildJoinMode::BROADCAST);
    ASSERT_NE(greater, nullptr);
    ASSERT_NE(greater->get_membership_filter(), nullptr);
    EXPECT_GT(greater->get_membership_filter()->size(), 0);
    greater->get_min_max_filter()->evaluate(probe.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 1, 1});

    auto* less = RuntimeFilterFactory::create_min_max_filter(&pool, TYPE_INT, false, true, make_int_column({10}),
                                                             TRuntimeFilterBuildJoinMode::BROADCAST);
    ASSERT_NE(less, nullptr);
    less->get_min_max_filter()->evaluate(probe.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 1, 0});
}

TEST(NLJoinRuntimeFilterTest, NullBoundaryCreatesAlwaysTrueMinMaxFilter) {
    ObjectPool pool;
    auto* filter = RuntimeFilterFactory::create_min_max_filter(&pool, TYPE_INT, true, true, nullptr,
                                                               TRuntimeFilterBuildJoinMode::BROADCAST);
    ASSERT_NE(filter, nullptr);
    ASSERT_NE(filter->get_membership_filter(), nullptr);
    EXPECT_GT(filter->get_membership_filter()->size(), 0);

    auto probe = ColumnHelper::cast_to_nullable_column(make_int_column({5, 15}));
    probe->append_nulls(1);
    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    filter->evaluate(probe.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 1, 1});

    const size_t max_size = RuntimeFilterSerde::max_size(RF_VERSION_V3, filter);
    std::vector<uint8_t> buffer(max_size, 0);
    const size_t actual_size = RuntimeFilterSerde::serialize(RF_VERSION_V3, filter, buffer.data());
    ASSERT_GT(actual_size, 0);
    RuntimeFilter* restored = nullptr;
    ASSERT_GT(RuntimeFilterSerde::deserialize(&pool, &restored, buffer.data(), actual_size), 0);
    ASSERT_NE(restored, nullptr);
    restored->evaluate(probe.get(), &ctx);
    expect_filter_eq(ctx.selection, {1, 1, 1});
}

TEST(NLJoinRuntimeFilterTest, MinMaxFilterSerdeRoundTrip) {
    ObjectPool pool;
    auto* filter = RuntimeFilterFactory::create_min_max_filter(&pool, TYPE_INT, true, true, make_int_column({10}),
                                                               TRuntimeFilterBuildJoinMode::BROADCAST);
    ASSERT_NE(filter, nullptr);

    const size_t max_size = RuntimeFilterSerde::max_size(RF_VERSION_V3, filter);
    std::vector<uint8_t> buffer(max_size, 0);
    const size_t actual_size = RuntimeFilterSerde::serialize(RF_VERSION_V3, filter, buffer.data());
    ASSERT_GT(actual_size, 0);
    ASSERT_LE(actual_size, max_size);

    RuntimeFilter* restored = nullptr;
    ASSERT_GT(RuntimeFilterSerde::deserialize(&pool, &restored, buffer.data(), actual_size), 0);
    ASSERT_NE(restored, nullptr);

    auto probe = make_int_column({5, 10, 15});
    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    restored->get_min_max_filter()->evaluate(probe.get(), &ctx);
    expect_filter_eq(ctx.selection, {0, 1, 1});
}

} // namespace starrocks
