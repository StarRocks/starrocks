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

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/agg_runtime_filter_builder.h"
#include "exprs/runtime_filter.h"
#include "testutil/column_test_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class AggMinMaxRuntimeFilterBuilderTest : public ::testing::Test {
public:
    void SetUp() override { _pool = std::make_unique<ObjectPool>(); }

protected:
    std::unique_ptr<ObjectPool> _pool;
};

// Test building min/max runtime filter from non-nullable column
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_non_nullable_column) {
    std::vector<int32_t> values = {5, 10, 3, 8, 15, 1};
    auto column = ColumnTestHelper::build_column<int32_t>(values);

    // Create a mock build descriptor (we don't need it for the actual build)
    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);

    // Min should be 1, Max should be 15
    EXPECT_EQ(minmax_rf->min(), 1);
    EXPECT_EQ(minmax_rf->max(), 15);
}

// Test building min/max runtime filter from nullable column
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_nullable_column) {
    std::vector<int32_t> values = {5, 10, 3, 8, 15, 1};
    std::vector<uint8_t> null_values = {0, 0, 1, 0, 0, 0}; // 3rd value is null
    auto column = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);

    // Min should be 1, Max should be 15 (null value should be ignored)
    EXPECT_EQ(minmax_rf->min(), 1);
    EXPECT_EQ(minmax_rf->max(), 15);
}

// Test building min/max runtime filter from empty column
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_empty_column) {
    auto column = ColumnHelper::create_column(TYPE_INT, false);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    // Should return a valid filter with full range
    EXPECT_TRUE(rf->always_true());
}

// Test building min/max runtime filter from all null column
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_all_null_column) {
    std::vector<int32_t> values = {1, 2, 3};
    std::vector<uint8_t> null_values = {1, 1, 1}; // All null
    auto column = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    // Should return a valid filter with full range (no values to filter)
    EXPECT_TRUE(rf->always_true());
}

// Test building min/max runtime filter with int64 type
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_int64_column) {
    std::vector<int64_t> values = {100L, 50L, 200L, 25L};
    auto column = ColumnTestHelper::build_column<int64_t>(values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_BIGINT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_BIGINT>*>(rf);

    EXPECT_EQ(minmax_rf->min(), 25L);
    EXPECT_EQ(minmax_rf->max(), 200L);
}

// Test building min/max runtime filter with single value
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_single_value) {
    std::vector<int32_t> values = {42};
    auto column = ColumnTestHelper::build_column<int32_t>(values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);

    EXPECT_EQ(minmax_rf->min(), 42);
    EXPECT_EQ(minmax_rf->max(), 42);
}

// Test building min/max runtime filter and evaluating it
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_filter_evaluation) {
    // Build filter with values 10-50
    std::vector<int32_t> agg_values = {10, 20, 30, 40, 50};
    auto agg_column = ColumnTestHelper::build_column<int32_t>(agg_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), agg_column.get());

    ASSERT_NE(rf, nullptr);

    // Create a column to evaluate
    std::vector<int32_t> probe_values = {5, 15, 25, 35, 45, 55};
    auto probe_column = ColumnTestHelper::build_column<int32_t>(probe_values);

    RuntimeFilter::RunningContext ctx;
    rf->evaluate(probe_column.get(), &ctx);

    // Values 5 and 55 should be filtered out, others should pass
    const auto& selection = ctx.selection;
    ASSERT_EQ(selection.size(), 6);
    EXPECT_EQ(selection[0], 0); // 5 - filtered
    EXPECT_EQ(selection[1], 1); // 15 - passed
    EXPECT_EQ(selection[2], 1); // 25 - passed
    EXPECT_EQ(selection[3], 1); // 35 - passed
    EXPECT_EQ(selection[4], 1); // 45 - passed
    EXPECT_EQ(selection[5], 0); // 55 - filtered
}

// Test for no GROUP BY case (single row result) - simulates SELECT MIN(x), MAX(x) FROM t
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_no_group_by_single_row) {
    // Simulate SELECT MIN(v1) FROM t - result is a single value
    std::vector<int32_t> min_values = {100}; // MIN result
    auto min_column = ColumnTestHelper::build_column<int32_t>(min_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), min_column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);

    // For single value, min == max
    EXPECT_EQ(minmax_rf->min(), 100);
    EXPECT_EQ(minmax_rf->max(), 100);

    // Test evaluation - only values equal to 100 should pass
    std::vector<int32_t> probe_values = {50, 100, 150};
    auto probe_column = ColumnTestHelper::build_column<int32_t>(probe_values);

    RuntimeFilter::RunningContext ctx;
    rf->evaluate(probe_column.get(), &ctx);

    const auto& selection = ctx.selection;
    ASSERT_EQ(selection.size(), 3);
    EXPECT_EQ(selection[0], 0); // 50 - filtered
    EXPECT_EQ(selection[1], 1); // 100 - passed
    EXPECT_EQ(selection[2], 0); // 150 - filtered
}

// Test for SELECT MIN(x), MAX(x) FROM t with different min and max values
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_no_group_by_min_max) {
    // When both MIN and MAX are computed, we need to build a filter that covers the range
    // For SELECT MIN(v1), MAX(v1) FROM t with results MIN=10, MAX=100
    std::vector<int32_t> min_max_values = {10, 100}; // MIN and MAX results combined
    auto column = ColumnTestHelper::build_column<int32_t>(min_max_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);

    // Filter range should be [10, 100]
    EXPECT_EQ(minmax_rf->min(), 10);
    EXPECT_EQ(minmax_rf->max(), 100);

    // Test evaluation
    std::vector<int32_t> probe_values = {5, 10, 50, 100, 200};
    auto probe_column = ColumnTestHelper::build_column<int32_t>(probe_values);

    RuntimeFilter::RunningContext ctx;
    rf->evaluate(probe_column.get(), &ctx);

    const auto& selection = ctx.selection;
    ASSERT_EQ(selection.size(), 5);
    EXPECT_EQ(selection[0], 0); // 5 - filtered
    EXPECT_EQ(selection[1], 1); // 10 - passed (boundary)
    EXPECT_EQ(selection[2], 1); // 50 - passed
    EXPECT_EQ(selection[3], 1); // 100 - passed (boundary)
    EXPECT_EQ(selection[4], 0); // 200 - filtered
}

// Test for different data types (column reference scenarios)
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_column_ref_bigint) {
    // Test with BIGINT type (common for column references)
    std::vector<int64_t> values = {1000L, 500L, 2000L, 100L};
    auto column = ColumnTestHelper::build_column<int64_t>(values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_BIGINT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_BIGINT>*>(rf);

    EXPECT_EQ(minmax_rf->min(), 100L);
    EXPECT_EQ(minmax_rf->max(), 2000L);
}

TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_column_ref_smallint) {
    // Test with SMALLINT type
    std::vector<int16_t> values = {100, 50, 200, 10};
    auto column = ColumnTestHelper::build_column<int16_t>(values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_SMALLINT);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_SMALLINT>*>(rf);

    EXPECT_EQ(minmax_rf->min(), 10);
    EXPECT_EQ(minmax_rf->max(), 200);
}

TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_column_ref_date) {
    // Test with DATE type (common use case for MIN/MAX)
    std::vector<int32_t> date_values = {19000, 18900, 19100, 18800}; // Days since epoch
    auto column = ColumnTestHelper::build_column<int32_t>(date_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_DATE);
    RuntimeFilter* rf = builder.build(_pool.get(), column.get());

    ASSERT_NE(rf, nullptr);
    auto* minmax_rf = down_cast<MinMaxRuntimeFilter<TYPE_DATE>*>(rf);

    EXPECT_EQ(minmax_rf->min(), 18800);
    EXPECT_EQ(minmax_rf->max(), 19100);
}

// Test simulating actual column scan filtering
TEST_F(AggMinMaxRuntimeFilterBuilderTest, test_column_ref_filtering) {
    // Simulate: SELECT MIN(v1) FROM t WHERE v2 > 10
    // MIN result is 100
    std::vector<int32_t> min_values = {100};
    auto min_column = ColumnTestHelper::build_column<int32_t>(min_values);

    RuntimeFilterBuildDescriptor* build_desc = nullptr;
    AggMinMaxRuntimeFilterBuilder builder(build_desc, TYPE_INT);
    RuntimeFilter* rf = builder.build(_pool.get(), min_column.get());

    ASSERT_NE(rf, nullptr);

    // Simulate scanning column v1 with values
    std::vector<int32_t> scan_values = {50, 100, 150, 200, 100};
    auto scan_column = ColumnTestHelper::build_column<int32_t>(scan_values);

    RuntimeFilter::RunningContext ctx;
    rf->evaluate(scan_column.get(), &ctx);

    const auto& selection = ctx.selection;
    ASSERT_EQ(selection.size(), 5);
    // Only value 100 passes (exact match with MIN)
    EXPECT_EQ(selection[0], 0); // 50 - filtered
    EXPECT_EQ(selection[1], 1); // 100 - passed
    EXPECT_EQ(selection[2], 0); // 150 - filtered
    EXPECT_EQ(selection[3], 0); // 200 - filtered
    EXPECT_EQ(selection[4], 1); // 100 - passed
}

} // namespace starrocks
