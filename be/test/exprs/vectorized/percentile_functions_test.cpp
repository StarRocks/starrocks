// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/percentile_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "util/percentile_value.h"

namespace starrocks::vectorized {
class PercentileFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        ctx_ptr.reset(FunctionContext::create_test_context());
        ctx = ctx_ptr.get();
    }

private:
    std::unique_ptr<FunctionContext> ctx_ptr;
    FunctionContext* ctx;
};

TEST_F(PercentileFunctionsTest, percentileEmptyTest) {
    Columns c;
    auto column = PercentileFunctions::percentile_empty(ctx, c).value();

    ASSERT_TRUE(column->is_constant());

    auto* percentile = ColumnHelper::get_const_value<TYPE_PERCENTILE>(column);

    ASSERT_EQ(61, percentile->serialize_size());
}

TEST_F(PercentileFunctionsTest, percentileHashTest) {
    Columns columns;
    auto s = DoubleColumn::create();
    s->append(1);
    s->append(2);
    s->append(3);
    columns.push_back(s);

    auto column = PercentileFunctions::percentile_hash(ctx, columns).value();
    ASSERT_TRUE(column->is_object());

    auto percentile = ColumnHelper::cast_to<TYPE_PERCENTILE>(column);
    ASSERT_EQ(1, percentile->get_object(0)->quantile(1));
    ASSERT_EQ(2, percentile->get_object(1)->quantile(1));
    ASSERT_EQ(3, percentile->get_object(2)->quantile(1));
}

TEST_F(PercentileFunctionsTest, percentileNullTest) {
    Columns columns;
    auto c1 = PercentileColumn::create();
    auto c1_null = NullableColumn::create(c1, NullColumn::create());
    c1_null->append_nulls(1);
    columns.push_back(c1_null);

    auto c2 = DoubleColumn::create();
    auto c2_null = NullableColumn::create(c2, NullColumn::create());
    c2_null->append_nulls(1);
    columns.push_back(c2_null);

    ColumnPtr column = PercentileFunctions::percentile_approx_raw(ctx, columns).value();
    ASSERT_TRUE(column->is_nullable());
    auto result = ColumnHelper::as_column<NullableColumn>(column);
    ASSERT_TRUE(result->is_null(0));
}

} // namespace starrocks::vectorized
