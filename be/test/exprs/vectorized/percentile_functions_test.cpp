// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/percentile_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace starrocks {
namespace vectorized {
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
    auto column = PercentileFunctions::percentile_empty(ctx, c);

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

    auto column = PercentileFunctions::percentile_hash(ctx, columns);
    ASSERT_TRUE(column->is_object());

    auto percentile = ColumnHelper::cast_to<TYPE_PERCENTILE>(column);
    ASSERT_EQ(1, percentile->get_object(0)->quantile(1));
    ASSERT_EQ(2, percentile->get_object(1)->quantile(1));
    ASSERT_EQ(3, percentile->get_object(2)->quantile(1));
}

} // namespace vectorized
} // namespace starrocks
