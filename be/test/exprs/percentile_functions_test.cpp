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

#include "exprs/percentile_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "util/percentile_value.h"

namespace starrocks {
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
    columns.push_back(std::move(s));

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
    auto c1_null = NullableColumn::create(std::move(c1), NullColumn::create());
    c1_null->append_nulls(1);
    columns.emplace_back(std::move(c1_null));

    auto c2 = DoubleColumn::create();
    auto c2_null = NullableColumn::create(std::move(c2), NullColumn::create());
    c2_null->append_nulls(1);
    columns.emplace_back(std::move(c2_null));

    ColumnPtr column = PercentileFunctions::percentile_approx_raw(ctx, columns).value();
    ASSERT_TRUE(column->is_nullable());
    auto result = ColumnHelper::as_column<NullableColumn>(column);
    ASSERT_TRUE(result->is_null(0));
}

} // namespace starrocks
