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

#include "column/object_column.h"

#include <gtest/gtest.h>

#include <vector>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/function_context.h"
#include "exprs/percentile_functions.h"
#include "types/percentile_value.h"

namespace starrocks {

TEST(ObjectColumnTest, Percentile_test_swap_column) {
    Columns columns;
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto s = DoubleColumn::create();
    s->append(1);
    s->append(2);
    s->append(3);
    columns.emplace_back(s);

    auto column = PercentileFunctions::percentile_hash(ctx, columns).value();
    ASSERT_TRUE(column->is_object());

    auto percentile = ColumnHelper::cast_to<TYPE_PERCENTILE>(column);
    ASSERT_EQ(1, percentile->get_object(0)->quantile(1));
    ASSERT_EQ(2, percentile->get_object(1)->quantile(1));
    ASSERT_EQ(3, percentile->get_object(2)->quantile(1));

    auto s1 = DoubleColumn::create();
    s1->append(4);
    columns.clear();
    columns.emplace_back(s1);
    auto column1 = PercentileFunctions::percentile_hash(ctx, columns).value();
    ASSERT_TRUE(column1->is_object());

    std::vector<uint32_t> idx = {1};
    auto mutable_column = Column::mutate(std::move(column));
    mutable_column->update_rows(*column1.get(), idx.data());

    percentile = ColumnHelper::cast_to<TYPE_PERCENTILE>(mutable_column);
    ASSERT_EQ(1, percentile->get_object(0)->quantile(1));
    ASSERT_EQ(4, percentile->get_object(1)->quantile(1));
    ASSERT_EQ(3, percentile->get_object(2)->quantile(1));

    delete ctx;
}

} // namespace starrocks
