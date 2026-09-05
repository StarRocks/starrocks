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

#include <vector>

#include "base/testutil/parallel_test.h"
#include "column/column_view/column_view.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks {

PARALLEL_TEST(NullableColumnTest, test_append_selective_from_nullable_view) {
    auto default_column = NullableColumn::create(Int32Column::create(), NullColumn::create());
    default_column->append_default_not_null_value();
    auto view_column = ColumnView::create(std::move(default_column), 0, 0);

    auto src_col1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    src_col1->append_datum((int32_t)10);
    src_col1->append_datum({});
    src_col1->append_datum((int32_t)30);
    view_column->append(*src_col1);

    auto src_col2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    src_col2->append_datum((int32_t)40);
    src_col2->append_datum({});
    view_column->append(*src_col2);

    auto dst_col = NullableColumn::create(Int32Column::create(), NullColumn::create());
    std::vector<uint32_t> indexes = {4, 1, 0, 3};
    dst_col->append_selective(*view_column, indexes.data(), 0, indexes.size());

    ASSERT_EQ(indexes.size(), dst_col->size());
    ASSERT_TRUE(dst_col->has_null());
    ASSERT_EQ(2, dst_col->null_count());
    ASSERT_TRUE(dst_col->is_null(0));
    ASSERT_TRUE(dst_col->is_null(1));
    ASSERT_FALSE(dst_col->is_null(2));
    ASSERT_FALSE(dst_col->is_null(3));
    ASSERT_EQ(10, dst_col->get(2).get_int32());
    ASSERT_EQ(40, dst_col->get(3).get_int32());
}

} // namespace starrocks
