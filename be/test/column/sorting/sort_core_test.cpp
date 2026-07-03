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

#include <atomic>
#include <vector>

#include "base/testutil/assert.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/sorting/sort_permute.h"
#include "column/sorting/sorting.h"

namespace starrocks {
namespace {

ColumnPtr int_column(const std::vector<int32_t>& values) {
    auto column = Int32Column::create();
    for (auto value : values) {
        column->append(value);
    }
    return column;
}

TEST(ColumnSortCoreTest, sort_and_tie_columns_orders_single_column) {
    std::atomic<bool> cancel{false};
    Columns columns{int_column({3, 1, 2})};
    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    auto perm = create_small_permutation(3);

    ASSERT_OK(sort_and_tie_columns(cancel, columns, sort_desc, perm));

    ASSERT_EQ(3, perm.size());
    EXPECT_EQ(1, perm[0].index_in_chunk);
    EXPECT_EQ(2, perm[1].index_in_chunk);
    EXPECT_EQ(0, perm[2].index_in_chunk);
}

TEST(ColumnSortCoreTest, sort_and_tie_columns_orders_nullable_column) {
    std::atomic<bool> cancel{false};
    auto nullable = NullableColumn::create(Int32Column::create(), NullColumn::create());
    nullable->append_datum(Datum(3));
    nullable->append_nulls(1);
    nullable->append_datum(Datum(1));
    Columns columns{std::move(nullable)};
    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    auto perm = create_small_permutation(3);

    ASSERT_OK(sort_and_tie_columns(cancel, columns, sort_desc, perm));

    ASSERT_EQ(3, perm.size());
    EXPECT_EQ(1, perm[0].index_in_chunk);
    EXPECT_EQ(2, perm[1].index_in_chunk);
    EXPECT_EQ(0, perm[2].index_in_chunk);
}

TEST(ColumnSortCoreTest, stable_sort_and_tie_columns_preserves_equal_order) {
    std::atomic<bool> cancel{false};
    Columns columns{int_column({2, 1, 1, 2})};
    SortDescs sort_desc(std::vector<int>{1}, std::vector<int>{-1});
    auto perm = create_small_permutation(4);

    ASSERT_OK(stable_sort_and_tie_columns(cancel, columns, sort_desc, &perm));

    ASSERT_EQ(4, perm.size());
    EXPECT_EQ(1, perm[0].index_in_chunk);
    EXPECT_EQ(2, perm[1].index_in_chunk);
    EXPECT_EQ(0, perm[2].index_in_chunk);
    EXPECT_EQ(3, perm[3].index_in_chunk);
}

TEST(ColumnSortCoreTest, materialize_column_by_permutation_single_reorders_values) {
    auto input = Int32Column::create();
    input->append(10);
    input->append(20);
    input->append(30);
    auto output = Int32Column::create();
    SmallPermutation perm{{2}, {0}, {1}};

    materialize_column_by_permutation_single(output.get(), input.get(), perm);

    ASSERT_EQ(3, output->size());
    EXPECT_EQ(30, output->get(0).get_int32());
    EXPECT_EQ(10, output->get(1).get_int32());
    EXPECT_EQ(20, output->get(2).get_int32());
}

} // namespace
} // namespace starrocks
