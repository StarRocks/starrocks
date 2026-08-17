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
#include <optional>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/sorting/sort_helper.h"
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

// Regression test for https://github.com/StarRocks/starrocks/issues/77374
//
// A full sort compares the rows twice: once when a buffered run is sorted, and once when the sorted runs are
// merged. The merge phase compares by `compare_chunk_row`, so a run sorted by `sort_and_tie_columns` must be
// ordered by the very same comparator, no matter where the NULLs nested in an ARRAY/MAP/STRUCT end up.
class NestedNullSortTest : public testing::TestWithParam<std::pair<SortDescs, std::vector<uint32_t>>> {
protected:
    // The `arr` values of issue #77374
    static ColumnPtr array_column() {
        const std::vector<std::vector<std::optional<int32_t>>> arrays = {
                {std::nullopt}, {5}, {4}, {3}, {std::nullopt, 2}, {7}, {std::nullopt, 9}, {8}};

        auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto offsets = UInt32Column::create();
        offsets->append(0);
        uint32_t offset = 0;
        for (const auto& array : arrays) {
            for (const auto& element : array) {
                if (element.has_value()) {
                    elements->append_datum(Datum(element.value()));
                } else {
                    elements->append_nulls(1);
                }
                offset++;
            }
            offsets->append(offset);
        }
        return ArrayColumn::create(std::move(elements), std::move(offsets));
    }
};

TEST_P(NestedNullSortTest, sorted_run_agrees_with_the_merge_comparator) {
    const auto& [sort_desc, expected] = GetParam();
    std::atomic<bool> cancel{false};
    Columns columns{array_column()};
    auto perm = create_small_permutation(columns[0]->size());

    ASSERT_OK(sort_and_tie_columns(cancel, columns, sort_desc, perm));

    std::vector<uint32_t> sorted;
    for (const auto& item : perm) {
        sorted.push_back(item.index_in_chunk);
    }
    EXPECT_EQ(expected, sorted);

    // The order of the run must be non-descending for the comparator the merge phase uses
    for (size_t i = 1; i < perm.size(); i++) {
        EXPECT_LE(compare_chunk_row(sort_desc, columns, columns, perm[i - 1].index_in_chunk, perm[i].index_in_chunk), 0)
                << "row " << perm[i - 1].index_in_chunk << " must not succeed row " << perm[i].index_in_chunk;
    }
}

INSTANTIATE_TEST_SUITE_P(ArrayWithNestedNull, NestedNullSortTest,
                         testing::Values(
                                 // asc, null first
                                 std::make_pair(SortDescs(std::vector<int>{1}, std::vector<int>{-1}),
                                                std::vector<uint32_t>{0, 4, 6, 3, 2, 1, 5, 7}),
                                 // desc, null last: the reverse of `asc, null first`
                                 std::make_pair(SortDescs(std::vector<int>{-1}, std::vector<int>{-1}),
                                                std::vector<uint32_t>{7, 5, 1, 2, 3, 6, 4, 0}),
                                 // asc, null last
                                 std::make_pair(SortDescs(std::vector<int>{1}, std::vector<int>{1}),
                                                std::vector<uint32_t>{3, 2, 1, 5, 7, 0, 4, 6}),
                                 // desc, null first: the reverse of `asc, null last`
                                 std::make_pair(SortDescs(std::vector<int>{-1}, std::vector<int>{1}),
                                                std::vector<uint32_t>{6, 4, 0, 7, 5, 1, 2, 3})));

} // namespace
} // namespace starrocks
