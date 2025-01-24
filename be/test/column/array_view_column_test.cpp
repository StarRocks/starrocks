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

#include "column/array_view_column.h"

#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "testutil/parallel_test.h"

namespace starrocks {

ColumnPtr create_int32_array_column(const std::vector<std::vector<int32_t>>& values) {
    auto offsets = UInt32Column::create();
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    offsets->append(0);
    for (const auto& value : values) {
        for (auto v : value) {
            elements->append_datum(v);
        }
        offsets->append(elements->size());
    }
    return ArrayColumn::create(elements, offsets);
}

PARALLEL_TEST(ArrayViewColumnTest, test_from_array) {
    auto array_column = create_int32_array_column({{1}, {2, 3}});
    auto array_view_column = ArrayViewColumn::from_array_column(array_column);
    ASSERT_TRUE(array_view_column->is_array_view());
    ASSERT_FALSE(array_view_column->is_nullable());
    ASSERT_EQ(array_view_column->size(), 2);
    ASSERT_EQ(array_view_column->debug_string(), "[[1],[2,3]]");
}

PARALLEL_TEST(ArrayViewColumnTest, test_to_array) {
    auto array_column = create_int32_array_column({{1}, {2, 3}});
    auto array_view_column = ArrayViewColumn::from_array_column(array_column);
    auto result = ArrayViewColumn::to_array_column(array_view_column);
    ASSERT_TRUE(result->is_array());
    ASSERT_EQ(result->debug_string(), "[[1], [2,3]]");
}

PARALLEL_TEST(ArrayViewColumnTest, test_get_elements) {
    auto array_column = create_int32_array_column({{1}, {2, 3}});
    auto array_view_column =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column));

    ASSERT_EQ(array_view_column->get_element_size(0), 1);
    ASSERT_EQ(array_view_column->debug_item(0), "[1]");
    ASSERT_EQ(array_view_column->get_element_size(1), 2);
    ASSERT_EQ(array_view_column->debug_item(1), "[2,3]");

    ASSERT_EQ(array_view_column->get_element_null_count(0), 0);
    ASSERT_EQ(array_view_column->get_element_null_count(1), 0);
}

PARALLEL_TEST(ArrayViewColumnTest, test_append) {
    auto array_column_1 = create_int32_array_column({{}, {1}, {2, 3}});
    auto array_view_column_1 =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column_1));
    auto array_view_column_3 =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column_1));

    auto array_column_2 = create_int32_array_column({{}, {1}, {2, 3}});
    auto array_view_column_2 =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column_2));

    std::vector<uint32_t> indexes{2, 1};
    // not support to append data from another array
    ASSERT_THROW(array_view_column_1->append(*array_view_column_2, 0, 1), std::runtime_error);
    ASSERT_THROW(array_view_column_1->append_selective(*array_view_column_2, indexes.data(), 0, 1), std::runtime_error);
    ASSERT_THROW(array_view_column_1->append_value_multiple_times(*array_view_column_2, 0, 5), std::runtime_error);

    array_view_column_1->append(*array_view_column_3, 0, 1);
    ASSERT_EQ(array_view_column_1->debug_string(), "[[],[1],[2,3],[]]");

    array_view_column_1->append_selective(*array_view_column_3, indexes.data(), 0, 2);
    ASSERT_EQ(array_view_column_1->debug_string(), "[[],[1],[2,3],[],[2,3],[1]]");

    array_view_column_1->append_value_multiple_times(*array_view_column_3, 0, 2);
    ASSERT_EQ(array_view_column_1->debug_string(), "[[],[1],[2,3],[],[2,3],[1],[],[]]");
}

PARALLEL_TEST(ArrayViewColumnTest, test_compare) {
    auto array_column_1 = create_int32_array_column({{}, {1}, {2, 3}});
    auto array_view_column_1 =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column_1));

    auto array_column_2 = create_int32_array_column({{1}, {2, 3}, {4}});
    auto array_view_column_2 =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column_2));

    std::vector<int> expected_compare_results = {-1, -1, -1, 0, -1, -1, 1, 0, -1};
    std::vector<int> expected_equal_results = {0, 0, 0, 1, 0, 0, 0, 1, 0};
    size_t left_rows = array_column_1->size();
    size_t right_rows = array_column_2->size();
    for (size_t i = 0; i < left_rows; i++) {
        for (size_t j = 0; j < right_rows; j++) {
            ASSERT_EQ(array_view_column_1->compare_at(i, j, *array_view_column_2, 0),
                      expected_compare_results[i * left_rows + j]);
            ASSERT_EQ(array_view_column_1->equals(i, *array_view_column_2, j, true),
                      expected_equal_results[i * left_rows + j]);
        }
    }
    std::vector<int8_t> actual_results;
    array_view_column_1->compare_column(*array_view_column_2, &actual_results);
    ASSERT_EQ(actual_results.size(), 3);
    ASSERT_EQ(actual_results[0], -1);
    ASSERT_EQ(actual_results[1], -1);
    ASSERT_EQ(actual_results[2], -1);
}

PARALLEL_TEST(ArrayViewColumnTest, test_filter_range) {
    auto array_column = create_int32_array_column({});
    const int total_rows = 127;
    for (int32_t i = 0; i < total_rows; i++) {
        array_column->append_datum(DatumArray{i, i * 2});
    }
    auto array_view_column =
            std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column));
    Filter filter(total_rows, 1);
    // filter nothing
    array_view_column->filter_range(filter, 0, total_rows);
    ASSERT_EQ(array_view_column->size(), total_rows);
    for (int i = 0; i < total_rows; i++) {
        auto array = array_view_column->get(i).get_array();
        ASSERT_EQ(2, array.size());
        ASSERT_EQ(i, array[0].get_int32());
        ASSERT_EQ(i * 2, array[1].get_int32());
    }

    // filter last 20 rows
    for (int i = total_rows - 20; i < total_rows; i++) {
        filter[i] = 0;
    }
    array_view_column->filter_range(filter, total_rows - 20, total_rows);
    ASSERT_EQ(array_view_column->size(), total_rows - 20);
    // element column won't be changed
    ASSERT_EQ(array_view_column->elements().size(), total_rows * 2);
    ASSERT_EQ(array_view_column->offsets_column()->size(), total_rows - 20);
    ASSERT_EQ(array_view_column->lengths_column()->size(), total_rows - 20);
    // first `total_rows-20` rows won't be changed
    for (int i = 0; i < total_rows - 20; i++) {
        auto array = array_view_column->get(i).get_array();
        ASSERT_EQ(2, array.size());
        ASSERT_EQ(i, array[0].get_int32());
        ASSERT_EQ(i * 2, array[1].get_int32());
    }
    // filter all
    filter.clear();
    filter.resize(total_rows - 20, 0);
    array_view_column->filter_range(filter, 0, total_rows - 20);
    ASSERT_EQ(array_view_column->size(), 0);
    // element column won't be changed
    ASSERT_EQ(array_view_column->elements().size(), total_rows * 2);
    ASSERT_EQ(array_view_column->offsets_column()->size(), 0);
    ASSERT_EQ(array_view_column->lengths_column()->size(), 0);
}

PARALLEL_TEST(ArrayViewColumnTest, test_other_manipulations) {
    auto array_column = create_int32_array_column({{1, 2}, {3}, {4, 5}});
    {
        // replicate
        auto array_view_column =
                std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column));
        Buffer<uint32_t> offsets{0, 2, 3, 6};
        auto column = array_view_column->replicate(offsets).value();
        ASSERT_TRUE(column->is_array_view());
        auto result = std::dynamic_pointer_cast<ArrayViewColumn>(column);
        ASSERT_EQ(result->size(), 6);
        // element column won't be changed
        ASSERT_EQ(result->elements().size(), 5);

        std::vector<std::vector<int32_t>> expected_results = {{1, 2}, {1, 2}, {3}, {4, 5}, {4, 5}, {4, 5}};
        for (size_t i = 0; i < expected_results.size(); i++) {
            auto array = result->get(i).get_array();
            ASSERT_EQ(array.size(), expected_results[i].size());
            for (size_t j = 0; j < array.size(); j++) {
                ASSERT_EQ(array[j].get_int32(), expected_results[i][j]);
            }
        }
    }
    {
        auto array_view_column =
                std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column));
        array_view_column->remove_first_n_values(1);
        ASSERT_EQ(array_view_column->size(), 2);
        // elements column won't be changed
        ASSERT_EQ(array_view_column->elements().size(), 5);
        ASSERT_EQ(array_view_column->debug_item(0), "[3]");
        ASSERT_EQ(array_view_column->debug_item(1), "[4,5]");

        array_view_column->remove_first_n_values(1);
        ASSERT_EQ(array_view_column->size(), 1);
        // elements column won't be changed
        ASSERT_EQ(array_view_column->elements().size(), 5);
        ASSERT_EQ(array_view_column->debug_item(0), "[4,5]");

        array_view_column->remove_first_n_values(1);
        ASSERT_EQ(array_view_column->size(), 0);
        // elements column won't be changed
        ASSERT_EQ(array_view_column->elements().size(), 5);
    }
    {
        // assign
        auto array_view_column =
                std::dynamic_pointer_cast<ArrayViewColumn>(ArrayViewColumn::from_array_column(array_column));

        array_view_column->assign(5, 0);
        ASSERT_EQ(array_view_column->size(), 5);
        ASSERT_EQ(array_view_column->elements().size(), 5);
        for (size_t i = 0; i < 5; i++) {
            ASSERT_EQ(array_view_column->debug_item(i), "[1,2]");
        }
    }
}
} // namespace starrocks