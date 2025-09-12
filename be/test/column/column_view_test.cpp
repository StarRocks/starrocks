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
#include "column/column_view/column_view.h"

#include <gtest/gtest.h>

#include <numeric>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_view/column_view_helper.h"
#include "runtime/types.h"
#include "testutil/parallel_test.h"
#include "types/logical_type.h"

namespace starrocks {
static ColumnPtr create_int32_array_column(const std::vector<std::vector<int32_t>>& values, bool is_nullable) {
    UInt32Column::Ptr offsets = UInt32Column::create();
    NullableColumn::Ptr elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    offsets->append(0);
    for (const auto& value : values) {
        for (auto v : value) {
            elements->append_datum(v);
        }
        offsets->append(elements->size());
    }

    auto array_column = ArrayColumn::create(elements, offsets);
    if (is_nullable) {
        auto null_column = NullColumn::create();
        null_column->resize(values.size());
        return NullableColumn::create(array_column, null_column);
    } else {
        return array_column;
    }
}
static void test_array_column_view_helper(bool nullable, bool append_default, long concat_row_limit,
                                          long concat_bytes_limit, std::vector<uint32_t> selection,
                                          std::string expect_result) {
    auto child_type_desc = TypeDescriptor(LogicalType::TYPE_INT);
    auto type_desc = TypeDescriptor::create_array_type(child_type_desc);
    auto opt_array_column_view =
            ColumnViewHelper::create_column_view(type_desc, nullable, concat_row_limit, concat_bytes_limit);
    DCHECK(opt_array_column_view.has_value());
    auto array_column_view = down_cast<ColumnView*>(opt_array_column_view.value().get());
    auto num_rows = 0;
    if (append_default) {
        array_column_view->append_default();
        num_rows += 1;
    }
    DCHECK_EQ(array_column_view->size(), num_rows);
    const auto array_col0 = create_int32_array_column({{}, {1}, {1, 2}, {1, 2, 3}, {1, 2, 3, 4}}, nullable);
    num_rows += 5;
    array_column_view->append(*array_col0);
    DCHECK_EQ(array_column_view->size(), num_rows);

    const auto array_col1 = create_int32_array_column({{5}, {6, 7}, {8}, {9, 10}}, nullable);
    array_column_view->append(*array_col1, 2, 2);
    num_rows += 2;
    DCHECK_EQ(array_column_view->size(), num_rows);

    const auto array_col2 = create_int32_array_column({{11}, {12, 13}, {14}, {15, 16}}, nullable);
    auto indexes = std::vector<uint32_t>({1, 3});
    array_column_view->append_selective(*array_col2, indexes.data(), 0, 2);
    num_rows += 2;
    DCHECK_EQ(array_column_view->size(), num_rows);

    const auto final_array_column = array_column_view->clone_empty();
    array_column_view->append_to(*final_array_column, selection.data(), 0, selection.size());
    ASSERT_EQ(final_array_column->debug_string(), expect_result);
}

PARALLEL_TEST(ColumnViewTest, test_not_nullable_with_append_default) {
    std::vector<uint32_t> selection(10);
    std::iota(selection.begin(), selection.end(), 0);
    test_array_column_view_helper(false, true, 0, 0, selection,
                                  "[[], [], [1], [1,2], [1,2,3], [1,2,3,4], [8], [9,10], [12,13], [15,16]]");
    test_array_column_view_helper(false, true, 1L << 63, 1L << 63, selection,
                                  "[[], [], [1], [1,2], [1,2,3], [1,2,3,4], [8], [9,10], [12,13], [15,16]]");
}

PARALLEL_TEST(ColumnViewTest, test_not_nullable_without_append_default) {
    std::vector<uint32_t> selection(5);
    std::iota(selection.begin(), selection.end(), 4);
    test_array_column_view_helper(false, false, 0, 0, selection, "[[1,2,3,4], [8], [9,10], [12,13], [15,16]]");
    test_array_column_view_helper(false, false, 1L << 63, 1L << 63, selection,
                                  "[[1,2,3,4], [8], [9,10], [12,13], [15,16]]");
}

PARALLEL_TEST(ColumnViewTest, test_nullable_with_append_default) {
    std::vector<uint32_t> selection(10);
    std::iota(selection.begin(), selection.end(), 0);
    test_array_column_view_helper(false, true, 0, 0, selection,
                                  "[[], [], [1], [1,2], [1,2,3], [1,2,3,4], [8], [9,10], [12,13], [15,16]]");
    test_array_column_view_helper(false, true, 1L << 63, 1L << 63, selection,
                                  "[[], [], [1], [1,2], [1,2,3], [1,2,3,4], [8], [9,10], [12,13], [15,16]]");
}

PARALLEL_TEST(ColumnViewTest, test_nullable_without_append_default) {
    std::vector<uint32_t> selection(5);
    std::iota(selection.begin(), selection.end(), 4);
    test_array_column_view_helper(false, false, 0, 0, selection, "[[1,2,3,4], [8], [9,10], [12,13], [15,16]]");
    test_array_column_view_helper(false, false, 1L << 63, 1L << 63, selection,
                                  "[[1,2,3,4], [8], [9,10], [12,13], [15,16]]");
}

PARALLEL_TEST(ColumnViewTest, test_create_struct_column_view) {
    TypeDescriptor type_desc = TypeDescriptor(LogicalType::TYPE_STRUCT);
    type_desc.field_names.push_back("field1");
    type_desc.field_names.push_back("field2");
    TypeDescriptor field1_type_desc = TypeDescriptor::create_varchar_type(20);
    TypeDescriptor field2_type_desc = TypeDescriptor::create_varchar_type(20);
    type_desc.children.push_back(field1_type_desc);
    type_desc.children.push_back(field2_type_desc);
    for (auto nullable : {true, false}) {
        auto opt_struct_column_view = ColumnViewHelper::create_column_view(type_desc, nullable, 0, 0);
        DCHECK(opt_struct_column_view.has_value());
        auto struct_column_view = std::move(opt_struct_column_view.value());
        DCHECK(struct_column_view->is_struct_view());
    }
}
PARALLEL_TEST(ColumnViewTest, test_create_json_column_view) {
    TypeDescriptor type_desc = TypeDescriptor(LogicalType::TYPE_JSON);
    for (auto nullable : {true, false}) {
        auto opt_json_column_view = ColumnViewHelper::create_column_view(type_desc, nullable, 0, 0);
        DCHECK(opt_json_column_view.has_value());
        auto json_column_view = std::move(opt_json_column_view.value());
        DCHECK(json_column_view->is_json_view());
    }
}
PARALLEL_TEST(ColumnViewTest, test_create_variant_column_view) {
    TypeDescriptor type_desc = TypeDescriptor(LogicalType::TYPE_VARIANT);
    for (auto nullable : {true, false}) {
        auto opt_variant_column_view = ColumnViewHelper::create_column_view(type_desc, nullable, 0, 0);
        DCHECK(opt_variant_column_view.has_value());
        auto variant_column_view = std::move(opt_variant_column_view.value());
        DCHECK(variant_column_view->is_variant_view());
        EXPECT_TRUE(variant_column_view->is_variant_view());
    }
}
PARALLEL_TEST(ColumnViewTest, test_create_binary_column_view) {
    for (auto ltype : {LogicalType::TYPE_VARBINARY, LogicalType::TYPE_VARCHAR, LogicalType::TYPE_CHAR}) {
        for (auto nullable : {true, false}) {
            TypeDescriptor type_desc = TypeDescriptor(ltype);
            auto opt_binary_column_view = ColumnViewHelper::create_column_view(type_desc, nullable, 0, 0);
            DCHECK(opt_binary_column_view.has_value());
            auto binary_column_view = std::move(opt_binary_column_view.value());
            DCHECK(binary_column_view->is_binary_view());
        }
    }
}
PARALLEL_TEST(ColumnViewTest, test_create_map_column_view) {
    TypeDescriptor type_desc = TypeDescriptor(LogicalType::TYPE_MAP);
    TypeDescriptor field1_type_desc = TypeDescriptor::create_varchar_type(20);
    TypeDescriptor field2_type_desc = TypeDescriptor::create_varchar_type(20);
    type_desc.children.push_back(field1_type_desc);
    type_desc.children.push_back(field2_type_desc);
    for (auto nullable : {true, false}) {
        auto opt_map_column_view = ColumnViewHelper::create_column_view(type_desc, nullable, 0, 0);
        DCHECK(opt_map_column_view.has_value());
        auto map_column_view = std::move(opt_map_column_view.value());
        DCHECK(map_column_view->is_map_view());
    }
}
} // namespace starrocks