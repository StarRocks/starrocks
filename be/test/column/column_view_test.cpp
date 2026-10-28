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

#include "base/testutil/parallel_test.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_view/column_view_helper.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {
static ColumnPtr create_int32_array_column(const std::vector<std::vector<int32_t>>& values, bool is_nullable) {
    auto offsets = UInt32Column::create();
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    offsets->append(0);
    for (const auto& value : values) {
        for (auto v : value) {
            elements->append_datum(v);
        }
        offsets->append(elements->size());
    }

    auto array_column = ArrayColumn::create(std::move(elements), std::move(offsets));
    if (is_nullable) {
        auto null_column = NullColumn::create();
        null_column->resize(values.size());
        return NullableColumn::create(std::move(array_column), std::move(null_column));
    } else {
        return array_column;
    }
}
static void test_array_column_view_helper(bool nullable, bool append_default, long concat_row_limit,
                                          long concat_bytes_limit, std::vector<uint32_t> selection,
                                          const std::string& expect_result) {
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
    array_column_view->append_selective_to(*final_array_column, selection.data(), 0, selection.size());
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
    type_desc.field_names.emplace_back("field1");
    type_desc.field_names.emplace_back("field2");
    TypeDescriptor field1_type_desc = TypeDescriptor::create_varchar_type(20);
    TypeDescriptor field2_type_desc = TypeDescriptor::create_varchar_type(20);
    type_desc.children.emplace_back(field1_type_desc);
    type_desc.children.emplace_back(field2_type_desc);
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
    type_desc.children.emplace_back(field1_type_desc);
    type_desc.children.emplace_back(field2_type_desc);
    for (auto nullable : {true, false}) {
        auto opt_map_column_view = ColumnViewHelper::create_column_view(type_desc, nullable, 0, 0);
        DCHECK(opt_map_column_view.has_value());
        auto map_column_view = std::move(opt_map_column_view.value());
        DCHECK(map_column_view->is_map_view());
    }
}

// A selection may be longer than the view, because `from` and `count` window the INDEXES array
// rather than the view's rows, and the same row may be selected any number of times.
//
// That is the ordinary shape in a hash join: _copy_build_nullable_column selects
// _probe_state->count positions -- one per output row of the probe chunk -- out of the build
// column, so any build side smaller than a probe chunk selects more positions than it has rows.
// append_selective_to used to assert `from + count <= _num_rows`, comparing the number of
// positions against the number of rows, and aborted debug builds on exactly this input. Both
// concat settings are covered because the assertion sat before the concat branch.
static void test_select_more_positions_than_rows(long concat_rows_limit, long concat_bytes_limit) {
    TypeDescriptor type_desc = TypeDescriptor::create_varchar_type(20);
    auto opt_view = ColumnViewHelper::create_column_view(type_desc, true, concat_rows_limit, concat_bytes_limit);
    ASSERT_TRUE(opt_view.has_value());
    auto column_view = std::move(opt_view.value());

    // the join seeds the build column with one default row, then appends the build side
    column_view->append_default();
    auto src = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    src->append_datum(Slice("aa"));
    src->append_datum(Slice("bb"));
    column_view->append(*src);
    ASSERT_EQ(3, column_view->size());

    // 100 selected positions out of 3 rows, every ordinal in range
    std::vector<uint32_t> selection(100);
    for (size_t i = 0; i < selection.size(); ++i) {
        selection[i] = static_cast<uint32_t>(i % 3);
    }
    auto dest = column_view->clone_empty();
    column_view->append_selective_to(*dest, selection.data(), 0, selection.size());

    // Compared against the same positions selected one at a time, so the expectation does not
    // depend on how a value prints. clone_empty() yields the underlying column, which implements
    // debug_item; the view itself does not.
    auto expected = column_view->clone_empty();
    for (unsigned int idx : selection) {
        column_view->append_selective_to(*expected, &idx, 0, 1);
    }
    ASSERT_EQ(selection.size(), dest->size());
    ASSERT_EQ(expected->size(), dest->size());
    for (size_t i = 0; i < selection.size(); ++i) {
        ASSERT_EQ(expected->debug_item(i), dest->debug_item(i));
    }

    // a window that does not start at 0 selects the same way
    auto tail = column_view->clone_empty();
    column_view->append_selective_to(*tail, selection.data(), 10, 20);
    ASSERT_EQ(20, tail->size());
    for (size_t i = 0; i < 20; ++i) {
        ASSERT_EQ(expected->debug_item(10 + i), tail->debug_item(i));
    }
}

PARALLEL_TEST(ColumnViewTest, test_select_more_positions_than_rows_without_concat) {
    test_select_more_positions_than_rows(0, 0);
}

PARALLEL_TEST(ColumnViewTest, test_select_more_positions_than_rows_with_concat) {
    test_select_more_positions_than_rows(1L << 62, 1L << 62);
}
} // namespace starrocks
