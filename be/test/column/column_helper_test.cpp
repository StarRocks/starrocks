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

#include "column/column_helper.h"

#include "column/column_builder.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "gtest/gtest.h"

namespace starrocks {

class ColumnHelperTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    MutableColumnPtr create_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(false);
    }

    MutableColumnPtr create_nullable_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"), true);
        return builder.build(false);
    }

    MutableColumnPtr create_const_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(true);
    }

    MutableColumnPtr create_only_null_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append_null();
        return builder.build(true);
    }
};

TEST_F(ColumnHelperTest, cast_to_nullable_column) {
    auto col = ColumnHelper::cast_to_nullable_column(create_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());

    col = ColumnHelper::cast_to_nullable_column(create_nullable_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());
}

TEST_F(ColumnHelperTest, align_return_type) {
    auto nullable_column = create_nullable_column();
    auto not_null_column = create_column();
    auto col_size = not_null_column->size();
    nullable_column->append(*ColumnHelper::align_return_type(
            std::move(not_null_column), TypeDescriptor::from_logical_type(TYPE_VARCHAR), col_size, true));
    ASSERT_TRUE(nullable_column->is_nullable());
    ASSERT_FALSE(nullable_column->is_constant());
}

TEST_F(ColumnHelperTest, get_data_column_by_type) {
    auto column = create_nullable_column();
    const auto* data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());

    column = create_const_column();
    data_column = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(column.get());
    ASSERT_TRUE(data_column->is_binary());
}

TEST_F(ColumnHelperTest, get_null_column) {
    auto column = create_nullable_column();
    const auto* null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");

    column = create_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_TRUE(null_column == nullptr);

    column = create_const_column();
    null_column = ColumnHelper::get_null_column(column.get());
    ASSERT_EQ(null_column->get_name(), "integral-1");
}

TEST_F(ColumnHelperTest, normalize_column_type_no_change) {
    // Same type: no conversion needed
    auto col = Int32Column::create();
    col->append(1);
    col->append(2);
    ColumnPtr ptr = col;

    TypeDescriptor type_int(TYPE_INT);
    auto result = ColumnHelper::normalize_column_type(ptr, type_int);
    // Should return the same pointer (no conversion)
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_widen_tinyint_to_smallint) {
    // TINYINT (int8_t) -> SMALLINT (int16_t)
    auto col = Int8Column::create();
    col->append(1);
    col->append(-5);
    col->append(127);
    ColumnPtr ptr = col;

    TypeDescriptor type_smallint(TYPE_SMALLINT);
    auto result = ColumnHelper::normalize_column_type(ptr, type_smallint);
    // Should return a new column with correct type
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 3);
    ASSERT_EQ(result->type_size(), sizeof(int16_t));

    // Verify values are correctly converted
    auto* converted = down_cast<const Int16Column*>(result.get());
    ASSERT_EQ(converted->get_data()[0], 1);
    ASSERT_EQ(converted->get_data()[1], -5);
    ASSERT_EQ(converted->get_data()[2], 127);
}

TEST_F(ColumnHelperTest, normalize_column_type_nullable) {
    // Nullable TINYINT -> SMALLINT
    auto data_col = Int8Column::create();
    data_col->append(10);
    data_col->append(20);
    auto null_col = NullColumn::create();
    null_col->append(0);
    null_col->append(1);
    auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
    ColumnPtr ptr = nullable;

    TypeDescriptor type_smallint(TYPE_SMALLINT);
    auto result = ColumnHelper::normalize_column_type(ptr, type_smallint);
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_TRUE(result->is_nullable());

    auto* result_nullable = down_cast<const NullableColumn*>(result.get());
    auto* result_data = down_cast<const Int16Column*>(result_nullable->data_column().get());
    ASSERT_EQ(result_data->get_data()[0], 10);
    ASSERT_EQ(result_data->get_data()[1], 20);
    // Null flags should be preserved
    ASSERT_EQ(result_nullable->null_column()->get_data()[0], 0);
    ASSERT_EQ(result_nullable->null_column()->get_data()[1], 1);
}

TEST_F(ColumnHelperTest, normalize_column_type_struct_nested) {
    // STRUCT with a mismatched inner field: STRUCT<TINYINT> -> STRUCT<SMALLINT>
    auto field = Int8Column::create();
    field->append(42);
    auto null_col = NullColumn::create();
    null_col->append(0);
    auto nullable_field = NullableColumn::create(std::move(field), std::move(null_col));

    Columns fields;
    fields.push_back(std::move(nullable_field));
    std::vector<std::string> names = {"f1"};
    auto struct_col = StructColumn::create(std::move(fields), names);
    ColumnPtr ptr = struct_col;

    TypeDescriptor type_struct;
    type_struct.type = TYPE_STRUCT;
    type_struct.children.emplace_back(TYPE_SMALLINT);
    type_struct.field_names.push_back("f1");

    auto result = ColumnHelper::normalize_column_type(ptr, type_struct);
    ASSERT_NE(result.get(), ptr.get());

    auto* result_struct = down_cast<const StructColumn*>(result.get());
    auto* result_field = down_cast<const NullableColumn*>(result_struct->field_column_raw_ptr(0));
    auto* result_data = down_cast<const Int16Column*>(result_field->data_column().get());
    ASSERT_EQ(result_data->get_data()[0], 42);
}

} // namespace starrocks
