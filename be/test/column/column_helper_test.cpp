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

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "gtest/gtest.h"
#include "testutil/column_test_helper.h"

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
    auto col = Int32Column::create();
    col->append(1);
    col->append(2);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_widen_tinyint_to_smallint) {
    auto col = Int8Column::create();
    col->append(1);
    col->append(-5);
    col->append(127);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_SMALLINT));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 3);
    ASSERT_EQ(result->type_size(), sizeof(int16_t));

    auto* converted = down_cast<const Int16Column*>(result.get());
    ASSERT_EQ(converted->get_data()[0], 1);
    ASSERT_EQ(converted->get_data()[1], -5);
    ASSERT_EQ(converted->get_data()[2], 127);
}

TEST_F(ColumnHelperTest, normalize_column_type_nullable) {
    auto data_col = Int8Column::create();
    data_col->append(10);
    data_col->append(20);
    auto null_col = NullColumn::create();
    null_col->append(0);
    null_col->append(1);
    ColumnPtr ptr = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_SMALLINT));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_TRUE(result->is_nullable());

    auto* result_nullable = down_cast<const NullableColumn*>(result.get());
    auto* result_data = down_cast<const Int16Column*>(result_nullable->data_column().get());
    ASSERT_EQ(result_data->get_data()[0], 10);
    ASSERT_EQ(result_data->get_data()[1], 20);
    ASSERT_EQ(result_nullable->null_column()->get_data()[0], 0);
    ASSERT_EQ(result_nullable->null_column()->get_data()[1], 1);
}

TEST_F(ColumnHelperTest, normalize_column_type_struct_nested) {
    auto field = Int8Column::create();
    field->append(42);
    auto null_col = NullColumn::create();
    null_col->append(0);
    ColumnPtr nullable_field = NullableColumn::create(std::move(field), std::move(null_col));

    Columns fields;
    fields.emplace_back(std::move(nullable_field));
    ColumnPtr ptr = StructColumn::create(fields, {"f1"});

    TypeDescriptor type_struct;
    type_struct.type = TYPE_STRUCT;
    type_struct.children.emplace_back(TYPE_SMALLINT);
    type_struct.field_names.emplace_back("f1");

    auto result = ColumnHelper::normalize_column_type(ptr, type_struct);
    ASSERT_NE(result.get(), ptr.get());

    auto* result_struct = down_cast<const StructColumn*>(result.get());
    auto* result_field = down_cast<const NullableColumn*>(result_struct->field_column_raw_ptr(0));
    auto* result_data = down_cast<const Int16Column*>(result_field->data_column().get());
    ASSERT_EQ(result_data->get_data()[0], 42);
}

TEST_F(ColumnHelperTest, normalize_column_type_only_null) {
    auto col = ColumnHelper::create_const_null_column(3);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_const_column) {
    auto inner = Int8Column::create();
    inner->append(7);
    auto const_col = ConstColumn::create(std::move(inner), 3);
    ColumnPtr ptr = const_col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_SMALLINT));
    ASSERT_FALSE(result->is_constant());
    ASSERT_EQ(result->size(), 3);
    auto* converted = down_cast<const Int16Column*>(result.get());
    EXPECT_EQ(converted->get_data()[0], 7);
    EXPECT_EQ(converted->get_data()[1], 7);
    EXPECT_EQ(converted->get_data()[2], 7);
}

TEST_F(ColumnHelperTest, normalize_column_type_nullable_no_change) {
    auto data_col = Int32Column::create();
    data_col->append(100);
    auto null_col = NullColumn::create();
    null_col->append(0);
    ColumnPtr ptr = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_widen_smallint_to_int) {
    auto col = Int16Column::create();
    col->append(100);
    col->append(-200);
    col->append(32767);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 3);
    auto* converted = down_cast<const Int32Column*>(result.get());
    EXPECT_EQ(converted->get_data()[0], 100);
    EXPECT_EQ(converted->get_data()[1], -200);
    EXPECT_EQ(converted->get_data()[2], 32767);
}

TEST_F(ColumnHelperTest, normalize_column_type_widen_int_to_bigint) {
    auto col = Int32Column::create();
    col->append(1000000);
    col->append(-999);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_BIGINT));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 2);
    auto* converted = down_cast<const Int64Column*>(result.get());
    EXPECT_EQ(converted->get_data()[0], 1000000);
    EXPECT_EQ(converted->get_data()[1], -999);
}

TEST_F(ColumnHelperTest, normalize_column_type_float_to_double) {
    auto col = FloatColumn::create();
    col->append(1.5f);
    col->append(-3.25f);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_DOUBLE));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 2);
    auto* converted = down_cast<const DoubleColumn*>(result.get());
    EXPECT_FLOAT_EQ(converted->get_data()[0], 1.5);
    EXPECT_FLOAT_EQ(converted->get_data()[1], -3.25);
}

TEST_F(ColumnHelperTest, normalize_column_type_bigint_to_largeint) {
    auto col = Int64Column::create();
    col->append(123456789LL);
    col->append(-1LL);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_LARGEINT));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 2);
    auto* converted = down_cast<const Int128Column*>(result.get());
    EXPECT_EQ(converted->get_data()[0], static_cast<int128_t>(123456789LL));
    EXPECT_EQ(converted->get_data()[1], static_cast<int128_t>(-1LL));
}

TEST_F(ColumnHelperTest, normalize_column_type_double_to_float) {
    auto col = DoubleColumn::create();
    col->append(2.5);
    col->append(-7.75);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_FLOAT));
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_EQ(result->size(), 2);
    auto* converted = down_cast<const FloatColumn*>(result.get());
    EXPECT_FLOAT_EQ(converted->get_data()[0], 2.5f);
    EXPECT_FLOAT_EQ(converted->get_data()[1], -7.75f);
}

TEST_F(ColumnHelperTest, normalize_column_type_varchar_no_conversion) {
    auto col = BinaryColumn::create();
    col->append(Slice("hello"));
    col->append(Slice("world"));
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_VARCHAR));
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_map) {
    auto key_data = Int8Column::create();
    key_data->append(1);
    auto key_null = NullColumn::create();
    key_null->append(0);
    auto keys = NullableColumn::create(std::move(key_data), std::move(key_null));

    auto val_data = Int8Column::create();
    val_data->append(10);
    auto val_null = NullColumn::create();
    val_null->append(0);
    auto values = NullableColumn::create(std::move(val_data), std::move(val_null));

    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(1);

    ColumnPtr ptr = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));

    TypeDescriptor type_map;
    type_map.type = TYPE_MAP;
    type_map.children.emplace_back(TYPE_SMALLINT);
    type_map.children.emplace_back(TYPE_SMALLINT);

    auto result = ColumnHelper::normalize_column_type(ptr, type_map);
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_TRUE(result->is_map());

    auto* map_result = down_cast<const MapColumn*>(result.get());
    auto* result_keys = down_cast<const NullableColumn*>(map_result->keys_column().get());
    auto* result_key_data = down_cast<const Int16Column*>(result_keys->data_column().get());
    EXPECT_EQ(result_key_data->get_data()[0], 1);

    auto* result_values = down_cast<const NullableColumn*>(map_result->values_column().get());
    auto* result_val_data = down_cast<const Int16Column*>(result_values->data_column().get());
    EXPECT_EQ(result_val_data->get_data()[0], 10);
}

TEST_F(ColumnHelperTest, normalize_column_type_map_no_change) {
    auto keys = NullableColumn::create(Int16Column::create(), NullColumn::create());
    auto values = NullableColumn::create(Int16Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(0);
    ColumnPtr ptr = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));

    TypeDescriptor type_map;
    type_map.type = TYPE_MAP;
    type_map.children.emplace_back(TYPE_SMALLINT);
    type_map.children.emplace_back(TYPE_SMALLINT);

    auto result = ColumnHelper::normalize_column_type(ptr, type_map);
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_array) {
    auto elem_data = Int8Column::create();
    elem_data->append(5);
    elem_data->append(6);
    auto elem_null = NullColumn::create();
    elem_null->append(0);
    elem_null->append(0);
    auto elements = NullableColumn::create(std::move(elem_data), std::move(elem_null));

    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(2);

    ColumnPtr ptr = ArrayColumn::create(std::move(elements), std::move(offsets));

    TypeDescriptor type_arr;
    type_arr.type = TYPE_ARRAY;
    type_arr.children.emplace_back(TYPE_SMALLINT);

    auto result = ColumnHelper::normalize_column_type(ptr, type_arr);
    ASSERT_NE(result.get(), ptr.get());
    ASSERT_TRUE(result->is_array());

    auto* arr_result = down_cast<const ArrayColumn*>(result.get());
    auto* result_elements = down_cast<const NullableColumn*>(arr_result->elements_column().get());
    auto* result_elem_data = down_cast<const Int16Column*>(result_elements->data_column().get());
    EXPECT_EQ(result_elem_data->get_data()[0], 5);
    EXPECT_EQ(result_elem_data->get_data()[1], 6);
}

TEST_F(ColumnHelperTest, normalize_column_type_array_no_change) {
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(0);
    ColumnPtr ptr = ArrayColumn::create(std::move(elements), std::move(offsets));

    TypeDescriptor type_arr;
    type_arr.type = TYPE_ARRAY;
    type_arr.children.emplace_back(TYPE_INT);

    auto result = ColumnHelper::normalize_column_type(ptr, type_arr);
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_struct_multi_field_partial) {
    auto f1_data = Int16Column::create();
    f1_data->append(100);
    auto f1_null = NullColumn::create();
    f1_null->append(0);
    ColumnPtr f1 = NullableColumn::create(std::move(f1_data), std::move(f1_null));

    auto f2_data = Int8Column::create();
    f2_data->append(42);
    auto f2_null = NullColumn::create();
    f2_null->append(0);
    ColumnPtr f2 = NullableColumn::create(std::move(f2_data), std::move(f2_null));

    Columns fields{f1, f2};
    ColumnPtr ptr = StructColumn::create(fields, {"f1", "f2"});

    TypeDescriptor type_struct;
    type_struct.type = TYPE_STRUCT;
    type_struct.children.emplace_back(TYPE_SMALLINT);
    type_struct.field_names.emplace_back("f1");
    type_struct.children.emplace_back(TYPE_INT);
    type_struct.field_names.emplace_back("f2");

    auto result = ColumnHelper::normalize_column_type(ptr, type_struct);
    ASSERT_NE(result.get(), ptr.get());

    auto* result_struct = down_cast<const StructColumn*>(result.get());
    auto* rf1 = down_cast<const NullableColumn*>(result_struct->field_column_raw_ptr(0));
    EXPECT_EQ(down_cast<const Int16Column*>(rf1->data_column().get())->get_data()[0], 100);
    auto* rf2 = down_cast<const NullableColumn*>(result_struct->field_column_raw_ptr(1));
    EXPECT_EQ(down_cast<const Int32Column*>(rf2->data_column().get())->get_data()[0], 42);
}

TEST_F(ColumnHelperTest, normalize_column_type_struct_no_change) {
    auto f1_data = Int32Column::create();
    f1_data->append(99);
    auto f1_null = NullColumn::create();
    f1_null->append(0);
    ColumnPtr f1 = NullableColumn::create(std::move(f1_data), std::move(f1_null));

    Columns fields{f1};
    ColumnPtr ptr = StructColumn::create(fields, {"f1"});

    TypeDescriptor type_struct;
    type_struct.type = TYPE_STRUCT;
    type_struct.children.emplace_back(TYPE_INT);
    type_struct.field_names.emplace_back("f1");

    auto result = ColumnHelper::normalize_column_type(ptr, type_struct);
    ASSERT_EQ(result.get(), ptr.get());
}

TEST_F(ColumnHelperTest, normalize_column_type_const_nullable) {
    auto data_col = Int8Column::create();
    data_col->append(42);
    auto null_col = NullColumn::create();
    null_col->append(0);
    auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
    auto const_col = ConstColumn::create(std::move(nullable), 2);
    ColumnPtr ptr = const_col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    ASSERT_FALSE(result->is_constant());
    ASSERT_TRUE(result->is_nullable());
    ASSERT_EQ(result->size(), 2);

    auto* result_nullable = down_cast<const NullableColumn*>(result.get());
    auto* result_data = down_cast<const Int32Column*>(result_nullable->data_column().get());
    EXPECT_EQ(result_data->get_data()[0], 42);
    EXPECT_EQ(result_data->get_data()[1], 42);
}

TEST_F(ColumnHelperTest, normalize_column_type_tinyint_to_bigint) {
    auto col = Int8Column::create();
    col->append(-128);
    col->append(0);
    col->append(127);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_BIGINT));
    ASSERT_NE(result.get(), ptr.get());
    auto* converted = down_cast<const Int64Column*>(result.get());
    EXPECT_EQ(converted->get_data()[0], -128);
    EXPECT_EQ(converted->get_data()[1], 0);
    EXPECT_EQ(converted->get_data()[2], 127);
}

} // namespace starrocks
