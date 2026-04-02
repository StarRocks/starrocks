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

// GetContainer::get_data
TEST_F(ColumnHelperTest, get_container_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3});
    auto data = GetContainer<TYPE_INT>::get_data(col.get());
    ASSERT_EQ(data.size(), 3);
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 2);
    EXPECT_EQ(data[2], 3);
}

TEST_F(ColumnHelperTest, get_container_varchar) {
    auto col = ColumnTestHelper::build_column<Slice>({"hello", "world"});
    auto data = GetContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "hello");
    EXPECT_EQ(data[1].to_string(), "world");
}

TEST_F(ColumnHelperTest, get_container_large_binary) {
    auto col = LargeBinaryColumn::create();
    col->append_string("abc");
    col->append_string("def");
    auto data = GetContainer<TYPE_VARCHAR>::get_data(col.get());
    ASSERT_EQ(data.size(), 2);
    EXPECT_EQ(data[0].to_string(), "abc");
    EXPECT_EQ(data[1].to_string(), "def");
}

// GetContainer::get_data(column, row)
TEST_F(ColumnHelperTest, get_container_get_data_with_row_fixed_length) {
    auto col = ColumnTestHelper::build_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_container_get_data_with_row_const_column) {
    // ConstColumn: is_constant() == true, index is always 0
    auto inner = ColumnTestHelper::build_column<int32_t>({42});
    auto const_col = ConstColumn::create(std::move(inner), 5);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(const_col.get(), 0), 42);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(const_col.get(), 3), 42);
}

TEST_F(ColumnHelperTest, get_container_get_data_with_row_nullable) {
    // NullableColumn: get_data_column unwraps it, row index used as-is
    auto col = ColumnTestHelper::build_nullable_column<int32_t>({10, 20, 30});
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 0), 10);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 1), 20);
    EXPECT_EQ(GetContainer<TYPE_INT>::get_data(col.get(), 2), 30);
}

TEST_F(ColumnHelperTest, get_data_column_from_const_nullable_column) {
    auto data_column = BinaryColumn::create();
    data_column->append(Slice("v1"));
    auto null_column = NullColumn::create();
    null_column->append(0);
    ColumnPtr nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
    ColumnPtr const_nullable_column = ConstColumn::create(nullable_column, 4);

    const Column* const_data = ColumnHelper::get_data_column(const_nullable_column.get());
    ASSERT_FALSE(const_data->is_nullable());
    ASSERT_FALSE(const_data->is_constant());
    ASSERT_TRUE(const_data->is_binary());

    const auto* typed_data = ColumnHelper::get_data_column_by_type<TYPE_VARCHAR>(const_nullable_column.get());
    ASSERT_TRUE(typed_data->is_binary());
    ASSERT_EQ(typed_data->get_slice(0).to_string(), "v1");
}

// ColumnHelper::append_column_value
TEST_F(ColumnHelperTest, append_column_value_int32) {
    auto col = Int32Column::create();
    ColumnHelper::append_column_value<TYPE_INT>(col.get(), 42);
    ColumnHelper::append_column_value<TYPE_INT>(col.get(), 99);
    EXPECT_EQ(col->debug_string(), "[42, 99]");
}

TEST_F(ColumnHelperTest, append_column_value_varchar) {
    auto col = BinaryColumn::create();
    ColumnHelper::append_column_value<TYPE_VARCHAR>(col.get(), Slice("hello"));
    ColumnHelper::append_column_value<TYPE_VARCHAR>(col.get(), Slice("world"));
    EXPECT_EQ(col->debug_string(), "['hello', 'world']");
}

TEST_F(ColumnHelperTest, append_column_value_large_binary) {
    auto col = LargeBinaryColumn::create();
    ColumnHelper::append_column_value<TYPE_VARBINARY>(col.get(), Slice("foo"));
    ColumnHelper::append_column_value<TYPE_VARBINARY>(col.get(), Slice("bar"));
    EXPECT_EQ(col->debug_string(), "['foo', 'bar']");
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

// --- Coverage gap: only_null() early return (line 449-451) ---
TEST_F(ColumnHelperTest, normalize_column_type_only_null) {
    auto col = ColumnHelper::create_const_null_column(3);
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    // only_null column returned as-is
    ASSERT_EQ(result.get(), ptr.get());
}

// --- Coverage gap: ConstColumn unfolding path (line 456-459) ---
TEST_F(ColumnHelperTest, normalize_column_type_const_column) {
    // ConstColumn wrapping an int8 scalar, target is int16
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

// --- Coverage gap: NullableColumn pointer stability when no change needed ---
TEST_F(ColumnHelperTest, normalize_column_type_nullable_no_change) {
    auto data_col = Int32Column::create();
    data_col->append(100);
    auto null_col = NullColumn::create();
    null_col->append(0);
    ColumnPtr ptr = NullableColumn::create(std::move(data_col), std::move(null_col));

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_INT));
    // Types already match — must return original pointer
    ASSERT_EQ(result.get(), ptr.get());
}

// --- Coverage gap: ScalarColumnTypeNormalizer case 2 (int16 source) ---
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

// --- Coverage gap: ScalarColumnTypeNormalizer case 4, int path (int32 source → int64) ---
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

// --- Coverage gap: ScalarColumnTypeNormalizer case 4, float path (float source → double) ---
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

// --- Coverage gap: ScalarColumnTypeNormalizer case 8, int path (int64 source → int128) ---
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

// --- Coverage gap: ScalarColumnTypeNormalizer case 8, double path (double → float) ---
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

// --- Coverage gap: non-scalar type returns original column (line 514-516) ---
TEST_F(ColumnHelperTest, normalize_column_type_varchar_no_conversion) {
    auto col = BinaryColumn::create();
    col->append(Slice("hello"));
    col->append(Slice("world"));
    ColumnPtr ptr = col;

    auto result = ColumnHelper::normalize_column_type(ptr, TypeDescriptor(TYPE_VARCHAR));
    // VARCHAR is not a normalizable numeric type — pointer unchanged
    ASSERT_EQ(result.get(), ptr.get());
}

// --- Coverage gap: MAP column path (lines 489-499) ---
TEST_F(ColumnHelperTest, normalize_column_type_map) {
    // Build a MAP<int8, int8> column, normalize to MAP<SMALLINT, SMALLINT>
    auto keys = NullableColumn::create(Int8Column::create(), NullColumn::create());
    auto values = NullableColumn::create(Int8Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);

    down_cast<Int8Column*>(down_cast<NullableColumn*>(keys.get())->data_column().get())->append(1);
    down_cast<NullColumn*>(down_cast<NullableColumn*>(keys.get())->null_column().get())->append(0);
    down_cast<Int8Column*>(down_cast<NullableColumn*>(values.get())->data_column().get())->append(10);
    down_cast<NullColumn*>(down_cast<NullableColumn*>(values.get())->null_column().get())->append(0);
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
    auto* key_data = down_cast<const Int16Column*>(result_keys->data_column().get());
    EXPECT_EQ(key_data->get_data()[0], 1);

    auto* result_values = down_cast<const NullableColumn*>(map_result->values_column().get());
    auto* val_data = down_cast<const Int16Column*>(result_values->data_column().get());
    EXPECT_EQ(val_data->get_data()[0], 10);
}

// --- Coverage gap: MAP pointer stability when types match ---
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

// --- Coverage gap: ARRAY column path (lines 500-507) ---
TEST_F(ColumnHelperTest, normalize_column_type_array) {
    auto elements = NullableColumn::create(Int8Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);

    down_cast<Int8Column*>(down_cast<NullableColumn*>(elements.get())->data_column().get())->append(5);
    down_cast<NullColumn*>(down_cast<NullableColumn*>(elements.get())->null_column().get())->append(0);
    down_cast<Int8Column*>(down_cast<NullableColumn*>(elements.get())->data_column().get())->append(6);
    down_cast<NullColumn*>(down_cast<NullableColumn*>(elements.get())->null_column().get())->append(0);
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
    auto* elem_data = down_cast<const Int16Column*>(result_elements->data_column().get());
    EXPECT_EQ(elem_data->get_data()[0], 5);
    EXPECT_EQ(elem_data->get_data()[1], 6);
}

// --- Coverage gap: ARRAY pointer stability when types match ---
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

// --- Coverage gap: STRUCT with multiple fields, partial change ---
TEST_F(ColumnHelperTest, normalize_column_type_struct_multi_field_partial) {
    // f1: int16 (already matches), f2: int8 (needs conversion to int32)
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
    type_struct.children.emplace_back(TYPE_SMALLINT); // f1 matches
    type_struct.field_names.emplace_back("f1");
    type_struct.children.emplace_back(TYPE_INT); // f2 needs conversion
    type_struct.field_names.emplace_back("f2");

    auto result = ColumnHelper::normalize_column_type(ptr, type_struct);
    ASSERT_NE(result.get(), ptr.get());

    auto* result_struct = down_cast<const StructColumn*>(result.get());
    // f1 unchanged
    auto* rf1 = down_cast<const NullableColumn*>(result_struct->field_column_raw_ptr(0));
    EXPECT_EQ(down_cast<const Int16Column*>(rf1->data_column().get())->get_data()[0], 100);
    // f2 converted int8 → int32
    auto* rf2 = down_cast<const NullableColumn*>(result_struct->field_column_raw_ptr(1));
    EXPECT_EQ(down_cast<const Int32Column*>(rf2->data_column().get())->get_data()[0], 42);
}

// --- Coverage gap: STRUCT pointer stability when no fields need change ---
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

// --- Coverage gap: ConstColumn wrapping a NullableColumn with type mismatch ---
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

// --- Coverage gap: ScalarColumnTypeNormalizer int8 → int64 (wider jump) ---
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
