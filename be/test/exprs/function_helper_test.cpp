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

#include "exprs/function_helper.h"

#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {
struct FunctionHelperTest : public ::testing::Test {};
TEST_F(FunctionHelperTest, testMergeOnlyNullColumnMergeWithNullColumn) {
    auto only_null_column = ColumnHelper::create_const_null_column(10);
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column =
            FunctionHelper::merge_column_and_null_column(std::move(only_null_column), std::move(null_column));
    ASSERT_TRUE(result_column->only_null());
    ASSERT_EQ(result_column->size(), 10);
}

TEST_F(FunctionHelperTest, testMergeConstColumnMergeWithNullColumn) {
    auto data_column = Decimal128Column::create(38, 10);
    data_column->append((int128_t)111);
    auto const_column = ConstColumn::create(std::move(data_column), 10);
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column = FunctionHelper::merge_column_and_null_column(std::move(const_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<const NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<const Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}

TEST_F(FunctionHelperTest, testMergeNullableColumnMergeWithNullColumn) {
    auto nullable_column = NullableColumn::create(Decimal128Column::create(38, 10), NullColumn::create());
    nullable_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        if (i % 3 == 0) {
            nullable_column->append_nulls(1);
        } else {
            Datum datum((int128_t)111);
            nullable_column->append_datum(datum);
        }
    }
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column =
            FunctionHelper::merge_column_and_null_column(std::move(nullable_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<const NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<const Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0 && i % 3 != 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}
TEST_F(FunctionHelperTest, testMergeNotNullableColumnMergeWithNullColumn) {
    auto data_column = Decimal128Column::create(38, 10);
    data_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        data_column->append((int128_t)111);
    }
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column = FunctionHelper::merge_column_and_null_column(std::move(data_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<const NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<const Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}

TEST_F(FunctionHelperTest, testCreateColumnScalarNullableAndNonNullable) {
    TypeDescriptor type_desc(TYPE_INT);

    auto non_nullable = FunctionHelper::create_column(type_desc, false);
    ASSERT_NE(non_nullable, nullptr);
    ASSERT_FALSE(non_nullable->is_nullable());
    ASSERT_TRUE(non_nullable->is_numeric());

    auto nullable = FunctionHelper::create_column(type_desc, true);
    ASSERT_NE(nullable, nullptr);
    ASSERT_TRUE(nullable->is_nullable());
    auto* nullable_column = down_cast<NullableColumn*>(nullable.get());
    ASSERT_TRUE(nullable_column->data_column()->is_numeric());
}

TEST_F(FunctionHelperTest, testCreateColumnArray) {
    TypeDescriptor array_type = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));

    auto array_column = FunctionHelper::create_column(array_type, false);
    ASSERT_NE(array_column, nullptr);
    ASSERT_TRUE(array_column->is_array());

    auto* col = down_cast<ArrayColumn*>(array_column.get());
    ASSERT_TRUE(col->elements().is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(col->elements_column_raw_ptr())->is_numeric());
    ASSERT_EQ(col->size(), 0);
    ASSERT_EQ(col->offsets().size(), 1);
    ASSERT_EQ(col->offsets().immutable_data().back(), 0);
}

TEST_F(FunctionHelperTest, testCreateColumnStruct) {
    std::vector<std::string> field_names{"c1", "c2"};
    std::vector<TypeDescriptor> field_types{TypeDescriptor(TYPE_INT),
                                            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT))};
    TypeDescriptor struct_type = TypeDescriptor::create_struct_type(field_names, field_types);

    auto struct_column = FunctionHelper::create_column(struct_type, false);
    ASSERT_NE(struct_column, nullptr);
    ASSERT_TRUE(struct_column->is_struct());

    auto* col = down_cast<StructColumn*>(struct_column.get());
    ASSERT_EQ(col->fields_size(), 2);
    ASSERT_TRUE(col->field_column_raw_ptr(0)->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(col->field_column_raw_ptr(0))->is_numeric());
    ASSERT_TRUE(col->field_column_raw_ptr(1)->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(col->field_column_raw_ptr(1))->is_array());
}

TEST_F(FunctionHelperTest, testCreateColumnMapUnknownFallbackToNull) {
    TypeDescriptor map_type =
            TypeDescriptor::create_map_type(TypeDescriptor(TYPE_UNKNOWN), TypeDescriptor(TYPE_UNKNOWN));

    auto map_column = FunctionHelper::create_column(map_type, false);
    ASSERT_NE(map_column, nullptr);
    ASSERT_TRUE(map_column->is_map());

    auto* col = down_cast<MapColumn*>(map_column.get());
    ASSERT_TRUE(col->keys().is_nullable());
    ASSERT_TRUE(col->values().is_nullable());
    auto* key_data = ColumnHelper::get_data_column(col->keys_column_raw_ptr());
    auto* value_data = ColumnHelper::get_data_column(col->values_column_raw_ptr());
    ASSERT_NE(dynamic_cast<const NullColumn*>(key_data), nullptr);
    ASSERT_NE(dynamic_cast<const NullColumn*>(value_data), nullptr);
    ASSERT_EQ(key_data->size(), 0);
    ASSERT_EQ(value_data->size(), 0);
}
} // namespace starrocks
