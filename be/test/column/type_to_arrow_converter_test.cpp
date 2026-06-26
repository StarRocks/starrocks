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

#include "column/arrow/type_to_arrow_converter.h"

#include <arrow/type.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"

namespace starrocks {

TEST(TypeToArrowConverterTest, MapsScalarAndNestedTypes) {
    std::shared_ptr<arrow::DataType> type;

    ASSERT_OK(convert_to_arrow_type(TypeDescriptor(TYPE_INT), &type));
    EXPECT_EQ(arrow::Type::INT32, type->id());

    auto decimal = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 4);
    ASSERT_OK(convert_to_arrow_type(decimal, &type));
    EXPECT_EQ(arrow::Type::DECIMAL128, type->id());
    auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(type);
    EXPECT_EQ(20, decimal_type->precision());
    EXPECT_EQ(4, decimal_type->scale());

    auto array = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT));
    ASSERT_OK(convert_to_arrow_type(array, &type));
    ASSERT_EQ(arrow::Type::LIST, type->id());
    auto list_type = std::static_pointer_cast<arrow::ListType>(type);
    EXPECT_EQ(arrow::Type::INT64, list_type->value_type()->id());

    auto map = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_DOUBLE));
    ASSERT_OK(convert_to_arrow_type(map, &type));
    ASSERT_EQ(arrow::Type::MAP, type->id());
    auto map_type = std::static_pointer_cast<arrow::MapType>(type);
    EXPECT_EQ(arrow::Type::STRING, map_type->key_type()->id());
    EXPECT_EQ(arrow::Type::DOUBLE, map_type->item_type()->id());

    auto structure =
            TypeDescriptor::create_struct_type({"k", "v"}, {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_JSON)});
    ASSERT_OK(convert_to_arrow_type(structure, &type));
    ASSERT_EQ(arrow::Type::STRUCT, type->id());
    auto struct_type = std::static_pointer_cast<arrow::StructType>(type);
    ASSERT_EQ(2, struct_type->num_fields());
    EXPECT_EQ("k", struct_type->field(0)->name());
    EXPECT_EQ(arrow::Type::INT32, struct_type->field(0)->type()->id());
    EXPECT_EQ("v", struct_type->field(1)->name());
    EXPECT_EQ(arrow::Type::STRING, struct_type->field(1)->type()->id());
}

TEST(TypeToArrowConverterTest, MapsFlightSqlSpecialTypes) {
    std::shared_ptr<arrow::DataType> type;

    ASSERT_OK(convert_to_arrow_type_for_flight_sql(TypeDescriptor(TYPE_DATE), &type));
    EXPECT_EQ(arrow::Type::DATE32, type->id());

    ASSERT_OK(convert_to_arrow_type_for_flight_sql(TypeDescriptor(TYPE_DATETIME), &type));
    ASSERT_EQ(arrow::Type::TIMESTAMP, type->id());
    auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(type);
    EXPECT_EQ(arrow::TimeUnit::MICRO, timestamp_type->unit());
    EXPECT_EQ("UTC", timestamp_type->timezone());

    std::shared_ptr<arrow::Field> field;
    ASSERT_OK(convert_to_arrow_field_for_flight_sql(TypeDescriptor(TYPE_HLL), "hll_col", false, &field, 1));
    EXPECT_EQ("hll_col", field->name());
    EXPECT_TRUE(field->nullable());
    EXPECT_EQ(arrow::Type::BINARY, field->type()->id());
}

TEST(TypeToArrowConverterTest, RejectsInvalidStructShape) {
    TypeDescriptor type(TYPE_STRUCT);
    type.field_names = {"only_name"};
    type.children = {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_BIGINT)};

    std::shared_ptr<arrow::DataType> arrow_type;
    EXPECT_FALSE(convert_to_arrow_type(type, &arrow_type).ok());
}

} // namespace starrocks
