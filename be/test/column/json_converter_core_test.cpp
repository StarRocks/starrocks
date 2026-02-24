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

#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/json_converter.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "types/json_value.h"

namespace starrocks {

TEST(JsonConverterCoreTest, CastNumericBooleanStringAndJson) {
    JsonValue number_json = JsonValue::from_int(123);

    ColumnBuilder<TYPE_INT> int_builder(1);
    Status int_status = cast_vpjson_to<TYPE_INT, true>(number_json.to_vslice(), int_builder);
    ASSERT_TRUE(int_status.ok()) << int_status;
    auto int_column = int_builder.build(false);
    auto* int_data = ColumnHelper::cast_to_raw<TYPE_INT>(int_column.get());
    ASSERT_EQ(1, int_data->size());
    EXPECT_EQ(123, int_data->get_data()[0]);

    JsonValue bool_string_json = JsonValue::from_string(Slice("true"));
    ColumnBuilder<TYPE_BOOLEAN> bool_builder(1);
    Status bool_status = cast_vpjson_to<TYPE_BOOLEAN, true>(bool_string_json.to_vslice(), bool_builder);
    ASSERT_TRUE(bool_status.ok()) << bool_status;
    auto bool_column = bool_builder.build(false);
    auto* bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(bool_column.get());
    ASSERT_EQ(1, bool_data->size());
    EXPECT_EQ(1, bool_data->get_data()[0]);

    ColumnBuilder<TYPE_VARCHAR> string_builder(1);
    Status string_status = cast_vpjson_to<TYPE_VARCHAR, true>(number_json.to_vslice(), string_builder);
    ASSERT_TRUE(string_status.ok()) << string_status;
    auto string_column = string_builder.build(false);
    auto* string_data = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(string_column.get());
    ASSERT_EQ(1, string_data->size());
    EXPECT_EQ("123", string_data->get_slice(0).to_string());

    auto object_json = JsonValue::parse(Slice("{\"k\": 1}"));
    ASSERT_TRUE(object_json.ok());
    ColumnBuilder<TYPE_JSON> json_builder(1);
    Status json_status = cast_vpjson_to<TYPE_JSON, true>(object_json->to_vslice(), json_builder);
    ASSERT_TRUE(json_status.ok()) << json_status;
    auto json_column = json_builder.build(false);
    auto* json_data = down_cast<JsonColumn*>(json_column.get());
    ASSERT_EQ(1, json_data->size());
    EXPECT_EQ(0, json_data->get_object(0)->compare(object_json.value()));
}

TEST(JsonConverterCoreTest, ThrowAndNullOnInvalidBooleanString) {
    JsonValue invalid_bool_json = JsonValue::from_string(Slice("not_bool"));

    ColumnBuilder<TYPE_BOOLEAN> throw_builder(1);
    auto throw_status = cast_vpjson_to<TYPE_BOOLEAN, true>(invalid_bool_json.to_vslice(), throw_builder);
    EXPECT_FALSE(throw_status.ok());

    ColumnBuilder<TYPE_BOOLEAN> null_builder(1);
    Status null_status = cast_vpjson_to<TYPE_BOOLEAN, false>(invalid_bool_json.to_vslice(), null_builder);
    ASSERT_TRUE(null_status.ok()) << null_status;
    auto null_column = null_builder.build(false);
    ASSERT_TRUE(null_column->is_nullable());
    auto* nullable = down_cast<NullableColumn*>(null_column.get());
    EXPECT_TRUE(nullable->is_null(0));
}

TEST(JsonConverterCoreTest, DateAndDatetimeUnsupportedBehavior) {
    JsonValue date_json = JsonValue::from_string(Slice("2025-02-02"));
    ColumnBuilder<TYPE_DATE> date_throw_builder(1);
    Status date_throw_status = cast_vpjson_to<TYPE_DATE, true>(date_json.to_vslice(), date_throw_builder);
    EXPECT_FALSE(date_throw_status.ok());

    ColumnBuilder<TYPE_DATE> date_null_builder(1);
    Status date_null_status = cast_vpjson_to<TYPE_DATE, false>(date_json.to_vslice(), date_null_builder);
    ASSERT_TRUE(date_null_status.ok()) << date_null_status;
    auto date_null_column = date_null_builder.build(false);
    ASSERT_TRUE(date_null_column->is_nullable());
    auto* nullable_date = down_cast<NullableColumn*>(date_null_column.get());
    EXPECT_TRUE(nullable_date->is_null(0));

    JsonValue datetime_json = JsonValue::from_string(Slice("2025-02-02 01:02:03"));
    ColumnBuilder<TYPE_DATETIME> datetime_throw_builder(1);
    Status datetime_throw_status =
            cast_vpjson_to<TYPE_DATETIME, true>(datetime_json.to_vslice(), datetime_throw_builder);
    EXPECT_FALSE(datetime_throw_status.ok());

    ColumnBuilder<TYPE_DATETIME> datetime_null_builder(1);
    Status datetime_null_status =
            cast_vpjson_to<TYPE_DATETIME, false>(datetime_json.to_vslice(), datetime_null_builder);
    ASSERT_TRUE(datetime_null_status.ok()) << datetime_null_status;
    auto datetime_null_column = datetime_null_builder.build(false);
    ASSERT_TRUE(datetime_null_column->is_nullable());
    auto* nullable_datetime = down_cast<NullableColumn*>(datetime_null_column.get());
    EXPECT_TRUE(nullable_datetime->is_null(0));
}

} // namespace starrocks
