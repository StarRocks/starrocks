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

#include "formats/json/nullable_column.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks {

class AddNullableColumnTest : public ::testing::Test {};

TEST_F(AddNullableColumnTest, test_add_numeric) {
    TypeDescriptor t(TYPE_FLOAT);
    auto column = ColumnHelper::create_column(t, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": 3.14}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_nullable_column(column.get(), t, "f_float", &val, false);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_binary) {
    TypeDescriptor t = TypeDescriptor::create_char_type(20);
    auto column = ColumnHelper::create_column(t, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": "3.14"}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_nullable_column(column.get(), t, "f_float", &val, false);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['3.14']", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_boolean) {
    TypeDescriptor t = TypeDescriptor::create_char_type(20);
    auto column = ColumnHelper::create_column(t, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_boolean": true}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_boolean");

    auto st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['1']", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_invalid_as_null) {
    TypeDescriptor t{TYPE_INT};
    auto column = ColumnHelper::create_column(t, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_object": {"f_int": 1}}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_object");

    auto st = add_nullable_column(column.get(), t, "f_object", &val, true);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[NULL]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_invalid) {
    TypeDescriptor t{TYPE_INT};
    auto column = ColumnHelper::create_column(t, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_object": {"f_int": 1}}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_object");

    auto st = add_nullable_column(column.get(), t, "f_object", &val, false);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNullableColumnTest, add_null_numeric_array) {
    auto desc = TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_INT));
    auto column = ColumnHelper::create_column(desc, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_array": [null]}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_array");

    auto st = add_nullable_column(column.get(), desc, "f_array", &val, false);
    ASSERT_TRUE(st.ok());
    column->check_or_die();
}

} // namespace starrocks
