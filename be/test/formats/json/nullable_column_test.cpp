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
#include "testutil/assert.h"
#include "util/json_converter.h"

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
    TypeDescriptor t(TYPE_BOOLEAN);
    auto column = ColumnHelper::create_column(t, true);

    // boolean
    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_boolean": true}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_boolean");
    auto st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": false}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[0]", column->debug_string());

    // number
    column->reset_column();
    json = R"(  { "f_boolean": 1}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": 0}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[0]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": -1}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": 0.1}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": 9223372036854775808}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": 18446744073709551616}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    // string
    column->reset_column();
    json = R"(  { "f_boolean": "TrUe"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": "1"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": "-1"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": "10"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[1]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": "FaLse"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[0]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": "0"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[0]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_boolean": "FaLs"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.is_invalid_argument());

    column->reset_column();
    json = R"(  { "f_boolean": "0.1"}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.is_invalid_argument());

    // array
    column->reset_column();
    json = R"(  { "f_boolean": [1]}  )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_boolean");
    st = add_nullable_column(column.get(), t, "f_boolean", &val, false);
    ASSERT_TRUE(st.is_invalid_argument());
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

TEST_F(AddNullableColumnTest, add_boolean_array) {
    auto desc = TypeDescriptor::create_array_type(TypeDescriptor::from_logical_type(TYPE_BOOLEAN));
    auto column = ColumnHelper::create_column(desc, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_array": [null, 0.1, -0.1, 1, -1, 0, "TRUE", "false", "1"]}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_array");

    auto st = add_nullable_column(column.get(), desc, "f_array", &val, false);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[[NULL,1,1,1,1,0,1,0,1]]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_struct) {
    TypeDescriptor type_desc = TypeDescriptor::create_struct_type(
            {"key1", "key2"}, {TypeDescriptor::create_varchar_type(10), TypeDescriptor::create_varchar_type(10)});
    auto column = ColumnHelper::create_column(type_desc, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key0": {"key1": "foo", "key2": "bar", "key3": "baz" }}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field_unordered("key0");

    ASSERT_OK(add_nullable_column(column.get(), type_desc, "root_key", &val, true));

    ASSERT_EQ("[{key1:'foo',key2:'bar'}]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_struct_null) {
    TypeDescriptor type_desc = TypeDescriptor::create_struct_type(
            {"key1", "key2"}, {TypeDescriptor::create_varchar_type(10), TypeDescriptor::create_varchar_type(10)});
    auto column = ColumnHelper::create_column(type_desc, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key0": null}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field_unordered("key0");

    ASSERT_OK(add_nullable_column(column.get(), type_desc, "root_key", &val, true));

    ASSERT_EQ("[NULL]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_map) {
    TypeDescriptor type_desc = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(10),
                                                               TypeDescriptor::create_varchar_type(10));

    auto column = ColumnHelper::create_column(type_desc, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key0": {"key1": "foo", "key2": "bar", "key3": "baz" }}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field_unordered("key0");

    ASSERT_OK(add_nullable_column(column.get(), type_desc, "root_key", &val, true));

    ASSERT_EQ("[{'key1':'foo','key2':'bar','key3':'baz'}]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_add_map_null) {
    TypeDescriptor type_desc = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(10),
                                                               TypeDescriptor::create_varchar_type(10));

    auto column = ColumnHelper::create_column(type_desc, true);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key0": null}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field_unordered("key0");

    ASSERT_OK(add_nullable_column(column.get(), type_desc, "root_key", &val, true));

    ASSERT_EQ("[NULL]", column->debug_string());
}

TEST_F(AddNullableColumnTest, test_invalid_json_convert) {
    {
        simdjson::ondemand::parser parser;
        auto json = R"(  { "key" : "abc \ssn\"" }  )"_padded;
        auto doc = parser.iterate(json);
        simdjson::ondemand::value val = doc.find_field("key");
        auto st = convert_from_simdjson(val).status();
        ASSERT_TRUE(!st.ok());
        ASSERT_TRUE(st.message().find("Failed to convert simdjson value") != std::string_view::npos &&
                    st.message().find("<truncated>") == std::string_view::npos);
    }

    {
        simdjson::ondemand::parser parser;
        std::string json_str = R"(  { "key" : "abc \ssn\"" }  )";
        auto pos = json_str.find('c');
        std::string to_insert(2000, 'a');
        json_str.insert(pos, to_insert);
        auto json = simdjson::padded_string(json_str);

        auto doc = parser.iterate(json);
        simdjson::ondemand::value val = doc.find_field("key");
        auto st = convert_from_simdjson(val).status();
        ASSERT_TRUE(!st.ok());
        ASSERT_TRUE(st.message().find("Failed to convert simdjson value") != std::string_view::npos &&
                    st.message().find("<truncated>") != std::string_view::npos);
    }
}
} // namespace starrocks
