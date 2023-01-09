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

#include "formats/json/binary_column.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "runtime/types.h"

namespace starrocks {

class AddBinaryColumnTest : public ::testing::Test {};

TEST_F(AddBinaryColumnTest, test_add_string) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_string": "3.14"}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_string");

    auto st = add_binary_column(column.get(), t, "f_string", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['3.14']", column->debug_string());
}

TEST_F(AddBinaryColumnTest, test_add_number) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": 3.14}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_binary_column(column.get(), t, "f_float", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['3.14']", column->debug_string());
}

TEST_F(AddBinaryColumnTest, test_add_boolean) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_boolean": true}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_boolean");

    auto st = add_binary_column(column.get(), t, "f_boolean", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['1']", column->debug_string());
}

TEST_F(AddBinaryColumnTest, test_add_object) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(20);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_object": {"f_int": 1}}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_object");

    auto st = add_binary_column(column.get(), t, "f_object", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['{\"f_int\":1}']", column->debug_string());
}

TEST_F(AddBinaryColumnTest, test_add_invalid) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_varchar_type(1);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_object": {"f": 1}}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_object");

    auto st = add_binary_column(column.get(), t, "f_object", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

} // namespace starrocks
