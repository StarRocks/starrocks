// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "formats/json/binary_column.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
