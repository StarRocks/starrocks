// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "formats/json/numeric_column.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

class AddNumericColumnTest : public ::testing::Test {};

TEST_F(AddNumericColumnTest, test_add_number) {
    auto column = FixedLengthColumn<float>::create();
    TypeDescriptor t(TYPE_FLOAT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": 3.14}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_numeric_column<float>(column.get(), t, "f_float", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_string) {
    auto column = FixedLengthColumn<float>::create();
    TypeDescriptor t(TYPE_FLOAT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": "3.14"}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_numeric_column<float>(column.get(), t, "f_float", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_invalid) {
    auto column = FixedLengthColumn<float>::create();
    TypeDescriptor t(TYPE_FLOAT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_object": {"f": 1}}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_object");

    auto st = add_numeric_column<float>(column.get(), t, "f_object", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNumericColumnTest, test_add_int_overflow) {
    auto column = FixedLengthColumn<int32_t>::create();
    TypeDescriptor t(TYPE_INT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_bigint": 2147483648} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_bigint");

    auto st = add_numeric_column<int32_t>(column.get(), t, "f_bigint", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

} // namespace starrocks::vectorized
