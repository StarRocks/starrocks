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

#include "formats/json/numeric_column.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "runtime/types.h"

namespace starrocks {

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

TEST_F(AddNumericColumnTest, test_add_int64_lowerbound) {
    auto column = FixedLengthColumn<int64_t>::create();
    TypeDescriptor t(TYPE_BIGINT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": -9223372036854775808} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");

    auto st = add_numeric_column<int64_t>(column.get(), t, "f_int64", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[-9223372036854775808]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_int64_overflow) {
    auto column = FixedLengthColumn<int64_t>::create();
    TypeDescriptor t(TYPE_BIGINT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": -9223372036854775809} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");
    auto st = add_numeric_column<int64_t>(column.get(), t, "f_int64", &val);
    ASSERT_TRUE(st.is_data_quality_error());
}

TEST_F(AddNumericColumnTest, test_add_int64_overflow2) {
    auto column = FixedLengthColumn<int64_t>::create();
    TypeDescriptor t(TYPE_BIGINT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": 9223372036854775808} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");

    auto st = add_numeric_column<int64_t>(column.get(), t, "f_int64", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNumericColumnTest, test_add_int128) {
    auto column = FixedLengthColumn<int128_t>::create();
    TypeDescriptor t(TYPE_LARGEINT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int128": 9223372036854775808} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int128");

    auto st = add_numeric_column<int128_t>(column.get(), t, "f_int128", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[9223372036854775808]", column->debug_string());
}

// Currently simdjson can not parse number < -9223372036854775808 (lower bound of int64_t)
// or > 18446744073709551615 (upper bound of uint64_t)
TEST_F(AddNumericColumnTest, test_add_int128_invalid) {
    auto column = FixedLengthColumn<int128_t>::create();
    TypeDescriptor t(TYPE_LARGEINT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int128": -9223372036854775809} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int128");

    auto st = add_numeric_column<int128_t>(column.get(), t, "f_int128", &val);
    ASSERT_TRUE(st.is_data_quality_error());

    json = R"(  { "f_int128": 18446744073709551616} )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_int128");

    st = add_numeric_column<int128_t>(column.get(), t, "f_int128", &val);
    ASSERT_TRUE(st.is_data_quality_error());
}

} // namespace starrocks
