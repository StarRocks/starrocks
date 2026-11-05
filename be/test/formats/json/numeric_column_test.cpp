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

#include <cmath>
#include <vector>

#include "column/fixed_length_column.h"
#include "types/type_descriptor.h"

namespace starrocks {

class AddNumericColumnTest : public ::testing::Test {};

TEST_F(AddNumericColumnTest, test_add_number) {
    auto column = FixedLengthColumn<float>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": 3.14}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_numeric_column<float>(column.get(), TYPE_FLOAT_DESC, "f_float", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_string) {
    auto column = FixedLengthColumn<float>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_float": "3.14"}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_float");

    auto st = add_numeric_column<float>(column.get(), TYPE_FLOAT_DESC, "f_float", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[3.14]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_invalid) {
    auto column = FixedLengthColumn<float>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_object": {"f": 1}}  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_object");

    auto st = add_numeric_column<float>(column.get(), TYPE_FLOAT_DESC, "f_object", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNumericColumnTest, test_add_int_overflow) {
    auto column = FixedLengthColumn<int32_t>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_bigint": 2147483648} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_bigint");

    auto st = add_numeric_column<int32_t>(column.get(), TYPE_INT_DESC, "f_bigint", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNumericColumnTest, test_add_int64_lowerbound) {
    auto column = FixedLengthColumn<int64_t>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": -9223372036854775808} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");

    auto st = add_numeric_column<int64_t>(column.get(), TYPE_BIGINT_DESC, "f_int64", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("[-9223372036854775808]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_int64_overflow) {
    auto column = FixedLengthColumn<int64_t>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": -9223372036854775809} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");
    auto st = add_numeric_column<int64_t>(column.get(), TYPE_BIGINT_DESC, "f_int64", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNumericColumnTest, test_add_int64_overflow2) {
    auto column = FixedLengthColumn<int64_t>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": 9223372036854775808} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");

    auto st = add_numeric_column<int64_t>(column.get(), TYPE_BIGINT_DESC, "f_int64", &val);
    ASSERT_TRUE(st.is_invalid_argument());
}

TEST_F(AddNumericColumnTest, test_add_int64_overflow3) {
    auto column = FixedLengthColumn<int64_t>::create();

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int64": 18446744073709551616} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int64");

    auto st = add_numeric_column<int64_t>(column.get(), TYPE_BIGINT_DESC, "f_int64", &val);
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

TEST_F(AddNumericColumnTest, test_add_int128_big_integer) {
    auto column = FixedLengthColumn<int128_t>::create();
    TypeDescriptor t(TYPE_LARGEINT);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "f_int128": -9223372036854775809} )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("f_int128");

    auto st = add_numeric_column<int128_t>(column.get(), t, "f_int128", &val);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[-9223372036854775809]", column->debug_string());

    column->reset_column();
    json = R"(  { "f_int128": 18446744073709551616} )"_padded;
    doc = parser.iterate(json);
    val = doc.find_field("f_int128");

    st = add_numeric_column<int128_t>(column.get(), t, "f_int128", &val);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("[18446744073709551616]", column->debug_string());
}

TEST_F(AddNumericColumnTest, test_add_uint64_overflow_range_20_digits) {
    // 18446744073709551615 (2^64 - 1) is unsigned_integer; 18446744073709551616 (2^64) and
    // 99999999999999999999 (10^20 - 1) are the lower and upper bounds of 20-digit integers in [2^64, 10^20),
    // classified as BIGINT_NUMBER (big_integer) in simdjson 5.0+.
    simdjson::ondemand::parser parser;
    auto json_u64_max = R"(  { "f_num": 18446744073709551615} )"_padded;
    auto json_min = R"(  { "f_num": 18446744073709551616} )"_padded;
    auto json_max = R"(  { "f_num": 99999999999999999999} )"_padded;

    {
        auto col_i64 = FixedLengthColumn<int64_t>::create();
        auto doc = parser.iterate(json_u64_max);
        simdjson::ondemand::value val = doc.find_field("f_num");
        auto st = add_numeric_column<int64_t>(col_i64.get(), TYPE_BIGINT_DESC, "f_num", &val);
        ASSERT_TRUE(st.is_invalid_argument());

        doc = parser.iterate(json_min);
        val = doc.find_field("f_num");
        st = add_numeric_column<int64_t>(col_i64.get(), TYPE_BIGINT_DESC, "f_num", &val);
        ASSERT_TRUE(st.is_invalid_argument());

        doc = parser.iterate(json_max);
        val = doc.find_field("f_num");
        st = add_numeric_column<int64_t>(col_i64.get(), TYPE_BIGINT_DESC, "f_num", &val);
        ASSERT_TRUE(st.is_invalid_argument());
    }
    {
        auto col_i128 = FixedLengthColumn<int128_t>::create();
        TypeDescriptor t(TYPE_LARGEINT);
        auto doc = parser.iterate(json_u64_max);
        simdjson::ondemand::value val = doc.find_field("f_num");
        auto st = add_numeric_column<int128_t>(col_i128.get(), t, "f_num", &val);
        ASSERT_TRUE(st.ok());

        doc = parser.iterate(json_min);
        val = doc.find_field("f_num");
        st = add_numeric_column<int128_t>(col_i128.get(), t, "f_num", &val);
        ASSERT_TRUE(st.ok());

        doc = parser.iterate(json_max);
        val = doc.find_field("f_num");
        st = add_numeric_column<int128_t>(col_i128.get(), t, "f_num", &val);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ("[18446744073709551615, 18446744073709551616, 99999999999999999999]", col_i128->debug_string());
    }
}

TEST_F(AddNumericColumnTest, test_add_double_big_integer) {
    auto column = FixedLengthColumn<double>::create();
    simdjson::ondemand::parser parser;

    auto json_min = R"(  { "f_double": 18446744073709551616} )"_padded;
    auto doc = parser.iterate(json_min);
    simdjson::ondemand::value val = doc.find_field("f_double");
    auto st = add_numeric_column<double>(column.get(), TYPE_DOUBLE_DESC, "f_double", &val);
    ASSERT_TRUE(st.ok());
    ASSERT_DOUBLE_EQ(18446744073709551616.0, column->get_data()[0]);

    auto json_max = R"(  { "f_double": 99999999999999999999} )"_padded;
    doc = parser.iterate(json_max);
    val = doc.find_field("f_double");
    st = add_numeric_column<double>(column.get(), TYPE_DOUBLE_DESC, "f_double", &val);
    ASSERT_TRUE(st.ok());
    ASSERT_DOUBLE_EQ(1e20, column->get_data()[1]);

    // 41-digit integer (1e40) exceeds int128_t::max() but fits in double.
    auto json_1e40 = R"(  { "f_double": 10000000000000000000000000000000000000000} )"_padded;
    doc = parser.iterate(json_1e40);
    val = doc.find_field("f_double");
    st = add_numeric_column<double>(column.get(), TYPE_DOUBLE_DESC, "f_double", &val);
    ASSERT_TRUE(st.ok());
    ASSERT_DOUBLE_EQ(1e40, column->get_data()[2]);
}

TEST_F(AddNumericColumnTest, test_add_big_integer_trailing_garbage) {
    simdjson::ondemand::parser parser;
    for (std::string_view raw :
         {R"(  { "f_num": 99999999999999999999abc} )", R"(  { "f_num": 123456789012345678901x} )"}) {
        simdjson::padded_string json(raw);

        auto col_i128 = FixedLengthColumn<int128_t>::create();
        TypeDescriptor t(TYPE_LARGEINT);
        auto doc = parser.iterate(json);
        simdjson::ondemand::value val = doc.find_field("f_num");
        auto st = add_numeric_column<int128_t>(col_i128.get(), t, "f_num", &val);
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(st.is_data_quality_error());

        auto col_double = FixedLengthColumn<double>::create();
        doc = parser.iterate(json);
        val = doc.find_field("f_num");
        st = add_numeric_column<double>(col_double.get(), TYPE_DOUBLE_DESC, "f_num", &val);
        ASSERT_FALSE(st.ok());
        ASSERT_TRUE(st.is_data_quality_error());
    }
}

TEST_F(AddNumericColumnTest, test_add_4digit_group_boundaries_and_float_edges) {
    simdjson::ondemand::parser parser;

    // 3-, 4-, 5-, 8-, 12-, 16-, 19-digit signed integers covering simdjson#2873 4-digit group boundaries
    const std::vector<int64_t> expected_ints = {999,
                                                1000,
                                                9999,
                                                10000,
                                                99999999,
                                                100000000,
                                                999999999999LL,
                                                9999999999999999LL,
                                                9223372036854775807LL,
                                                -999,
                                                -1000,
                                                -9999,
                                                -10000,
                                                -99999999,
                                                -999999999999LL,
                                                -9999999999999999LL,
                                                std::numeric_limits<int64_t>::min()};
    auto col_i64 = FixedLengthColumn<int64_t>::create();
    for (int64_t v : expected_ints) {
        std::string raw = R"({"f_int64": )" + std::to_string(v) + "}";
        simdjson::padded_string json(raw);
        auto doc = parser.iterate(json);
        simdjson::ondemand::value val = doc.find_field("f_int64");
        auto st = add_numeric_column<int64_t>(col_i64.get(), TYPE_BIGINT_DESC, "f_int64", &val);
        ASSERT_TRUE(st.ok()) << "v=" << v << " status=" << st;
    }
    ASSERT_EQ(expected_ints.size(), col_i64->size());
    for (size_t i = 0; i < expected_ints.size(); ++i) {
        EXPECT_EQ(expected_ints[i], col_i64->get_data()[i]);
    }

    // Float edge cases (#2876): 0.0, -0.0, 1e-308, DBL_MIN, denormal, DBL_MAX, multi-digit decimals, and overflow
    const std::vector<std::pair<std::string_view, double>> float_cases = {
            {R"({"f_dbl": 0.0})", 0.0},
            {R"({"f_dbl": -0.0})", -0.0},
            {R"({"f_dbl": 1e-308})", 1e-308},
            {R"({"f_dbl": 2.2250738585072014e-308})", 2.2250738585072014e-308},
            {R"({"f_dbl": 5e-324})", 5e-324},
            {R"({"f_dbl": 1.7976931348623157e308})", 1.7976931348623157e308},
            {R"({"f_dbl": 1234.5678})", 1234.5678},
            {R"({"f_dbl": 99999999.125})", 99999999.125},
    };
    auto col_dbl = FixedLengthColumn<double>::create();
    for (const auto& [raw, expected] : float_cases) {
        simdjson::padded_string json(raw);
        auto doc = parser.iterate(json);
        simdjson::ondemand::value val = doc.find_field("f_dbl");
        auto st = add_numeric_column<double>(col_dbl.get(), TYPE_DOUBLE_DESC, "f_dbl", &val);
        ASSERT_TRUE(st.ok()) << "raw=" << raw << " status=" << st;
    }
    ASSERT_EQ(float_cases.size(), col_dbl->size());
    for (size_t i = 0; i < float_cases.size(); ++i) {
        EXPECT_DOUBLE_EQ(float_cases[i].second, col_dbl->get_data()[i]);
    }
    EXPECT_TRUE(std::signbit(col_dbl->get_data()[1]));

    for (std::string_view overflow_raw : {R"({"f_dbl": 1e309})", R"({"f_dbl": 1e500})"}) {
        simdjson::padded_string json(overflow_raw);
        auto doc = parser.iterate(json);
        simdjson::ondemand::value val = doc.find_field("f_dbl");
        auto st = add_numeric_column<double>(col_dbl.get(), TYPE_DOUBLE_DESC, "f_dbl", &val);
        ASSERT_FALSE(st.ok()) << "overflow_raw=" << overflow_raw;
    }
}

} // namespace starrocks
