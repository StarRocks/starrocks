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

#include "runtime/decimalv3.h"

#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <vector>

#include "runtime/large_int_value.h"
#include "util/logging.h"
namespace starrocks {

class TestDecimalV3 : public ::testing::Test {};

using StringDecimalCastCases = std::vector<std::tuple<std::string, int, int, bool, std::string>>;
template <typename T>
void test_string_decimal_cast(StringDecimalCastCases& cases) {
    for (auto& c : cases) {
        T value;
        auto s = std::get<0>(c);
        auto precision = std::get<1>(c);
        auto scale = std::get<2>(c);
        auto expect_ok = std::get<3>(c);
        auto expect_s = std::get<4>(c);
        auto actual_ok = DecimalV3Cast::from_string<T>(&value, precision, scale, s.data(), s.size());
        auto actual_s = DecimalV3Cast::to_string<T>(value, precision, scale);
        std::cout << "s=" << s << ", precision=" << precision << ", scale=" << scale << ", expect_ok=" << expect_ok
                  << ", expect_s=" << expect_s << ", actual_ok=" << actual_ok << ", actual_s=" << actual_s << std::endl;
        EXPECT_EQ(expect_ok, actual_ok);
        EXPECT_EQ(expect_s, actual_s);
    }
}

TEST_F(TestDecimalV3, testStringDecimal128Cast) {
    StringDecimalCastCases cases = {
            {"3.14", 3, 2, false, "3.14"},
            {"3.14", 3, 2, false, "3.14"},
            {"4.00", 3, 2, false, "4.00"},
            {"0.0123456789012345678901234567890123456", 37, 37, false, "0.0123456789012345678901234567890123456"},
            {"0.01234567890123456789012345678901234567", 38, 38, false, "0.01234567890123456789012345678901234567"},
            {"1234567890123456789012345678901234567", 37, 0, false, "1234567890123456789012345678901234567"},
            {"12345678901234567890123456789012345678", 38, 0, false, "12345678901234567890123456789012345678"},
    };
    test_string_decimal_cast<int128_t>(cases);
}

TEST_F(TestDecimalV3, testStringDecimal128CastWithRounding) {
    StringDecimalCastCases cases = {
            {"360.000000", 10, 2, false, "360.00"},
            {"360.004999", 10, 2, false, "360.00"},
            {"360.005000", 10, 2, false, "360.01"},
            {"360.005001", 10, 2, false, "360.01"},
    };
    test_string_decimal_cast<int128_t>(cases);
}

TEST_F(TestDecimalV3, testStringDecimal64Cast) {
    StringDecimalCastCases cases = {
            {".12", 3, 2, false, "0.12"},
            {"1234567890.00", 12, 2, false, "1234567890.00"},
            {"0.12345678901234567", 17, 17, false, "0.12345678901234567"},
            {"0.123456789012345678", 18, 18, false, "0.123456789012345678"},
            {"1.23456789012345678", 18, 17, false, "1.23456789012345678"},
            {"12.3456789012345678", 18, 16, false, "12.3456789012345678"},
            {"12345678901234.5678", 18, 4, false, "12345678901234.5678"},
            {"12345678901234567.8", 18, 1, false, "12345678901234567.8"},
            {"123456789012345678", 18, 0, false, "123456789012345678"},
            {"123456789012345678.0", 18, 0, false, "123456789012345678"},
    };
    test_string_decimal_cast<int64_t>(cases);
}

TEST_F(TestDecimalV3, testStringDecimal32Cast) {
    StringDecimalCastCases cases = {
            {".12", 9, 2, false, "0.12"},
            {"123456789.00", 9, 0, false, "123456789"},
            {"0.123456789", 9, 9, false, "0.123456789"},
            {"1.23456789", 9, 8, false, "1.23456789"},
            {"12.3456789", 9, 7, false, "12.3456789"},
            {"123.456789", 9, 6, false, "123.456789"},
            {"1234.56789", 9, 5, false, "1234.56789"},
            {"12345.6789", 9, 4, false, "12345.6789"},
            {"123456.789", 9, 3, false, "123456.789"},
            {"1234567.89", 9, 2, false, "1234567.89"},
            {"12345678.9", 9, 1, false, "12345678.9"},
            {"1.00000009", 9, 8, false, "1.00000009"},
    };
    test_string_decimal_cast<int32_t>(cases);
}

using FloatDecimalCaseCases = std::vector<std::tuple<double, int, int, bool, double>>;
template <typename F, typename D>
void test_float_decimal_cast(FloatDecimalCaseCases& cases) {
    for (auto& c : cases) {
        auto float_value = static_cast<F>(std::get<0>(c));
        auto precision = std::get<1>(c);
        auto scale = std::get<2>(c);
        auto expect_overflow = std::get<3>(c);
        auto expect_result = static_cast<F>(std::get<4>(c));
        D dec_value;
        F actual_result;
        DecimalV3Cast::from_float<F, D>(float_value, get_scale_factor<D>(scale), &dec_value);
        DecimalV3Cast::to_float<D, F>(dec_value, get_scale_factor<D>(scale), &actual_result);
        std::cout << "float_value=" << float_value << ", precision=" << precision << ", scale=" << scale
                  << ", expect_overflow=" << expect_overflow << ", expect_result=" << expect_result
                  << ", actual_result=" << actual_result << std::endl;
        ASSERT_FALSE((actual_result != expect_result) ^ expect_overflow);
    }
}

TEST_F(TestDecimalV3, testFloatDecimal32Cast) {
    FloatDecimalCaseCases cases = {
            {3.25, 9, 2, false, 3.25},
            {3.125, 9, 3, false, 3.125},
            {3.0625, 9, 4, false, 3.0625},
            {3.03125, 9, 5, false, 3.03125},
            {3.015625, 9, 6, false, 3.015625},
            {4.0078125, 9, 7, false, 4.0078125},
            {4.00390625, 9, 8, false, 4.00390625},
            {0.001953125, 9, 9, false, 0.001953125},
            {100.001953125, 9, 9, true, 100.001953125},
            {1000.001953125, 9, 9, true, 1000.001953125},
    };
    test_float_decimal_cast<float, int32_t>(cases);
    test_float_decimal_cast<double, int32_t>(cases);
}

TEST_F(TestDecimalV3, testFloatDecimal32CastRound) {
    FloatDecimalCaseCases cases = {
            {3.2519, 9, 2, false, 3.25},
            {3.2549, 9, 2, false, 3.25},
            {3.2550, 9, 2, false, 3.26},
            {-3.2519, 9, 2, false, -3.25},
            {-3.2549, 9, 2, false, -3.25},
            {-3.2550, 9, 2, false, -3.26},
            {100.0019531254, 9, 9, true, 100.001953125},
            {1000.0019531255, 9, 9, true, 1000.001953125},
    };
    test_float_decimal_cast<float, int32_t>(cases);
    test_float_decimal_cast<double, int32_t>(cases);
}

TEST_F(TestDecimalV3, testFloatDecimal64Cast) {
    FloatDecimalCaseCases cases = {
            {3.25, 18, 2, false, 3.25},         {3.125, 18, 3, false, 3.125},
            {3.0625, 18, 4, false, 3.0625},     {3.03125, 18, 5, false, 3.03125},
            {3.25, 18, 16, false, 3.25},        {3.125, 18, 15, false, 3.125},
            {3.0625, 18, 14, false, 3.0625},    {3.03125, 18, 13, false, 3.03125},
            {999.0625, 18, 16, true, 999.0625}, {9999.03125, 18, 15, true, 9999.03125},
    };
    test_float_decimal_cast<float, int64_t>(cases);
    test_float_decimal_cast<double, int64_t>(cases);
}

TEST_F(TestDecimalV3, testFloatDecimal128Cast) {
    FloatDecimalCaseCases cases = {
            {3.25, 38, 2, false, 3.25},
            {3.125, 38, 3, false, 3.125},
            {3.0625, 38, 4, false, 3.0625},
            {3.03125, 38, 5, false, 3.03125},
            {3.015625, 38, 6, false, 3.015625},
            {4.0078125, 38, 7, false, 4.0078125},
            {4.00390625, 38, 8, false, 4.00390625},
            {999.25, 38, 36, true, 999.25},
            {9999.125, 38, 35, true, 9999.125},
            {99999.0625, 38, 34, true, 99999.0625},
            {999999.03125, 38, 33, true, 999999.03125},
    };
    test_float_decimal_cast<float, int128_t>(cases);
    test_float_decimal_cast<double, int128_t>(cases);
}

using IntegerDecimalCastCases = std::vector<std::tuple<int64_t, int, int, bool, int64_t>>;
template <typename I, typename D>
void test_integer_decimal_cast(IntegerDecimalCastCases& cases) {
    for (auto& c : cases) {
        auto int_value = static_cast<I>(std::get<0>(c));
        auto precision = std::get<1>(c);
        auto scale = std::get<2>(c);
        auto expect_overflow = std::get<3>(c);
        auto expect_result = static_cast<I>(std::get<4>(c));
        D dec_value;
        I actual_result;
        auto actual_overflow =
                DecimalV3Cast::from_integer<I, D, true>(int_value, get_scale_factor<D>(scale), &dec_value);
        DecimalV3Cast::to_integer<D, I, true>(dec_value, get_scale_factor<D>(scale), &actual_result);
        std::cout << "int_value=" << int_value << ", precision=" << precision << ", scale=" << scale
                  << ", expect_overflow=" << expect_overflow << ", expect_result=" << expect_result
                  << ", actual_overflow=" << actual_overflow << ", actual_result=" << actual_result << std::endl;
        ASSERT_EQ(actual_overflow, expect_overflow);
        ASSERT_TRUE(actual_overflow || (actual_result == expect_result));
    }
}

TEST_F(TestDecimalV3, testIntDecimal32Cast) {
    IntegerDecimalCastCases cases = {
            {1, 9, 2, false, 1},     {11, 9, 2, false, 11},   {127, 9, 3, false, 127},
            {127, 9, 4, false, 127}, {127, 9, 5, false, 127}, {127, 9, 6, false, 127},
            {127, 9, 7, false, 127}, {127, 9, 8, true, 127},  {127, 9, 9, true, 127},
    };
    test_integer_decimal_cast<int8_t, int32_t>(cases);
    test_integer_decimal_cast<int16_t, int32_t>(cases);
    test_integer_decimal_cast<int32_t, int32_t>(cases);
    test_integer_decimal_cast<int64_t, int32_t>(cases);
    test_integer_decimal_cast<int128_t, int32_t>(cases);
}

TEST_F(TestDecimalV3, testIntDecimal64Cast) {
    IntegerDecimalCastCases cases = {
            {10001, 18, 2, false, 10001},  {21111, 18, 2, false, 21111},  {31234, 18, 3, false, 31234},
            {31234, 18, 13, false, 31234}, {31234, 18, 14, false, 31234}, {31234, 18, 15, true, 31234},
            {31234, 18, 16, true, 31234},  {31234, 18, 17, true, 31234},  {31234, 18, 18, true, 31234},
    };
    test_integer_decimal_cast<int16_t, int64_t>(cases);
    test_integer_decimal_cast<int32_t, int64_t>(cases);
    test_integer_decimal_cast<int64_t, int64_t>(cases);
    test_integer_decimal_cast<int128_t, int64_t>(cases);
}

TEST_F(TestDecimalV3, testIntDecimal128Cast) {
    IntegerDecimalCastCases cases = {
            {123456789, 38, 2, false, 123456789},    {1234567891, 38, 2, false, 1234567891},
            {1234567891, 38, 28, false, 1234567891}, {1234567891, 38, 29, false, 1234567891},
            {1234567891, 38, 30, true, 1234567891},  {1234567891, 38, 31, true, 1234567891},
            {1234567891, 38, 32, true, 1234567891},  {1234567891, 38, 33, true, 1234567891},
            {1234567891, 38, 34, true, 1234567891},  {1234567891, 38, 35, true, 1234567891},
            {1234567891, 38, 36, true, 1234567891},  {1234567891, 38, 37, true, 1234567891},
            {1234567891, 38, 38, true, 1234567891},
    };
    test_integer_decimal_cast<int32_t, int128_t>(cases);
    test_integer_decimal_cast<int64_t, int128_t>(cases);
    test_integer_decimal_cast<int128_t, int128_t>(cases);
}

using DecimalDecimalCastCases = std::vector<std::tuple<std::string, int, int, bool, std::string>>;
template <typename D1, typename D2, bool is_scale_up>
void test_decimal_decimal_cast(DecimalDecimalCastCases& cases) {
    for (auto& c : cases) {
        auto str_decimal = std::get<0>(c);
        auto precision = decimal_precision_limit<D1>;
        auto scale = std::get<1>(c);
        auto adjust_scale = std::get<2>(c);
        auto expect_overflow = std::get<3>(c);
        auto expect_result = std::get<4>(c);
        auto result_precision = decimal_precision_limit<D2>;
        auto result_scale = is_scale_up ? (scale + adjust_scale) : (scale - adjust_scale);
        D1 decimal_value;
        DecimalV3Cast::from_string<D1>(&decimal_value, precision, scale, str_decimal.data(), str_decimal.size());
        bool actual_overflow = false;
        D2 result_value;
        if constexpr (is_scale_up) {
            actual_overflow = DecimalV3Cast::to_decimal<D1, D2, D2, true, true>(
                    decimal_value, get_scale_factor<D2>(adjust_scale), &result_value);
        } else {
            actual_overflow = DecimalV3Cast::to_decimal<D1, D2, D1, false, true>(
                    decimal_value, get_scale_factor<D1>(adjust_scale), &result_value);
        }
        auto actual_result = DecimalV3Cast::to_string<D2>(result_value, result_precision, result_scale);
        std::cout << "str_decimal=" << str_decimal << ", precision=" << precision << ", scale=" << scale
                  << ", adjust_scale=" << adjust_scale << ", expect_overflow=" << expect_overflow
                  << ", expect_result=" << expect_result << ", result_precision=" << result_precision
                  << ", result_scale=" << result_scale << ", actual_overflow=" << actual_overflow
                  << ", actual_result=" << actual_result << std::endl;
        ASSERT_EQ(actual_overflow, expect_overflow);
        ASSERT_TRUE(actual_overflow || (actual_result == expect_result));
    }
}

TEST_F(TestDecimalV3, testNarrowDecimalScaleUpToWidenDecimal) {
    DecimalDecimalCastCases cases0 = {
            {"3.14", 2, 1, false, "3.140"},
            {"3.14", 2, 2, false, "3.1400"},
            {"3.14", 2, 3, false, "3.14000"},
            {"3.14", 2, 4, false, "3.140000"},
            {"3.14", 2, 34, false, "3.140000000000000000000000000000000000"},
            {"3.14", 2, 35, false, "3.1400000000000000000000000000000000000"},
            {"3.14", 2, 36, true, "3.14"},
            {"3.14", 16, 21, false, "3.1400000000000000000000000000000000000"},
            {"3.14", 17, 21, true, "3.14"},
            {"3.14", 16, 22, true, "3.14"},
    };
    test_decimal_decimal_cast<int64_t, int128_t, true>(cases0);
    test_decimal_decimal_cast<int128_t, int128_t, true>(cases0);

    DecimalDecimalCastCases cases1 = {
            {"3.14", 2, 1, false, "3.140"},
            {"3.14", 2, 2, false, "3.1400"},
            {"3.14", 2, 3, false, "3.14000"},
            {"3.14", 2, 4, false, "3.140000"},
            {"3.14", 2, 15, false, "3.14000000000000000"},
            {"3.14", 2, 16, false, "3.140000000000000000"},
            {"3.14", 6, 11, false, "3.14000000000000000"},
            {"3.14", 6, 12, false, "3.140000000000000000"},
    };
    test_decimal_decimal_cast<int32_t, int64_t, true>(cases1);
    test_decimal_decimal_cast<int64_t, int64_t, true>(cases1);

    DecimalDecimalCastCases cases2 = {
            {"3.14", 2, 1, false, "3.140"}, {"3.14", 7, 30, false, "3.1400000000000000000000000000000000000"},
            {"3.14", 7, 31, true, "3.14"},  {"3.14", 6, 32, true, "3.14"},
            {"3.14", 5, 33, true, "3.14"},  {"3.14", 6, 31, false, "3.1400000000000000000000000000000000000"},
    };
    test_decimal_decimal_cast<int32_t, int128_t, true>(cases2);
    test_decimal_decimal_cast<int64_t, int128_t, true>(cases2);
    test_decimal_decimal_cast<int128_t, int128_t, true>(cases2);
}

TEST_F(TestDecimalV3, testNarrowDecimalScaleDownToWidenDecimal) {
    DecimalDecimalCastCases cases = {
            {"3.1415926", 8, 0, false, "3.14159260"}, {"3.1415926", 8, 1, false, "3.1415926"},
            {"3.1415926", 8, 2, false, "3.141593"},   {"3.1415926", 8, 3, false, "3.14159"},
            {"3.1415926", 8, 4, false, "3.1416"},     {"3.1415926", 8, 5, false, "3.142"},
            {"3.1415926", 8, 6, false, "3.14"},       {"3.1415926", 8, 7, false, "3.1"},
            {"3.1415926", 8, 8, false, "3"},
    };
    test_decimal_decimal_cast<int128_t, int128_t, false>(cases);
    test_decimal_decimal_cast<int64_t, int64_t, false>(cases);
    test_decimal_decimal_cast<int32_t, int32_t, false>(cases);
    test_decimal_decimal_cast<int32_t, int64_t, false>(cases);
    test_decimal_decimal_cast<int32_t, int128_t, false>(cases);
    test_decimal_decimal_cast<int64_t, int128_t, false>(cases);
}

TEST_F(TestDecimalV3, testWidenDecimalScaleUpToNarrowDecimal) {
    DecimalDecimalCastCases cases = {
            {"3.1415926", 7, 1, false, "3.14159260"},
            {"3.1415926", 10, 8, false, "3.141592600000000000"},
            {"3.1415926", 11, 7, false, "3.141592600000000000"},
            {"3.1415926", 12, 6, false, "3.141592600000000000"},
    };
    test_decimal_decimal_cast<int128_t, int64_t, true>(cases);
}

TEST_F(TestDecimalV3, testWidenDecimalScaleDownToNarrowDecimal) {
    DecimalDecimalCastCases cases = {
            {"3.1415926", 37, 30, false, "3.1415926"},
            {"3.1415926", 36, 29, false, "3.1415926"},
            {"3.1415926", 36, 30, false, "3.141593"},
            {"3.1415926", 36, 18, false, "3.141592600000000000"},
    };
    test_decimal_decimal_cast<int128_t, int64_t, false>(cases);
}

template <typename T>
using DecimalArithmeticsCases = std::vector<std::tuple<T, T, bool, T>>;

template <typename T>
DecimalArithmeticsCases<T> prepare_add_data() {
    T max_value = get_max<T>();
    T min_value = get_min<T>();
    T half_max_value = max_value / 2;
    T half_min_value = min_value / 2;
    DecimalArithmeticsCases<T> cases = {
            {0, 0, false, 0},
            {half_max_value, -half_max_value, false, 0},
            {half_min_value, -half_min_value, false, 0},
            {half_max_value - 1, half_max_value - 1, false, half_max_value * 2 - 2},
            {max_value - half_max_value, half_max_value, false, max_value},
            {half_min_value + 1, half_min_value + 1, false, half_min_value * 2 + 2},
            {min_value - half_min_value, half_min_value, false, min_value},
            {max_value, 1, true, max_value + 1},
            {max_value, half_max_value, true, max_value + half_max_value},
            {max_value, max_value, true, max_value + max_value},
            {min_value, -1, true, min_value - 1},
            {min_value, half_min_value, true, min_value + half_min_value},
            {min_value, min_value, true, min_value + min_value},
    };
    return cases;
}

template <typename T, typename Func>
void test_decimal_arithmetics(DecimalArithmeticsCases<T>& cases, Func func) {
    int i = 0;
    for (auto& c : cases) {
        auto x = std::get<0>(c);
        auto y = std::get<1>(c);
        auto expect_overflow = std::get<2>(c);
        auto expect_z = std::get<3>(c);
        T actual_z;
        std::cout << "RUN case#" << i << std::endl;
        ++i;
        auto actual_overflow = func(x, y, &actual_z);
        ASSERT_EQ(actual_overflow, expect_overflow);
        ASSERT_EQ(actual_z, expect_z);
    }
}

TEST_F(TestDecimalV3, testDecimalAdd) {
    auto int32_cases = prepare_add_data<int32_t>();
    test_decimal_arithmetics(int32_cases, DecimalV3Arithmetics<int32_t, true>::add);
    auto int64_cases = prepare_add_data<int64_t>();
    test_decimal_arithmetics(int64_cases, DecimalV3Arithmetics<int64_t, true>::add);
    auto int128_cases = prepare_add_data<int128_t>();
    test_decimal_arithmetics(int128_cases, DecimalV3Arithmetics<int128_t, true>::add);
}

template <typename T>
DecimalArithmeticsCases<T> prepare_sub_data() {
    T max_value = get_max<T>();
    T min_value = get_min<T>();
    T half_max_value = max_value / 2;
    T half_min_value = min_value / 2;
    DecimalArithmeticsCases<T> cases = {
            {0, 0, false, 0},
            {half_max_value, half_max_value, false, 0},
            {half_min_value, half_min_value, false, 0},
            {half_max_value - 1, -(half_max_value - 1), false, half_max_value * 2 - 2},
            {max_value - half_max_value, -half_max_value, false, max_value},
            {half_min_value + 1, -(half_min_value + 1), false, half_min_value * 2 + 2},
            {min_value - half_min_value, -half_min_value, false, min_value},
            {max_value, -1, true, max_value + 1},
            {max_value, -half_max_value, true, max_value + half_max_value},
            {max_value, -max_value, true, max_value + max_value},
            {min_value, 1, true, min_value - 1},
            {min_value, -half_min_value, true, min_value + half_min_value},
            {min_value, -min_value, false, min_value + min_value},
    };
    return cases;
}

TEST_F(TestDecimalV3, testDecimalSub) {
    auto int32_cases = prepare_sub_data<int32_t>();
    test_decimal_arithmetics(int32_cases, DecimalV3Arithmetics<int32_t, true>::sub);
    auto int64_cases = prepare_sub_data<int64_t>();
    test_decimal_arithmetics(int64_cases, DecimalV3Arithmetics<int64_t, true>::sub);
    auto int128_cases = prepare_sub_data<int128_t>();
    test_decimal_arithmetics(int128_cases, DecimalV3Arithmetics<int128_t, true>::sub);
}

template <typename T>
DecimalArithmeticsCases<T> prepare_mul_data() {
    T max_value = get_max<T>();
    T min_value = get_min<T>();
    T value0 = static_cast<T>(1) << (sizeof(T) / 2);
    T value1 = value0 / 2;
    DecimalArithmeticsCases<T> cases = {
            {0, 0, false, 0},
            {0, value0, false, 0},
            {value0, 0, false, 0},
            {value0, value0, false, value0 * value0},
            {value1, value1, false, value1 * value1},
            {value1 - 1, value1 - 1, false, (value1 - 1) * (value1 - 1)},
            {value1, value0, false, value1 * value0},
            {value0, value1, false, value0 * value1},
            {value0, value1 >> 1, false, value0 * (value1 >> 1)},
            {max_value, max_value, true, max_value * max_value},
            {min_value, min_value, true, min_value * min_value},

            {value0, -value0, false, -value0 * value0},
            {-value1, value1, false, -value1 * value1},
            {value1 - 1, -(value1 - 1), false, -(value1 - 1) * (value1 - 1)},
            {value1, -value0, false, -value1 * value0},
            {value0, -value1, false, -value0 * value1},
            {value0, -(value1 >> 1), false, -value0 * (value1 >> 1)},
            {max_value, -max_value, true, -max_value * max_value},
            {min_value, -min_value, true, -min_value * min_value},

            {-value0, -value0, false, value0 * value0},
            {-value1, -value1, false, value1 * value1},
            {-(value1 - 1), -(value1 - 1), false, (value1 - 1) * (value1 - 1)},
            {-value1, -value0, false, value1 * value0},
            {-value0, -value1, false, value0 * value1},
            {-value0, -(value1 >> 1), false, value0 * (value1 >> 1)},
            {-max_value, -max_value, true, max_value * max_value},
            {-min_value, -min_value, true, min_value * min_value},
    };
    return cases;
}

TEST_F(TestDecimalV3, testDecimalMul) {
    auto int32_cases = prepare_mul_data<int32_t>();
    test_decimal_arithmetics(int32_cases, DecimalV3Arithmetics<int32_t, true>::mul);
    auto int64_cases = prepare_mul_data<int64_t>();
    test_decimal_arithmetics(int64_cases, DecimalV3Arithmetics<int64_t, true>::mul);
    auto int128_cases = prepare_mul_data<int128_t>();
    test_decimal_arithmetics(int128_cases, DecimalV3Arithmetics<int128_t, true>::mul);
}

TEST_F(TestDecimalV3, testParseHighScaleDecimalString) {
    std::vector<std::tuple<std::string, std::string>> test_cases = {
            {"0.0000000000000000000000000", "0.000000"},
            {"1455434.99999999999999999999", "1455435.000000"},
            {"1000.11233454589877", "1000.112335"},
    };
    for (auto& tc : test_cases) {
        int128_t value;
        auto& s = std::get<0>(tc);
        auto& expect = std::get<1>(tc);
        auto ok = DecimalV3Cast::from_string<int128_t>(&value, 27, 6, s.c_str(), s.size());
        ASSERT_EQ(ok, false);
        ASSERT_EQ(DecimalV3Cast::to_string<int128_t>(value, 27, 6), expect);
    }
}

TEST_F(TestDecimalV3, testParseDecimalStringWithLeadingZeros) {
    std::vector<std::tuple<std::string, std::string, bool>> test_cases = {
            {"0.0000000000000000000123456", "0.000000000", false},
            {"0.0000000000000000000123456e+19", "0.123456000", false},
            {"0.0000000000000000000123456e+18", "0.012345600", false},
            {"0.0000000000000000000123456e+256", "0.000000000", true},
    };
    for (auto& tc : test_cases) {
        int32_t value;
        auto& s = std::get<0>(tc);
        auto& expect = std::get<1>(tc);
        auto& expect_fail = std::get<2>(tc);
        auto fail = DecimalV3Cast::from_string<int32_t>(&value, 9, 9, s.c_str(), s.size());
        ASSERT_EQ(fail, expect_fail);
        if (!fail) {
            ASSERT_EQ(DecimalV3Cast::to_string<int128_t>(value, 9, 9), expect);
        }
    }
}
TEST_F(TestDecimalV3, testFromStringWithOverflowAllowed) {
    std::vector<std::tuple<std::string, std::string, bool>> test_cases = {
            {"1.2", "1.2000000000000000000000000000000000000", false},
            {"9.9999999999999999999999999999999999999", "9.9999999999999999999999999999999999999", false},
            {"-9.9999999999999999999999999999999999999", "-9.9999999999999999999999999999999999999", false},
            {"11.2", "10.0000000000000000000000000000000000000", false},
            {"-1.2", "-1.2000000000000000000000000000000000000", false},
            {"-11.2", "-10.0000000000000000000000000000000000000", false},
            {"1E307", "10.0000000000000000000000000000000000000", false},
            {"-1E307", "-10.0000000000000000000000000000000000000", false},
            {"1.79769e+308", "10.0000000000000000000000000000000000000", false},
            {"-1.79769e+308", "-10.0000000000000000000000000000000000000", false},
            {"-inf", "", true},
            {"+inf", "", true},
            {"abc", "", true}

    };
    for (auto& tc : test_cases) {
        int128_t value;
        auto& s = std::get<0>(tc);
        auto& expect = std::get<1>(tc);
        auto& expect_fail = std::get<2>(tc);
        auto fail = DecimalV3Cast::from_string_with_overflow_allowed<int128_t>(&value, 37, s.c_str(), s.size());
        ASSERT_EQ(fail, expect_fail);
        if (!fail) {
            ASSERT_EQ(DecimalV3Cast::to_string<int128_t>(value, 38, 37), expect);
        }
    }
}

TEST_F(TestDecimalV3, testDecimalToStringWithFraction) {
    std::vector<std::tuple<std::string, std::string>> test_cases = {
            {"1000", "1000.00000"},      {"1000.0", "1000.00000"},     {"1000.00", "1000.00000"},
            {"1000.000", "1000.00000"},  {"1000.0000", "1000.00000"},  {"1000.00000", "1000.00000"},
            {"1000.1", "1000.10000"},    {"1000.01", "1000.01000"},    {"1000.001", "1000.00100"},
            {"1000.0001", "1000.00010"}, {"1000.00001", "1000.00001"},
    };
    for (auto& tc : test_cases) {
        auto& from_s = std::get<0>(tc);
        auto& to_s = std::get<1>(tc);

        int32_t dec32;
        auto fail = DecimalV3Cast::from_string<int32_t>(&dec32, 9, 5, from_s.c_str(), from_s.size());
        ASSERT_FALSE(fail);
        ASSERT_EQ(DecimalV3Cast::to_string<int32_t>(dec32, 9, 5), to_s);

        int64_t dec64;
        fail = DecimalV3Cast::from_string<int64_t>(&dec64, 9, 5, from_s.c_str(), from_s.size());
        ASSERT_FALSE(fail);
        ASSERT_EQ(DecimalV3Cast::to_string<int64_t>(dec64, 9, 5), to_s);

        int128_t dec128;
        fail = DecimalV3Cast::from_string<int128_t>(&dec128, 9, 5, from_s.c_str(), from_s.size());
        ASSERT_FALSE(fail);
        ASSERT_EQ(DecimalV3Cast::to_string<int128_t>(dec128, 9, 5), to_s);
    }
}

TEST_F(TestDecimalV3, testDecimalToStringWithoutFraction) {
    std::vector<std::tuple<std::string, std::string>> test_cases = {
            {"1000", "1000"},      {"1000.0", "1000"},    {"1000.00", "1000"},    {"1000.900", "1001"},
            {"1000.1234", "1000"}, {"1000.5000", "1001"}, {"1000.50000", "1001"}, {"1000.49999", "1000"},
    };
    for (auto& tc : test_cases) {
        auto& from_s = std::get<0>(tc);
        auto& to_s = std::get<1>(tc);

        int32_t dec32;
        auto fail = DecimalV3Cast::from_string<int32_t>(&dec32, 9, 0, from_s.c_str(), from_s.size());
        ASSERT_FALSE(fail);
        ASSERT_EQ(DecimalV3Cast::to_string<int32_t>(dec32, 9, 0), to_s);

        int64_t dec64;
        fail = DecimalV3Cast::from_string<int64_t>(&dec64, 9, 0, from_s.c_str(), from_s.size());
        ASSERT_FALSE(fail);
        ASSERT_EQ(DecimalV3Cast::to_string<int64_t>(dec64, 9, 0), to_s);

        int128_t dec128;
        fail = DecimalV3Cast::from_string<int128_t>(&dec128, 9, 0, from_s.c_str(), from_s.size());
        ASSERT_FALSE(fail);
        ASSERT_EQ(DecimalV3Cast::to_string<int128_t>(dec128, 9, 0), to_s);
    }
}

} // namespace starrocks
