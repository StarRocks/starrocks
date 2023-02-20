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

#include <exprs/arithmetic_operation.h>
#include <gtest/gtest.h>
namespace starrocks {
class ArithmeticOperationTest : public ::testing::Test {};
TEST_F(ArithmeticOperationTest, test_decimal_div_integer) {
    std::vector<std::tuple<std::string, int64_t, std::string>> test_cases{
            {"100.00", 3, "33.33333333"},
            {"99999.99", 99999, "1.00000990"},
            {"99999.99", -99999, "-1.00000990"},
            {"99999.99", 1, "99999.99000000"},
            {"99999.99", -1, "-99999.99000000"},
            {"-99999.99", 99999, "-1.00000990"},
            {"-99999.99", -99999, "1.00000990"},
            {"-99999.99", 1, "-99999.99000000"},
            {"-99999.99", -1, "99999.99000000"},
            {"0", 99999, "0.00000000"},
            {"0", -99999, "0.00000000"},
            {"0", 1, "0.00000000"},
            {"0", -1, "0.00000000"},
            {"0.01", 99999, "0.00000010"},
            {"0.01", -99999, "-0.00000010"},
            {"0.01", 1, "0.01000000"},
            {"0.01", -1, "-0.01000000"},
            {"-0.01", 99999, "-0.00000010"},
            {"-0.01", -99999, "0.00000010"},
            {"-0.01", 1, "-0.01000000"},
            {"-0.01", -1, "0.01000000"},
            {"11335.34", -3263, "-3.47390132"},
            {"32079.78", -60418, "-0.53096395"},
            {"43133.76", 83998, "0.51350937"},
            {"88386.72", 5864, "15.07276944"},
            {"59189.68", 59960, "0.98715277"},
            {"86391.15", -61349, "-1.40819166"},
            {"25836.94", 28147, "0.91792873"},
            {"-6677.44", -94939, "0.07033400"},
            {"-63852.69", -45922, "1.39045969"},
            {"78861.91", -99804, "-0.79016783"},
            {"-34524.83", 87255, "-0.39567738"},
            {"-34611.88", 70648, "-0.48992017"},
            {"98120", -62114, "-1.57967608"},
            {"95315.27", 25100, "3.79742112"},
            {"-56789.47", 35763, "-1.58793921"},
            {"76888.44", -90684, "-0.84787217"},
            {"1444.71", -89273, "-0.01618306"},
            {"47828.74", -99611, "-0.48015520"},
            {"-17496.81", -93048, "0.18804069"},
            {"40103.37", 78941, "0.50801700"},
    };

    for (auto& tc : test_cases) {
        auto& s_dividend = std::get<0>(tc);
        auto& divisor = std::get<1>(tc);
        auto& s_expect_quotient = std::get<2>(tc);
        int64_t dividend = 0;
        DecimalV3Cast::from_string<int64_t>(&dividend, 7, 2, s_dividend.c_str(), s_dividend.size());
        auto quotient = decimal_div_integer<int128_t>(int128_t(dividend), int128_t(divisor), 2);
        auto s_actual_quotient = DecimalV3Cast::to_string<int128_t>(quotient, 38, 8);
        ASSERT_EQ(s_expect_quotient, s_actual_quotient);
    }

    for (auto& tc : test_cases) {
        auto& s_dividend = std::get<0>(tc);
        auto& divisor = std::get<1>(tc);
        auto& s_expect_quotient = std::get<2>(tc);
        int128_t dividend = 0;
        DecimalV3Cast::from_string<int128_t>(&dividend, 7, 2, s_dividend.c_str(), s_dividend.size());
        auto quotient = decimal_div_integer<int128_t>(int128_t(dividend), int128_t(divisor), 2);
        auto s_actual_quotient = DecimalV3Cast::to_string<int128_t>(quotient, 38, 8);
        ASSERT_EQ(s_expect_quotient, s_actual_quotient);
    }
}
} // namespace starrocks
