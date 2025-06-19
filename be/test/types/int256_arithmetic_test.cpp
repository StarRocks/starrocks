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

#include <chrono>
#include <limits>
#include <random>
#include <vector>

#include "types/int256.h"
#include "types/large_int_value.h"

namespace starrocks {

class Int256ArithmeticTest : public testing::Test {
public:
    Int256ArithmeticTest() = default;

protected:
    void SetUp() override {}
    void TearDown() override {}

    static std::vector<int256_t> getTestValues() {
        return {
                int256_t(0),                                   // Zero
                int256_t(1),                                   // One
                int256_t(-1),                                  // Negative one
                int256_t(42),                                  // Small positive
                int256_t(-42),                                 // Small negative
                int256_t(0x7FFFFFFF),                          // 32-bit max
                int256_t(-0x80000000),                         // 32-bit min
                int256_t(0x7FFFFFFFFFFFFFFFLL),                // 64-bit max
                int256_t(-0x8000000000000000LL),               // 64-bit min
                int256_t(static_cast<__int128>(1) << 100),     // Large 128-bit
                int256_t(-(static_cast<__int128>(1) << 100)),  // Large negative 128-bit
                int256_t(1, 0),                                // 2^128
                int256_t(-1, 0),                               // -2^128
                int256_t(0, static_cast<uint128_t>(-1)),       // 2^128 - 1
                INT256_MAX,                                    // Maximum value
                INT256_MIN,                                    // Minimum value
                int256_t(INT256_MAX.high, INT256_MAX.low - 1), // Near maximum
                int256_t(INT256_MIN.high, INT256_MIN.low + 1), // Near minimum
        };
    }
};

// =============================================================================
// Multiplication Edge Cases and Overflow Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_zero_cases) {
    auto test_values = getTestValues();

    for (const auto& value : test_values) {
        // Any number multiplied by zero should be zero
        ASSERT_EQ(int256_t(0), value * int256_t(0)) << "Failed for value: " << value.to_string();
        ASSERT_EQ(int256_t(0), int256_t(0) * value) << "Failed for value: " << value.to_string();

        // Any number multiplied by one should be itself
        ASSERT_EQ(value, value * int256_t(1)) << "Failed for value: " << value.to_string();
        ASSERT_EQ(value, int256_t(1) * value) << "Failed for value: " << value.to_string();
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_sign_handling) {
    std::vector<std::pair<int256_t, int256_t>> sign_test_cases = {
            {int256_t(42), int256_t(13)},   // positive × positive = positive
            {int256_t(-42), int256_t(13)},  // negative × positive = negative
            {int256_t(42), int256_t(-13)},  // positive × negative = negative
            {int256_t(-42), int256_t(-13)}, // negative × negative = positive
    };

    for (const auto& [a, b] : sign_test_cases) {
        const int256_t result = a * b;

        if ((a.high < 0) != (b.high < 0)) {
            ASSERT_TRUE(result.high < 0) << "Expected negative result for " << a.to_string() << " × " << b.to_string()
                                         << " = " << result.to_string();
        } else {
            ASSERT_TRUE(result.high >= 0) << "Expected positive result for " << a.to_string() << " × " << b.to_string()
                                          << " = " << result.to_string();
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_64bit_optimization) {
    const uint64_t a = 0xFFFFFFFF; // 2^32 - 1
    const uint64_t b = 0xFFFFFFFF; // 2^32 - 1

    const int256_t val_a(static_cast<long long>(a));
    const int256_t val_b(static_cast<long long>(b));
    const int256_t result = val_a * val_b;

    const uint128_t expected_low = static_cast<uint128_t>(a) * static_cast<uint128_t>(b);
    ASSERT_EQ(0, result.high);
    ASSERT_EQ(expected_low, result.low);

    const int256_t a_dec(1000000); // 10^6
    const int256_t b_dec(2000000); // 2×10^6
    const int256_t result_dec = a_dec * b_dec;
    const int256_t expected_dec = parse_int256("2000000000000"); // 2×10^12
    ASSERT_EQ(expected_dec, result_dec);

    const int256_t a_large(123456789);
    const int256_t b_large(987654321);
    const int256_t result_large = a_large * b_large;
    const int256_t expected_large = parse_int256("121932631112635269");
    ASSERT_EQ(expected_large, result_large);

    const int256_t near_max(9223372036854775807LL); // 2^63 - 1
    const int256_t small_mult(2);
    const int256_t result_boundary = near_max * small_mult;
    const int256_t expected_boundary = parse_int256("18446744073709551614");
    ASSERT_EQ(expected_boundary, result_boundary);

    const int256_t neg_a(-123456789);
    const int256_t pos_b(1000000);
    const int256_t result_neg = neg_a * pos_b;
    const int256_t expected_neg = parse_int256("-123456789000000");
    ASSERT_EQ(expected_neg, result_neg) << "Failed negative 64-bit multiplication";
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_128bit_optimization) {
    // Test the 128×128 multiplication path
    const uint128_t a = (static_cast<uint128_t>(1) << 64) + 1; // 2^64 + 1
    const uint128_t b = (static_cast<uint128_t>(1) << 64) - 1; // 2^64 - 1

    const int256_t val_a(0, a);
    const int256_t val_b(0, b);
    const int256_t result = val_a * val_b;

    // Expected: (2^64 + 1)(2^64 - 1) = 2^128 - 1
    ASSERT_EQ(0, result.high);
    ASSERT_EQ(static_cast<uint128_t>(-1), result.low);

    const int256_t a_128 = parse_int256("340282366920938463463374607431768211456"); // 2^128
    const int256_t b_128(2);
    const int256_t result_128 = a_128 * b_128;
    const int256_t expected_128 = parse_int256("680564733841876926926749214863536422912"); // 2^129
    ASSERT_EQ(expected_128, result_128);

    const int256_t big_a = parse_int256("123456789012345678901234567890");
    const int256_t big_b = parse_int256("987654321098765432109876543210");
    const int256_t result_big = big_a * big_b;
    const int256_t expected_big = parse_int256("121932631137021795226185032733622923332237463801111263526900");
    ASSERT_EQ(expected_big, result_big);

    const int256_t prime_a = parse_int256("170141183460469231731687303715884105727");
    const int256_t prime_b(3);
    const int256_t result_prime = prime_a * prime_b;
    const int256_t expected_prime = parse_int256("510423550381407695195061911147652317181");
    ASSERT_EQ(expected_prime, result_prime);

    const int256_t boundary_a = parse_int256("18446744073709551616"); // 2^64
    const int256_t boundary_b = parse_int256("18446744073709551615"); // 2^64 - 1
    const int256_t result_boundary = boundary_a * boundary_b;
    const int256_t expected_boundary = parse_int256("340282366920938463444927863358058659840");
    ASSERT_EQ(expected_boundary, result_boundary);

    const int256_t neg_128 = parse_int256("-123456789012345678901234567890");
    const int256_t pos_small(12345);
    const int256_t result_neg_128 = neg_128 * pos_small;
    const int256_t expected_neg_128 = parse_int256("-1524074060357407406035740740602050");
    ASSERT_EQ(expected_neg_128, result_neg_128);
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_64x256_optimization) {
    {
        constexpr uint64_t small = 1000000;
        const int256_t large(0x123456789ABCDEF0LL, 0xFEDCBA9876543210ULL);

        const int256_t result = int256_t(static_cast<long long>(small)) * large;

        ASSERT_NE(int256_t(0), result);
        ASSERT_TRUE(abs(result) > abs(large));
    }

    {
        const int256_t large_base = parse_int256("12345678901234567890123456789012345678901234567890");
        const int256_t multiplier(12345);
        const int256_t result = large_base * multiplier;
        const int256_t expected = parse_int256("152407406035740740603574074060357407406035740740602050");
        ASSERT_EQ(expected, result);
    }

    {
        const int256_t big_a = parse_int256("12345678901234567890123456789012345678");
        const int256_t big_b = parse_int256("98765432109876543210987654321098765432");
        const int256_t result = big_a * big_b;
        const int256_t expected =
                parse_int256("1219326311370217952261850327338667885854747751864349946654322511812221002896");
        ASSERT_EQ(expected, result);
    }

    {
        const int256_t scientific_a = parse_int256("602214076000000000000000000");
        const int256_t scientific_b = parse_int256("299792458000000000");
        const int256_t result = scientific_a * scientific_b;
        const int256_t expected = parse_int256("180539238086238808000000000000000000000000000");
        ASSERT_EQ(expected, result);
    }

    {
        const int256_t mersenne_like = parse_int256("2305843009213693951");
        const int256_t large_prime = parse_int256("1000000000000000000000000000057");
        const int256_t result = mersenne_like * large_prime;
        const int256_t expected = parse_int256("2305843009213693951000000000131433051525180555207");
        ASSERT_EQ(expected, result);
    }

    {
        const int256_t power_base = parse_int256("340282366920938463463374607431768211456"); // 2^128
        const int256_t small_factor(255);                                                    // 2^8 - 1
        const int256_t result = power_base * small_factor;
        const int256_t expected = parse_int256("86772003564839308183160524895100893921280");
        ASSERT_EQ(expected, result);
    }

    {
        const int256_t neg_large = parse_int256("-123456789012345678901234567890123456789");
        const int256_t pos_medium = parse_int256("987654321098765432109876543");
        const int256_t result = neg_large * pos_medium;
        const int256_t expected = parse_int256("-121932631137021795226185032707818930270769699763964487123185200427");
        ASSERT_EQ(expected, result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_128x256_optimization) {
    {
        const uint128_t small = (static_cast<uint128_t>(1) << 100); // 2^100
        const int256_t large(1, 0);                                 // 2^128

        const int256_t val_small(0, small);
        const int256_t result = val_small * large;

        // Expected: 2^100 × 2^128 = 2^228
        // This should fit in 256 bits since 228 < 256
        ASSERT_NE(int256_t(0), result);
        ASSERT_TRUE(result.high > 0);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_overflow_detection) {
    // Case 1: Maximum positive × 2 should overflow to negative
    {
        const int256_t max_val = INT256_MAX;
        const int256_t two(2);
        const int256_t result = max_val * two;

        // Should overflow and become negative
        ASSERT_TRUE(result.high < 0);
    }

    // Case 2: Large positive numbers causing overflow
    {
        const int256_t large1(static_cast<int128_t>((static_cast<uint128_t>(1) << 126)), 0); // 2^254
        const int256_t large2(4);
        const int256_t result = large1 * large2;

        ASSERT_EQ(int256_t(0), result);
    }

    // Case 3: Critical overflow case: -1 × INT256_MIN
    {
        int256_t neg_one(-1);
        int256_t min_val = INT256_MIN;
        int256_t result = neg_one * min_val;

        // This is a critical overflow case in signed arithmetic
        // -1 × (-2^255) = 2^255, which cannot be represented in signed 256-bit
        ASSERT_EQ(INT256_MIN, result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_boundary_values) {
    ASSERT_EQ(0, int256_t(1, 0) * int256_t(1, 0));
    ASSERT_EQ(parse_int256("340282366920938463463374607431768211456"),
              int256_t(0, static_cast<uint128_t>(1) << 127) * int256_t(2));
    ASSERT_EQ(INT256_MAX, INT256_MAX * int256_t(1));
    ASSERT_EQ(parse_int256("-340282366920938463463374607431768211458"),
              int256_t(INT256_MAX.high, INT256_MAX.low / 2) * int256_t(2));
    ASSERT_EQ(INT256_MIN, INT256_MIN * int256_t(1));
    ASSERT_EQ(INT256_MIN, INT256_MIN * int256_t(-1));
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, multiplication_correctness) {
    EXPECT_EQ(int256_t(0), int256_t(123456) * int256_t(0));
    EXPECT_EQ(int256_t(42), int256_t(21) * int256_t(2));
    EXPECT_EQ(int256_t(-100), int256_t(10) * int256_t(-10));

    const int256_t big1 = parse_int256("123456789012345678901");
    const int256_t big2 = parse_int256("987654321098765432109");
    const int256_t expected = parse_int256("121932631137021795225845145533336229232209");
    EXPECT_EQ(expected, big1 * big2);

    EXPECT_EQ(parse_int256("-121932631137021795225845145533336229232209"), big1 * -big2);
    EXPECT_EQ(parse_int256("-121932631137021795225845145533336229232209"), -big1 * big2);
    EXPECT_EQ(expected, -big1 * -big2);

    const int256_t max_times_2 = INT256_MAX * int256_t(2);
    EXPECT_TRUE(max_times_2.high < 0);
}

// =============================================================================
// Division Basic Cases Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_basic_cases) {
    // Zero divided by any non-zero number should be zero
    auto test_values = getTestValues();
    for (const auto& value : test_values) {
        if (value != int256_t(0)) {
            ASSERT_EQ(int256_t(0), int256_t(0) / value);
        }
    }

    // Any number divided by 1 should be itself
    for (const auto& value : test_values) {
        ASSERT_EQ(value, value / int256_t(1));
    }

    // Any number divided by itself should be 1
    for (const auto& value : test_values) {
        if (value != int256_t(0)) {
            ASSERT_EQ(int256_t(1), value / value);
        }
    }

    // Basic division cases
    ASSERT_EQ(int256_t(5), int256_t(10) / int256_t(2));
    ASSERT_EQ(int256_t(3), int256_t(15) / int256_t(5));
    ASSERT_EQ(int256_t(0), int256_t(7) / int256_t(10));
    ASSERT_EQ(int256_t(123), int256_t(123456) / int256_t(1000));
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_zero_exception) {
    auto test_values = getTestValues();

    for (const auto& value : test_values) {
        ASSERT_THROW(value / int256_t(0), std::domain_error)
                << "Division by zero should throw domain_error for value: " << value.to_string();
        break;
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_sign_handling) {
    std::vector<std::tuple<int256_t, int256_t, int256_t>> sign_test_cases = {
            // positive / positive = positive
            {int256_t(100), int256_t(10), int256_t(10)},
            {int256_t(42), int256_t(6), int256_t(7)},

            // negative / positive = negative
            {int256_t(-100), int256_t(10), int256_t(-10)},
            {int256_t(-42), int256_t(6), int256_t(-7)},

            // positive / negative = negative
            {int256_t(100), int256_t(-10), int256_t(-10)},
            {int256_t(42), int256_t(-6), int256_t(-7)},

            // negative / negative = positive
            {int256_t(-100), int256_t(-10), int256_t(10)},
            {int256_t(-42), int256_t(-6), int256_t(7)},
    };

    for (const auto& [dividend, divisor, expected] : sign_test_cases) {
        const int256_t result = dividend / divisor;
        ASSERT_EQ(expected, result) << dividend.to_string() << " / " << divisor.to_string() << " = "
                                    << result.to_string() << ", expected " << expected.to_string();
    }
}

// =============================================================================
// Division Power of Two Optimization Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_power_of_two_optimization) {
    // Test division by powers of 2 (should use shift optimization)
    std::vector<std::pair<int256_t, int>> power_of_two_cases = {
            {int256_t(1024), 2},    // 1024 / 2 = 512
            {int256_t(1024), 4},    // 1024 / 4 = 256
            {int256_t(1024), 8},    // 1024 / 8 = 128
            {int256_t(1024), 16},   // 1024 / 16 = 64
            {int256_t(1024), 32},   // 1024 / 32 = 32
            {int256_t(1024), 64},   // 1024 / 64 = 16
            {int256_t(1024), 128},  // 1024 / 128 = 8
            {int256_t(1024), 256},  // 1024 / 256 = 4
            {int256_t(1024), 512},  // 1024 / 512 = 2
            {int256_t(1024), 1024}, // 1024 / 1024 = 1
    };

    for (const auto& [dividend, power] : power_of_two_cases) {
        const int256_t divisor(power);
        const int256_t expected = dividend / divisor;
        const int256_t shift_result = dividend >> (__builtin_ctz(power));

        ASSERT_EQ(expected, shift_result);

        ASSERT_EQ(int256_t(1024 / power), expected);
    }

    // Test large power of 2 divisions
    const int256_t large_dividend = parse_int256("1208925819614629174706176");          // 2^80
    ASSERT_EQ(parse_int256("604462909807314587353088"), large_dividend / int256_t(2));  // 2^79
    ASSERT_EQ(parse_int256("151115727451828646838272"), large_dividend / int256_t(8));  // 2^77
    ASSERT_EQ(parse_int256("1180591620717411303424"), large_dividend / int256_t(1024)); // 2^70

    // Test negative dividends with power of 2
    const int256_t neg_dividend(-1024);
    ASSERT_EQ(int256_t(-512), neg_dividend / int256_t(2));
    ASSERT_EQ(int256_t(-128), neg_dividend / int256_t(8));
    ASSERT_EQ(int256_t(-32), neg_dividend / int256_t(32));
}

// =============================================================================
// Division Algorithm Branch Tests: Different size combinations
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_small_dividend_various_divisors) {
    // Small dividends (< 64-bit) with various divisor sizes
    std::vector<std::tuple<std::string, std::string, std::string>> cases_small_various = {
            {"98765432", "12345", "8000"},
            {"87654321", "9876", "8875"},
            {"123456789", "456789", "270"},
            {"999888777", "111222", "8990"},

            {"987654321098", "234567890", "4210"},
            {"876543210987", "234567890", "3736"},
            {"765432109876", "456789012", "1675"},

            {"12345678", "98765432109876543210", "0"},
            {"87654321", "87654321098765432109", "0"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : cases_small_various) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected_str, result.to_string());

        if (dividend >= divisor) {
            ASSERT_TRUE(result >= int256_t(1)) << "Small/Various division should be >= 1 when dividend >= divisor";
        } else {
            ASSERT_EQ(int256_t(0), result) << "Small/Various division should be 0 when dividend < divisor";
        }

        const int256_t remainder = dividend % divisor;
        ASSERT_EQ(dividend, result * divisor + remainder) << "Division-modulo relationship failed";
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_medium_dividend_algorithm_branches) {
    // Medium dividends (64-bit range) testing different algorithm paths
    std::vector<std::tuple<std::string, std::string, std::string>> cases_medium_branches = {
            {"9876543210987654", "98765", "100000437513"}, {"8765432109876543", "87654", "100000366325"},
            {"7654321098765432", "76543", "100000275645"}, {"6543210987654321", "65432", "100000167924"},

            {"9876543210987654", "1234567890", "8000000"}, {"8765432109876543", "2345678901", "3736842"},
            {"7654321098765432", "3456789012", "2214286"}, {"6543210987654321", "4567890123", "1432436"},

            {"9876543210987654", "9876543210987655", "0"}, // dividend < divisor
            {"8765432109876543", "8765432109876542", "1"}, // dividend > divisor by 1
            {"7654321098765432", "3827160549382716", "2"}, // dividend = 2 * divisor
            {"6543210987654321", "2181070329218107", "3"}, // dividend ≈ 3 * divisor
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : cases_medium_branches) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected_str, result.to_string());

        const int256_t remainder = dividend % divisor;
        ASSERT_EQ(dividend, result * divisor + remainder)
                << "Division-modulo relationship failed for medium branch test";
        ASSERT_TRUE(abs(remainder) < abs(divisor)) << "Remainder should be smaller than divisor";
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_large_dividend_multi_precision_branches) {
    // Large dividends testing multi-precision division branches
    std::vector<std::tuple<std::string, std::string, std::string>> cases_large_branches = {
            {"12345678901234567890123", "98765", "125000545752387666"},
            {"98765432109876543210987", "43210", "2285707755377841777"},
            {"87654321098765432109876", "87654", "1000003663252851348"},
            {"76543210987654321098765", "32109", "2383855336125519981"},

            {"12345678901234567890123", "9876543210", "1249999988734"},
            {"98765432109876543210987", "4321098765", "22856555122011"},
            {"87654321098765432109876", "8765432109", "10000000001000"},
            {"76543210987654321098765", "3210987654", "23837902613017"},

            {"12345678901234567890123", "1234567890123456", "10000000"},
            {"98765432109876543210987", "9876543210987654", "10000000"},
            {"87654321098765432109876", "8765432109876543", "10000000"},
            {"76543210987654321098765", "7654321098765432", "10000000"},

            {"123456789012345678901234567890", "98765432109876543", "1249999988609"},
            {"987654321098765432109876543210", "87654321098765432", "11267605620787"},
            {"876543210987654321098765432109", "76543210987654321", "11451612751508"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : cases_large_branches) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t result = dividend / divisor;
        ASSERT_EQ(expected_str, result.to_string());
        ASSERT_TRUE(result >= int256_t(0)) << "Large division result should be non-negative";

        const int256_t remainder = dividend % divisor;
        ASSERT_EQ(dividend, result * divisor + remainder)
                << "Division-modulo relationship failed for large branch test";
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_very_large_dividend_edge_branches) {
    // Very large dividends (approaching 256-bit limits)
    std::vector<std::tuple<std::string, std::string, std::string>> cases_very_large = {
            {"1234567890123456789012345678901234567890123456789012345678901234567890", "98765",
             "12500054575238766658354130298195054603251389224816608572661380393"},
            {"9876543210987654321098765432109876543210987654321098765432109876543210", "43210",
             "228570775537784177762063536961580109771140653883848617575378613203"},
            {"8765432109876543210987654321098765432109876543210987654321098765432109", "87654",
             "100000366325285134859648781813708050198620445652348867756418403785"},

            {"1234567890123456789012345678901234567890123456789012345678901234567890", "9876543210987",
             "124999998860945781265568137254382428446850079635501469858"},
            {"9876543210987654321098765432109876543210987654321098765432109876543210", "4321098765432",
             "2285655511972601519541199742901057771651680980952438975705"},
            {"8765432109876543210987654321098765432109876543210987654321098765432109", "8765432109876",
             "1000000000000061971957667926723753525828588270295066217772"},

            {"1234567890123456789012345678901234567890123456789012345678901234567890", "98765432109876543210987",
             "12499999886093750001423910937495500702996996606"},
            {"9876543210987654321098765432109876543210987654321098765432109876543210", "43210987654321098765432",
             "228565551197254340008125806261053145748410388169"},
            {"8765432109876543210987654321098765432109876543210987654321098765432109", "87654321098765432109876",
             "100000000000000000000000619719576679228832299890"},

            {"987654321098765432109876543210987654321098765432109876543210987654321098765",
             "123456789012345678901234567890123456789012345678901234567890", "8000000072900000"},
            {"876543210987654321098765432109876543210987654321098765432109876543210987654",
             "987654321098765432109876543210987654321098765432109876543", "887500001025156249"},
            {"765432109876543210987654321098765432109876543210987654321098765432109876543",
             "876543210987654321098765432109876543210987654321098765432", "873239448188851404"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : cases_very_large) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t result = dividend / divisor;
        ASSERT_EQ(expected_str, result.to_string());
        ASSERT_TRUE(result >= int256_t(0)) << "Very large division result should be non-negative";

        const int256_t remainder = dividend % divisor;
        ASSERT_EQ(dividend, result * divisor + remainder) << "Division-modulo relationship failed for very large case";
        ASSERT_TRUE(abs(remainder) < abs(divisor)) << "Remainder should be smaller than divisor in very large case";
    }
}

// =============================================================================
// Algorithm Branch Boundary Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_algorithm_transition_boundaries) {
    std::vector<std::tuple<std::string, std::string, std::string>> boundary_cases = {

            {"9876543", "9876544", "0"},
            {"123456789", "123456788", "1"},

            {"4294967295", "4294967294", "1"}, // 2^32-1 cases
            {"4294967296", "2147483648", "2"}, // Just over 32-bit
            {"8589934591", "4294967295", "2"}, // 2*2^32-1

            {"18446744073709551615", "9223372036854775808", "1"},  // 2^64-1 / 2^63
            {"18446744073709551616", "9223372036854775808", "2"},  // 2^64 / 2^63
            {"36893488147419103231", "18446744073709551615", "2"}, // (2^65-1) / (2^64-1)

            {"340282366920938463463374607431768211455", "170141183460469231731687303715884105727",
             "2"}, // 2^128-1 / (2^127-1)
            {"340282366920938463463374607431768211456", "170141183460469231731687303715884105728",
             "2"}, // 2^128 / 2^127

            {"99999999999999999999", "10000000000", "9999999999"},                 // 20-digit / 11-digit
            {"999999999999999999999999", "1000000000000", "999999999999"},         // 24-digit / 13-digit
            {"9999999999999999999999999999", "100000000000000", "99999999999999"}, // 28-digit / 15-digit
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : boundary_cases) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t result = dividend / divisor;
        ASSERT_EQ(expected_str, result.to_string());
        const int256_t remainder = dividend % divisor;
        ASSERT_EQ(dividend, result * divisor + remainder);
    }
}

// =============================================================================
// Special Pattern Tests for Algorithm Branches
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_special_patterns_algorithm_stress) {
    // Patterns that might stress different algorithm paths
    std::vector<std::tuple<std::string, std::string, std::string>> pattern_cases = {
            {"1048576000000", "1048576", "1000000"},
            {"2097152000000", "2097152", "1000000"},
            {"4194304000000", "4194304", "1000000"},

            {"123456789000000000", "1000000000", "123456789"},
            {"987654321000000000", "10000000000", "98765432"},
            {"456789123000000000", "100000000000", "4567891"},

            {"11235813213455891442333776109", "1123581321345589", "10000000000000"},
            {"112358132134558914423337761", "11235813213455891", "10000000000"},
            {"1123581321345589144233377", "1123581321345589", "1000000000"},

            {"121212121212121212121212", "121212121212", "1000000000001"},
            {"343434343434343434343434", "343434343434", "1000000000001"},
            {"565656565656565656565656", "565656565656", "1000000000001"},

            {"1000000000000000003000000000000000037", "1000000000000000003", "1000000000000000000"},
            {"9999999999999999989999999999999999989", "9999999999999999989", "1000000000000000000"},
            {"7777777777777777777777777777777777777", "7777777777777777777", "1000000000000000000"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : pattern_cases) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected_str, result.to_string());
        ASSERT_TRUE(result >= int256_t(0)) << "Pattern division result should be non-negative";

        const int256_t remainder = dividend % divisor;
        ASSERT_EQ(dividend, result * divisor + remainder) << "Division-modulo relationship failed for pattern case";
        ASSERT_TRUE(abs(remainder) < abs(divisor)) << "Remainder should be smaller than divisor for pattern case";
    }
}

// =============================================================================
// Division 64-bit Optimization Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_64bit_optimization) {
    std::vector<std::tuple<std::string, uint64_t, std::string>> test_cases_64bit = {
            {"123456789012345678901234567890", 1000000ULL, "123456789012345678901234"},
            {"999999999999999999999999999999", 999999999ULL, "1000000001000000001000"},
            {"340282366920938463463374607431768211456", 1000000000ULL, "340282366920938463463374607431"},
            {"18446744073709551616", 4294967296ULL, "4294967296"},
            {"602214076000000000000000000000000000000000000", 299792458ULL, "2008769933765311734426621232746288767"},
            {"4294967295000000000", 4294967295ULL, "1000000000"},
            {"18446744073709551615000000000000000000", 18446744073709551615ULL, "1000000000000000000"},
    };

    for (const auto& [dividend_str, divisor_val, expected_str] : test_cases_64bit) {
        const int256_t dividend = parse_int256(dividend_str);

        const int256_t divisor = int256_t(0, static_cast<uint128_t>(divisor_val));
        const int256_t expected = parse_int256(expected_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected, result) << "64-bit division failed: " << dividend_str << " / " << divisor_val << " = "
                                    << result.to_string() << ", expected " << expected_str;
    }
}

// =============================================================================
// Division 128-bit Optimization Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_128bit_optimization) {
    // Test 128-bit by 128-bit division
    std::vector<std::tuple<std::string, std::string, std::string>> test_cases_128bit = {
            {"340282366920938463463374607431768211455", "170141183460469231731687303715884105727", "2"},
            {"123456789012345678901234567890", "12345678901234567890", "10000000000"},

            {"1134903170373294346240", "433494437", "2618033989611"},

            {"170141183460469231731687303715884105727", "3", "56713727820156410577229101238628035242"},
            {"340282366920938463463374607431768211455", "7", "48611766702991209066196372490252601636"},

            {"602214076000000000000000000000", "299792458000000", "2008769933765311"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : test_cases_128bit) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t expected = parse_int256(expected_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected, result) << "128-bit division failed: " << dividend_str << " / " << divisor_str << " = "
                                    << result.to_string() << ", expected " << expected_str;
    }
}

// =============================================================================
// Division 256-bit Full Algorithm Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_256bit_full_algorithm) {
    // Test full 256-bit by 256-bit division
    std::vector<std::tuple<std::string, std::string, std::string>> test_cases_256bit = {
            {"12345678901234567890123456789012345678901234567890123456789012345678",
             "987654321098765432109876543210987654321098765432109876543210", "12499999"},

            {"792089237316195423570985008687907853269984665640564039457584007913129639935",
             "792089237316195423570985008687907853269984665640564039457584007913129639934", "1"},

            {INT256_MAX.to_string(), "170141183460469231731687303715884105728",
             "340282366920938463463374607431768211455"},

            {"618970019642690137449562111", "162259276829213363391578010288127", "0"},

            {"162259276829213363391578010288128618970019642690137449562111", "162259276829213363391578010288127",
             "1000000000000000000000000000"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : test_cases_256bit) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t expected = parse_int256(expected_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected, result) << "256-bit division failed: " << dividend_str << " / " << divisor_str << " = "
                                    << result.to_string() << ", expected " << expected_str;
    }
}

// =============================================================================
// Division Boundary Value Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_boundary_values) {
    // Division involving maximum and minimum values
    ASSERT_EQ(int256_t(0), INT256_MAX / (INT256_MAX + int256_t(1))); // This wraps to MIN
    ASSERT_EQ(INT256_MAX, INT256_MAX / int256_t(1));
    ASSERT_EQ(int256_t(-1), INT256_MAX / (-INT256_MAX));

    // Division of minimum value
    ASSERT_EQ(INT256_MIN, INT256_MIN / int256_t(1));
    ASSERT_EQ(INT256_MIN, INT256_MIN / int256_t(-1)); // Special overflow case

    // Near boundary divisions
    const int256_t near_max = int256_t(INT256_MAX.high, INT256_MAX.low - 1);
    ASSERT_EQ(int256_t(0), near_max / INT256_MAX);

    const int256_t half_max = INT256_MAX / int256_t(2);
    ASSERT_EQ(int256_t(2), INT256_MAX / half_max);

    // Powers of 2 near boundaries
    const int256_t power_254 = int256_t(1) << 254; // 2^254
    ASSERT_EQ(int256_t(2), power_254 / (power_254 / int256_t(2)));

    // Large prime-like divisions
    const int256_t large_prime_like = parse_int256("170141183460469231731687303715884105727");
    ASSERT_EQ(int256_t(2), (large_prime_like * int256_t(2)) / large_prime_like);
}

// =============================================================================
// Modulo Operation Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, modulo_basic_cases) {
    // Basic modulo operations
    ASSERT_EQ(int256_t(0), int256_t(10) % int256_t(2));
    ASSERT_EQ(int256_t(1), int256_t(10) % int256_t(3));
    ASSERT_EQ(int256_t(2), int256_t(17) % int256_t(5));
    ASSERT_EQ(int256_t(0), int256_t(100) % int256_t(10));

    // Zero modulo any non-zero number should be zero
    auto test_values = getTestValues();
    for (const auto& value : test_values) {
        if (value != int256_t(0)) {
            ASSERT_EQ(int256_t(0), int256_t(0) % value) << "0 % " << value.to_string() << " should be 0";
        }
    }

    // Any number modulo itself should be zero
    for (const auto& value : test_values) {
        if (value != int256_t(0)) {
            ASSERT_EQ(int256_t(0), value % value) << value.to_string() << " % " << value.to_string() << " should be 0";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, modulo_sign_handling) {
    // Test modulo with different sign combinations
    std::vector<std::tuple<int256_t, int256_t, int256_t>> modulo_sign_cases = {
            {int256_t(17), int256_t(5), int256_t(2)},    // 17 % 5 = 2
            {int256_t(-17), int256_t(5), int256_t(-2)},  // -17 % 5 = -2 (dividend sign)
            {int256_t(17), int256_t(-5), int256_t(2)},   // 17 % -5 = 2 (dividend sign)
            {int256_t(-17), int256_t(-5), int256_t(-2)}, // -17 % -5 = -2 (dividend sign)

            {int256_t(100), int256_t(7), int256_t(2)},    // 100 % 7 = 2
            {int256_t(-100), int256_t(7), int256_t(-2)},  // -100 % 7 = -2
            {int256_t(100), int256_t(-7), int256_t(2)},   // 100 % -7 = 2
            {int256_t(-100), int256_t(-7), int256_t(-2)}, // -100 % -7 = -2
    };

    for (const auto& [dividend, divisor, expected] : modulo_sign_cases) {
        const int256_t result = dividend % divisor;
        ASSERT_EQ(expected, result) << dividend.to_string() << " % " << divisor.to_string() << " = "
                                    << result.to_string() << ", expected " << expected.to_string();
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, modulo_large_numbers) {
    // Test modulo with large numbers
    const int256_t large_dividend = parse_int256("123456789012345678901234567890123456789");
    const int256_t large_divisor = parse_int256("987654321098765432109876543210");
    const int256_t expected_remainder = parse_int256("850308642085030864208626543209");

    ASSERT_EQ(expected_remainder, large_dividend % large_divisor) << "Large number modulo failed";

    // Test with scientific-like constants
    const int256_t avogadro_like = parse_int256("602214076000000000000000000");
    const int256_t planck_like = parse_int256("6626070040000000000000000000000000");
    const int256_t result = planck_like % avogadro_like;

    // Verify: quotient * divisor + remainder = dividend
    const int256_t quotient = planck_like / avogadro_like;
    ASSERT_EQ(planck_like, quotient * avogadro_like + result)
            << "Division-modulo relationship failed for large numbers";
}

// =============================================================================
// Division-Modulo Relationship Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_modulo_relationship) {
    // Verify the fundamental relationship: dividend = quotient * divisor + remainder
    std::vector<std::pair<int256_t, int256_t>> test_pairs = {
            {int256_t(123456), int256_t(789)},
            {int256_t(-123456), int256_t(789)},
            {int256_t(123456), int256_t(-789)},
            {int256_t(-123456), int256_t(-789)},
            {parse_int256("123456789012345678901234567890"), parse_int256("987654321098765432109")},
            {parse_int256("-123456789012345678901234567890"), parse_int256("987654321098765432109")},
            {INT256_MAX / int256_t(3), int256_t(7)},
            {INT256_MIN / int256_t(3), int256_t(11)},
    };

    for (const auto& [dividend, divisor] : test_pairs) {
        const int256_t quotient = dividend / divisor;
        const int256_t remainder = dividend % divisor;
        const int256_t reconstructed = quotient * divisor + remainder;

        ASSERT_EQ(dividend, reconstructed)
                << "Division-modulo relationship failed: " << dividend.to_string() << " != " << quotient.to_string()
                << " * " << divisor.to_string() << " + " << remainder.to_string();

        // Verify remainder is smaller in absolute value than divisor
        ASSERT_TRUE(abs(remainder) < abs(divisor))
                << "Remainder " << remainder.to_string() << " should be smaller than divisor " << divisor.to_string();
    }
}

// =============================================================================
// Division Precision Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_precision_tests) {
    // Test division precision with known exact results
    std::vector<std::tuple<std::string, std::string, std::string>> precision_cases = {
            {"1000000000000000000000000000000", "1000000000000000", "1000000000000000"},
            {"999999999999999999999999999999", "333333333333333333333333333333", "3"},

            {"12345678987654321012345678987654321", "12345678987654321", "1000000000000000001"},

            {"620448401733239439360000", "479001600", "1295295050649600"},
    };

    for (const auto& [dividend_str, divisor_str, expected_str] : precision_cases) {
        const int256_t dividend = parse_int256(dividend_str);
        const int256_t divisor = parse_int256(divisor_str);
        const int256_t expected = parse_int256(expected_str);
        const int256_t result = dividend / divisor;

        ASSERT_EQ(expected, result) << "Precision test failed: " << dividend_str << " / " << divisor_str << " = "
                                    << result.to_string() << ", expected " << expected_str;

        // Verify no remainder for exact divisions
        ASSERT_EQ(int256_t(0), dividend % divisor) << "Should be exact division with no remainder";
    }
}

// NOLINTNEXTLINE
TEST_F(Int256ArithmeticTest, division_truncation_behavior) {
    // Test that division truncates towards zero (not floor division)
    std::vector<std::tuple<int256_t, int256_t, int256_t>> truncation_cases = {
            {int256_t(7), int256_t(3), int256_t(2)},   // 7/3 = 2.33... -> 2
            {int256_t(-7), int256_t(3), int256_t(-2)}, // -7/3 = -2.33... -> -2
            {int256_t(7), int256_t(-3), int256_t(-2)}, // 7/-3 = -2.33... -> -2
            {int256_t(-7), int256_t(-3), int256_t(2)}, // -7/-3 = 2.33... -> 2

            {int256_t(999), int256_t(100), int256_t(9)},   // 999/100 = 9.99 -> 9
            {int256_t(-999), int256_t(100), int256_t(-9)}, // -999/100 = -9.99 -> -9
    };

    for (const auto& [dividend, divisor, expected] : truncation_cases) {
        const int256_t result = dividend / divisor;
        ASSERT_EQ(expected, result) << "Truncation test failed: " << dividend.to_string() << " / "
                                    << divisor.to_string() << " = " << result.to_string() << ", expected "
                                    << expected.to_string();
    }
}

} // end namespace starrocks