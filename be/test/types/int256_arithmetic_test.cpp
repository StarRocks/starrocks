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

} // end namespace starrocks
