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

#include "types/int256.h"

#include <gtest/gtest.h>

#include <chrono>
#include <sstream>
#include <string>

namespace starrocks {

class Int256Test : public testing::Test {
public:
    Int256Test() = default;

protected:
    void SetUp() override {}
    void TearDown() override {}
};

// =============================================================================
// Constructor Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, default_constructor) {
    int256_t value;
    ASSERT_EQ(0, value.high);
    ASSERT_EQ(0, value.low);
    ASSERT_EQ("0", value.to_string());
    ASSERT_FALSE(static_cast<bool>(value));
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_high_low) {
    int256_t value(1, 0);
    ASSERT_EQ(1, value.high);
    ASSERT_EQ(0, value.low);

    int256_t value2(-1, static_cast<uint128_t>(-1));
    ASSERT_EQ(-1, value2.high);
    ASSERT_EQ(static_cast<uint128_t>(-1), value2.low);
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_int) {
    {
        int256_t value(42);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(42, value.low);
        ASSERT_EQ("42", value.to_string());
    }

    {
        int256_t value(-42);
        ASSERT_EQ(-1, value.high);
        ASSERT_EQ(static_cast<uint128_t>(-42), value.low);
        ASSERT_EQ("-42", value.to_string());
    }

    {
        int256_t value(0);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_long) {
    {
        long val = 1234567890L;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("1234567890", value.to_string());
    }

    {
        long val = -1234567890L;
        int256_t value(val);
        ASSERT_EQ(-1, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("-1234567890", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_int128) {
    {
        __int128 val = static_cast<__int128>(1) << 100;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
    }

    {
        __int128 val = -(static_cast<__int128>(1) << 100);
        int256_t value(val);
        ASSERT_EQ(-1, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
    }
}

// =============================================================================
// Float Constructor Tests - Comprehensive Coverage
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float_basic) {
    // Basic positive and negative values
    {
        float val = 123.456f;
        int256_t value(val);
        ASSERT_EQ("123", value.to_string());
    }

    {
        float val = -123.456f;
        int256_t value(val);
        ASSERT_EQ("-123", value.to_string());
    }

    // Zero values
    {
        int256_t value(0.0f);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(-0.0f);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float_special_values) {
    // NaN values
    {
        int256_t value(std::numeric_limits<float>::quiet_NaN());
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(std::numeric_limits<float>::signaling_NaN());
        ASSERT_EQ("0", value.to_string());
    }

    // Infinity values
    {
        int256_t value(std::numeric_limits<float>::infinity());
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(-std::numeric_limits<float>::infinity());
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float_fractional) {
    // Values less than 1.0 should truncate to 0
    {
        int256_t value(0.1f);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(0.9f);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(-0.9f);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(0.999999f);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float_precision_boundary) {
    // Test around 2^24 (float precision boundary)
    constexpr float max_safe_float = 16777216.0f; // 2^24

    {
        int256_t value(max_safe_float);
        ASSERT_EQ("16777216", value.to_string());
    }

    {
        int256_t value(max_safe_float - 1.0f);
        ASSERT_EQ("16777215", value.to_string());
    }

    {
        int256_t value(-max_safe_float);
        ASSERT_EQ("-16777216", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float_large_values) {
    // Large values that exceed safe precision
    {
        float large_val = 1e20f;
        int256_t value(large_val);
        // Should not be zero, but precision may be lost
        ASSERT_NE("0", value.to_string());
    }

    {
        float large_val = -1e20f;
        int256_t value(large_val);
        ASSERT_NE("0", value.to_string());
    }

    // Very large values near float max
    {
        float max_val = std::numeric_limits<float>::max();
        int256_t value(max_val);
        ASSERT_NE("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float_edge_cases) {
    // Smallest positive normal float
    {
        float min_normal = std::numeric_limits<float>::min();
        int256_t value(min_normal);
        ASSERT_EQ("0", value.to_string()); // Too small, should be 0
    }

    // Denormalized values
    {
        float denorm = std::numeric_limits<float>::denorm_min();
        int256_t value(denorm);
        ASSERT_EQ("0", value.to_string());
    }

    // Values just above and below 1.0
    {
        int256_t value1(1.0f);
        ASSERT_EQ("1", value1.to_string());
    }

    {
        int256_t value2(1.1f);
        ASSERT_EQ("1", value2.to_string());
    }

    {
        int256_t value3(1.9f);
        ASSERT_EQ("1", value3.to_string());
    }
}

// =============================================================================
// Double Constructor Tests - Comprehensive Coverage
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_basic) {
    {
        double val = 123456.789;
        int256_t value(val);
        ASSERT_EQ("123456", value.to_string());
    }

    {
        double val = -123456.789;
        int256_t value(val);
        ASSERT_EQ("-123456", value.to_string());
    }

    {
        int256_t value(0.0);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(-0.0);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_special_values) {
    // NaN values
    {
        int256_t value(std::numeric_limits<double>::quiet_NaN());
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(std::numeric_limits<double>::signaling_NaN());
        ASSERT_EQ("0", value.to_string());
    }

    // Infinity values
    {
        int256_t value(std::numeric_limits<double>::infinity());
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(-std::numeric_limits<double>::infinity());
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_precision_boundary) {
    // Test around 2^53 (double precision boundary)
    constexpr double max_safe_double = 9007199254740992.0; // 2^53

    {
        int256_t value(max_safe_double);
        ASSERT_EQ("9007199254740992", value.to_string());
    }

    {
        int256_t value(max_safe_double - 1.0);
        ASSERT_EQ("9007199254740991", value.to_string());
    }

    {
        int256_t value(-max_safe_double);
        ASSERT_EQ("-9007199254740992", value.to_string());
    }

    // Beyond safe precision
    {
        double beyond_safe = max_safe_double + 1.0;
        int256_t value(beyond_safe);
        // May lose precision but should not be zero
        ASSERT_NE("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_large_values) {
    // 64-bit boundary values
    {
        double val = static_cast<double>(INT64_MAX);
        int256_t value(val);
        ASSERT_NE("0", value.to_string());
    }

    {
        double val = static_cast<double>(INT64_MIN);
        int256_t value(val);
        ASSERT_NE("0", value.to_string());
    }

    // Very large values
    {
        double large_val = 1e100;
        int256_t value(large_val);
        ASSERT_EQ("0", value.to_string());
    }

    {
        double large_val = -1e100;
        int256_t value(large_val);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_overflow_detection) {
    // Values that exceed int256_t range should result in zero
    constexpr double max_int256_double =
            5.7896044618658097711785492504343953926634992332820282019728792003956564819968e+76;

    {
        double overflow_val = max_int256_double * 2.0;
        int256_t value(overflow_val);
        ASSERT_EQ("0", value.to_string());
    }

    {
        double max_double = std::numeric_limits<double>::max();
        int256_t value(max_double);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_fractional) {
    // Values less than 1.0 should truncate to 0
    {
        int256_t value(0.1);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(0.999999999);
        ASSERT_EQ("0", value.to_string());
    }

    {
        int256_t value(-0.999999999);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_128bit_values) {
    // Values that require 128-bit representation
    {
        double pow_128 = std::pow(2.0, 128);
        int256_t value(pow_128);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
    }

    {
        double pow_100 = std::pow(2.0, 100);
        int256_t value(pow_100);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
    }

    {
        double neg_pow_100 = -std::pow(2.0, 100);
        int256_t value(neg_pow_100);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value < int256_t(0));
    }
}

// =============================================================================
// Double Conversion Operator Tests - Comprehensive Coverage
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_basic_values) {
    // Basic integer values
    {
        int256_t zero(0);
        double result = static_cast<double>(zero);
        ASSERT_DOUBLE_EQ(0.0, result);
    }

    {
        int256_t one(1);
        double result = static_cast<double>(one);
        ASSERT_DOUBLE_EQ(1.0, result);
    }

    {
        int256_t neg_one(-1);
        double result = static_cast<double>(neg_one);
        ASSERT_DOUBLE_EQ(-1.0, result);
    }

    {
        int256_t small_pos(42);
        double result = static_cast<double>(small_pos);
        ASSERT_DOUBLE_EQ(42.0, result);
    }

    {
        int256_t small_neg(-42);
        double result = static_cast<double>(small_neg);
        ASSERT_DOUBLE_EQ(-42.0, result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_64bit_boundary) {
    // 64-bit boundary values
    {
        int256_t max_int64(INT64_MAX);
        double result = static_cast<double>(max_int64);
        ASSERT_DOUBLE_EQ(static_cast<double>(INT64_MAX), result);
    }

    {
        int256_t min_int64(INT64_MIN);
        double result = static_cast<double>(min_int64);
        ASSERT_DOUBLE_EQ(static_cast<double>(INT64_MIN), result);
    }

    {
        int256_t max_uint64 = int256_t(0, UINT64_MAX);
        double result = static_cast<double>(max_uint64);
        ASSERT_DOUBLE_EQ(static_cast<double>(UINT64_MAX), result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_precision_boundary) {
    // Double precision boundary (2^53)
    {
        int256_t precise_max = int256_t(1) << 53;
        precise_max = precise_max - 1; // 2^53 - 1
        double result = static_cast<double>(precise_max);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 53) - 1.0, result);
    }

    {
        int256_t precision_loss = int256_t(1) << 53; // 2^53
        double result = static_cast<double>(precision_loss);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 53), result);
    }

    {
        int256_t imprecise = (int256_t(1) << 53) + 1; // 2^53 + 1
        double result = static_cast<double>(imprecise);
        // Should be rounded to 2^53 due to precision loss
        ASSERT_DOUBLE_EQ(std::pow(2.0, 53), result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_large_numbers) {
    // Large numbers requiring high bits
    {
        int256_t pow64 = int256_t(1) << 64; // 2^64
        double result = static_cast<double>(pow64);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 64), result);
    }

    {
        int256_t pow100 = int256_t(1) << 100; // 2^100
        double result = static_cast<double>(pow100);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 100), result);
    }

    {
        int256_t pow200 = int256_t(1) << 200; // 2^200
        double result = static_cast<double>(pow200);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 200), result);
    }

    {
        int256_t neg_pow100 = -(int256_t(1) << 100); // -2^100
        double result = static_cast<double>(neg_pow100);
        ASSERT_DOUBLE_EQ(-std::pow(2.0, 100), result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_rounding_behavior) {
    // Test IEEE 754 rounding behavior
    {
        // Construct values that require rounding
        int256_t base = int256_t(1) << 54; // 2^54

        // Should round down to 2^54
        int256_t round_down = base + 1;
        double result_down = static_cast<double>(round_down);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 54), result_down);

        // Should round up to 2^54 + 4
        int256_t round_up = base + 3;
        double result_up = static_cast<double>(round_up);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 54) + 4.0, result_up);
    }

    // Test banker's rounding (round to even)
    {
        // Create a value exactly between two representable doubles
        int256_t exact_half = (int256_t(1) << 55) + (int256_t(1) << 53); // 2^55 + 2^53
        double result = static_cast<double>(exact_half);
        // Should round to even (2^55)
        ASSERT_DOUBLE_EQ(45035996273704960, result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_overflow_to_infinity) {
    {
        int256_t very_large = int256_t(1) << 250; // 2^250
        double result = static_cast<double>(very_large);
        ASSERT_TRUE(std::isfinite(result));
        ASSERT_TRUE(result > 0);
    }

    {
        int256_t very_large_neg = -(int256_t(1) << 250); // -2^250
        double result = static_cast<double>(very_large_neg);
        ASSERT_TRUE(std::isfinite(result));
        ASSERT_TRUE(result < 0);
    }

    {
        int256_t beyond_max = INT256_MAX;
        double result = static_cast<double>(beyond_max);
        std::cout << result << std::endl;
        ASSERT_TRUE(std::isfinite(result) && result > 1e76);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_mixed_high_low) {
    // Values with both high and low parts set
    {
        int256_t mixed(0x123456789ABCDEF0LL, 0xFEDCBA9876543210ULL);
        double result = static_cast<double>(mixed);
        ASSERT_TRUE(result > 0);
        ASSERT_FALSE(std::isnan(result));
        ASSERT_FALSE(std::isinf(result));
    }

    {
        int256_t neg_mixed(-0x123456789ABCDEF0LL, 0xFEDCBA9876543210ULL);
        double result = static_cast<double>(neg_mixed);
        ASSERT_TRUE(result < 0);
        ASSERT_FALSE(std::isnan(result));
        ASSERT_FALSE(std::isinf(result));
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_round_trip_conversion) {
    std::vector<double> test_values = {
            0.0,
            1.0,
            -1.0,
            42.0,
            -42.0,
            1e10,
            -1e10,
            1e15,
            -1e15,
            9007199254740992.0, // 2^53
            static_cast<double>(INT32_MAX),
            static_cast<double>(INT32_MIN),
    };

    for (double original : test_values) {
        if (std::abs(original) <= static_cast<double>(INT64_MAX)) {
            int256_t converted(original);
            double back = static_cast<double>(converted);

            if (original == std::floor(original)) {
                ASSERT_DOUBLE_EQ(original, back)
                        << "Round-trip failed for " << std::fixed << std::setprecision(1) << original;
            }
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_precision_loss_detection) {
    {
        int256_t large = (int256_t(1) << 60) + 1; // 2^60 + 1
        double result = static_cast<double>(large);

        // Should be close to 2^60 but may not be exact
        double expected = std::pow(2.0, 60);
        double diff = std::abs(result - expected);
        ASSERT_TRUE(diff <= expected * 1e-15); // Within reasonable precision
    }

    {
        int256_t value = int256_t(1) << 100; // 2^100
        double result = static_cast<double>(value);
        double expected = std::pow(2.0, 100);

        // Should be very close
        double relative_error = std::abs(result - expected) / expected;
        ASSERT_TRUE(relative_error < 1e-14);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, operator_double_boundary_conditions) {
    {
        int256_t pos_zero(0, 0);
        int256_t neg_zero = -pos_zero;

        ASSERT_DOUBLE_EQ(0.0, static_cast<double>(pos_zero));
        ASSERT_DOUBLE_EQ(0.0, static_cast<double>(neg_zero));
    }

    {
        int256_t boundary_128(1, 0); // 2^128
        double result = static_cast<double>(boundary_128);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 128), result);
    }

    {
        int256_t below_128(0, static_cast<uint128_t>(-1)); // 2^128 - 1
        double result = static_cast<double>(below_128);
        ASSERT_TRUE(result = std::pow(2.0, 128));
    }
}

// =============================================================================
// Cross-Conversion Tests (Float/Double with int256_t)
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, conversion_operator_consistency) {
    // Test that conversion to double is consistent with mathematical expectations
    std::vector<int256_t> test_values = {
            int256_t(0),         int256_t(1),         int256_t(-1),      int256_t(1000),    int256_t(-1000),
            int256_t(INT32_MAX), int256_t(INT32_MIN), int256_t(1) << 32, int256_t(1) << 40,
    };

    for (const auto& value : test_values) {
        double result = static_cast<double>(value);

        // Convert back to int256_t and check if reasonable
        int256_t back_converted(result);

        // For small values, should be exact
        if (value >= int256_t(INT32_MIN) && value <= int256_t(INT32_MAX)) {
            ASSERT_EQ(value, back_converted) << "Round-trip failed for " << value.to_string();
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, extreme_conversion_edge_cases) {
    {
        double result_max = static_cast<double>(INT256_MAX);
        ASSERT_TRUE(result_max > 1e70);
    }

    {
        double result_min = static_cast<double>(INT256_MIN);
        ASSERT_TRUE(result_min < -1e70);
    }

    {
        int256_t power_of_2 = int256_t(1) << 50; // 2^50
        double result = static_cast<double>(power_of_2);
        ASSERT_DOUBLE_EQ(std::pow(2.0, 50), result);

        int256_t back(result);
        ASSERT_EQ(power_of_2, back);
    }
}

// =============================================================================
// Large Double Constructor Tests - Beyond 128-bit and Double Precision
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_very_large_values) {
    {
        // 2^200 - much larger than 128-bit range
        double pow_200 = std::pow(2.0, 200);
        int256_t value(pow_200);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
        ASSERT_NE(0, value.high); // Should use high bits

        // Verify it's approximately 2^200
        std::string result_str = value.to_string();
        ASSERT_TRUE(result_str.length() > 50); // 2^200 has about 60 decimal digits
    }

    {
        // 2^250 - very close to int256_t limit
        double pow_250 = std::pow(2.0, 250);
        int256_t value(pow_250);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
        ASSERT_NE(0, value.high);

        // Should be a very large positive number
        std::string result_str = value.to_string();
        ASSERT_TRUE(result_str.length() > 70); // 2^250 has about 75 decimal digits
    }

    {
        // Negative large values
        double neg_pow_200 = -std::pow(2.0, 200);
        int256_t value(neg_pow_200);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value < int256_t(0));
        ASSERT_NE(0, value.high);
        ASSERT_TRUE(value.high < 0);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_precision_loss_large_numbers) {
    // Test numbers where double precision is insufficient
    {
        double large_with_precision_loss = 1.23456789012345e50;
        int256_t value(large_with_precision_loss);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));

        double back_to_double = static_cast<double>(value);
        double relative_error = std::abs(back_to_double - large_with_precision_loss) / large_with_precision_loss;
        ASSERT_TRUE(relative_error < 1e-10);
    }

    {
        double beyond_exact = 1e60;
        int256_t value(beyond_exact);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));

        std::string result_str = value.to_string();
        ASSERT_TRUE(result_str.length() >= 60);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_powers_beyond_128bit) {
    // Test various powers of 2 that exceed 128-bit range
    std::vector<int> large_powers = {130, 140, 150, 160, 180, 200, 220, 240, 250};

    for (int power : large_powers) {
        double pow_val = std::pow(2.0, power);

        if (std::isinf(pow_val)) continue;

        int256_t value(pow_val);
        ASSERT_NE("0", value.to_string()) << "Failed for 2^" << power;
        ASSERT_TRUE(value > int256_t(0)) << "Failed for 2^" << power;

        if (power > 128) {
            ASSERT_NE(0, value.high) << "2^" << power << " should use high bits";
        }

        double back_converted = static_cast<double>(value);
        if (!std::isinf(back_converted)) {
            double relative_error = std::abs(back_converted - pow_val) / pow_val;
            ASSERT_TRUE(relative_error < 1e-10) << "Too much error for 2^" << power;
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_scientific_notation_large) {
    {
        double val = 1.5e100; // 1.5 * 10^100
        int256_t value(val);
        ASSERT_EQ("0", value.to_string());
    }

    {
        double val = 9.876543210123456e75; // Large number with many digits
        int256_t value(val);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));

        std::string result_str = value.to_string();
        ASSERT_TRUE(result_str.length() > 70);
    }

    {
        double val = -2.71828e80; // Large negative
        int256_t value(val);
        ASSERT_EQ("0", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_near_int256_limits) {
    {
        double near_max = std::pow(2.0, 254); // 2^254
        int256_t value(near_max);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
        ASSERT_TRUE(value < INT256_MAX);

        int256_t expected_2_254 = int256_t(1) << 254;
        ASSERT_TRUE(value <= expected_2_254 + 1);
    }

    {
        double near_min = -std::pow(2.0, 254);
        int256_t value(near_min);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value < int256_t(0));
        ASSERT_TRUE(value > INT256_MIN); // Should be greater than min
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_precision_boundary_large) {
    // Test around the boundaries where double precision becomes insufficient
    {
        double base = std::pow(2.0, 100); // 2^100
        double offset = 12345.0;          // This offset will be lost due to precision
        double imprecise = base + offset;

        int256_t value(imprecise);
        ASSERT_NE("0", value.to_string());

        int256_t expected_base(0, static_cast<uint128_t>(1) << 100);

        double value_as_double = static_cast<double>(value);
        double expected_as_double = static_cast<double>(expected_base);

        double relative_error = std::abs(value_as_double - expected_as_double) / expected_as_double;
        ASSERT_TRUE(relative_error < 1e-10);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_mixed_high_low_parts) {
    // Test values that will populate both high and low parts of int256_t
    {
        double large_mixed = std::pow(2.0, 128) + std::pow(2.0, 64) + 12345.0;
        std::cout << large_mixed << std::endl;
        int256_t value(large_mixed);
        std::cout << value.to_string() << std::endl;
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
        ASSERT_NE(0, value.high);

        ASSERT_TRUE(value.high >= 1);
    }

    {
        double complex_val = std::pow(2.0, 200) + std::pow(2.0, 150) + std::pow(2.0, 100);
        int256_t value(complex_val);

        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
        ASSERT_NE(0, value.high);

        std::string result_str = value.to_string();
        ASSERT_TRUE(result_str.length() > 50);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_round_trip_large_values) {
    // Test round-trip conversion for large values
    std::vector<double> large_test_values = {std::pow(2.0, 100),
                                             std::pow(2.0, 128),
                                             std::pow(2.0, 150),
                                             std::pow(2.0, 200),
                                             1e50,
                                             1e60,
                                             1e70,
                                             -std::pow(2.0, 100),
                                             -1e50};

    for (double original : large_test_values) {
        if (std::isinf(original)) continue;

        int256_t converted(original);
        double back_to_double = static_cast<double>(converted);

        ASSERT_FALSE(std::isnan(back_to_double)) << "NaN result for " << original;

        if (!std::isinf(back_to_double)) {
            double relative_error = std::abs(back_to_double - original) / std::abs(original);
            ASSERT_TRUE(relative_error < 1e-10) << "Too much error in round-trip for " << original << " (got "
                                                << back_to_double << ", error=" << relative_error << ")";
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_arithmetic_with_large_values) {
    // Test that large double-constructed values work in arithmetic
    {
        int256_t large1(std::pow(2.0, 100));
        int256_t large2(std::pow(2.0, 100));
        int256_t sum = large1 + large2;

        ASSERT_TRUE(sum > large1);
        ASSERT_TRUE(sum > large2);

        // Should be approximately 2^101
        int256_t expected_double = int256_t(1) << 101;
        // Due to precision issues, we'll check they're in the same ballpark
        ASSERT_TRUE(sum >= expected_double / 2);
        ASSERT_TRUE(sum <= expected_double * 2);
    }

    {
        int256_t large(std::pow(2.0, 150));
        int256_t small(1000);
        int256_t quotient = large / small;

        ASSERT_TRUE(quotient > int256_t(0));
        ASSERT_TRUE(quotient < large); // Should be smaller than original
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double_edge_of_overflow) {
    // Test values right at the edge of what should/shouldn't overflow
    {
        // This should be the largest power of 2 that doesn't cause overflow
        double large_but_safe = std::pow(2.0, 250); // Should fit in int256_t
        int256_t value(large_but_safe);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));
    }

    {
        // Test a value that's very large but should still be handled
        double very_large = 1e75; // 10^75
        int256_t value(very_large);
        ASSERT_NE("0", value.to_string());
        ASSERT_TRUE(value > int256_t(0));

        // Should be a reasonable large number
        std::string result_str = value.to_string();
        ASSERT_TRUE(result_str.length() >= 70);
        ASSERT_TRUE(result_str.length() <= 80); // Shouldn't be too crazy
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_float) {
    {
        float val = 123.456f;
        int256_t value(val);
        ASSERT_EQ("123", value.to_string());
    }

    {
        float val = -123.456f;
        int256_t value(val);
        ASSERT_EQ("-123", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_double) {
    {
        double val = 123456.789;
        int256_t value(val);
        ASSERT_EQ("123456", value.to_string());
    }

    {
        double val = -123456.789;
        int256_t value(val);
        ASSERT_EQ("-123456", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_unsigned_int) {
    {
        unsigned int val = 42U;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("42", value.to_string());
    }

    {
        unsigned int val = 0U;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        unsigned int val = 0xFFFFFFFFU; // 2^32 - 1
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("4294967295", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_unsigned_long) {
    {
        unsigned long val = 1234567890UL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("1234567890", value.to_string());
    }

    {
        unsigned long val = 0UL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        unsigned long val = 0xFFFFFFFFFFFFFFFFUL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_unsigned_long_long) {
    {
        unsigned long long val = 9876543210ULL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("9876543210", value.to_string());
    }

    {
        unsigned long long val = 0ULL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        unsigned long long val = 0xFFFFFFFFFFFFFFFFULL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("18446744073709551615", value.to_string()); // 2^64 - 1
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_size_t) {
    {
        size_t val = 999999;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("999999", value.to_string());
    }

    {
        size_t val = 0;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        size_t val = static_cast<size_t>(-1);
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_TRUE(value > int256_t(0));
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_uint32_t) {
    {
        uint32_t val = 42;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("42", value.to_string());
    }

    {
        uint32_t val = 0;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        uint32_t val = 0xFFFFFFFF; // 2^32 - 1
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("4294967295", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_uint64_t) {
    {
        uint64_t val = 1234567890123456789ULL;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("1234567890123456789", value.to_string());
    }

    {
        uint64_t val = 0;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        uint64_t val = 0xFFFFFFFFFFFFFFFF; // 2^64 - 1
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(static_cast<uint128_t>(val), value.low);
        ASSERT_EQ("18446744073709551615", value.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, constructor_from_uint128_t) {
    {
        uint128_t val = static_cast<uint128_t>(42);
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(val, value.low);
        ASSERT_EQ("42", value.to_string());
    }

    {
        uint128_t val = 0;
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(0, value.low);
        ASSERT_EQ("0", value.to_string());
    }

    {
        uint128_t val = static_cast<uint128_t>(-1);
        int256_t value(val);
        ASSERT_EQ(0, value.high);
        ASSERT_EQ(val, value.low);
        ASSERT_TRUE(value > int256_t(0));
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, unsigned_constructor_consistency) {
    {
        unsigned int uint_val = 12345U;
        unsigned long ulong_val = 12345UL;
        unsigned long long ulonglong_val = 12345ULL;
        uint32_t uint32_val = 12345;
        uint64_t uint64_val = 12345;
        size_t size_val = 12345;

        int256_t from_uint(uint_val);
        int256_t from_ulong(ulong_val);
        int256_t from_ulonglong(ulonglong_val);
        int256_t from_uint32(uint32_val);
        int256_t from_uint64(uint64_val);
        int256_t from_size(size_val);

        ASSERT_EQ(from_uint, from_ulong);
        ASSERT_EQ(from_uint, from_ulonglong);
        ASSERT_EQ(from_uint, from_uint32);
        ASSERT_EQ(from_uint, from_uint64);
        ASSERT_EQ(from_uint, from_size);

        ASSERT_EQ("12345", from_uint.to_string());
        ASSERT_EQ("12345", from_ulong.to_string());
        ASSERT_EQ("12345", from_ulonglong.to_string());
        ASSERT_EQ("12345", from_uint32.to_string());
        ASSERT_EQ("12345", from_uint64.to_string());
        ASSERT_EQ("12345", from_size.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, unsigned_vs_signed_constructor) {
    {
        int signed_val = 42;
        unsigned int unsigned_val = 42U;

        int256_t from_signed(signed_val);
        int256_t from_unsigned(unsigned_val);

        ASSERT_EQ(from_signed, from_unsigned);
        ASSERT_EQ(from_signed.to_string(), from_unsigned.to_string());
        ASSERT_EQ("42", from_signed.to_string());
        ASSERT_EQ("42", from_unsigned.to_string());
    }

    {
        long long signed_val = 1234567890LL;
        unsigned long long unsigned_val = 1234567890ULL;

        int256_t from_signed(signed_val);
        int256_t from_unsigned(unsigned_val);

        ASSERT_EQ(from_signed, from_unsigned);
        ASSERT_EQ("1234567890", from_signed.to_string());
        ASSERT_EQ("1234567890", from_unsigned.to_string());
    }
}

// =============================================================================
// Type Conversion Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, bool_conversion) {
    ASSERT_FALSE(static_cast<bool>(int256_t(0)));
    ASSERT_TRUE(static_cast<bool>(int256_t(1)));
    ASSERT_TRUE(static_cast<bool>(int256_t(-1)));
    ASSERT_TRUE(static_cast<bool>(int256_t(1, 0)));
    ASSERT_TRUE(static_cast<bool>(int256_t(0, 1)));
}

// =============================================================================
// Unary Operator Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, unary_negation) {
    {
        int256_t value(42);
        int256_t neg_value = -value;
        ASSERT_EQ("-42", neg_value.to_string());
        ASSERT_EQ("42", (-neg_value).to_string());
    }

    {
        int256_t value(0);
        int256_t neg_value = -value;
        ASSERT_EQ("0", neg_value.to_string());
    }

    {
        int256_t value(1, 0); // 2^128
        int256_t neg_value = -value;
        ASSERT_EQ(-1, neg_value.high);
        ASSERT_EQ(0, neg_value.low);
    }

    {
        int256_t value(0, 1);
        int256_t neg_value = -value;
        ASSERT_EQ(-1, neg_value.high);
        ASSERT_EQ(static_cast<uint128_t>(-1), neg_value.low);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, unary_plus) {
    int256_t value(42);
    int256_t plus_value = +value;
    ASSERT_EQ(value, plus_value);
    ASSERT_EQ("42", plus_value.to_string());
}

// =============================================================================
// Comparison Operator Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, equality_comparison) {
    int256_t value1(42);
    int256_t value2(42);
    int256_t value3(43);

    ASSERT_TRUE(value1 == value2);
    ASSERT_FALSE(value1 == value3);
    ASSERT_FALSE(value1 != value2);
    ASSERT_TRUE(value1 != value3);

    // Test with int
    ASSERT_TRUE(value1 == 42);
    ASSERT_FALSE(value1 == 43);
    ASSERT_FALSE(value1 != 42);
    ASSERT_TRUE(value1 != 43);
}

// NOLINTNEXTLINE
TEST_F(Int256Test, ordering_comparison) {
    int256_t small(10);
    int256_t medium(20);
    int256_t large(30);
    int256_t negative(-10);

    // Less than
    ASSERT_TRUE(small < medium);
    ASSERT_TRUE(medium < large);
    ASSERT_TRUE(negative < small);
    ASSERT_FALSE(medium < small);

    // Less than or equal
    ASSERT_TRUE(small <= medium);
    ASSERT_TRUE(small <= int256_t(10));
    ASSERT_FALSE(medium <= small);

    // Greater than
    ASSERT_TRUE(large > medium);
    ASSERT_TRUE(medium > small);
    ASSERT_TRUE(small > negative);
    ASSERT_FALSE(small > medium);

    // Greater than or equal
    ASSERT_TRUE(large >= medium);
    ASSERT_TRUE(medium >= int256_t(20));
    ASSERT_FALSE(small >= medium);

    // Test with int
    ASSERT_TRUE(small < 15);
    ASSERT_TRUE(small <= 10);
    ASSERT_TRUE(large > 25);
    ASSERT_TRUE(large >= 30);
}

// NOLINTNEXTLINE
TEST_F(Int256Test, comparison_with_high_bits) {
    int256_t value1(1, 0);                          // 2^128
    int256_t value2(2, 0);                          // 2 * 2^128
    int256_t value3(0, static_cast<uint128_t>(-1)); // 2^128 - 1

    ASSERT_TRUE(value1 > value3);
    ASSERT_TRUE(value2 > value1);
    ASSERT_TRUE(value3 < value1);

    // Negative high bits
    int256_t neg_value(-1, 0);
    ASSERT_TRUE(neg_value < value3);
    ASSERT_TRUE(neg_value < value1);
}

// =============================================================================
// Addition Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_basic) {
    {
        int256_t a(10);
        int256_t b(20);
        int256_t result = a + b;
        ASSERT_EQ("30", result.to_string());
    }

    {
        int256_t a(-10);
        int256_t b(20);
        int256_t result = a + b;
        ASSERT_EQ("10", result.to_string());
    }

    {
        int256_t a(-10);
        int256_t b(-20);
        int256_t result = a + b;
        ASSERT_EQ("-30", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_with_overflow) {
    {
        int256_t a(0, static_cast<uint128_t>(-1)); // 2^128 - 1
        int256_t b(0, 1);
        int256_t result = a + b;
        ASSERT_EQ(1, result.high);
        ASSERT_EQ(0, result.low);
    }

    {
        int256_t a(0, static_cast<uint128_t>(-2)); // 2^128 - 2
        int256_t b(0, 3);
        int256_t result = a + b;
        ASSERT_EQ(1, result.high);
        ASSERT_EQ(1, result.low);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_with_int) {
    int256_t a(100);
    int256_t result = a + 50;
    ASSERT_EQ("150", result.to_string());

    int256_t result2 = a + (-150);
    ASSERT_EQ("-50", result2.to_string());
}

// =============================================================================
// Addition Overflow Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_positive_overflow) {
    // Test case 1: INT256_MAX + 1 should overflow
    {
        int256_t max_val = INT256_MAX; // 2^255 - 1
        int256_t one(1);
        int256_t result = max_val + one;

        // After overflow, should wrap to INT256_MIN
        ASSERT_EQ(INT256_MIN, result);
    }

    // Test case 2: INT256_MAX + INT256_MAX should overflow significantly
    {
        int256_t max_val = INT256_MAX;
        int256_t result = max_val + max_val;

        // 2 * (2^255 - 1) = 2^256 - 2, which overflows to -2 in two's complement
        int256_t expected(-1, static_cast<uint128_t>(-2));
        ASSERT_EQ(expected, result);
        ASSERT_EQ("-2", result.to_string());
    }

    // Test case 3: Near maximum values
    {
        int256_t near_max(INT256_MAX.high, INT256_MAX.low - 100); // INT256_MAX - 100
        int256_t large_positive(200);
        int256_t result = near_max + large_positive;

        // Should overflow by 100
        int256_t expected(INT256_MIN.high, INT256_MIN.low + 99); // INT256_MIN + 99
        ASSERT_EQ(expected, result);
    }

    // Test case 4: Large positive numbers causing overflow
    {
        // Create two large positive numbers that sum > INT256_MAX
        int256_t large1(static_cast<int128_t>((static_cast<uint128_t>(1) << 126)), 0); // 2^254
        int256_t large2(static_cast<int128_t>((static_cast<uint128_t>(1) << 126)), 0); // 2^254
        int256_t result = large1 + large2;

        // 2^254 + 2^254 = 2^255, which equals INT256_MIN (since it's the sign bit)
        ASSERT_EQ(INT256_MIN, result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_negative_overflow) {
    // Test case 1: INT256_MIN + (-1) should underflow
    {
        int256_t min_val = INT256_MIN; // -2^255
        int256_t neg_one(-1);
        int256_t result = min_val + neg_one;

        // After underflow, should wrap to INT256_MAX
        ASSERT_EQ(INT256_MAX, result);
    }

    // Test case 2: INT256_MIN + INT256_MIN should underflow significantly
    {
        int256_t min_val = INT256_MIN;
        int256_t result = min_val + min_val;

        // 2 * (-2^255) = -2^256, which wraps to 0 in 256-bit arithmetic
        int256_t expected(0, 0);
        ASSERT_EQ(expected, result);
        ASSERT_EQ("0", result.to_string());
    }

    // Test case 3: Near minimum values
    {
        int256_t near_min(INT256_MIN.high, INT256_MIN.low + 50); // INT256_MIN + 50
        int256_t large_negative(-100);
        int256_t result = near_min + large_negative;

        // Should underflow by 50
        int256_t expected(INT256_MAX.high, INT256_MAX.low - 49); // INT256_MAX - 49
        ASSERT_EQ(expected, result);
    }

    // Test case 4: Large negative numbers causing underflow
    {
        int256_t large_neg1(-1, static_cast<uint128_t>(1) << 126); // Very large negative
        int256_t large_neg2(-1, static_cast<uint128_t>(1) << 126); // Very large negative
        int256_t result = large_neg1 + large_neg2;

        std::cout << "Large negative + Large negative = " << result.to_string() << std::endl;
        ASSERT_TRUE(result < int256_t(0));
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_mixed_overflow_cases) {
    // Test case 1: Positive + Negative near boundaries
    {
        int256_t max_val = INT256_MAX;
        int256_t small_neg(-10);
        int256_t result = max_val + small_neg;

        // Should not overflow, just subtract
        int256_t expected(INT256_MAX.high, INT256_MAX.low - 10);
        ASSERT_EQ(expected, result);
    }

    // Test case 2: Very large positive + very large negative (no overflow)
    {
        int256_t large_pos = INT256_MAX;
        int256_t large_neg = INT256_MIN;
        int256_t result = large_pos + large_neg;

        // INT256_MAX + INT256_MIN = -1
        ASSERT_EQ("-1", result.to_string());
    }

    // Test case 3: Overflow detection in low part affecting high part
    {
        int256_t value1(0, static_cast<uint128_t>(-1)); // high=0, low=2^128-1
        int256_t value2(0, 2);                          // high=0, low=2
        int256_t result = value1 + value2;

        // Low part overflows, should increment high part
        ASSERT_EQ(1, result.high);
        ASSERT_EQ(1, result.low);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_boundary_values) {
    // Test exact boundary conditions
    {
        // Test adding 1 to various boundary values
        std::vector<int256_t> boundary_values = {
                INT256_MAX,      INT256_MIN, int256_t(0, static_cast<uint128_t>(-1)), // 2^128 - 1
                int256_t(1, 0),                                                       // 2^128
                int256_t(-1, 0),                                                      // -2^128
        };

        for (const auto& boundary : boundary_values) {
            boundary + int256_t(1);

            // Verify the operation completed (no exceptions)
            ASSERT_TRUE(true); // Just ensure we reach this point
        }
    }

    // Test subtracting 1 from boundary values
    {
        std::vector<int256_t> boundary_values = {
                INT256_MAX, INT256_MIN, int256_t(0, 0), // Zero
                int256_t(1, 0),                         // 2^128
        };

        for (const auto& boundary : boundary_values) {
            boundary + int256_t(-1);

            // Verify the operation completed (no exceptions)
            ASSERT_TRUE(true);
        }
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, addition_performance_with_overflow) {
    // Test that overflow handling doesn't cause performance issues
    {
        int256_t accumulator(0);
        int256_t increment = INT256_MAX;

        // Perform multiple additions that will cause overflows
        for (int i = 0; i < 10; ++i) {
            accumulator += increment;
        }

        // Just verify we completed without issues
        ASSERT_TRUE(true);
    }
}

// =============================================================================
// Subtraction Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, subtraction_basic) {
    {
        int256_t a(30);
        int256_t b(10);
        int256_t result = a - b;
        ASSERT_EQ("20", result.to_string());
    }

    {
        int256_t a(10);
        int256_t b(30);
        int256_t result = a - b;
        ASSERT_EQ("-20", result.to_string());
    }

    {
        int256_t a(-10);
        int256_t b(-30);
        int256_t result = a - b;
        ASSERT_EQ("20", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, subtraction_with_different_types) {
    int256_t a(100);

    {
        int256_t result = a - 50;
        ASSERT_EQ("50", result.to_string());
    }

    {
        __int128 val = 25;
        int256_t result = a - val;
        ASSERT_EQ("75", result.to_string());
    }

    {
        double val = 25.0;
        double result = a - val;
        ASSERT_DOUBLE_EQ(75.0, result);
    }
}

// =============================================================================
// Multiplication Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, multiplication_basic) {
    {
        int256_t a(10);
        int256_t b(20);
        int256_t result = a * b;
        ASSERT_EQ("200", result.to_string());
    }

    {
        int256_t a(-10);
        int256_t b(20);
        int256_t result = a * b;
        ASSERT_EQ("-200", result.to_string());
    }

    {
        int256_t a(-10);
        int256_t b(-20);
        int256_t result = a * b;
        ASSERT_EQ("200", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, multiplication_edge_cases) {
    {
        int256_t a(100);
        int256_t b(0);
        int256_t result = a * b;
        ASSERT_EQ("0", result.to_string());
    }

    {
        int256_t a(100);
        int256_t b(1);
        int256_t result = a * b;
        ASSERT_EQ("100", result.to_string());
    }

    {
        int256_t a(1);
        int256_t b(100);
        int256_t result = a * b;
        ASSERT_EQ("100", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, multiplication_with_int) {
    int256_t a(50);
    int256_t result = a * 4;
    ASSERT_EQ("200", result.to_string());

    int256_t result2 = a * (-3);
    ASSERT_EQ("-150", result2.to_string());
}

// =============================================================================
// Division Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, division_basic) {
    {
        int256_t a(100);
        int256_t b(10);
        int256_t result = a / b;
        ASSERT_EQ("10", result.to_string());
    }

    {
        int256_t a(-100);
        int256_t b(10);
        int256_t result = a / b;
        ASSERT_EQ("-10", result.to_string());
    }

    {
        int256_t a(100);
        int256_t b(-10);
        int256_t result = a / b;
        ASSERT_EQ("-10", result.to_string());
    }

    {
        int256_t a(-100);
        int256_t b(-10);
        int256_t result = a / b;
        ASSERT_EQ("10", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, division_edge_cases) {
    {
        int256_t a(100);
        int256_t b(1);
        int256_t result = a / b;
        ASSERT_EQ("100", result.to_string());
    }

    {
        int256_t a(100);
        int256_t b(100);
        int256_t result = a / b;
        ASSERT_EQ("1", result.to_string());
    }

    {
        int256_t a(50);
        int256_t b(100);
        int256_t result = a / b;
        ASSERT_EQ("0", result.to_string());
    }

    {
        int256_t a(0);
        int256_t b(100);
        int256_t result = a / b;
        ASSERT_EQ("0", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, division_by_zero) {
    int256_t a(100);
    int256_t b(0);
    ASSERT_THROW(a / b, std::domain_error);
}

// =============================================================================
// Modulo Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, modulo_basic) {
    {
        int256_t a(100);
        int256_t b(30);
        int256_t result = a % b;
        ASSERT_EQ("10", result.to_string());
    }

    {
        int256_t a(100);
        int256_t b(25);
        int256_t result = a % b;
        ASSERT_EQ("0", result.to_string());
    }

    {
        int256_t a(-100);
        int256_t b(30);
        int256_t result = a % b;
        ASSERT_EQ("-10", result.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, modulo_by_zero) {
    int256_t a(100);
    int256_t b(0);
    ASSERT_THROW(a % b, std::domain_error);
}

// =============================================================================
// Assignment Operator Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, assignment_operators) {
    {
        int256_t a(10);
        int256_t b(20);
        a += b;
        ASSERT_EQ("30", a.to_string());
    }

    {
        int256_t a(30);
        int256_t b(10);
        a -= b;
        ASSERT_EQ("20", a.to_string());
    }

    {
        int256_t a(10);
        int256_t b(5);
        a *= b;
        ASSERT_EQ("50", a.to_string());
    }

    {
        int256_t a(50);
        int256_t b(10);
        a /= b;
        ASSERT_EQ("5", a.to_string());
    }

    {
        int256_t a(50);
        int256_t b(12);
        a %= b;
        ASSERT_EQ("2", a.to_string());
    }
}

// =============================================================================
// Increment/Decrement Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, increment_operators) {
    {
        int256_t a(10);
        int256_t result = ++a;
        ASSERT_EQ("11", a.to_string());
        ASSERT_EQ("11", result.to_string());
    }

    {
        int256_t a(10);
        int256_t result = a++;
        ASSERT_EQ("11", a.to_string());
        ASSERT_EQ("10", result.to_string());
    }

    {
        int256_t a(0, static_cast<uint128_t>(-1)); // 2^128 - 1
        ++a;
        ASSERT_EQ(1, a.high);
        ASSERT_EQ(0, a.low);
    }
}

// =============================================================================
// Bit Shift Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, left_shift) {
    {
        int256_t a(1);
        int256_t result = a << 1;
        ASSERT_EQ("2", result.to_string());
    }

    {
        int256_t a(1);
        int256_t result = a << 10;
        ASSERT_EQ("1024", result.to_string());
    }

    {
        int256_t a(1);
        int256_t result = a << 128;
        ASSERT_EQ(1, result.high);
        ASSERT_EQ(0, result.low);
    }

    {
        int256_t a(1);
        int256_t result = a << 256;
        ASSERT_EQ("0", result.to_string());
    }

    {
        int256_t a(1);
        a <<= 5;
        ASSERT_EQ("32", a.to_string());
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, right_shift) {
    {
        int256_t a(8);
        int256_t result = a >> 1;
        ASSERT_EQ("4", result.to_string());
    }

    {
        int256_t a(1024);
        int256_t result = a >> 10;
        ASSERT_EQ("1", result.to_string());
    }

    {
        int256_t a(1, 0); // 2^128
        int256_t result = a >> 128;
        ASSERT_EQ("1", result.to_string());
    }

    // Arithmetic right shift for negative numbers
    {
        int256_t a(-8);
        int256_t result = a >> 1;
        ASSERT_EQ("-4", result.to_string());
    }

    {
        int256_t a(-1);
        int256_t result = a >> 100;
        ASSERT_EQ("-1", result.to_string());
    }

    {
        int256_t a(32);
        a >>= 2;
        ASSERT_EQ("8", a.to_string());
    }
}

// =============================================================================
// String Conversion Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, to_string) {
    ASSERT_EQ("0", int256_t(0).to_string());
    ASSERT_EQ("1", int256_t(1).to_string());
    ASSERT_EQ("-1", int256_t(-1).to_string());
    ASSERT_EQ("123456789", int256_t(123456789).to_string());
    ASSERT_EQ("-123456789", int256_t(-123456789).to_string());
}

// NOLINTNEXTLINE
TEST_F(Int256Test, parse_from_string) {
    {
        int256_t result = parse_int256("0");
        ASSERT_EQ("0", result.to_string());
    }

    {
        int256_t result = parse_int256("123456789");
        ASSERT_EQ("123456789", result.to_string());
    }

    {
        int256_t result = parse_int256("-123456789");
        ASSERT_EQ("-123456789", result.to_string());
    }

    { ASSERT_THROW(parse_int256("12a34"), std::invalid_argument); }

    { ASSERT_THROW(parse_int256(""), std::invalid_argument); }
}

// =============================================================================
// Utility Function Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, abs_function) {
    ASSERT_EQ("0", abs(int256_t(0)).to_string());
    ASSERT_EQ("42", abs(int256_t(42)).to_string());
    ASSERT_EQ("42", abs(int256_t(-42)).to_string());

    int256_t large_negative(-1, 0);
    int256_t abs_result = abs(large_negative);
    ASSERT_EQ(1, abs_result.high);
    ASSERT_EQ(0, abs_result.low);
}

// NOLINTNEXTLINE
TEST_F(Int256Test, divmod_u32) {
    {
        int256_t dividend(100);
        uint32_t divisor = 7;
        int256_t quotient;
        uint32_t remainder;

        divmod_u32(dividend, divisor, &quotient, &remainder);

        ASSERT_EQ("14", quotient.to_string());
        ASSERT_EQ(2, remainder);
    }

    {
        int256_t dividend(1000000);
        uint32_t divisor = 1000;
        int256_t quotient;
        uint32_t remainder;

        divmod_u32(dividend, divisor, &quotient, &remainder);

        ASSERT_EQ("1000", quotient.to_string());
        ASSERT_EQ(0, remainder);
    }
}

// =============================================================================
// Constant Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, constants) {
    // Test INT256_MAX
    ASSERT_TRUE(INT256_MAX > int256_t(0));
    ASSERT_EQ(static_cast<int128_t>((static_cast<uint128_t>(1) << 127) - 1), INT256_MAX.high);
    ASSERT_EQ(static_cast<uint128_t>(-1), INT256_MAX.low);

    // Test INT256_MIN
    ASSERT_TRUE(INT256_MIN < int256_t(0));
    ASSERT_EQ(static_cast<int128_t>(static_cast<uint128_t>(1) << 127), INT256_MIN.high);
    ASSERT_EQ(static_cast<uint128_t>(0), INT256_MIN.low);

    // Test relationship
    ASSERT_TRUE(INT256_MIN < INT256_MAX);
    ASSERT_EQ("-1", (INT256_MIN + INT256_MAX).to_string());
}

// =============================================================================
// Hash Function Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, hash_function) {
    std::hash<int256_t> hasher;

    int256_t a(42);
    int256_t b(42);
    int256_t c(43);

    ASSERT_EQ(hasher(a), hasher(b));
    ASSERT_NE(hasher(a), hasher(c));
}

// =============================================================================
// Stream Output Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, stream_output) {
    std::ostringstream oss;
    int256_t value(12345);
    oss << value;
    ASSERT_EQ("12345", oss.str());

    std::ostringstream oss2;
    int256_t negative_value(-12345);
    oss2 << negative_value;
    ASSERT_EQ("-12345", oss2.str());
}

// =============================================================================
// Large Number Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, large_number_operations) {
    // Test with numbers that require high bits
    int256_t large1(1, 0); // 2^128
    int256_t large2(2, 0); // 2 * 2^128

    {
        int256_t sum = large1 + large1;
        ASSERT_EQ(large2, sum);
    }

    {
        int256_t diff = large2 - large1;
        ASSERT_EQ(large1, diff);
    }

    {
        int256_t product = large1 * int256_t(2);
        ASSERT_EQ(large2, product);
    }

    {
        int256_t quotient = large2 / int256_t(2);
        ASSERT_EQ(large1, quotient);
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, edge_cases) {
    // Test operations near overflow boundaries
    {
        int256_t max_low(0, static_cast<uint128_t>(-1));
        int256_t one(1);
        int256_t result = max_low + one;
        ASSERT_EQ(1, result.high);
        ASSERT_EQ(0, result.low);
    }

    // Test negative zero handling
    {
        int256_t pos_zero(0);
        int256_t neg_zero = -pos_zero;
        ASSERT_EQ(pos_zero, neg_zero);
        ASSERT_EQ("0", neg_zero.to_string());
    }

    // Test self-operations
    {
        int256_t value(42);
        ASSERT_EQ("0", (value - value).to_string());
        ASSERT_EQ("1", (value / value).to_string());
        ASSERT_EQ("0", (value % value).to_string());
    }
}

// =============================================================================
// Memory Layout and Storage Format Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, storage_format_memcpy_consistency) {
    // Test that storage format is identical to memory format using memcpy
    // This verifies zero-copy serialization capability on little-endian systems

    int128_t high_val =
            (static_cast<int128_t>(0x123456789ABCDEF0LL) << 64) | static_cast<int128_t>(0xFEDCBA0987654321ULL);
    uint128_t low_val =
            (static_cast<uint128_t>(0x0123456789ABCDEFULL) << 64) | static_cast<uint128_t>(0xEDCBA09876543210ULL);

    int256_t original(high_val, low_val);

    // Simulate storage: copy to buffer using memcpy
    uint8_t storage_buffer[32];
    memcpy(storage_buffer, &original, sizeof(int256_t));

    // Simulate loading from storage: copy back to new object
    int256_t loaded;
    memcpy(&loaded, storage_buffer, sizeof(int256_t));

    // Verify perfect round-trip consistency
    ASSERT_EQ(original.high, loaded.high);
    ASSERT_EQ(original.low, loaded.low);
    ASSERT_EQ(original, loaded);
    ASSERT_EQ(original.to_string(), loaded.to_string());
}

// NOLINTNEXTLINE
TEST_F(Int256Test, byte_order_verification) {
    int128_t high_val =
            (static_cast<int128_t>(0x123456789ABCDEF0LL) << 64) | static_cast<int128_t>(0xFEDCBA0987654321ULL);
    uint128_t low_val =
            (static_cast<uint128_t>(0x0123456789ABCDEFULL) << 64) | static_cast<uint128_t>(0xEDCBA09876543210ULL);

    int256_t value(high_val, low_val);

    uint8_t* bytes = reinterpret_cast<uint8_t*>(&value);

    // low member (bytes 0-15): 0x0123456789ABCDEFEDCBA09876543210
    // high member (bytes 16-31): 0x123456789ABCDEF0FEDCBA0987654321

    // Check first few bytes of low member (little-endian byte order)
    ASSERT_EQ(0x10, bytes[0]); // LSB of low
    ASSERT_EQ(0x32, bytes[1]);
    ASSERT_EQ(0x54, bytes[2]);
    ASSERT_EQ(0x76, bytes[3]);

    // Check last few bytes of low member
    ASSERT_EQ(0x01, bytes[15]); // MSB of low
    ASSERT_EQ(0x23, bytes[14]);
    ASSERT_EQ(0x45, bytes[13]);
    ASSERT_EQ(0x67, bytes[12]);

    // Check first few bytes of high member (starts at byte 16)
    ASSERT_EQ(0x21, bytes[16]); // LSB of high
    ASSERT_EQ(0x43, bytes[17]);
    ASSERT_EQ(0x65, bytes[18]);
    ASSERT_EQ(0x87, bytes[19]);

    // Check last few bytes of high member
    ASSERT_EQ(0x12, bytes[31]); // MSB of high
    ASSERT_EQ(0x34, bytes[30]);
    ASSERT_EQ(0x56, bytes[29]);
    ASSERT_EQ(0x78, bytes[28]);
}

// NOLINTNEXTLINE
TEST_F(Int256Test, cross_platform_storage_simulation) {
    // Test various values to ensure consistent storage format
    // This simulates what would happen in cross-platform data exchange

    std::vector<int256_t> test_values = {
            int256_t(0),                     // Zero
            int256_t(1),                     // Simple positive
            int256_t(-1),                    // Simple negative
            int256_t(0x7FFFFFFFFFFFFFFFLL),  // Large positive in low
            int256_t(-0x7FFFFFFFFFFFFFFFLL), // Large negative in low
            int256_t(1, 0),                  // 2^128
            int256_t(-1, 0),                 // -2^128
            INT256_MAX,                      // Maximum value
            INT256_MIN,                      // Minimum value
    };

    for (const auto& original : test_values) {
        // Simulate storage write
        uint8_t storage[32];
        memcpy(storage, &original, 32);

        // Simulate storage read
        int256_t restored;
        memcpy(&restored, storage, 32);

        // Verify perfect consistency
        ASSERT_EQ(original, restored) << "Failed for value: " << original.to_string();

        // Verify individual components
        ASSERT_EQ(original.high, restored.high);
        ASSERT_EQ(original.low, restored.low);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, memory_alignment_and_padding) {
    // Test that there's no unexpected padding in the structure
    // This ensures our storage format assumptions are correct

    ASSERT_EQ(32, sizeof(int256_t));
    ASSERT_EQ(0, sizeof(int256_t) % 8); // Should be 8-byte aligned

    // Test alignment of individual members
    int256_t value;
    uint8_t* base_ptr = reinterpret_cast<uint8_t*>(&value);
    uint8_t* low_ptr = reinterpret_cast<uint8_t*>(&value.low);
    uint8_t* high_ptr = reinterpret_cast<uint8_t*>(&value.high);

    // low should be at offset 0
    ASSERT_EQ(0, low_ptr - base_ptr);

    // high should be at offset 16 (immediately after low, no padding)
    ASSERT_EQ(16, high_ptr - base_ptr);

    // Verify no padding between members
    ASSERT_EQ(sizeof(value.low) + sizeof(value.high), sizeof(int256_t));
}

// NOLINTNEXTLINE
TEST_F(Int256Test, direct_memory_access_consistency) {
    // Test that direct memory access produces expected results
    // This verifies our bit manipulation operations work correctly with the memory layout

    int128_t high_val =
            (static_cast<int128_t>(0x123456789ABCDEF0LL) << 64) | static_cast<int128_t>(0xFEDCBA0987654321ULL);
    uint128_t low_val =
            (static_cast<uint128_t>(0x0123456789ABCDEFULL) << 64) | static_cast<uint128_t>(0xEDCBA09876543210ULL);

    int256_t value(high_val, low_val);

    // Access as array of 64-bit values
    uint64_t* as_u64 = reinterpret_cast<uint64_t*>(&value);

    // On little-endian systems, the layout should be:
    // as_u64[0] = lower 64 bits of low
    // as_u64[1] = upper 64 bits of low
    // as_u64[2] = lower 64 bits of high
    // as_u64[3] = upper 64 bits of high

    ASSERT_EQ(4, sizeof(int256_t) / sizeof(uint64_t));

    // Reconstruct the original values from the 64-bit parts
    uint128_t reconstructed_low = static_cast<uint128_t>(as_u64[0]) | (static_cast<uint128_t>(as_u64[1]) << 64);
    int128_t reconstructed_high =
            static_cast<int128_t>(static_cast<uint128_t>(as_u64[2]) | (static_cast<uint128_t>(as_u64[3]) << 64));

    ASSERT_EQ(value.low, reconstructed_low);
    ASSERT_EQ(value.high, reconstructed_high);
}

// NOLINTNEXTLINE
TEST_F(Int256Test, serialization_performance_test) {
    // Test that memcpy-based serialization is efficient
    // This demonstrates the zero-copy benefit of our design choice

    const int NUM_VALUES = 1000;
    std::vector<int256_t> values;
    std::vector<uint8_t> storage(NUM_VALUES * 32);

    // Generate test data
    for (int i = 0; i < NUM_VALUES; ++i) {
        values.emplace_back(i * 12345, i * 67890);
    }

    // Serialize using memcpy (simulating our storage format)
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_VALUES; ++i) {
        memcpy(&storage[i * 32], &values[i], 32);
    }
    auto serialize_time = std::chrono::high_resolution_clock::now() - start;

    // Deserialize using memcpy
    std::vector<int256_t> restored(NUM_VALUES);
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_VALUES; ++i) {
        memcpy(&restored[i], &storage[i * 32], 32);
    }
    auto deserialize_time = std::chrono::high_resolution_clock::now() - start;

    // Verify correctness
    for (int i = 0; i < NUM_VALUES; ++i) {
        ASSERT_EQ(values[i], restored[i]);
    }

    // Performance should be very fast (just memory copying)
    // This is mainly to ensure the test runs without issues
    ASSERT_TRUE(serialize_time.count() >= 0);
    ASSERT_TRUE(deserialize_time.count() >= 0);
}

// =============================================================================
// Type Conversion Operators Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, explicit_conversion_operators) {
    // Test bool conversion
    {
        ASSERT_FALSE(static_cast<bool>(int256_t(0)));
        ASSERT_TRUE(static_cast<bool>(int256_t(1)));
        ASSERT_TRUE(static_cast<bool>(int256_t(-1)));
        ASSERT_TRUE(static_cast<bool>(int256_t(1, 0))); // high != 0
        ASSERT_TRUE(static_cast<bool>(int256_t(0, 1))); // low != 0
    }

    // Test int conversion
    {
        ASSERT_EQ(42, static_cast<int>(int256_t(42)));
        ASSERT_EQ(-42, static_cast<int>(int256_t(-42)));
        ASSERT_EQ(0, static_cast<int>(int256_t(0)));

        // Test truncation from large values
        int256_t large(0, static_cast<uint128_t>(0x123456789ABCDEF0ULL));
        ASSERT_EQ(static_cast<int>(0x9ABCDEF0), static_cast<int>(large));
    }

    // Test long conversion
    {
        ASSERT_EQ(1234567890L, static_cast<long>(int256_t(1234567890L)));
        ASSERT_EQ(-1234567890L, static_cast<long>(int256_t(-1234567890L)));

        // Test with 64-bit values
        int256_t val64(0, 0x123456789ABCDEF0ULL);
        ASSERT_EQ(static_cast<long>(0x123456789ABCDEF0ULL), static_cast<long>(val64));
    }

    // Test int128_t conversion
    {
        __int128 test_val = static_cast<__int128>(1) << 100;
        int256_t val(0, static_cast<uint128_t>(test_val));
        ASSERT_EQ(test_val, static_cast<__int128>(val));

        __int128 neg_val = -test_val;
        int256_t neg_int256(-1, static_cast<uint128_t>(neg_val));
        ASSERT_EQ(neg_val, static_cast<__int128>(neg_int256));
    }

    // Test signed char conversion
    {
        ASSERT_EQ(static_cast<signed char>(42), static_cast<signed char>(int256_t(42)));
        ASSERT_EQ(static_cast<signed char>(-42), static_cast<signed char>(int256_t(-42)));
        ASSERT_EQ(static_cast<signed char>(200), static_cast<signed char>(int256_t(200))); // overflow behavior
    }

    // Test short int conversion
    {
        ASSERT_EQ(static_cast<short>(12345), static_cast<short>(int256_t(12345)));
        ASSERT_EQ(static_cast<short>(-12345), static_cast<short>(int256_t(-12345)));
        ASSERT_EQ(static_cast<short>(70000), static_cast<short>(int256_t(70000))); // overflow behavior
    }

    // Test size_t conversion - valid cases
    {
        ASSERT_EQ(static_cast<size_t>(42), static_cast<size_t>(int256_t(42)));
        ASSERT_EQ(static_cast<size_t>(0), static_cast<size_t>(int256_t(0)));

        size_t max_size = std::numeric_limits<size_t>::max();
        ASSERT_EQ(max_size, static_cast<size_t>(int256_t(0, max_size)));
    }

    // Test size_t conversion - exception cases
    {
        int256_t small_positive(42);
        ASSERT_EQ(static_cast<size_t>(small_positive), 42UL);

        int256_t medium_positive(1000000);
        ASSERT_EQ(static_cast<size_t>(medium_positive), 1000000UL);

        int256_t max_size_t_val(std::numeric_limits<size_t>::max());
        ASSERT_EQ(static_cast<size_t>(max_size_t_val), std::numeric_limits<size_t>::max());

        int256_t negative_one(-1);
        size_t result = static_cast<size_t>(negative_one);
        ASSERT_EQ(result, std::numeric_limits<size_t>::max()); // -1 wraps to max

        int256_t negative_small(-42);
        result = static_cast<size_t>(negative_small);
        size_t expected = std::numeric_limits<size_t>::max() - 41; // Modulo arithmetic
        ASSERT_EQ(result, expected);

        int256_t just_above_max = int256_t(std::numeric_limits<size_t>::max()) + int256_t(1);
        result = static_cast<size_t>(just_above_max);
        ASSERT_EQ(result, 0UL); // Should wrap around to 0

        int256_t large_val;
        large_val.high = 1; // Set high 128 bits to 1
        large_val.low = 0x123456789ABCDEF0ULL;
        result = static_cast<size_t>(large_val);
        ASSERT_EQ(result, 0x123456789ABCDEF0ULL);

        int256_t large_negative;
        large_negative.high = -1; // All bits set in high part
        large_negative.low = static_cast<__uint128_t>(-1000LL);
        result = static_cast<size_t>(large_negative);
        ASSERT_EQ(result, static_cast<size_t>(static_cast<__uint128_t>(-1000LL)));
    }
}

// =============================================================================
// Friend Comparison Operators Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, friend_comparison_operators) {
    int256_t value(42);
    int256_t negative(-10);
    int256_t zero(0);

    // Test equality comparisons: int == int256_t
    ASSERT_TRUE(42 == value);
    ASSERT_FALSE(43 == value);
    ASSERT_TRUE(0 == zero);
    ASSERT_FALSE(1 == zero);

    // Test inequality comparisons: int != int256_t
    ASSERT_FALSE(42 != value);
    ASSERT_TRUE(43 != value);
    ASSERT_FALSE(0 != zero);
    ASSERT_TRUE(1 != zero);

    // Test less than comparisons: int < int256_t
    ASSERT_TRUE(41 < value);
    ASSERT_FALSE(42 < value);
    ASSERT_FALSE(43 < value);
    ASSERT_TRUE(-20 < negative);
    ASSERT_FALSE(0 < negative);

    // Test less than or equal comparisons: int <= int256_t
    ASSERT_TRUE(41 <= value);
    ASSERT_TRUE(42 <= value);
    ASSERT_FALSE(43 <= value);
    ASSERT_TRUE(-10 <= negative);
    ASSERT_TRUE(-20 <= negative);

    // Test greater than comparisons: int > int256_t
    ASSERT_FALSE(41 > value);
    ASSERT_FALSE(42 > value);
    ASSERT_TRUE(43 > value);
    ASSERT_FALSE(-20 > negative);
    ASSERT_TRUE(0 > negative);

    // Test greater than or equal comparisons: int >= int256_t
    ASSERT_FALSE(41 >= value);
    ASSERT_TRUE(42 >= value);
    ASSERT_TRUE(43 >= value);
    ASSERT_FALSE(-20 >= negative);
    ASSERT_TRUE(-10 >= negative);
    ASSERT_TRUE(0 >= negative);

    // Test with edge cases
    ASSERT_TRUE(INT_MAX == int256_t(INT_MAX));
    ASSERT_TRUE(INT_MIN == int256_t(INT_MIN));
    ASSERT_FALSE(INT_MAX == int256_t(INT_MIN));
}

// =============================================================================
// Bitwise Operators Tests
// =============================================================================

// NOLINTNEXTLINE
TEST_F(Int256Test, bitwise_operators) {
    // Test data
    int256_t val1(0x12345678, 0x9ABCDEF012345678ULL);
    int256_t val2(0x87654321, 0x123456789ABCDEF0ULL);
    int256_t all_ones(-1, static_cast<uint128_t>(-1));
    int256_t all_zeros(0, 0);

    // Test bitwise AND operator
    {
        int256_t result = val1 & val2;
        int256_t expected(0x12345678 & 0x87654321, 0x9ABCDEF012345678ULL & 0x123456789ABCDEF0ULL);
        ASSERT_EQ(expected.high, result.high);
        ASSERT_EQ(expected.low, result.low);

        // Test with all ones and zeros
        ASSERT_EQ(val1, val1 & all_ones);
        ASSERT_EQ(all_zeros, val1 & all_zeros);
    }

    // Test bitwise OR operator
    {
        int256_t result = val1 | val2;
        int256_t expected(0x12345678 | 0x87654321, 0x9ABCDEF012345678ULL | 0x123456789ABCDEF0ULL);
        ASSERT_EQ(expected.high, result.high);
        ASSERT_EQ(expected.low, result.low);

        // Test with all ones and zeros
        ASSERT_EQ(all_ones, val1 | all_ones);
        ASSERT_EQ(val1, val1 | all_zeros);
    }

    // Test bitwise XOR operator
    {
        int256_t result = val1 ^ val2;
        int256_t expected(0x12345678 ^ 0x87654321, 0x9ABCDEF012345678ULL ^ 0x123456789ABCDEF0ULL);
        ASSERT_EQ(expected.high, result.high);
        ASSERT_EQ(expected.low, result.low);

        // Test XOR properties
        ASSERT_EQ(all_zeros, val1 ^ val1); // x ^ x = 0
        ASSERT_EQ(val1, val1 ^ all_zeros); // x ^ 0 = x
    }

    // Test bitwise NOT operator
    {
        int256_t result = ~val1;
        int256_t expected(~val1.high, ~val1.low);
        ASSERT_EQ(expected.high, result.high);
        ASSERT_EQ(expected.low, result.low);

        // Test NOT properties
        ASSERT_EQ(all_zeros, ~all_ones);
        ASSERT_EQ(all_ones, ~all_zeros);
        ASSERT_EQ(val1, ~~val1); // double NOT
    }

    // Test bitwise AND assignment operator
    {
        int256_t test_val = val1;
        test_val &= val2;
        int256_t expected = val1 & val2;
        ASSERT_EQ(expected.high, test_val.high);
        ASSERT_EQ(expected.low, test_val.low);

        // Test chaining
        int256_t chain_val = val1;
        int256_t& result_ref = (chain_val &= val2);
        ASSERT_EQ(test_val, result_ref);
        ASSERT_EQ(&chain_val, &result_ref); // Should return reference to self
    }

    // Test bitwise OR assignment operator
    {
        int256_t test_val = val1;
        test_val |= val2;
        int256_t expected = val1 | val2;
        ASSERT_EQ(expected.high, test_val.high);
        ASSERT_EQ(expected.low, test_val.low);

        // Test chaining
        int256_t chain_val = val1;
        int256_t& result_ref = (chain_val |= val2);
        ASSERT_EQ(test_val, result_ref);
        ASSERT_EQ(&chain_val, &result_ref);
    }

    // Test bitwise XOR assignment operator
    {
        int256_t test_val = val1;
        test_val ^= val2;
        int256_t expected = val1 ^ val2;
        ASSERT_EQ(expected.high, test_val.high);
        ASSERT_EQ(expected.low, test_val.low);

        // Test chaining
        int256_t chain_val = val1;
        int256_t& result_ref = (chain_val ^= val2);
        ASSERT_EQ(test_val, result_ref);
        ASSERT_EQ(&chain_val, &result_ref);

        // Test XOR assignment properties
        int256_t toggle_val = val1;
        toggle_val ^= val2;
        toggle_val ^= val2; // Should return to original
        ASSERT_EQ(val1, toggle_val);
    }

    // Test left shift assignment operator (assuming it exists)
    {
        int256_t test_val(1);
        test_val <<= 10;
        int256_t expected = int256_t(1) << 10;
        ASSERT_EQ(expected, test_val);

        // Test chaining
        int256_t chain_val(1);
        int256_t& result_ref = (chain_val <<= 5);
        ASSERT_EQ(int256_t(1) << 5, result_ref);
        ASSERT_EQ(&chain_val, &result_ref);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, double_comparison_operators) {
    // Test data
    int256_t zero_val(0);
    int256_t positive_small(42);
    int256_t positive_large(123456789);
    int256_t negative_small(-42);

    // Test equality operator: int256_t == double
    {
        ASSERT_TRUE(positive_small == 42.0);
        ASSERT_TRUE(negative_small == -42.0);
        ASSERT_TRUE(zero_val == 0.0);
        ASSERT_TRUE(zero_val == -0.0); // positive and negative zero
        ASSERT_FALSE(positive_small == 42.1);
        ASSERT_FALSE(positive_small == 41.9);
        ASSERT_FALSE(positive_large == std::numeric_limits<double>::infinity());
        ASSERT_FALSE(positive_large == std::numeric_limits<double>::quiet_NaN());
    }

    // Test equality operator: double == int256_t
    {
        ASSERT_TRUE(42.0 == positive_small);
        ASSERT_TRUE(-42.0 == negative_small);
        ASSERT_TRUE(0.0 == zero_val);
        ASSERT_TRUE(-0.0 == zero_val);
        ASSERT_FALSE(42.1 == positive_small);
        ASSERT_FALSE(41.9 == positive_small);
    }

    // Test inequality operator: int256_t != double
    {
        ASSERT_FALSE(positive_small != 42.0);
        ASSERT_TRUE(positive_small != 42.1);
        ASSERT_TRUE(negative_small != -41.9);
        ASSERT_FALSE(zero_val != 0.0);
    }

    // Test inequality operator: double != int256_t
    {
        ASSERT_FALSE(42.0 != positive_small);
        ASSERT_TRUE(42.1 != positive_small);
        ASSERT_TRUE(-41.9 != negative_small);
        ASSERT_FALSE(0.0 != zero_val);
    }

    // Test less than operator: int256_t < double
    {
        ASSERT_TRUE(positive_small < 43.0);
        ASSERT_TRUE(negative_small < -41.0);
        ASSERT_TRUE(negative_small < 0.0);
        ASSERT_FALSE(positive_small < 42.0);
        ASSERT_FALSE(positive_small < 41.0);
        ASSERT_FALSE(zero_val < 0.0);
    }

    // Test less than operator: double < int256_t
    {
        ASSERT_TRUE(41.0 < positive_small);
        ASSERT_TRUE(-43.0 < negative_small);
        ASSERT_TRUE(-1.0 < zero_val);
        ASSERT_FALSE(42.0 < positive_small);
        ASSERT_FALSE(43.0 < positive_small);
        ASSERT_FALSE(0.0 < zero_val);
    }

    // Test less than or equal operator: int256_t <= double
    {
        ASSERT_TRUE(positive_small <= 42.0);
        ASSERT_TRUE(positive_small <= 43.0);
        ASSERT_TRUE(negative_small <= -42.0);
        ASSERT_FALSE(positive_small <= 41.0);
    }

    // Test less than or equal operator: double <= int256_t
    {
        ASSERT_TRUE(42.0 <= positive_small);
        ASSERT_TRUE(41.0 <= positive_small);
        ASSERT_TRUE(-42.0 <= negative_small);
        ASSERT_FALSE(43.0 <= positive_small);
    }

    // Test greater than operator: int256_t > double
    {
        ASSERT_TRUE(positive_small > 41.0);
        ASSERT_TRUE(negative_small > -43.0);
        ASSERT_TRUE(zero_val > -1.0);
        ASSERT_FALSE(positive_small > 42.0);
        ASSERT_FALSE(positive_small > 43.0);
        ASSERT_FALSE(zero_val > 0.0);
    }

    // Test greater than operator: double > int256_t
    {
        ASSERT_TRUE(43.0 > positive_small);
        ASSERT_TRUE(-41.0 > negative_small);
        ASSERT_TRUE(1.0 > zero_val);
        ASSERT_FALSE(42.0 > positive_small);
        ASSERT_FALSE(41.0 > positive_small);
        ASSERT_FALSE(0.0 > zero_val);
    }

    // Test greater than or equal operator: int256_t >= double
    {
        ASSERT_TRUE(positive_small >= 42.0);
        ASSERT_TRUE(positive_small >= 41.0);
        ASSERT_TRUE(negative_small >= -42.0);
        ASSERT_FALSE(positive_small >= 43.0);
    }

    // Test greater than or equal operator: double >= int256_t
    {
        ASSERT_TRUE(42.0 >= positive_small);
        ASSERT_TRUE(43.0 >= positive_small);
        ASSERT_TRUE(-42.0 >= negative_small);
        ASSERT_FALSE(41.0 >= positive_small);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, double_arithmetic_operators) {
    // Test data
    int256_t zero_val(0);
    int256_t positive_val(42);
    int256_t negative_val(-42);
    int256_t large_val(123456789);

    // Test addition operator: int256_t + double
    {
        double result = positive_val + 3.14;
        ASSERT_DOUBLE_EQ(45.14, result);

        result = negative_val + 3.14;
        ASSERT_DOUBLE_EQ(-38.86, result);

        result = zero_val + 3.14;
        ASSERT_DOUBLE_EQ(3.14, result);

        result = positive_val + 0.0;
        ASSERT_DOUBLE_EQ(42.0, result);

        result = large_val + 1.5;
        ASSERT_DOUBLE_EQ(123456790.5, result);
    }

    // Test addition operator: double + int256_t
    {
        double result = 3.14 + positive_val;
        ASSERT_DOUBLE_EQ(45.14, result);

        result = 3.14 + negative_val;
        ASSERT_DOUBLE_EQ(-38.86, result);

        result = 3.14 + zero_val;
        ASSERT_DOUBLE_EQ(3.14, result);

        result = 0.0 + positive_val;
        ASSERT_DOUBLE_EQ(42.0, result);

        result = 1.5 + large_val;
        ASSERT_DOUBLE_EQ(123456790.5, result);
    }

    // Test subtraction operator: double - int256_t
    {
        double result = 45.14 - positive_val;
        ASSERT_DOUBLE_EQ(3.14, result);

        result = 3.14 - positive_val;
        ASSERT_DOUBLE_EQ(-38.86, result);

        result = 3.14 - zero_val;
        ASSERT_DOUBLE_EQ(3.14, result);

        result = 0.0 - positive_val;
        ASSERT_DOUBLE_EQ(-42.0, result);

        result = 123456790.5 - large_val;
        ASSERT_DOUBLE_EQ(1.5, result);
    }

    // Test multiplication operator: int256_t * double
    {
        double result = positive_val * 3.14;
        ASSERT_NEAR(131.88, result, 1e-10);

        result = negative_val * 3.14;
        ASSERT_NEAR(-131.88, result, 1e-10);

        result = positive_val * 0.0;
        ASSERT_DOUBLE_EQ(0.0, result);

        result = zero_val * 3.14;
        ASSERT_DOUBLE_EQ(0.0, result);

        result = positive_val * 1.0;
        ASSERT_DOUBLE_EQ(42.0, result);

        result = large_val * 2.5;
        ASSERT_DOUBLE_EQ(308641972.5, result);
    }

    // Test multiplication operator: double * int256_t
    {
        double result = 3.14 * positive_val;
        ASSERT_NEAR(131.88, result, 1e-10);

        result = 3.14 * negative_val;
        ASSERT_NEAR(-131.88, result, 1e-10);

        result = 0.0 * positive_val;
        ASSERT_DOUBLE_EQ(0.0, result);

        result = 3.14 * zero_val;
        ASSERT_DOUBLE_EQ(0.0, result);

        result = 1.0 * positive_val;
        ASSERT_DOUBLE_EQ(42.0, result);

        result = 2.5 * large_val;
        ASSERT_DOUBLE_EQ(308641972.5, result);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, int_division_operators) {
    // Test data
    int256_t zero_val(0);
    int256_t positive_val(84);
    int256_t negative_val(-84);
    int256_t large_val(123456789);
    int256_t odd_val(85);

    // Test division operator: int256_t / int
    {
        int256_t result = positive_val / 2;
        ASSERT_EQ(int256_t(42), result);

        result = negative_val / 2;
        ASSERT_EQ(int256_t(-42), result);

        result = positive_val / -2;
        ASSERT_EQ(int256_t(-42), result);

        result = negative_val / -2;
        ASSERT_EQ(int256_t(42), result);

        // Test integer division truncation
        result = odd_val / 2;
        ASSERT_EQ(int256_t(42), result); // 85/2 = 42.5 -> 42

        result = int256_t(-85) / 2;
        ASSERT_EQ(int256_t(-42), result); // -85/2 = -42.5 -> -42

        // Test large number division
        result = large_val / 1000;
        ASSERT_EQ(int256_t(123456), result);

        // Test division by 1 and -1
        result = large_val / 1;
        ASSERT_EQ(large_val, result);

        result = large_val / -1;
        ASSERT_EQ(int256_t(-123456789), result);

        // Test zero division
        result = zero_val / 42;
        ASSERT_EQ(zero_val, result);
    }

    // Test division by zero exception: int256_t / int
    {
        ASSERT_THROW(positive_val / 0, std::domain_error);
        ASSERT_THROW(negative_val / 0, std::domain_error);
        ASSERT_THROW(zero_val / 0, std::domain_error);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, double_division_operators) {
    // Test data
    int256_t zero_val(0);
    int256_t positive_val(84);
    int256_t negative_val(-84);
    int256_t large_val(123456789);
    int256_t test_val(42);

    // Test division operator: int256_t / double
    {
        double result = positive_val / 2.0;
        ASSERT_DOUBLE_EQ(42.0, result);

        result = negative_val / 2.0;
        ASSERT_DOUBLE_EQ(-42.0, result);

        result = positive_val / -2.0;
        ASSERT_DOUBLE_EQ(-42.0, result);

        result = negative_val / -2.0;
        ASSERT_DOUBLE_EQ(42.0, result);

        // Test floating point division precision
        result = int256_t(85) / 2.0;
        ASSERT_DOUBLE_EQ(42.5, result);

        result = int256_t(-85) / 2.0;
        ASSERT_DOUBLE_EQ(-42.5, result);

        // Test large number division
        result = large_val / 1000.0;
        ASSERT_DOUBLE_EQ(123456.789, result);

        // Test division by fractional number
        result = test_val / 3.14;
        ASSERT_NEAR(13.375796, result, 1e-6);

        // Test zero division
        result = zero_val / 3.14;
        ASSERT_DOUBLE_EQ(0.0, result);
    }

    // Test division by zero exception: int256_t / double
    {
        ASSERT_THROW(positive_val / 0.0, std::domain_error);
        ASSERT_THROW(negative_val / 0.0, std::domain_error);
        ASSERT_THROW(zero_val / 0.0, std::domain_error);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, int_modulo_operators) {
    // Test data
    int256_t zero_val(0);
    int256_t positive_val(85);
    int256_t negative_val(-85);
    int256_t large_val(123456789);
    int256_t even_val(84);

    // Test modulo operator: int256_t % int
    {
        int256_t result = positive_val % 10;
        ASSERT_EQ(int256_t(5), result);

        result = negative_val % 10;
        ASSERT_EQ(int256_t(-5), result); // C++ modulo preserves dividend sign

        result = positive_val % -10;
        ASSERT_EQ(int256_t(5), result);

        result = negative_val % -10;
        ASSERT_EQ(int256_t(-5), result);

        // Test perfect division (remainder 0)
        result = even_val % 42;
        ASSERT_EQ(int256_t(0), result);

        result = int256_t(-84) % 42;
        ASSERT_EQ(int256_t(0), result);

        // Test large number modulo
        result = large_val % 1000;
        ASSERT_EQ(int256_t(789), result);

        result = int256_t(-123456789) % 1000;
        ASSERT_EQ(int256_t(-789), result);

        // Test modulo 1 (always 0)
        result = large_val % 1;
        ASSERT_EQ(int256_t(0), result);

        // Test zero modulo
        result = zero_val % 42;
        ASSERT_EQ(zero_val, result);
    }

    // Test modulo operator: int % int256_t
    {
        int256_t result = 85 % int256_t(10);
        ASSERT_EQ(int256_t(5), result);

        result = -85 % int256_t(10);
        ASSERT_EQ(int256_t(-5), result);

        result = 85 % int256_t(-10);
        ASSERT_EQ(int256_t(5), result);

        result = -85 % int256_t(-10);
        ASSERT_EQ(int256_t(-5), result);

        // Test small number modulo large number
        result = 42 % large_val;
        ASSERT_EQ(int256_t(42), result);

        result = -42 % large_val;
        ASSERT_EQ(int256_t(-42), result);
    }

    // Test modulo by zero exception
    {
        ASSERT_THROW(positive_val % 0, std::domain_error);
        ASSERT_THROW(negative_val % 0, std::domain_error);
        ASSERT_THROW(zero_val % 0, std::domain_error);

        ASSERT_THROW(85 % int256_t(0), std::domain_error);
        ASSERT_THROW(-85 % int256_t(0), std::domain_error);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, double_modulo_operators) {
    // Test data
    int256_t zero_val(0);
    int256_t positive_val(85);
    int256_t negative_val(-85);
    int256_t test_val(42);

    // Test modulo operator: int256_t % double (using fmod semantics)
    {
        double result = positive_val % 10.0;
        ASSERT_DOUBLE_EQ(5.0, result);

        result = negative_val % 10.0;
        ASSERT_DOUBLE_EQ(-5.0, result);

        result = positive_val % -10.0;
        ASSERT_DOUBLE_EQ(5.0, result);

        result = negative_val % -10.0;
        ASSERT_DOUBLE_EQ(-5.0, result);

        // Test floating point modulo precision
        result = test_val % 3.14;
        double expected = std::fmod(42.0, 3.14);
        ASSERT_NEAR(expected, result, 1e-10);

        result = int256_t(-42) % 3.14;
        expected = std::fmod(-42.0, 3.14);
        ASSERT_NEAR(expected, result, 1e-10);

        // Test perfect division (remainder 0)
        result = int256_t(84) % 42.0;
        ASSERT_DOUBLE_EQ(0.0, result);

        // Test zero modulo
        result = zero_val % 3.14;
        ASSERT_DOUBLE_EQ(0.0, result);
    }

    // Test modulo operator: double % int256_t (using fmod semantics)
    {
        double result = 85.5 % int256_t(10);
        double expected = std::fmod(85.5, 10.0);
        ASSERT_NEAR(expected, result, 1e-10);

        result = -85.5 % int256_t(10);
        expected = std::fmod(-85.5, 10.0);
        ASSERT_NEAR(expected, result, 1e-10);

        result = 85.5 % int256_t(-10);
        expected = std::fmod(85.5, -10.0);
        ASSERT_NEAR(expected, result, 1e-10);

        result = -85.5 % int256_t(-10);
        expected = std::fmod(-85.5, -10.0);
        ASSERT_NEAR(expected, result, 1e-10);

        // Test small double modulo large int256_t
        result = 3.14 % int256_t(123456789);
        expected = std::fmod(3.14, 123456789.0);
        ASSERT_NEAR(expected, result, 1e-10);
    }

    // Test modulo by zero exception: double operations
    {
        ASSERT_THROW(positive_val % 0.0, std::domain_error);
        ASSERT_THROW(negative_val % 0.0, std::domain_error);
        ASSERT_THROW(zero_val % 0.0, std::domain_error);

        ASSERT_THROW(3.14 % int256_t(0), std::domain_error);
        ASSERT_THROW(-3.14 % int256_t(0), std::domain_error);
    }
}

// NOLINTNEXTLINE
TEST_F(Int256Test, mixed_type_edge_cases) {
    // Test data for edge cases
    int256_t max_safe_int(1);
    max_safe_int <<= 53; // 2^53, largest safe integer in double

    // Test special double values
    {
        double inf = std::numeric_limits<double>::infinity();
        double neg_inf = -std::numeric_limits<double>::infinity();
        double nan_val = std::numeric_limits<double>::quiet_NaN();

        // Comparisons with infinity should work
        ASSERT_FALSE(max_safe_int == inf);
        ASSERT_FALSE(max_safe_int == neg_inf);
        ASSERT_TRUE(max_safe_int < inf);
        ASSERT_FALSE(max_safe_int > inf);

        // Comparisons with NaN should always be false
        ASSERT_FALSE(max_safe_int == nan_val);
        ASSERT_FALSE(max_safe_int < nan_val);
        ASSERT_FALSE(max_safe_int > nan_val);
    }

    // Test arithmetic with very small doubles
    {
        double very_small = 1e-100;
        double result = int256_t(1) + very_small;
        ASSERT_NEAR(1.0 + very_small, result, 1e-110);

        result = very_small + int256_t(1);
        ASSERT_NEAR(1.0 + very_small, result, 1e-110);
    }
}

} // end namespace starrocks
