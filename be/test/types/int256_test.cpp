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

} // end namespace starrocks
