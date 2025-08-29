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

#include <glog/logging.h>

#include <algorithm>
#include <cmath>

namespace starrocks {

namespace {

// -----------------------------------------------------------------------------
// Bit manipulation utilities
// -----------------------------------------------------------------------------

/// Count leading zeros in 256-bit integer
/// @param num Input value
/// @return Number of leading zero bits (0-256)
int count_leading_zeros(const int256_t& num) {
    uint64_t high_high = static_cast<uint64_t>(num.high >> 64);
    if (high_high != 0) {
        return __builtin_clzll(high_high);
    }

    uint64_t high_low = static_cast<uint64_t>(num.high);
    if (high_low != 0) {
        return 64 + __builtin_clzll(high_low);
    }

    uint64_t low_high = static_cast<uint64_t>(num.low >> 64);
    if (low_high != 0) {
        return 128 + __builtin_clzll(low_high);
    }

    uint64_t low_low = static_cast<uint64_t>(num.low);
    if (low_low != 0) {
        return 192 + __builtin_clzll(low_low);
    }

    return 256;
}

/// Count trailing zeros in 256-bit integer
/// @param value Input value
/// @return Number of trailing zero bits (0-256)
int count_trailing_zeros(const int256_t& value) {
    uint64_t low_low = static_cast<uint64_t>(value.low);
    if (low_low != 0) {
        return __builtin_ctzll(low_low);
    }

    uint64_t low_high = static_cast<uint64_t>(value.low >> 64);
    if (low_high != 0) {
        return 64 + __builtin_ctzll(low_high);
    }

    uint64_t high_low = static_cast<uint64_t>(value.high);
    if (high_low != 0) {
        return 128 + __builtin_ctzll(high_low);
    }

    uint64_t high_high = static_cast<uint64_t>(value.high >> 64);
    if (high_high != 0) {
        return 192 + __builtin_ctzll(high_high);
    }

    return 256;
}

bool is_power_of_2(const int256_t& value) {
    if (value.high < 0 || (value.high == 0 && value.low == 0)) {
        return false;
    }

    if (value.high == 0) {
        return (value.low & (value.low - 1)) == 0;
    }

    if (value.low != 0) {
        return false;
    }

    uint128_t h = static_cast<uint128_t>(value.high);
    return (h & (h - 1)) == 0;
}

// -----------------------------------------------------------------------------
// Division implementation helpers
// -----------------------------------------------------------------------------

/// Convert int256_t to 4 64-bit parts
/// @param value Input value
/// @param parts Output array of 4 uint64_t values
inline void to_u64_parts(const int256_t& value, uint64_t parts[4]) {
    parts[0] = static_cast<uint64_t>(value.low);
    parts[1] = static_cast<uint64_t>(value.low >> 64);
    parts[2] = static_cast<uint64_t>(value.high);
    parts[3] = static_cast<uint64_t>(value.high >> 64);
}

/// Convert 4 64-bit parts to int256_t
/// @param parts Input array of 4 uint64_t values
/// @return Reconstructed int256_t value
inline int256_t from_u64_parts(const uint64_t parts[4]) {
    int256_t result;
    result.low = static_cast<uint128_t>(parts[0]) | (static_cast<uint128_t>(parts[1]) << 64);
    result.high = static_cast<int128_t>(static_cast<uint128_t>(parts[2]) | (static_cast<uint128_t>(parts[3]) << 64));
    return result;
}

// -----------------------------------------------------------------------------
// Division algorithm selection and implementation
// -----------------------------------------------------------------------------

/// Information about operand sizes for division algorithm selection
struct DivisionInfo {
    bool dividend_is_128bit;
    bool divisor_is_128bit;
    bool divisor_is_64bit;
    bool divisor_is_power_of_2;
    int divisor_shift; // For power of 2 divisors
};

DivisionInfo analyze_division(const int256_t& dividend, const int256_t& divisor) {
    DivisionInfo info;

    info.dividend_is_128bit = (dividend.high == 0);
    info.divisor_is_128bit = (divisor.high == 0);
    info.divisor_is_64bit = info.divisor_is_128bit && (static_cast<uint64_t>(divisor.low >> 64) == 0);
    info.divisor_is_power_of_2 = is_power_of_2(divisor);
    info.divisor_shift = info.divisor_is_power_of_2 ? count_trailing_zeros(divisor) : 0;

    return info;
}

/// Fast division by 64-bit divisor using chunked algorithm
/// @param dividend Dividend value (positive)
/// @param divisor 64-bit divisor (positive)
/// @return Quotient
int256_t divide_by_64bit(const int256_t& dividend, uint64_t divisor) {
    uint64_t dividend_parts[4];
    to_u64_parts(dividend, dividend_parts);

    uint64_t quotient_parts[4] = {0, 0, 0, 0};
    uint64_t remainder = 0;

    // Process from most significant to least significant
    for (int i = 3; i >= 0; --i) {
        uint128_t temp = (static_cast<uint128_t>(remainder) << 64) | dividend_parts[i];
        quotient_parts[i] = static_cast<uint64_t>(temp / divisor);
        remainder = static_cast<uint64_t>(temp % divisor);
    }

    return from_u64_parts(quotient_parts);
}

/// Fast division when both operands are 128-bit
/// @param dividend 128-bit dividend
/// @param divisor 128-bit divisor
/// @return Quotient
int256_t divide_128by128(uint128_t dividend, uint128_t divisor) {
    uint128_t quotient = dividend / divisor;
    return int256_t(0, quotient);
}

/// Full 256-bit by 256-bit division using binary long division
/// @param dividend 256-bit dividend (positive)
/// @param divisor 256-bit divisor (positive)
/// @return Quotient
int256_t divide_256by256(const int256_t& dividend, const int256_t& divisor) {
    if (dividend < divisor) {
        return int256_t(0);
    }

    int256_t remainder = dividend;
    int256_t quotient = 0;
    const int divisor_bits = 256 - count_leading_zeros(divisor);

    while (true) {
        const int rem_zeros = count_leading_zeros(remainder);
        const int rem_bits = 256 - rem_zeros;

        if (rem_bits < divisor_bits) {
            break;
        }

        int shift = rem_bits - divisor_bits;

        int256_t shifted_divisor = divisor << shift;
        if (shifted_divisor > remainder) {
            shift--;
            if (shift < 0) {
                break;
            }
            shifted_divisor = divisor << shift;
        }

        remainder = remainder - shifted_divisor;
        quotient = quotient + (int256_t(1) << shift);
    }

    return quotient;
}

/// Core division implementation for positive operands
/// @param dividend Positive dividend
/// @param divisor Positive divisor
/// @return Quotient
int256_t divide_positive(const int256_t& dividend, const int256_t& divisor) {
    const DivisionInfo info = analyze_division(dividend, divisor);

    if (info.divisor_is_power_of_2) {
        return dividend >> info.divisor_shift;
    }

    if (info.divisor_is_64bit) {
        return divide_by_64bit(dividend, static_cast<uint64_t>(divisor.low));
    }

    if (info.divisor_is_128bit && info.dividend_is_128bit) {
        return divide_128by128(dividend.low, divisor.low);
    }

    return divide_256by256(dividend, divisor);
}

} // anonymous namespace

// Constructor from floating point types
int256_t::int256_t(float value) {
    if (std::isnan(value) || std::isinf(value)) {
        low = 0;
        high = 0;
        return;
    }

    if (value == 0.0f) {
        low = 0;
        high = 0;
        return;
    }

    bool negative = std::signbit(value);
    float abs_value = std::abs(value);

    if (abs_value < 1.0f) {
        low = 0;
        high = 0;
        return;
    }

    constexpr float max_safe_float = 16777216.0f; // 2^24

    constexpr float max_float = 3.4028235e38f;

    high = 0;

    if (abs_value >= max_safe_float) {
        int exponent;
        float mantissa = std::frexp(abs_value, &exponent);

        mantissa *= 2.0f;
        exponent -= 1;

        uint32_t mantissa_bits = static_cast<uint32_t>((mantissa - 1.0f) * (1U << 23));

        uint32_t significand = (1U << 23) | mantissa_bits;

        if (exponent <= 64) {
            if (exponent >= 23) {
                low = static_cast<uint128_t>(significand) << (exponent - 23);
            } else {
                low = static_cast<uint128_t>(significand) >> (23 - exponent);
            }
        } else if (exponent <= 128) {
            if (exponent >= 87) {
                int high_shift = exponent - 87;
                if (high_shift >= 64) {
                    low = static_cast<uint128_t>(max_float);
                    high = 0;
                } else {
                    low = static_cast<uint128_t>(significand) << 64;
                    high = static_cast<int128_t>(static_cast<uint64_t>(significand) >> (64 - high_shift));
                }
            } else if (exponent >= 23) {
                low = static_cast<uint128_t>(significand) << (exponent - 23);
                high = 0;
            } else {
                low = static_cast<uint128_t>(significand) >> (23 - exponent);
                high = 0;
            }
        } else {
            low = static_cast<uint128_t>(max_float);
            high = 0;
        }
    } else {
        if (abs_value <= static_cast<float>(UINT64_MAX)) {
            low = static_cast<uint128_t>(static_cast<uint64_t>(abs_value));
        } else {
            low = static_cast<uint128_t>(abs_value);
        }
    }

    if (negative) {
        *this = -*this;
    }
}

// Constructor from floating double types with proper overflow detection
int256_t::int256_t(double value) {
    if (std::isnan(value) || std::isinf(value) || value == 0.0) {
        low = 0;
        high = 0;
        return;
    }

    double abs_value = std::abs(value);

    if (abs_value < 1.0) {
        low = 0;
        high = 0;
        return;
    }

    constexpr double max_safe_double = 9007199254740992.0; // 2^53

    constexpr double max_int256_double =
            5.7896044618658097711785492504343953926634992332820282019728792003956564819968e+76;

    if (abs_value >= max_int256_double) {
        low = 0;
        high = 0;
        return;
    }

    if (abs_value >= max_safe_double) {
        constexpr double pow_128 = 340282366920938463463374607431768211456.0; // 2^128

        bool negative = value < 0.0;
        double abs_val = std::abs(value);

        double high_part = std::floor(abs_val / pow_128);
        double low_part = abs_val - (high_part * pow_128);

        if (high_part > 0) {
            high = static_cast<int128_t>(high_part);
        } else {
            high = 0;
        }

        low = static_cast<uint128_t>(low_part);

        if (negative) {
            *this = -*this;
        }

    } else {
        int64_t int_value = static_cast<int64_t>(value);

        if (int_value >= 0) {
            low = static_cast<uint128_t>(int_value);
            high = 0;
        } else {
            uint64_t abs_int = static_cast<uint64_t>(-int_value);
            low = static_cast<uint128_t>(abs_int);
            high = 0;
            *this = -*this;
        }
    }
}

// =============================================================================
// Type Conversion Operators Implementation
// =============================================================================
int256_t::operator double() const {
    if (*this == 0) return 0.0;

    const bool negative = (high < 0);
    const int256_t abs_val = negative ? -*this : *this;

    const int leading_zeros = count_leading_zeros(abs_val);
    if (leading_zeros == 256) return 0.0;

    const int msb_pos = 255 - leading_zeros;

    const int double_precision = 53;

    if (msb_pos < double_precision) {
        double result = static_cast<double>(static_cast<uint64_t>(abs_val.low));
        return negative ? -result : result;
    } else if (msb_pos < 128) {
        const int excess_bits = msb_pos + 1 - double_precision;
        uint128_t shifted = static_cast<uint128_t>(abs_val.low) >> excess_bits;

        if (excess_bits > 0) {
            uint128_t remainder = static_cast<uint128_t>(abs_val.low) - (shifted << excess_bits);
            uint128_t half = uint128_t(1) << (excess_bits - 1);

            if (remainder > half || (remainder == half && (shifted & 1) == 1)) {
                shifted += 1;
            }
        }

        double mantissa = static_cast<double>(static_cast<uint64_t>(shifted));
        double result = mantissa * pow(2.0, excess_bits);
        return negative ? -result : result;

    } else {
        const int excess_bits = msb_pos + 1 - double_precision;

        int256_t shifted = abs_val >> excess_bits;

        if (excess_bits > 0) {
            int256_t remainder = abs_val - (shifted << excess_bits);
            int256_t half = int256_t(1) << (excess_bits - 1);

            if (remainder > half || (remainder == half && (shifted.low & 1) == 1)) {
                shifted = shifted + 1;
            }
        }

        double mantissa;
        if (shifted.high == 0) {
            mantissa = static_cast<double>(static_cast<uint64_t>(shifted.low));
        } else {
            mantissa = static_cast<double>(static_cast<uint64_t>(shifted.low));
            constexpr double pow_128 = 340282366920938463463374607431768211456.0; // 2^128
            mantissa += static_cast<double>(static_cast<uint64_t>(shifted.high)) * pow_128;
        }

        double result = mantissa * pow(2.0, excess_bits);

        if (std::isinf(result)) {
            result = negative ? -std::numeric_limits<double>::infinity() : std::numeric_limits<double>::infinity();
        }

        return negative ? -result : result;
    }
}

// =============================================================================
// Multiplication Runtime Implementation - Helper Functions
// =============================================================================
/**
 * @brief Multiply two 64-bit unsigned integers to produce a 256-bit result
 *
 * This is the simplest case where both operands fit in 64 bits.
 * The mathematical result is at most 128 bits, which easily fits in int256_t.
 *
 * Mathematical formula: result = a × b (where a, b ≤ 2^64 - 1)
 * Maximum result: (2^64 - 1)² = 2^128 - 2^65 + 1 < 2^128
 *
 * @param a First 64-bit operand
 * @param b Second 64-bit operand
 * @param is_negative Whether the final result should be negative
 * @return Product as int256_t with high=0 and low containing the 128-bit result
 */
int256_t int256_t::multiply_64x64(const uint64_t a, const uint64_t b, const bool is_negative) {
    const uint128_t product = static_cast<uint128_t>(a) * static_cast<uint128_t>(b);
    const int256_t result(0, product);
    return is_negative ? -result : result;
};

/**
 * @brief Multiply two 128-bit unsigned integers to produce a 256-bit result
 *
 * This implements the classic "long multiplication" algorithm by breaking each
 * 128-bit number into two 64-bit parts and performing four 64×64 multiplications.
 *
 * Algorithm breakdown:
 * Let a = a_high × 2^64 + a_low, b = b_high × 2^64 + b_low
 * Then: a × b = (a_high × 2^64 + a_low) × (b_high × 2^64 + b_low)
 *             = a_high × b_high × 2^128     (p3 × 2^128)
 *             + a_high × b_low × 2^64       (p2 × 2^64)
 *             + a_low × b_high × 2^64       (p1 × 2^64)
 *             + a_low × b_low               (p0)
 *
 * Position mapping in 256-bit result:
 * ┌─────────────┬─────────────┬─────────────┬─────────────┐
 * │   255-192   │   191-128   │   127-64    │    63-0     │
 * ├─────────────┼─────────────┼─────────────┼─────────────┤
 * │     p3      │             │             │     p0      │ ← Direct placement
 * │             │ middle>>64  │ middle<<64  │             │ ← Middle terms
 * └─────────────┴─────────────┴─────────────┴─────────────┘
 *              result_high                result_low
 *
 * @param a First 128-bit operand
 * @param b Second 128-bit operand
 * @param is_negative Whether the final result should be negative
 * @return Product as int256_t containing the full 256-bit result
 */
int256_t int256_t::multiply_128x128(const uint128_t a, const uint128_t b, const bool is_negative) {
    const uint64_t a_low = static_cast<uint64_t>(a);
    const uint64_t a_high = static_cast<uint64_t>(a >> 64);
    const uint64_t b_low = static_cast<uint64_t>(b);
    const uint64_t b_high = static_cast<uint64_t>(b >> 64);

    const uint128_t p0 = static_cast<uint128_t>(a_low) * static_cast<uint128_t>(b_low);   // low × low
    const uint128_t p1 = static_cast<uint128_t>(a_low) * static_cast<uint128_t>(b_high);  // low × high
    const uint128_t p2 = static_cast<uint128_t>(a_high) * static_cast<uint128_t>(b_low);  // high × low
    const uint128_t p3 = static_cast<uint128_t>(a_high) * static_cast<uint128_t>(b_high); // high × high

    uint128_t result_low = p0;
    uint128_t result_high = p3;

    const uint128_t middle = p1 + p2;

    result_low += (middle << 64);
    result_high += (middle >> 64);

    if (result_low < p0) {
        result_high++;
    }

    const int256_t result(static_cast<int128_t>(result_high), result_low);
    return is_negative ? -result : result;
}

/**
 * @brief Multiply a 64-bit integer by a 256-bit integer
 *
 * This function implements multiplication of a small (64-bit) value with a large
 * (256-bit) value using the distributive property. The 256-bit number is treated
 * as an array of four 64-bit parts, and we multiply each part by the small value.
 *
 * Algorithm:
 * 1. Break large number into four 64-bit parts: [p3, p2, p1, p0]
 * 2. Multiply each part by small: small × p_i
 * 3. Handle carries between parts to prevent overflow
 * 4. Reconstruct 256-bit result from the four 64-bit results
 *
 * Mathematical representation:
 * large = p3×2^192 + p2×2^128 + p1×2^64 + p0
 * result = small × (p3×2^192 + p2×2^128 + p1×2^64 + p0)
 *        = (small×p3)×2^192 + (small×p2)×2^128 + (small×p1)×2^64 + (small×p0)
 *
 * @param small 64-bit operand (multiplicand)
 * @param large 256-bit operand (multiplier)
 * @param is_negative Whether the final result should be negative
 * @return Product as int256_t
 */
int256_t int256_t::multiply_64x256(const uint64_t small, const int256_t& large, const bool is_negative) {
    uint64_t large_parts[4];
    large_parts[0] = static_cast<uint64_t>(large.low);
    large_parts[1] = static_cast<uint64_t>(large.low >> 64);
    large_parts[2] = static_cast<uint64_t>(large.high);
    large_parts[3] = static_cast<uint64_t>(large.high >> 64);

    uint64_t result[4] = {0, 0, 0, 0};
    uint64_t carry = 0;

    for (int i = 0; i < 4; i++) {
        const uint128_t product = static_cast<uint128_t>(small) * static_cast<uint128_t>(large_parts[i]) + carry;
        result[i] = static_cast<uint64_t>(product);
        carry = static_cast<uint64_t>(product >> 64);
    }

    const uint128_t result_low = static_cast<uint128_t>(result[0]) | (static_cast<uint128_t>(result[1]) << 64);
    const int128_t result_high =
            static_cast<int128_t>(static_cast<uint128_t>(result[2]) | (static_cast<uint128_t>(result[3]) << 64));

    const int256_t final_result(result_high, result_low);
    return is_negative ? -final_result : final_result;
}

/**
 * @brief Multiply a 128-bit integer by a 256-bit integer
 *
 * This function breaks down the 128-bit operand into two 64-bit parts and
 * uses the 64×256 multiplication function twice, then combines the results
 * with appropriate bit shifting.
 *
 * Algorithm:
 * 1. Split 128-bit number: small = small_high×2^64 + small_low
 * 2. Compute: small × large = (small_high×2^64 + small_low) × large
 *                           = small_high×large×2^64 + small_low×large
 * 3. Use 64×256 multiplication for each term
 * 4. Shift and add results
 *
 * @param small 128-bit operand
 * @param large 256-bit operand
 * @param is_negative Whether the final result should be negative
 * @return Product as int256_t
 */
int256_t int256_t::multiply_128x256(const uint128_t small, const int256_t& large, const bool is_negative) {
    const uint64_t small_low = static_cast<uint64_t>(small);
    const uint64_t small_high = static_cast<uint64_t>(small >> 64);

    const int256_t product_low = multiply_64x256(small_low, large, false);
    int256_t product_high = multiply_64x256(small_high, large, false);

    product_high = product_high << 64;

    const int256_t result = product_low + product_high;
    return is_negative ? -result : result;
}

// =============================================================================
// Multiplication Runtime Implementation
// =============================================================================

int256_t int256_t::multiply_runtime(const int256_t& other) const {
    if ((*this == int256_t(0)) || (other == int256_t(0))) {
        return int256_t(0);
    }
    if (other == int256_t(1)) {
        return *this;
    }
    if (*this == int256_t(1)) {
        return other;
    }

    if (*this == INT256_MIN && other == int256_t(-1)) {
        return INT256_MIN;
    }
    if (other == INT256_MIN && *this == int256_t(-1)) {
        return INT256_MIN;
    }

    const bool is_negative = (high < 0) != (other.high < 0);
    const int256_t lhs = (high < 0) ? -*this : *this;
    const int256_t rhs = (other.high < 0) ? -other : other;

    const bool lhs_is_64bit = (lhs.high == 0) && (static_cast<uint64_t>(lhs.low >> 64) == 0);
    const bool rhs_is_64bit = (rhs.high == 0) && (static_cast<uint64_t>(rhs.low >> 64) == 0);
    const bool lhs_is_128bit = (lhs.high == 0);
    const bool rhs_is_128bit = (rhs.high == 0);

    if (lhs_is_64bit && rhs_is_64bit) {
        return multiply_64x64(static_cast<uint64_t>(lhs.low), static_cast<uint64_t>(rhs.low), is_negative);
    }

    if (lhs_is_128bit && rhs_is_128bit) {
        return multiply_128x128(lhs.low, rhs.low, is_negative);
    }

    if (lhs_is_64bit) {
        return multiply_64x256(static_cast<uint64_t>(lhs.low), rhs, is_negative);
    }
    if (rhs_is_64bit) {
        return multiply_64x256(static_cast<uint64_t>(rhs.low), lhs, is_negative);
    }

    if (lhs_is_128bit) {
        return multiply_128x256(lhs.low, rhs, is_negative);
    }
    if (rhs_is_128bit) {
        return multiply_128x256(rhs.low, lhs, is_negative);
    }

    return multiply_core_256bit(lhs, rhs, is_negative);
}

// =============================================================================
// Division and Modulo Operations
// =============================================================================

int256_t int256_t::operator/(const int256_t& other) const {
    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }
    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(1);

    if (other == int256_t(1)) {
        return *this;
    }

    if (other == int256_t(-1)) {
        if (*this == INT256_MIN) {
            return INT256_MIN;
        }
        return -*this;
    }

    const bool negative_result = (high < 0) != (other.high < 0);
    const int256_t dividend = (high < 0) ? -*this : *this;
    const int256_t divisor = (other.high < 0) ? -other : other;

    if (dividend < divisor) return int256_t(0);

    if (divisor == int256_t(2)) {
        const int256_t result = dividend >> 1;
        return negative_result ? -result : result;
    }

    const int256_t result = divide_positive(dividend, divisor);

    return negative_result ? -result : result;
}

int256_t int256_t::operator%(const int256_t& other) const {
    // Check for division by zero
    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    // Handle special cases
    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(0);

    // Calculate remainder using: dividend - (quotient * divisor)
    int256_t quotient = *this / other;
    int256_t remainder = *this - (quotient * other);

    return remainder;
}

// =============================================================================
// Utility Functions Implementation
// =============================================================================

std::string int256_t::to_string() const {
    if (high == 0 && low == 0) {
        return "0";
    }

    // Handle INT256_MIN special case directly
    if (*this == INT256_MIN) {
        return "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
    }

    int256_t temp = *this;
    bool negative = false;
    if (temp.high < 0) {
        negative = true;
        temp = -temp;
    }

    std::string result;
    while (temp.high != 0 || temp.low != 0) {
        uint32_t rem;
        int256_t quot;
        divmod_u32(temp, 10, &quot, &rem);
        result.push_back('0' + rem);
        temp = quot;
    }

    if (negative) result.push_back('-');
    std::reverse(result.begin(), result.end());

    return result;
}

void divmod_u32(const int256_t& value, uint32_t divisor, int256_t* quotient, uint32_t* remainder) {
    uint64_t parts[4];
    to_u64_parts(value, parts);

    uint64_t q[4] = {0, 0, 0, 0};
    uint64_t r = 0;

    // Process from most significant to least significant
    for (int i = 3; i >= 0; --i) {
        __uint128_t acc = (static_cast<__uint128_t>(r) << 64) | parts[i];
        q[i] = static_cast<uint64_t>(acc / divisor);
        r = static_cast<uint64_t>(acc % divisor);
    }

    *quotient = from_u64_parts(q);
    *remainder = r;
}

int256_t parse_int256(const std::string& str) {
    if (str.empty()) {
        throw std::invalid_argument("empty string");
    }

    int256_t result = 0;
    bool negative = false;
    size_t i = 0;

    if (str[0] == '-') {
        negative = true;
        i = 1;
        if (i >= str.size()) {
            throw std::invalid_argument("only minus sign");
        }
    }

    bool has_digits = false;

    for (; i < str.size(); ++i) {
        if (str[i] < '0' || str[i] > '9') {
            throw std::invalid_argument("invalid digit");
        }
        has_digits = true;
        result = result * 10 + (str[i] - '0');
    }

    if (!has_digits) {
        throw std::invalid_argument("no valid digits");
    }

    return negative ? -result : result;
}

} // namespace starrocks
