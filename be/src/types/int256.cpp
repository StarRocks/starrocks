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

namespace starrocks {

// =============================================================================
// Type Conversion Operators Implementation
// =============================================================================

// int256_t::operator double() const {
//     if (*this == 0) return 0.0;
//
//     const bool negative = (high < 0);
//     const int256_t abs_val = negative ? -*this : *this;
//
//     // Find the position of the most significant bit
//     int bit_count = 0;
//     int256_t temp = abs_val;
//     while (temp > 0) {
//         temp >>= 1;
//         bit_count++;
//     }
//
//     if (bit_count <= 53) {
//         // Can represent exactly in double
//         double result = static_cast<double>(static_cast<uint64_t>(abs_val.low));
//         if (abs_val.high != 0) {
//             result += static_cast<double>(static_cast<uint64_t>(abs_val.high)) * (1ULL << 32) * (1ULL << 32) *
//                       (1ULL << 32) * (1ULL << 32);
//         }
//         return negative ? -result : result;
//     } else {
//         // Need to round to fit in double precision
//         const int shift = bit_count - 53;
//         const int256_t rounded = (abs_val + (int256_t(1) << (shift - 1))) >> shift;
//         const double mantissa = static_cast<double>(static_cast<uint64_t>(rounded.low));
//         const double result = mantissa * pow(2.0, shift);
//         return negative ? -result : result;
//     }
// }

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
// Division Implementation
// =============================================================================

// TODO(stephen): optimize this operator in the next patch
int256_t int256_t::operator/(const int256_t& other) const {
    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(1);

    bool negative = (high < 0) != (other.high < 0);
    int256_t dividend = (high < 0) ? -*this : *this;
    int256_t divisor = (other.high < 0) ? -other : other;

    if (dividend < divisor) return int256_t(0);

    if (divisor.high == 0 && divisor.low == 1) {
        return negative ? -dividend : dividend;
    }

    int256_t quotient(0);
    int256_t remainder(0);

    for (int i = 255; i >= 0; i--) {
        remainder = remainder << 1;

        if (i >= 128) {
            if ((dividend.high >> (i - 128)) & 1) {
                remainder.low |= 1;
            }
        } else {
            if ((dividend.low >> i) & 1) {
                remainder.low |= 1;
            }
        }

        if (remainder >= divisor) {
            remainder = remainder - divisor;
            if (i >= 128) {
                quotient.high |= (static_cast<int128_t>(1) << (i - 128));
            } else {
                quotient.low |= (static_cast<uint128_t>(1) << i);
            }
        }
    }

    return negative ? -quotient : quotient;
}

// =============================================================================
// Modulo Implementation
// =============================================================================

int256_t int256_t::operator%(const int256_t& other) const {
    if (other.high == 0 && other.low == 0) {
        throw std::domain_error("Division by zero");
    }

    if (high == 0 && low == 0) return int256_t(0);
    if (*this == other) return int256_t(0);

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
    parts[0] = static_cast<uint64_t>(value.low);
    parts[1] = static_cast<uint64_t>(value.low >> 64);
    parts[2] = static_cast<uint64_t>(value.high);
    parts[3] = static_cast<uint64_t>(value.high >> 64);

    uint64_t q[4] = {0, 0, 0, 0};
    uint64_t r = 0;
    for (int i = 3; i >= 0; --i) {
        __uint128_t acc = (static_cast<__uint128_t>(r) << 64) | parts[i];
        q[i] = static_cast<uint64_t>(acc / divisor);
        r = static_cast<uint64_t>(acc % divisor);
    }
    *quotient = int256_t((static_cast<__int128_t>(q[3]) << 64) | q[2], (static_cast<__uint128_t>(q[1]) << 64) | q[0]);
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
