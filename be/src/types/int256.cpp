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

// =============================================================================
// Anonymous namespace for internal utilities
// =============================================================================

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

// =============================================================================
// Type Conversion Operators Implementation
// =============================================================================

int256_t::operator double() const {
    if (*this == 0) return 0.0;

    bool negative = (high < 0);
    int256_t abs_val = negative ? -*this : *this;

    // Find the position of the most significant bit
    int bit_count = 0;
    int256_t temp = abs_val;
    while (temp > 0) {
        temp >>= 1;
        bit_count++;
    }

    if (bit_count <= 53) {
        // Can represent exactly in double
        double result = static_cast<double>(static_cast<uint64_t>(abs_val.low));
        if (abs_val.high != 0) {
            result += static_cast<double>(static_cast<uint64_t>(abs_val.high)) * (1ULL << 32) * (1ULL << 32) *
                      (1ULL << 32) * (1ULL << 32);
        }
        return negative ? -result : result;
    } else {
        // Need to round to fit in double precision
        int shift = bit_count - 53;
        int256_t rounded = (abs_val + (int256_t(1) << (shift - 1))) >> shift;
        double mantissa = static_cast<double>(static_cast<uint64_t>(rounded.low));
        double result = mantissa * pow(2.0, shift);
        return negative ? -result : result;
    }
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
