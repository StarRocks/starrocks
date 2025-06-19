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

#pragma once

#include <fmt/format.h>

#include <cmath>
#include <string>
#include <type_traits>

#include "common/compiler_util.h"

namespace starrocks {

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

/**
*
* This file implements a 256-bit signed integer type using two 128-bit components.
*
* ## Architecture Support
*
* This implementation currently supports little-endian systems only
*
* ## Memory Layout
*
* Structure member layout follows little-endian conventions:
* - low member: declared first, contains lower 128 bits
* - high member: declared second, contains upper 128 bits
*
* For a 256-bit value 0x123456789ABCDEF0FEDCBA0987654321_0123456789ABCDEFEDCBA09876543210:
* - low (lower 128 bits):  0x0123456789ABCDEFEDCBA09876543210
* - high (upper 128 bits): 0x123456789ABCDEF0FEDCBA0987654321
*
* Little-endian memory layout:
* Address   | Content
* ----------|-----------
* 0x00-0x0F | low member (16 bytes)
* 0x10-0x1F | high member (16 bytes)
*
* ### Memory Layout Example:
* ```
* Memory grows from LEFT to RIGHT →
*
* Address:     0x1000                                           0x1010
*              ↓                                                ↓
* Structure:   [--------------low (128bit/16bytes)--------------][------------high (128bit/16bytes)------------]
*
* Byte order :  10 32 54 76 98 BA DC ED EF CD AB 89 67 45 23 01 | 21 43 65 87 09 BA DC FE F0 DE BC 9A 78 56 34 12
*              ↑                                            ↑   ↑                                              ↑
*              0x1000                                   0x100F  0x1010                                     0x101F
*              LSB of low                           MSB of low  LSB of high                           MSB of high
*
* ```
*
* ## Storage Format Consistency
*
* **The storage layer format is identical to the in-memory format**.
*
* ### Storage Example:
*
* **Memory representation:**
* ```
* Memory Address: 0x1000-0x101F (32 bytes total)
* [low: 0x0123456789ABCDEFEDCBA09876543210][high: 0x123456789ABCDEF0FEDCBA0987654321]
* Bytes: 10 32 54 76 98 BA DC ED EF CD AB 89 67 45 23 01 21 43 65 87 09 BA DC FE F0 DE BC 9A 78 56 34 12
* ```
*
* **Storage format (identical to memory):**
* ```
* File Offset: 0x0000-0x001F (32 bytes total)
* [low: 0x0123456789ABCDEFEDCBA09876543210][high: 0x123456789ABCDEF0FEDCBA0987654321]
* Bytes: 10 32 54 76 98 BA DC ED EF CD AB 89 67 45 23 01 21 43 65 87 09 BA DC FE F0 DE BC 9A 78 56 34 12
* ```
*
* **Key Points:**
* - Byte 0x00-0x0F in storage = low member in memory
* - Byte 0x10-0x1F in storage = high member in memory
* - No endianness conversion needed during I/O operations
* - Direct memcpy() between memory and storage is safe
*
* ## Value Representation
*
* The 256-bit value is logically represented as: `high * 2^128 + low`
*
* - Positive numbers: high >= 0, standard binary representation
* - Negative numbers: two's complement representation, high < 0
* - Zero value: high = 0, low = 0
*
* ### Examples:
*
* **Simple Values:**
* - Value 0: high = 0, low = 0
* - Value 1: high = 0, low = 1
* - Value 2^128: high = 1, low = 0
*
* **Negative Values (Two's Complement):**
* - Value -1: high = -1 (0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF), low = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
* - Value -2^128: high = -1, low = 0
*
* **Extreme Values:**
* - Maximum (2^255 - 1): high = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, low = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
* - Minimum (-2^255): high = 0x80000000000000000000000000000000, low = 0x00000000000000000000000000000000
*
*/

struct int256_t {
public:
    // =============================================================================
    // Member Variables
    // =============================================================================
    uint128_t low; // Lower 128 bits
    int128_t high; // Higher 128 bits

    // =============================================================================
    // Constructors
    // =============================================================================

    /// Default constructor - initializes to zero
    constexpr int256_t() : low(0), high(0) {}

    /// Constructor from high and low parts
    /// @param h High 128 bits
    /// @param l Low 128 bits
    constexpr int256_t(int128_t h, uint128_t l) : low(l), high(h) {}

    /// Constructor from signed integer types
    /// @param value Integer value to convert
    constexpr int256_t(int value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(long value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(long long value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(__int128 value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}

    /// Constructor from unsigned integer types
    /// @param value Integer value to convert
    constexpr int256_t(unsigned int value) : low(static_cast<uint128_t>(value)), high(0) {}
    constexpr int256_t(unsigned long value) : low(static_cast<uint128_t>(value)), high(0) {}
    constexpr int256_t(unsigned long long value) : low(static_cast<uint128_t>(value)), high(0) {}
    constexpr int256_t(uint128_t value) : low(value), high(0) {}

    /// Constructor from floating point types
    /// @param value Floating point value to convert (truncated to integer)
    constexpr int256_t(float value)
            : low(static_cast<uint128_t>(static_cast<long long>(value))), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(double value)
            : low(static_cast<uint128_t>(static_cast<long long>(value))), high(value < 0 ? -1 : 0) {}

    // =============================================================================
    // Type Conversion Operators
    // =============================================================================

    /// Convert to boolean (true if non-zero)
    operator bool() const { return high != 0 || low != 0; }
    //
    // /// High precision conversion using bit manipulation
    // operator double() const;
    //
    // /// Convert to size_t (safe conversion with overflow checking)
    // operator size_t() const {
    //     if (high < 0) {
    //         throw std::out_of_range("Cannot convert negative int256_t to size_t");
    //     }
    //
    //     if (high > 0) {
    //         throw std::out_of_range("int256_t value too large for size_t");
    //     }
    //
    //     constexpr uint128_t max_size_t = std::numeric_limits<size_t>::max();
    //     if (low > max_size_t) {
    //         throw std::out_of_range("int256_t value too large for size_t");
    //     }
    //
    //     return static_cast<size_t>(low);
    // }

    // =============================================================================
    // Unary Operators
    // =============================================================================

    /// Unary negation operator
    /// @return Negated value using two's complement
    constexpr int256_t operator-() const {
        if (low == 0) {
            return int256_t(-high, 0);
        } else {
            uint128_t new_low = ~low + 1;
            int128_t new_high = ~high;
            if (new_low == 0) {
                new_high++;
            }
            return int256_t(new_high, new_low);
        }
    }

    /// Unary plus operator (no-op)
    /// @return Copy of this value
    constexpr int256_t operator+() const { return *this; }

    // =============================================================================
    // Comparison Operators
    // =============================================================================

    /// Equality comparison with another int256_t
    /// @param other Value to compare with
    /// @return true if values are equal
    constexpr bool operator==(const int256_t& other) const { return high == other.high && low == other.low; }

    /// Inequality comparison with another int256_t
    /// @param other Value to compare with
    /// @return true if values are not equal
    constexpr bool operator!=(const int256_t& other) const { return !(*this == other); }

    /// Less than comparison with another int256_t
    /// @param other Value to compare with
    /// @return true if this value is less than other
    constexpr bool operator<(const int256_t& other) const {
        if (high != other.high) return high < other.high;
        return low < other.low;
    }

    /// Less than or equal comparison with another int256_t
    /// @param other Value to compare with
    /// @return true if this value is less than or equal to other
    constexpr bool operator<=(const int256_t& other) const { return *this < other || *this == other; }

    /// Greater than comparison with another int256_t
    /// @param other Value to compare with
    /// @return true if this value is greater than other
    constexpr bool operator>(const int256_t& other) const { return !(*this <= other); }

    /// Greater than or equal comparison with another int256_t
    /// @param other Value to compare with
    /// @return true if this value is greater than or equal to other
    constexpr bool operator>=(const int256_t& other) const { return !(*this < other); }

    /// Equality comparison with int
    /// @param other Integer value to compare with
    /// @return true if values are equal
    constexpr bool operator==(int other) const { return *this == int256_t(other); }

    /// Inequality comparison with int
    /// @param other Integer value to compare with
    /// @return true if values are not equal
    constexpr bool operator!=(int other) const { return *this != int256_t(other); }

    /// Less than comparison with int
    /// @param other Integer value to compare with
    /// @return true if this value is less than other
    constexpr bool operator<(int other) const { return *this < int256_t(other); }

    /// Less than or equal comparison with int
    /// @param other Integer value to compare with
    /// @return true if this value is less than or equal to other
    constexpr bool operator<=(int other) const { return *this <= int256_t(other); }

    /// Greater than comparison with int
    /// @param other Integer value to compare with
    /// @return true if this value is greater than other
    constexpr bool operator>(int other) const { return *this > int256_t(other); }

    /// Greater than or equal comparison with int
    /// @param other Integer value to compare with
    /// @return true if this value is greater than or equal to other
    constexpr bool operator>=(int other) const { return *this >= int256_t(other); }

    // =============================================================================
    // Arithmetic Operators - Addition
    // =============================================================================

    /// Addition with another int256_t
    /// @param other Value to add
    /// @return Sum of this value and other
    constexpr int256_t operator+(const int256_t& other) const {
        int256_t result;
        result.low = low + other.low;
        result.high = high + other.high;
        if (result.low < low) { // Check for overflow in low part
            result.high++;
        }
        return result;
    }

    /// Addition with int (optimized version)
    /// @param other Integer value to add
    /// @return Sum of this value and other
    constexpr int256_t operator+(int other) const { return *this + int256_t(other); }

    // =============================================================================
    // Arithmetic Operators - Subtraction
    // =============================================================================

    /// Subtraction with another int256_t
    /// @param other Value to subtract
    /// @return Difference of this value and other
    constexpr int256_t operator-(const int256_t& other) const { return *this + (-other); }

    /// Subtraction with int
    /// @param other Integer value to subtract
    /// @return Difference of this value and other
    int256_t operator-(int other) const { return *this - int256_t(other); }

    /// Subtraction with __int128
    /// @param other 128-bit integer value to subtract
    /// @return Difference of this value and other
    int256_t operator-(__int128 other) const { return *this - int256_t(other); }

    /// Subtraction with double (returns double)
    /// @param other Double value to subtract
    /// @return Difference as double
    double operator-(double other) const { return static_cast<double>(*this) - other; }

    // =============================================================================
    // Arithmetic Operators - Multiplication
    // =============================================================================

    /// Multiplication with another int256_t
    /// @param other Value to multiply by
    /// @return Product of this value and other
    constexpr int256_t operator*(const int256_t& other) const {
        if (LIKELY(!std::is_constant_evaluated())) {
            return multiply_runtime(other);
        }
        return multiply_constexpr(other);
    }

    /// Multiplication with int
    /// @param other Integer value to multiply by
    /// @return Product of this value and other
    int256_t operator*(int other) const { return *this * int256_t(other); }

    // =============================================================================
    // Arithmetic Operators - Division (Declaration only)
    // =============================================================================

    /// Division with another int256_t
    /// @param other Value to divide by
    /// @return Quotient of this value divided by other
    /// @throws std::domain_error if other is zero
    int256_t operator/(const int256_t& other) const;

    // =============================================================================
    // Arithmetic Operators - Modulo (Declaration only)
    // =============================================================================

    /// Modulo operation with another int256_t
    /// @param other Value to take modulo by
    /// @return Remainder of this value divided by other
    /// @throws std::domain_error if other is zero
    int256_t operator%(const int256_t& other) const;

    // =============================================================================
    // Assignment Operators
    // =============================================================================

    /// Addition assignment operator
    /// @param other Value to add to this
    /// @return Reference to this after addition
    constexpr int256_t& operator+=(const int256_t& other) {
        *this = *this + other;
        return *this;
    }

    /// Subtraction assignment operator
    /// @param other Value to subtract from this
    /// @return Reference to this after subtraction
    constexpr int256_t& operator-=(const int256_t& other) {
        *this = *this - other;
        return *this;
    }

    /// Multiplication assignment operator
    /// @param other Value to multiply this by
    /// @return Reference to this after multiplication
    constexpr int256_t& operator*=(const int256_t& other) {
        *this = *this * other;
        return *this;
    }

    /// Division assignment operator
    /// @param other Value to divide this by
    /// @return Reference to this after division
    /// @throws std::domain_error if other is zero
    int256_t& operator/=(const int256_t& other) {
        *this = *this / other;
        return *this;
    }

    /// Modulo assignment operator
    /// @param other Value to take modulo by
    /// @return Reference to this after modulo operation
    /// @throws std::domain_error if other is zero
    int256_t& operator%=(const int256_t& other) {
        *this = *this % other;
        return *this;
    }

    // =============================================================================
    // Increment/Decrement Operators
    // =============================================================================

    /// Pre-increment operator
    /// @return Reference to this after incrementing
    int256_t& operator++() {
        if (++low == 0) { // If low overflows to 0
            ++high;       // Increment high
        }
        return *this;
    }

    /// Post-increment operator
    /// @return Copy of this before incrementing
    int256_t operator++(int) {
        int256_t tmp = *this;
        ++(*this);
        return tmp;
    }

    // =============================================================================
    // Bit Shift Operators
    // =============================================================================

    /// Right shift operator (arithmetic shift for signed values)
    /// @param shift Number of bits to shift right
    /// @return This value shifted right by shift bits
    constexpr int256_t operator>>(int shift) const {
        if (shift <= 0) return *this;

        if (shift >= 256) {
            return (high < 0) ? int256_t(-1, -1) : int256_t(0, 0);
        }

        if (shift >= 128) {
            return int256_t((high < 0) ? -1 : 0, high >> (shift - 128));
        }

        return int256_t(high >> shift, static_cast<int128_t>((static_cast<uint128_t>(low) >> shift) |
                                                             (static_cast<uint128_t>(high) << (128 - shift))));
    }

    /// Right shift assignment operator
    /// @param shift Number of bits to shift right
    /// @return Reference to this after shifting
    constexpr int256_t& operator>>=(int shift) {
        *this = *this >> shift;
        return *this;
    }

    /// Left shift operator
    /// @param shift Number of bits to shift left
    /// @return This value shifted left by shift bits
    constexpr int256_t operator<<(int shift) const {
        if (shift <= 0) return *this;

        if (shift >= 256) {
            return int256_t(0, 0);
        }

        if (shift >= 128) {
            return int256_t(low << (shift - 128), 0);
        }

        return int256_t(static_cast<int128_t>((static_cast<uint128_t>(high) << shift) |
                                              (static_cast<uint128_t>(low) >> (128 - shift))),
                        low << shift);
    }

    /// Left shift assignment operator
    /// @param shift Number of bits to shift left
    /// @return Reference to this after shifting
    constexpr int256_t& operator<<=(int shift) { return *this = *this << shift; }

    // =============================================================================
    // Utility Methods
    // =============================================================================

    /// Convert to string representation
    /// @return String representation of this value
    std::string to_string() const;

private:
    // =============================================================================
    // Private Helper Methods for Multiplication
    // =============================================================================

    constexpr int256_t multiply_core_256bit(const int256_t& lhs, const int256_t& rhs, bool is_negative) const {
        uint64_t lhs_parts[4], rhs_parts[4];

        lhs_parts[0] = static_cast<uint64_t>(lhs.low);
        lhs_parts[1] = static_cast<uint64_t>(lhs.low >> 64);
        lhs_parts[2] = static_cast<uint64_t>(lhs.high);
        lhs_parts[3] = static_cast<uint64_t>(lhs.high >> 64);

        rhs_parts[0] = static_cast<uint64_t>(rhs.low);
        rhs_parts[1] = static_cast<uint64_t>(rhs.low >> 64);
        rhs_parts[2] = static_cast<uint64_t>(rhs.high);
        rhs_parts[3] = static_cast<uint64_t>(rhs.high >> 64);

        uint64_t result[4] = {0, 0, 0, 0};

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                int pos = i + j;

                if (pos >= 4) {
                    break;
                }

                uint128_t product = static_cast<uint128_t>(lhs_parts[i]) * static_cast<uint128_t>(rhs_parts[j]);

                uint128_t sum = static_cast<uint128_t>(result[pos]) + product;
                result[pos] = static_cast<uint64_t>(sum);

                uint64_t carry = static_cast<uint64_t>(sum >> 64);
                int carry_pos = pos + 1;

                while (carry > 0 && carry_pos < 4) {
                    uint128_t carry_sum = static_cast<uint128_t>(result[carry_pos]) + carry;
                    result[carry_pos] = static_cast<uint64_t>(carry_sum);
                    carry = static_cast<uint64_t>(carry_sum >> 64);
                    carry_pos++;
                }
            }
        }

        int256_t final_result;
        final_result.low = static_cast<uint128_t>(result[0]) | (static_cast<uint128_t>(result[1]) << 64);
        final_result.high =
                static_cast<int128_t>(static_cast<uint128_t>(result[2]) | (static_cast<uint128_t>(result[3]) << 64));

        return is_negative ? -final_result : final_result;
    }

    /// @param other Value to multiply by
    /// @return Product using full 256-bit multiplication
    constexpr int256_t multiply_constexpr(const int256_t& other) const {
        if ((*this == int256_t(0)) || (other == int256_t(0))) {
            return int256_t(0);
        }
        if (other == int256_t(1)) {
            return *this;
        }
        if (*this == int256_t(1)) {
            return other;
        }

        const bool is_negative = (high < 0) != (other.high < 0);
        const int256_t lhs = (high < 0) ? -*this : *this;
        const int256_t rhs = (other.high < 0) ? -other : other;

        return multiply_core_256bit(lhs, rhs, is_negative);
    }

    int256_t multiply_runtime(const int256_t& other) const;

    static int256_t multiply_64x64(uint64_t a, uint64_t b, bool is_negative);

    static int256_t multiply_128x128(uint128_t a, uint128_t b, bool is_negative);

    static int256_t multiply_64x256(uint64_t small, const int256_t& large, bool is_negative);

    static int256_t multiply_128x256(uint128_t small, const int256_t& large, bool is_negative);
};

// =============================================================================
// Constants
// =============================================================================

/// Maximum value for int256_t: 2^255 - 1
constexpr int256_t INT256_MAX{
        static_cast<int128_t>((static_cast<uint128_t>(1) << 127) - 1), // high: 2^127 - 1
        static_cast<uint128_t>(-1)                                     // low:  2^128 - 1
};

/// Minimum value for int256_t: -2^255
/// Binary: 1000000...000 (1 followed by 255 zeros)
/// Decimal: -57896044618658097711785492504343953926634992332820282019728792003956564819968
constexpr int256_t INT256_MIN{
        static_cast<int128_t>(static_cast<uint128_t>(1) << 127), // high: -2^127
        static_cast<uint128_t>(0)                                // low:  0
};

/// Constant value 1 for int256_t
constexpr int256_t INT256_ONE{
        static_cast<int128_t>(0), // high: 0
        static_cast<uint128_t>(1) // low:  1
};

/// Constant value -1 for int256_t
constexpr int256_t INT256_NEGATIVE_ONE{
        static_cast<int128_t>(-1), // high: -1 (all bits set)
        static_cast<uint128_t>(-1) // low:  all bits set (0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
};

// =============================================================================
// Utility Functions
// =============================================================================

/// Compute absolute value of int256_t
/// @param value Input value
/// @return Absolute value
inline int256_t abs(const int256_t& value) {
    return (value.high < 0) ? -value : value;
}

/// Divide int256_t by 32-bit unsigned integer with quotient and remainder
/// @param value Dividend
/// @param divisor 32-bit divisor
/// @param quotient Pointer to store quotient result
/// @param remainder Pointer to store remainder result
void divmod_u32(const int256_t& value, uint32_t divisor, int256_t* quotient, uint32_t* remainder);

/// Parse string to int256_t
/// @param str String representation of integer
/// @return Parsed int256_t value
/// @throws std::invalid_argument if string is not a valid integer
int256_t parse_int256(const std::string& str);

} // namespace starrocks

// =============================================================================
// Standard Library Extensions
// =============================================================================

namespace std {

/// Specialization of make_unsigned for int256_t
template <>
struct make_unsigned<starrocks::int256_t> {
    using type = starrocks::int256_t;
};

/// Hash function specialization for int256_t
template <>
struct hash<starrocks::int256_t> {
    std::size_t operator()(const starrocks::int256_t& v) const noexcept {
        std::size_t h1 = std::hash<starrocks::int128_t>{}(v.high);
        std::size_t h2 = std::hash<starrocks::uint128_t>{}(v.low);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

/// std::to_string overload for int256_t
/// @param value int256_t value to convert
/// @return String representation of the value
inline std::string to_string(const starrocks::int256_t& value) {
    return value.to_string();
}

/// Absolute value function specialization for int256_t
/// @param value Input int256_t value
/// @return Absolute value of the input
inline starrocks::int256_t abs(const starrocks::int256_t& value) {
    return starrocks::abs(value);
}

/// Stream output operator for int256_t
/// @param os Output stream
/// @param value int256_t value to output
/// @return Reference to output stream
inline std::ostream& operator<<(std::ostream& os, const starrocks::int256_t& value) {
    return os << value.to_string();
}

} // namespace std

/// Formatter specialization for int256_t (fmt library)
template <>
struct fmt::formatter<starrocks::int256_t> : formatter<std::string> {
    template <typename FormatContext>
    auto format(const starrocks::int256_t& value, FormatContext& ctx) {
        return formatter<std::string>::format(value.to_string(), ctx);
    }
};
