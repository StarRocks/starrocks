// src/types/int256.h
#pragma once

#include <fmt/format.h>

#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace starrocks {

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

struct int256_t {
public:
    // =============================================================================
    // Storage Layout Documentation
    // =============================================================================
    /*
     * Storage Layout:
     * - Fixed size: 32 bytes (256 bits)
     * - Byte order: Native endian (automatically handled)
     * - Layout: 
     *   Little-endian: [low_64bits][high_64bits][higher_64bits][highest_64bits]
     *   Big-endian: [highest_64bits][higher_64bits][high_64bits][low_64bits]
     * 
     * Example (Little-endian):
     * For value 0x1234567890ABCDEF_87654321FEDCBA90_1122334455667788_99AABBCCDDEEFF00
     * Storage bytes (hex):
     * 00 FF EE DD CC BB AA 99 88 77 66 55 44 33 22 11 90 BA DC FE 21 43 65 87 EF CD AB 90 78 56 34 12
     * 
     * Example (Big-endian):
     * For the same value:
     * Storage bytes (hex):
     * 12 34 56 78 90 AB CD EF 87 65 43 21 FE DC BA 90 11 22 33 44 55 66 77 88 99 AA BB CC DD EE FF 00
     */

    // =============================================================================
    // Endianness Support
    // =============================================================================
    enum class Endianness {
        LITTLE,  // Default
        BIG
    };

    // =============================================================================
    // Member Variables
    // =============================================================================
    union {
        struct {
            int128_t high;
            uint128_t low;
        };
        struct {
#if __BYTE_ORDER == __LITTLE_ENDIAN
            uint64_t parts[4]; // [low_64bits][high_64bits][higher_64bits][highest_64bits]
#else
            uint64_t parts[4]; // [highest_64bits][higher_64bits][high_64bits][low_64bits]
#endif
        };
    };

    // =============================================================================
    // Constructors
    // =============================================================================
    constexpr int256_t() : low(0), high(0) {}
    constexpr int256_t(int128_t h, uint128_t l) : low(l), high(h) {}

    // Integer constructors
    constexpr int256_t(int value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(long value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(long long value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(__int128 value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}

    // Floating point constructors
    constexpr int256_t(float value)
            : low(static_cast<uint128_t>(static_cast<long long>(value))), high(value < 0 ? -1 : 0) {}
    constexpr int256_t(double value)
            : low(static_cast<uint128_t>(static_cast<long long>(value))), high(value < 0 ? -1 : 0) {}

    // =============================================================================
    // Comparison Operators
    // =============================================================================
    constexpr bool operator==(const int256_t& other) const { return high == other.high && low == other.low; }
    constexpr bool operator!=(const int256_t& other) const { return !(*this == other); }
    constexpr bool operator<(const int256_t& other) const {
        if (high != other.high) return high < other.high;
        return low < other.low;
    }
    constexpr bool operator<=(const int256_t& other) const { return *this < other || *this == other; }
    constexpr bool operator>(const int256_t& other) const { return !(*this <= other); }
    constexpr bool operator>=(const int256_t& other) const { return !(*this < other); }

    // Comparison with int
    constexpr bool operator==(int other) const { return *this == int256_t(other); }
    constexpr bool operator!=(int other) const { return *this != int256_t(other); }
    constexpr bool operator<(int other) const { return *this < int256_t(other); }
    constexpr bool operator<=(int other) const { return *this <= int256_t(other); }
    constexpr bool operator>(int other) const { return *this > int256_t(other); }
    constexpr bool operator>=(int other) const { return *this >= int256_t(other); }

    // =============================================================================
    // Unary Operators
    // =============================================================================
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
    constexpr operator bool() const { return high != 0 || low != 0; }

    // =============================================================================
    // Arithmetic Operators (int256_t)
    // =============================================================================
    constexpr int256_t operator+(const int256_t& other) const {
        int256_t result;
        result.low = low + other.low;
        result.high = high + other.high;
        if (result.low < low) {
            result.high++;
        }
        return result;
    }

    constexpr int256_t operator-(const int256_t& other) const { return *this + (-other); }

    constexpr int256_t operator*(const int256_t& other) const {
        if ((high == 0 && low == 0) || (other.high == 0 && other.low == 0)) {
            return int256_t(0);
        }

        bool is_negative = (high < 0) != (other.high < 0);
        int256_t lhs = (high < 0) ? -*this : *this;
        int256_t rhs = (other.high < 0) ? -other : other;

        uint64_t result[4] = {0, 0, 0, 0};

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                int pos = i + j;

                if (pos >= 4) {
                    break;
                }

                __uint128_t product = static_cast<__uint128_t>(lhs.parts[i]) *
                                     static_cast<__uint128_t>(rhs.parts[j]);

                __uint128_t sum = static_cast<__uint128_t>(result[pos]) + product;
                result[pos] = static_cast<uint64_t>(sum);

                uint64_t carry = static_cast<uint64_t>(sum >> 64);
                int carry_pos = pos + 1;

                while (carry > 0 && carry_pos < 4) {
                    __uint128_t carry_sum = static_cast<__uint128_t>(result[carry_pos]) + carry;
                    result[carry_pos] = static_cast<uint64_t>(carry_sum);
                    carry = static_cast<uint64_t>(carry_sum >> 64);
                    carry_pos++;
                }
            }
        }

        int256_t final_result;
        final_result.parts[0] = result[0];
        final_result.parts[1] = result[1];
        final_result.parts[2] = result[2];
        final_result.parts[3] = result[3];

        return is_negative ? -final_result : final_result;
    }

    // =============================================================================
    // High-Performance Division Implementation
    // =============================================================================
    int256_t operator/(const int256_t& other) const;

    constexpr int256_t operator%(const int256_t& other) const {
        if (other.high == 0 && other.low == 0) {
            throw std::domain_error("Division by zero");
        }

        // Handle special cases first
        if (high == 0 && low == 0) return int256_t(0);
        if (*this == other) return int256_t(0);

        int256_t quotient = *this / other;
        int256_t remainder = *this - (quotient * other);
        return remainder;
    }

    // =============================================================================
    // Arithmetic Operators (primitive types)
    // =============================================================================
    constexpr int256_t operator+(int other) const {
        int256_t result = *this;
        result.low += other;
        if (result.low < low) {
            result.high++;
        }
        return result;
    }

    constexpr int256_t operator*(int other) const {
        int256_t result;
        result.high = high * other;
        result.low = low * other;
        if (other != 0) {
            result.high += (low * other) >> 64;
        }
        return result;
    }

    int256_t operator-(int other) const { return *this - int256_t(other); }
    int256_t operator-(long other) const { return *this - int256_t(other); }
    int256_t operator-(long long other) const { return *this - int256_t(other); }
    int256_t operator-(__int128 other) const { return *this - int256_t(other); }

    inline int256_t operator/(int other) const { return *this / int256_t(other); }
    inline int256_t operator/(long other) const { return *this / int256_t(other); }
    inline int256_t operator/(long long other) const { return *this / int256_t(other); }
    inline int256_t operator/(uint64_t other) const { return *this / int256_t(static_cast<int64_t>(other)); }
    inline int256_t operator/(__int128 other) const { return *this / int256_t(other); }

    inline int256_t operator%(int other) const { return *this % int256_t(other); }
    inline int256_t operator%(long other) const { return *this % int256_t(other); }
    inline int256_t operator%(long long other) const { return *this % int256_t(other); }
    inline int256_t operator%(uint64_t other) const { return *this % int256_t(static_cast<int64_t>(other)); }
    inline int256_t operator%(__int128 other) const { return *this % int256_t(other); }

    // =============================================================================
    // Assignment Operators
    // =============================================================================
    constexpr int256_t& operator+=(const int256_t& other) {
        *this = *this + other;
        return *this;
    }
    constexpr int256_t& operator-=(const int256_t& other) {
        *this = *this - other;
        return *this;
    }
    constexpr int256_t& operator*=(const int256_t& other) {
        *this = *this * other;
        return *this;
    }
    int256_t& operator/=(const int256_t& other) {
        *this = *this / other;
        return *this;
    }
    constexpr int256_t& operator%=(const int256_t& other) {
        *this = *this % other;
        return *this;
    }

    constexpr int256_t& operator+=(int other) {
        *this = *this + other;
        return *this;
    }
    constexpr int256_t& operator*=(int other) {
        *this = *this * other;
        return *this;
    }

    // =============================================================================
    // Increment/Decrement Operators
    // =============================================================================
    constexpr int256_t& operator++() {
        *this += int256_t(1);
        return *this;
    }

    constexpr int256_t operator++(int) {
        int256_t tmp = *this;
        ++(*this);
        return tmp;
    }

    // =============================================================================
    // Bit Shift Operators
    // =============================================================================
    constexpr int256_t operator>>(int shift) const {
        if (shift <= 0) return *this;

        if (shift >= 256) {
            return (high < 0) ? int256_t(-1, -1) : int256_t(0, 0);
        }

        if (shift >= 128) {
            return int256_t((high < 0) ? -1 : 0, high >> (shift - 128));
        }

        return int256_t(high >> shift,
                        static_cast<int128_t>((static_cast<uint128_t>(low) >> shift) |
                                              (static_cast<uint128_t>(high) << (128 - shift))));
    }

    constexpr int256_t& operator>>=(int shift) {
        *this = *this >> shift;
        return *this;
    }

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

    constexpr int256_t& operator<<=(int shift) { return *this = *this << shift; }

    // =============================================================================
    // Type Conversion Operators
    // =============================================================================
    explicit operator int() const { return static_cast<int>(low); }
    explicit operator long() const { return static_cast<long>(low); }
    explicit operator long long() const { return static_cast<long long>(low); }
    explicit operator double() const {
        return static_cast<double>(high) * std::pow(2.0, 128) + static_cast<double>(low);
    }

    // =============================================================================
    // Mixed Type Operations (double)
    // =============================================================================
    double operator-(double other) const { return static_cast<double>(*this) - other; }

    // =============================================================================
    // Friend Functions
    // =============================================================================
    friend int256_t operator-(int lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }
    friend int256_t operator-(long lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }
    friend int256_t operator-(long long lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }
    friend int256_t operator-(__int128 lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }
    friend double operator-(double lhs, const int256_t& rhs) { return lhs - static_cast<double>(rhs); }

    friend int256_t operator>>(const int256_t& value, size_t shift) { return value >> static_cast<int>(shift); }

    // =============================================================================
    // Serialization Functions
    // =============================================================================
    // Serialize to byte array (always in little-endian for storage)
    void serialize_to_bytes(uint8_t* buffer) const {
        for (int i = 0; i < 4; i++) {
            uint64_t part = parts[i];
            for (int j = 0; j < 8; j++) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
                buffer[i * 8 + j] = static_cast<uint8_t>(part & 0xFF);
#else
                buffer[i * 8 + (7 - j)] = static_cast<uint8_t>(part & 0xFF);
#endif
                part >>= 8;
            }
        }
    }

    // Deserialize from byte array (always from little-endian storage)
    static int256_t deserialize_from_bytes(const uint8_t* buffer) {
        int256_t result;
        for (int i = 0; i < 4; i++) {
            uint64_t part = 0;
            for (int j = 0; j < 8; j++) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
                part = (part << 8) | buffer[i * 8 + (7 - j)];
#else
                part = (part << 8) | buffer[i * 8 + j];
#endif
            }
            result.parts[i] = part;
        }
        return result;
    }

private:
    // =============================================================================
    // Forward Declarations for Division Optimization
    // =============================================================================
    static bool is_power_of_2(const int256_t& value);
    int256_t adaptive_divide(const int256_t &dividend, const int256_t &divisor) const;
    int256_t safe_abs() const;
    int count_trailing_zeros(int256_t divisor) const;
    int count_leading_zeros() const;

    int256_t power_of_2_divide(const int256_t &dividend, const int256_t &divisor) const;

    static int256_t divide_by_small_divisor(const int256_t &dividend, uint64_t divisor);

    int256_t divide_by_128bit(const int256_t &dividend, uint128_t divisor) const;

    int256_t divide_by_256bit(const int256_t &dividend, const int256_t &divisor) const;

};

// =============================================================================
// Constants
// =============================================================================
    inline const int256_t INT256_MAX = int256_t(
        static_cast<int128_t>(0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF),
        static_cast<uint128_t>(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
    );

    inline const int256_t INT256_MIN = []{
        int256_t v;
        v.high = static_cast<int128_t>(static_cast<uint128_t>(1) << 127); // -2^127
        v.low = 0;
        return v;
    }();

// =============================================================================
// Utility Functions
// =============================================================================
inline int256_t abs(const int256_t& value) {
    return (value.high < 0) ? -value : value;
}

inline void divmod_u32(const int256_t& value, uint32_t divisor, int256_t* quotient, uint32_t* remainder) {
    uint64_t parts[4];
    parts[0] = static_cast<uint64_t>(value.low);
    parts[1] = static_cast<uint64_t>(value.low >> 64);
    parts[2] = static_cast<uint64_t>(value.high);
    parts[3] = static_cast<uint64_t>(value.high >> 64);

    uint64_t q[4] = {0, 0, 0, 0};
    uint64_t r = 0;
    for (int i = 3; i >= 0; --i) {
        __uint128_t acc = (__uint128_t(r) << 64) | parts[i];
        q[i] = static_cast<uint64_t>(acc / divisor);
        r = static_cast<uint64_t>(acc % divisor);
    }
    *quotient = int256_t((__int128_t(q[3]) << 64) | q[2], (__uint128_t(q[1]) << 64) | q[0]);
    *remainder = r;
}

inline int256_t parse_int256(const std::string& str) {
    int256_t result = 0;
    bool negative = false;
    size_t i = 0;
    if (!str.empty() && str[0] == '-') {
        negative = true;
        i = 1;
    }
    for (; i < str.size(); ++i) {
        if (str[i] < '0' || str[i] > '9') {
            throw std::invalid_argument("invalid digit");
        }
        result = result * 10 + (str[i] - '0');
    }
    return negative ? -result : result;
}

std::string to_string(const int256_t& value);

} // namespace starrocks

// =============================================================================
// Global Namespace Functions
// =============================================================================
inline starrocks::int256_t abs(const starrocks::int256_t& value) {
    return starrocks::abs(value);
}

namespace std {
template <>
struct make_unsigned<starrocks::int256_t> {
    using type = starrocks::int256_t;
};

template <>
struct hash<starrocks::int256_t> {
    std::size_t operator()(const starrocks::int256_t& v) const noexcept {
        std::size_t h1 = std::hash<starrocks::int128_t>{}(v.high);
        std::size_t h2 = std::hash<starrocks::uint128_t>{}(v.low);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};
} // namespace std

// =============================================================================
// fmt Library Specialization
// =============================================================================
namespace fmt {
template <>
struct formatter<starrocks::int256_t> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const starrocks::int256_t& value, FormatContext& ctx) {
        return formatter<std::string_view>::format(starrocks::to_string(value), ctx);
    }
};
} // namespace fmt

