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
    // Member Variables
    // =============================================================================
    union {
        struct {
            uint128_t low;
            int128_t high;
        };
        struct {
            uint64_t parts[4]; // parts[0-1] = low, parts[2-3] = high
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

        bool negative = (high < 0);

        int256_t dividend = *this;
        int256_t divisor = other;
        if (dividend.high < 0) dividend = -dividend;
        if (divisor.high < 0) divisor = -divisor;

        if (dividend < divisor) {
            return negative ? -dividend : dividend;
        }

        if (divisor.high == 0 && divisor.low == 1) {
            return int256_t(0);
        }

        int256_t remainder(0);

        for (int i = 255; i >= 0; --i) {
            remainder = remainder + remainder;

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
            }
        }

        return negative ? -remainder : remainder;
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

private:
    // =============================================================================
    // Forward Declarations for Division Optimization
    // =============================================================================
    int find_msb(const int256_t& value) const;
    bool is_power_of_2(const int256_t& value) const;
    int count_trailing_zeros(const int256_t& value) const;
    int count_trailing_zeros_128(uint128_t value) const;
    int find_msb_64(uint64_t value) const;

    // Division algorithm declarations
    int256_t fast_small_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t barrett_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t burnikel_ziegler_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t power_of_2_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t adaptive_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t fast_divide_by_128(const int256_t& dividend, uint128_t divisor, bool negative) const;
    uint128_t divide_256_by_128(uint128_t high, uint128_t low, uint128_t divisor) const;
    uint128_t divide_256_by_64(uint128_t high, uint128_t low, uint64_t divisor) const;
    int256_t reciprocal_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t fast_binary_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    int256_t calculate_barrett_constant(const int256_t& divisor, int k) const;
    int256_t multiply_high_precision(const int256_t& a, const int256_t& b) const;
    int256_t newton_raphson_reciprocal(const int256_t& divisor, const int256_t& power_of_2) const;
    void recursive_divide_blocks(uint64_t* dividend_blocks, int dividend_size,
                               const uint64_t* divisor_blocks, int divisor_size,
                               uint64_t* quotient_blocks) const;
    void divide_by_single_block(uint64_t* dividend_blocks, int dividend_size,
                              uint64_t divisor_block, uint64_t* quotient_blocks) const;
    void divide_2n_by_n(uint64_t* dividend_2n, const uint64_t* divisor_n, int n, uint64_t* quotient) const;
    void divide_3half_by_2half(uint64_t* dividend, const uint64_t* divisor, int half_n, uint64_t* quotient) const;
    void refine_quotient_estimate(uint64_t* dividend, const uint64_t* divisor, int half_n,
                                uint64_t estimated_quotient, uint64_t* quotient) const;
    void calculate_remainder_and_continue(uint64_t* dividend_2n, const uint64_t* divisor_n, int n,
                                        uint64_t* quotient, uint64_t* temp_remainder) const;
    int256_t small_divisor_divide(const int256_t& dividend, const int256_t& divisor, bool negative) const;
    uint64_t modular_inverse_64(uint64_t value) const;
    int256_t montgomery_divide(const int256_t& dividend, uint64_t small_divisor, uint64_t inverse) const;
};

// =============================================================================
// Constants
// =============================================================================
inline const int256_t INT256_MAX =
        int256_t(std::numeric_limits<int128_t>::max(), std::numeric_limits<uint128_t>::max());
inline const int256_t INT256_MIN = int256_t(std::numeric_limits<int128_t>::lowest(), 0);

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

