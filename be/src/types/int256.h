// src/types/int256.h
#pragma once

#include <fmt/format.h>

#include <cmath>
#include <cstdint>
#include <string>
#include <type_traits>

namespace starrocks {

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

struct int256_t {
    uint128_t low;
    int128_t high;

    int256_t() : low(0), high(0) {}
    int256_t(int128_t h, uint128_t l) : low(l), high(h) {}

    int256_t(int value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    int256_t(long value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}
    int256_t(long long value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}

    int256_t(__int128 value) : low(static_cast<uint128_t>(value)), high(value < 0 ? -1 : 0) {}

    bool operator==(const int256_t& other) const { return high == other.high && low == other.low; }

    bool operator!=(const int256_t& other) const { return !(*this == other); }

    bool operator<(const int256_t& other) const {
        if (high != other.high) return high < other.high;
        return low < other.low;
    }

    bool operator<=(const int256_t& other) const { return *this < other || *this == other; }

    bool operator>(const int256_t& other) const { return !(*this <= other); }

    bool operator>=(const int256_t& other) const { return !(*this < other); }

    bool operator<(int other) const { return *this < int256_t(other); }

    bool operator>(int other) const { return *this > int256_t(other); }

    bool operator==(int other) const { return *this == int256_t(other); }

    bool operator!=(int other) const { return *this != int256_t(other); }

    bool operator<=(int other) const { return *this <= int256_t(other); }

    bool operator>=(int other) const { return *this >= int256_t(other); }

    int256_t operator-() const {
        int256_t result;
        if (low == 0) {
            result.high = -high;
            result.low = 0;
        } else {
            result.high = ~high;
            result.low = ~low + 1;
        }
        return result;
    }

    int256_t operator+(const int256_t& other) const {
        int256_t result;
        result.low = low + other.low;
        result.high = high + other.high;
        if (result.low < low) {
            result.high++;
        }
        return result;
    }

    int256_t operator-(const int256_t& other) const { return *this + (-other); }

    int256_t operator*(const int256_t& other) const {
        bool negative = (high < 0) != (other.high < 0);

        int256_t a = *this;
        int256_t b = other;
        if (a.high < 0) a = -a;
        if (b.high < 0) b = -b;

        int256_t result(0);
        int256_t temp = a;

        for (int i = 0; i < 256; ++i) {
            if (i < 128) {
                if ((b.low >> i) & 1) {
                    result = result + temp;
                }
            } else {
                if ((b.high >> (i - 128)) & 1) {
                    result = result + temp;
                }
            }

            temp = temp + temp;
        }

        return negative ? -result : result;
    }

    int256_t operator/(const int256_t& other) const {
        if (other.high == 0 && other.low == 0) {
            throw std::domain_error("Division by zero");
        }

        bool negative = (high < 0) != (other.high < 0);

        int256_t dividend = *this;
        int256_t divisor = other;
        if (dividend.high < 0) dividend = -dividend;
        if (divisor.high < 0) divisor = -divisor;

        if (dividend < divisor) {
            return int256_t(0);
        }

        if (divisor.high == 0 && divisor.low == 1) {
            return negative ? -dividend : dividend;
        }

        int256_t quotient(0);
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

                if (i >= 128) {
                    quotient.high |= (int128_t(1) << (i - 128));
                } else {
                    quotient.low |= (uint128_t(1) << i);
                }
            }
        }

        return negative ? -quotient : quotient;
    }

    int256_t operator%(const int256_t& other) const {
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

    int256_t& operator*=(const int256_t& other) {
        *this = *this * other;
        return *this;
    }

    int256_t& operator+=(const int256_t& other) {
        *this = *this + other;
        return *this;
    }

    int256_t& operator-=(const int256_t& other) {
        *this = *this - other;
        return *this;
    }

    int256_t& operator/=(const int256_t& other) {
        *this = *this / other;
        return *this;
    }

    int256_t& operator%=(const int256_t& other) {
        *this = *this % other;
        return *this;
    }

    explicit operator int() const { return static_cast<int>(low); }

    explicit operator long() const { return static_cast<long>(low); }

    explicit operator long long() const { return static_cast<long long>(low); }

    int256_t operator>>(int shift) const {
        int256_t result = *this;
        if (shift >= 256) {
            result.high = 0;
            result.low = 0;
        } else if (shift >= 128) {
            result.low = result.high >> (shift - 128);
            result.high = 0;
        } else {
            result.low = (result.low >> shift) | (result.high << (128 - shift));
            result.high = result.high >> shift;
        }
        return result;
    }

    int256_t& operator>>=(int shift) {
        *this = *this >> shift;
        return *this;
    }

    operator bool() const { return high != 0 || low != 0; }

    int256_t operator*(int other) const {
        int256_t result;
        result.high = high * other;
        result.low = low * other;
        if (other != 0) {
            result.high += (low * other) >> 64;
        }
        return result;
    }

    int256_t& operator*=(int other) {
        *this = *this * other;
        return *this;
    }

    int256_t operator+(int other) const {
        int256_t result = *this;
        result.low += other;
        if (result.low < low) {
            result.high++;
        }
        return result;
    }

    int256_t& operator+=(int other) {
        *this = *this + other;
        return *this;
    }

    int256_t operator-(int other) const { return *this - int256_t(other); }

    int256_t operator-(long other) const { return *this - int256_t(other); }

    int256_t operator-(long long other) const { return *this - int256_t(other); }

    int256_t operator-(__int128 other) const {
        int256_t result = *this;
        result -= int256_t(other);
        return result;
    }

    double operator-(double other) const { return static_cast<double>(*this) - other; }

    friend double operator-(double lhs, const int256_t& rhs) { return lhs - static_cast<double>(rhs); }

    explicit operator double() const {
        return static_cast<double>(high) * std::pow(2.0, 128) + static_cast<double>(low);
    }

    friend int256_t operator-(int lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }
    friend int256_t operator-(long lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }
    friend int256_t operator-(long long lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }

    friend int256_t operator-(__int128 lhs, const int256_t& rhs) { return int256_t(lhs) - rhs; }

    int256_t& operator++() {
        *this += int256_t(1);
        return *this;
    }

    int256_t operator++(int) {
        int256_t tmp = *this;
        ++(*this);
        return tmp;
    }

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
};

inline const int256_t INT256_MAX =
        int256_t(std::numeric_limits<int128_t>::max(), std::numeric_limits<uint128_t>::max());
inline const int256_t INT256_MIN = int256_t(std::numeric_limits<int128_t>::lowest(), 0);

inline int256_t abs(const int256_t& value) {
    if (value.high < 0) {
        return -value;
    }
    return value;
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
        if (str[i] < '0' || str[i] > '9') throw std::invalid_argument("invalid digit");
        result = result * 10 + (str[i] - '0');
    }
    if (negative) result = -result;
    return result;
}

std::string to_string(const int256_t& value);

} // namespace starrocks

inline starrocks::int256_t abs(const starrocks::int256_t& value) {
    return starrocks::abs(value);
}

namespace std {
template <>
struct make_unsigned<starrocks::int256_t> {
    using type = starrocks::int256_t;
};
} // namespace std

namespace fmt {
template <>
struct formatter<starrocks::int256_t> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const starrocks::int256_t& value, FormatContext& ctx) {
        return formatter<std::string_view>::format(starrocks::to_string(value), ctx);
    }
};
} // namespace fmt

namespace std {
template <>
struct hash<starrocks::int256_t> {
    std::size_t operator()(const starrocks::int256_t& v) const noexcept {
        std::size_t h1 = std::hash<starrocks::int128_t>{}(v.high);
        std::size_t h2 = std::hash<starrocks::uint128_t>{}(v.low);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};
} // namespace std