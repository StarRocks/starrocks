// src/types/int256.h
#pragma once

#include <cstdint>
#include <type_traits>
#include <string>
#include <fmt/format.h>

namespace starrocks {

    typedef __int128 int128_t;
    typedef unsigned __int128 uint128_t;

    struct int256_t {
        int128_t high;
        uint128_t low;

        int256_t() : high(0), low(0) {}
        int256_t(int128_t h, uint128_t l) : high(h), low(l) {}
        
        // 添加从基本整数类型的构造函数
        int256_t(int value) : high(value < 0 ? -1 : 0), low(static_cast<uint128_t>(value)) {}
        int256_t(long value) : high(value < 0 ? -1 : 0), low(static_cast<uint128_t>(value)) {}
        int256_t(long long value) : high(value < 0 ? -1 : 0), low(static_cast<uint128_t>(value)) {}

        // 基本比较操作符
        bool operator==(const int256_t& other) const {
            return high == other.high && low == other.low;
        }

        bool operator!=(const int256_t& other) const {
            return !(*this == other);
        }

        bool operator<(const int256_t& other) const {
            if (high != other.high) return high < other.high;
            return low < other.low;
        }

        bool operator<=(const int256_t& other) const {
            return *this < other || *this == other;
        }

        bool operator>(const int256_t& other) const {
            return !(*this <= other);
        }

        bool operator>=(const int256_t& other) const {
            return !(*this < other);
        }

        // 添加与基本整数类型的比较操作符
        bool operator<(int other) const {
            return *this < int256_t(other);
        }

        bool operator>(int other) const {
            return *this > int256_t(other);
        }

        bool operator==(int other) const {
            return *this == int256_t(other);
        }

        bool operator!=(int other) const {
            return *this != int256_t(other);
        }

        bool operator<=(int other) const {
            return *this <= int256_t(other);
        }

        bool operator>=(int other) const {
            return *this >= int256_t(other);
        }

        // 添加算术操作符
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
            if (result.low < low) { // 检查溢出
                result.high++;
            }
            return result;
        }

        int256_t operator-(const int256_t& other) const {
            return *this + (-other);
        }

        int256_t operator*(const int256_t& other) const {
            // 实现乘法
            int256_t result;
            // 这里需要实现256位乘法
            // 可以使用分块乘法算法
            return result;
        }

        int256_t operator/(const int256_t& other) const {
            // 实现除法
            int256_t result;
            // 这里需要实现256位除法
            return result;
        }

        int256_t operator%(const int256_t& other) const {
            // 实现取模
            int256_t result;
            // 这里需要实现256位取模
            return result;
        }

        // 添加赋值运算符
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

        // 添加类型转换运算符
        explicit operator int() const {
            return static_cast<int>(low);
        }

        explicit operator long() const {
            return static_cast<long>(low);
        }

        explicit operator long long() const {
            return static_cast<long long>(low);
        }

        // 添加右移操作符
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

        // 添加右移赋值操作符
        int256_t& operator>>=(int shift) {
            *this = *this >> shift;
            return *this;
        }

        // 添加到 bool 的隐式转换
        operator bool() const {
            return high != 0 || low != 0;
        }

        // 添加与 int 类型的乘法运算符
        int256_t operator*(int other) const {
            int256_t result;
            result.high = high * other;
            result.low = low * other;
            // 处理进位
            if (other != 0) {
                result.high += (low * other) >> 64;
            }
            return result;
        }

        // 添加与 int 类型的乘法赋值运算符
        int256_t& operator*=(int other) {
            *this = *this * other;
            return *this;
        }

        // 添加与 int 类型的加法运算符
        int256_t operator+(int other) const {
            int256_t result = *this;
            result.low += other;
            // 处理进位
            if (result.low < low) {
                result.high++;
            }
            return result;
        }

        // 添加与 int 类型的加法赋值运算符
        int256_t& operator+=(int other) {
            *this = *this + other;
            return *this;
        }
    };

    // MIN_INT256和MAX_INT256常量定义
    static const int256_t MIN_INT256 = {std::numeric_limits<int128_t>::lowest(), static_cast<uint128_t>(0)};
    static const int256_t MAX_INT256 = {std::numeric_limits<int128_t>::max(), std::numeric_limits<uint128_t>::max()};

    // 在 starrocks 命名空间中添加 abs 函数
    inline int256_t abs(const int256_t& value) {
        if (value.high < 0) {
            return -value;
        }
        return value;
    }

    // 在 starrocks 命名空间中声明 to_string 函数
    inline std::string to_string(const int256_t& value) {
        if (value.high == 0 && value.low == 0) {
            return "0";
        }
        
        std::string result;
        if (value.high < 0) {
            result = "-";
            // 处理负数
            int256_t abs_value = -value;
            // 将 abs_value 转换为字符串
            result += std::to_string(static_cast<long long>(abs_value.low));
            if (abs_value.high != 0) {
                result += std::to_string(static_cast<long long>(abs_value.high));
            }
        } else {
            // 处理正数
            result = std::to_string(static_cast<long long>(value.low));
            if (value.high != 0) {
                result += std::to_string(static_cast<long long>(value.high));
            }
        }
        return result;
    }

} // namespace starrocks

// 在全局命名空间中添加 abs 函数重载
inline starrocks::int256_t abs(const starrocks::int256_t& value) {
    return starrocks::abs(value);
}

// 为 std::make_unsigned 添加特化
namespace std {
    template <>
    struct make_unsigned<starrocks::int256_t> {
        using type = starrocks::int256_t;  // 由于 int256_t 已经包含了符号位，这里直接使用 int256_t
    };
}

// 在 fmt 命名空间中添加格式化支持
namespace fmt {
template <>
struct formatter<starrocks::int256_t> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const starrocks::int256_t& value, FormatContext& ctx) {
        return formatter<std::string_view>::format(starrocks::to_string(value), ctx);
    }
};
}