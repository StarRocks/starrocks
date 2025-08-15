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

#include <cstdint>
#include <limits>
#include <type_traits>

#include "types/int256.h"

namespace starrocks {

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

template <typename T>
inline constexpr bool is_underlying_type_of_decimal = false;
template <>
inline constexpr bool is_underlying_type_of_decimal<int32_t> = true;
template <>
inline constexpr bool is_underlying_type_of_decimal<int64_t> = true;
template <>
inline constexpr bool is_underlying_type_of_decimal<int128_t> = true;
template <>
inline constexpr bool is_underlying_type_of_decimal<int256_t> = true;

template <typename T>
inline constexpr int decimal_precision_limit = -1;
template <>
inline constexpr int decimal_precision_limit<int32_t> = 9;
template <>
inline constexpr int decimal_precision_limit<int64_t> = 18;
template <>
inline constexpr int decimal_precision_limit<int128_t> = 38;
template <>
inline constexpr int decimal_precision_limit<int256_t> = 76;

template <typename T, int n>
struct EXP10 {
    using type = std::enable_if_t<is_underlying_type_of_decimal<T>, T>;
    static constexpr type value = EXP10<T, n - 1>::value * static_cast<type>(10);
};

template <typename T>
struct EXP10<T, 0> {
    using type = std::enable_if_t<is_underlying_type_of_decimal<T>, T>;
    static constexpr type value = static_cast<type>(1);
};

inline constexpr int32_t exp10_int32(int n) {
    constexpr int32_t values[] = {EXP10<int32_t, 0>::value, EXP10<int32_t, 1>::value, EXP10<int32_t, 2>::value,
                                  EXP10<int32_t, 3>::value, EXP10<int32_t, 4>::value, EXP10<int32_t, 5>::value,
                                  EXP10<int32_t, 6>::value, EXP10<int32_t, 7>::value, EXP10<int32_t, 8>::value,
                                  EXP10<int32_t, 9>::value};
    return values[n];
}

inline constexpr int64_t exp10_int64(int n) {
    constexpr int64_t values[] = {
            EXP10<int64_t, 0>::value,  EXP10<int64_t, 1>::value,  EXP10<int64_t, 2>::value,  EXP10<int64_t, 3>::value,
            EXP10<int64_t, 4>::value,  EXP10<int64_t, 5>::value,  EXP10<int64_t, 6>::value,  EXP10<int64_t, 7>::value,
            EXP10<int64_t, 8>::value,  EXP10<int64_t, 9>::value,  EXP10<int64_t, 10>::value, EXP10<int64_t, 11>::value,
            EXP10<int64_t, 12>::value, EXP10<int64_t, 13>::value, EXP10<int64_t, 14>::value, EXP10<int64_t, 15>::value,
            EXP10<int64_t, 16>::value, EXP10<int64_t, 17>::value, EXP10<int64_t, 18>::value,
    };
    return values[n];
}

inline constexpr int128_t exp10_int128(int n) {
    constexpr int128_t values[] = {
            EXP10<int128_t, 0>::value,  EXP10<int128_t, 1>::value,  EXP10<int128_t, 2>::value,
            EXP10<int128_t, 3>::value,  EXP10<int128_t, 4>::value,  EXP10<int128_t, 5>::value,
            EXP10<int128_t, 6>::value,  EXP10<int128_t, 7>::value,  EXP10<int128_t, 8>::value,
            EXP10<int128_t, 9>::value,  EXP10<int128_t, 10>::value, EXP10<int128_t, 11>::value,
            EXP10<int128_t, 12>::value, EXP10<int128_t, 13>::value, EXP10<int128_t, 14>::value,
            EXP10<int128_t, 15>::value, EXP10<int128_t, 16>::value, EXP10<int128_t, 17>::value,
            EXP10<int128_t, 18>::value, EXP10<int128_t, 19>::value, EXP10<int128_t, 20>::value,
            EXP10<int128_t, 21>::value, EXP10<int128_t, 22>::value, EXP10<int128_t, 23>::value,
            EXP10<int128_t, 24>::value, EXP10<int128_t, 25>::value, EXP10<int128_t, 26>::value,
            EXP10<int128_t, 27>::value, EXP10<int128_t, 28>::value, EXP10<int128_t, 29>::value,
            EXP10<int128_t, 30>::value, EXP10<int128_t, 31>::value, EXP10<int128_t, 32>::value,
            EXP10<int128_t, 33>::value, EXP10<int128_t, 34>::value, EXP10<int128_t, 35>::value,
            EXP10<int128_t, 36>::value, EXP10<int128_t, 37>::value, EXP10<int128_t, 38>::value,
    };
    return values[n];
}

inline constexpr int256_t exp10_int256(int n) {
    constexpr int256_t values[] = {
            EXP10<int256_t, 0>::value,  EXP10<int256_t, 1>::value,  EXP10<int256_t, 2>::value,
            EXP10<int256_t, 3>::value,  EXP10<int256_t, 4>::value,  EXP10<int256_t, 5>::value,
            EXP10<int256_t, 6>::value,  EXP10<int256_t, 7>::value,  EXP10<int256_t, 8>::value,
            EXP10<int256_t, 9>::value,  EXP10<int256_t, 10>::value, EXP10<int256_t, 11>::value,
            EXP10<int256_t, 12>::value, EXP10<int256_t, 13>::value, EXP10<int256_t, 14>::value,
            EXP10<int256_t, 15>::value, EXP10<int256_t, 16>::value, EXP10<int256_t, 17>::value,
            EXP10<int256_t, 18>::value, EXP10<int256_t, 19>::value, EXP10<int256_t, 20>::value,
            EXP10<int256_t, 21>::value, EXP10<int256_t, 22>::value, EXP10<int256_t, 23>::value,
            EXP10<int256_t, 24>::value, EXP10<int256_t, 25>::value, EXP10<int256_t, 26>::value,
            EXP10<int256_t, 27>::value, EXP10<int256_t, 28>::value, EXP10<int256_t, 29>::value,
            EXP10<int256_t, 30>::value, EXP10<int256_t, 31>::value, EXP10<int256_t, 32>::value,
            EXP10<int256_t, 33>::value, EXP10<int256_t, 34>::value, EXP10<int256_t, 35>::value,
            EXP10<int256_t, 36>::value, EXP10<int256_t, 37>::value, EXP10<int256_t, 38>::value,
            EXP10<int256_t, 39>::value, EXP10<int256_t, 40>::value, EXP10<int256_t, 41>::value,
            EXP10<int256_t, 42>::value, EXP10<int256_t, 43>::value, EXP10<int256_t, 44>::value,
            EXP10<int256_t, 45>::value, EXP10<int256_t, 46>::value, EXP10<int256_t, 47>::value,
            EXP10<int256_t, 48>::value, EXP10<int256_t, 49>::value, EXP10<int256_t, 50>::value,
            EXP10<int256_t, 51>::value, EXP10<int256_t, 52>::value, EXP10<int256_t, 53>::value,
            EXP10<int256_t, 54>::value, EXP10<int256_t, 55>::value, EXP10<int256_t, 56>::value,
            EXP10<int256_t, 57>::value, EXP10<int256_t, 58>::value, EXP10<int256_t, 59>::value,
            EXP10<int256_t, 60>::value, EXP10<int256_t, 61>::value, EXP10<int256_t, 62>::value,
            EXP10<int256_t, 63>::value, EXP10<int256_t, 64>::value, EXP10<int256_t, 65>::value,
            EXP10<int256_t, 66>::value, EXP10<int256_t, 67>::value, EXP10<int256_t, 68>::value,
            EXP10<int256_t, 69>::value, EXP10<int256_t, 70>::value, EXP10<int256_t, 71>::value,
            EXP10<int256_t, 72>::value, EXP10<int256_t, 73>::value, EXP10<int256_t, 74>::value,
            EXP10<int256_t, 75>::value, EXP10<int256_t, 76>::value,
    };
    return values[n];
}

template <typename T>
inline constexpr T get_scale_factor(int n) {
    if constexpr (std::is_same_v<T, int32_t>)
        return exp10_int32(n);
    else if constexpr (std::is_same_v<T, int64_t>)
        return exp10_int64(n);
    else if constexpr (std::is_same_v<T, int128_t>)
        return exp10_int128(n);
    else if constexpr (std::is_same_v<T, int256_t>)
        return exp10_int256(n);
    else {
        static_assert(is_underlying_type_of_decimal<T>,
                      "Underlying type of decimal must be int32_t/int64_t/int128_t/int256_t");
        return T(0);
    }
}

template <typename T>
inline constexpr T get_max_decimal(int predision) {
    return get_scale_factor<T>(predision) - 1;
}

template <typename T>
inline constexpr T get_min_decimal(int predision) {
    return -get_max_decimal<T>(predision);
}

template <typename T>
inline constexpr T get_max_decimal() {
    return get_max_decimal<T>(decimal_precision_limit<T>);
}

template <typename T>
inline constexpr T get_min_decimal() {
    return -get_max_decimal<T>();
}

static const uint128_t UINT128_MAX = uint128_t(int128_t(-1L));
static const int128_t INT128_MAX = UINT128_MAX >> 1;
static const int128_t INT128_MIN = -INT128_MAX - 1;

template <typename T>
inline constexpr T get_max() {
    if constexpr (std::is_same_v<T, int128_t>) {
        return INT128_MAX;
    } else if constexpr (std::is_same_v<T, int256_t>) {
        return INT256_MAX;
    } else {
        return std::numeric_limits<T>::max();
    }
}

template <typename T>
inline constexpr T get_min() {
    if constexpr (std::is_same_v<T, int128_t>) {
        return INT128_MIN;
    } else if constexpr (std::is_same_v<T, int256_t>) {
        return INT256_MIN;
    } else {
        return std::numeric_limits<T>::lowest();
    }
}

template <typename T>
constexpr bool is_signed_integer =
        (std::is_integral_v<T> && std::is_signed_v<T>) || std::is_same_v<T, int128_t> || std::is_same_v<T, int256_t>;

template <typename T>
using DecimalType = std::enable_if_t<is_underlying_type_of_decimal<T>, T>;
template <typename T>
using FloatType = std::enable_if_t<std::is_floating_point_v<T>, T>;
template <typename T>
using IntegerType = std::enable_if_t<is_signed_integer<T>, T>;

} // namespace starrocks
