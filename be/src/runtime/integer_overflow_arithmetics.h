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

#include <runtime/int128_arithmetics_x86_64.h>
#include "runtime/int256_arithmetics_x86_64.h"
#include <util/decimal_types.h>

namespace starrocks {
typedef __int128 int128_t;
template <typename T>
inline bool add_overflow(T a, T b, T* c) {
    return __builtin_add_overflow(a, b, c);
}
template <>
inline bool add_overflow(int a, int b, int* c) {
    return __builtin_sadd_overflow(a, b, c);
}
template <>
inline bool add_overflow(long a, long b, long* c) {
    return __builtin_saddl_overflow(a, b, c);
}

template <>
inline bool add_overflow(long long a, long long b, long long* c) {
    return __builtin_saddll_overflow(a, b, c);
}

inline bool int128_add_overflow(int128_t a, int128_t b, int128_t* c) {
    *c = a + b;
    return ((a < 0) == (b < 0)) && ((*c < 0) != (a < 0));
}

template <>
inline bool add_overflow(int128_t a, int128_t b, int128_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return asm_add_overflow(a, b, c);
#else
    return int128_add_overflow(a, b, c);
#endif
}

template <>
inline bool add_overflow(int256_t a, int256_t b, int256_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return asm_add(a, b, *c);
#else
    *c = a + b;
    bool a_negative = a < 0;
    bool b_negative = b < 0;
    bool result_negative = *c < 0;
    return (a_negative == b_negative) && (a_negative != result_negative);
#endif
}

template <typename T>
inline bool sub_overflow(T a, T b, T* c) {
    return __builtin_sub_overflow(a, b, c);
}
template <>
inline bool sub_overflow(int a, int b, int* c) {
    return __builtin_ssub_overflow(a, b, c);
}
template <>
inline bool sub_overflow(long a, long b, long* c) {
    return __builtin_ssubl_overflow(a, b, c);
}

template <>
inline bool sub_overflow(long long a, long long b, long long* c) {
    return __builtin_ssubll_overflow(a, b, c);
}

inline bool int128_sub_overflow(int128_t a, int128_t b, int128_t* c) {
    *c = a - b;
    return ((a < 0) == (0 < b)) && ((*c < 0) != (a < 0));
}

template <>
inline bool sub_overflow(int128_t a, int128_t b, int128_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return asm_sub_overflow(a, b, c);
#else
    return int128_sub_overflow(a, b, c);
#endif
}

template <>
inline bool sub_overflow(int256_t a, int256_t b, int256_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return asm_sub(a, b, *c);
#else
    *c = a - b;
    return ((a < 0) == (0 < b)) && ((*c < 0) != (a < 0));
#endif
}


template <typename T>
inline bool mul_overflow(T a, T b, T* c) {
    return __builtin_mul_overflow(a, b, c);
}
template <>
inline bool mul_overflow(int a, int b, int* c) {
    return __builtin_smul_overflow(a, b, c);
}
template <>
inline bool mul_overflow(long a, long b, long* c) {
    return __builtin_smull_overflow(a, b, c);
}

template <>
inline bool mul_overflow(long long a, long long b, long long* c) {
    return __builtin_smulll_overflow(a, b, c);
}

// count leading zero for __int128
inline int clz128(unsigned __int128 v) {
    if (v == 0) return sizeof(__int128);
    unsigned __int128 shifted = v >> 64;
    if (shifted != 0) {
        return __builtin_clzll(shifted);
    } else {
        return __builtin_clzll(v) + 64;
    }
}

inline bool int128_mul_overflow(int128_t a, int128_t b, int128_t* c) {
    return __builtin_mul_overflow(a, b, c);
}

template <>
inline bool mul_overflow(int128_t a, int128_t b, int128_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return multi3(a, b, *c);
#else
    return int128_mul_overflow(a, b, c);
#endif
}

inline int count_bits(int256_t x) noexcept {
    if (x == 0) return 0;
    if (x >> 192) return 256 - __builtin_clzll(x >> 192) + 192;
    if (x >> 128) return 128 - __builtin_clzll(x >> 128) + 128;
    if (x >> 64)  return 64 - __builtin_clzll(x >> 64) + 64;
    return 64 - __builtin_clzll(static_cast<uint64_t>(x));
}

template <>
inline bool mul_overflow(int256_t a, int256_t b, int256_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return mul_256(a, b, *c);
#else
    constexpr int256_t MIN = std::numeric_limits<int256_t>::min();
    constexpr int256_t MAX = std::numeric_limits<int256_t>::max();
    constexpr uint256_t UMAX = static_cast<uint256_t>(INT256_MAX);

    if (a == 0 || b == 0) {
        result = int256_t(0);
        return false;
    }

    if (a == INT256_MAX) return (b != int256_t(1));
    if (b == INT256_MIN) return (a != int256_t(1));

    const bool negative = (a.high < 0) ^ (b.high < 0);
    const uint256_t abs_a = (a.high < 0) ? -static_cast<uint256_t>(a) : static_cast<uint256_t>(a);
    const uint256_t abs_b = (b.high < 0) ? -static_cast<uint256_t>(b) : static_cast<uint256_t>(b);

    const bool a_small = (abs_a >> 128) == 0;
    const bool b_small = (abs_b >> 128) == 0;
    if (a_small && b_small) {
        const uint256_t product = abs_a * abs_b;
        if (product <= UMAX) {
            result = negative ? -static_cast<int256_t>(product)
                              : static_cast<int256_t>(product);
            return false;
        }
        return true;
    }

    const int bits_a = count_bits(abs_a);
    const int bits_b = count_bits(abs_b);
    if (bits_a + bits_b < 255) {
        const uint256_t product = abs_a * abs_b;
        result = negative ? -static_cast<int256_t>(product)
                          : static_cast<int256_t>(product);
        return false;
    }
    if (bits_a + bits_b > 255) {
        return true;
    }

    const uint256_t max_div_b = UMAX / abs_b;
    if (abs_a > max_div_b) {
        return true;
    }

    const uint256_t product = abs_a * abs_b;
    result = negative ? -static_cast<int256_t>(product)
                      : static_cast<int256_t>(product);
    return false;
#endif
}


} // namespace starrocks
