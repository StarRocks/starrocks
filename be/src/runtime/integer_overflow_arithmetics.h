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
inline bool add_overflow(int256_t a, int256_t b, int256_t* c) {
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
inline bool sub_overflow(int256_t a, int256_t b, int256_t* c) {
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

inline int count_bits(const int256_t& x) noexcept {
    if (x == 0) return 0;

    for (int shift = 192; shift >= 0; shift -= 64) {
        uint64_t segment = static_cast<uint64_t>(x >> shift);
        if (segment != 0) {
            return shift + 64 - __builtin_clzll(segment);
        }
    }
    return 0;
}

template <>
inline bool mul_overflow(const int256_t a, const int256_t b, int256_t* c) {
    if (a == 0 || b == 0) {
        *c = int256_t(0);
        return false;
    }

    if (a == int256_t(1)) {
        *c = b;
        return false;
    }
    if (b == int256_t(1)) {
        *c = a;
        return false;
    }

    if (a == int256_t(-1)) {
        if (b == INT256_MIN) return true;
        *c = -b;
        return false;
    }
    if (b == int256_t(-1)) {
        if (a == INT256_MIN) return true;
        *c = -a;
        return false;
    }

    if (a == INT256_MIN || b == INT256_MIN) {
        return true;
    }

    if (a == INT256_MAX || b == INT256_MAX) {
        return true;
    }

    const bool negative = (a < 0) ^ (b < 0);
    const int256_t abs_a = (a.high < 0) ? -a : a;
    const int256_t abs_b = (b.high < 0) ? -b : b;

    const int abs_a_bits = count_bits(abs_a);
    const int abs_b_bits = count_bits(abs_b);

    if (abs_a_bits + abs_b_bits <= 255) {
        *c = a * b;
        return false;
    }

    if (abs_a_bits + abs_b_bits > 256) {
        if (negative) {
            if (abs_a_bits + abs_b_bits == 257) {
                const int256_t product_abs = abs_a * abs_b;
                if (product_abs == INT256_MIN) {
                    *c = INT256_MIN;
                    return false;
                }
            }
            return true;
        } else {
            return true;
        }
    }

    const int256_t& larger = (abs_a >= abs_b) ? abs_a : abs_b;
    const int256_t& smaller = (abs_a >= abs_b) ? abs_b : abs_a;

    const int256_t quotient = INT256_MAX / smaller;

    if (larger > quotient) {
        return true;
    }

    const int256_t result = abs_a * abs_b;
    *c = negative ? -result : result;
    return false;
}

} // namespace starrocks
