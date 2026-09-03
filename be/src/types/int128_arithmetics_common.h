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

// Generic, portable C++ implementation of __int128 arithmetic operations.
// Used on platforms without architecture-specific asm optimizations
// (e.g. RISC-V, generic ARM). This header provides exactly the same symbols as
// int128_arithmetics_x86_64.h (Int128Wrapper, asm_add, asm_mul, asm_mul32,
// asm_add_overflow, asm_sub_overflow, multi3, i64_x_i64_produce_i128,
// i32_x_i32_produce_i64, udiv128by64to64, udivmodti4, divmodti3) so that the
// rest of the codebase can include int128_arithmetics_x86_64.h unconditionally.

#include <common/compiler_util.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

namespace starrocks {

union Int128Wrapper {
    int128_t s128;
    uint128_t u128;
    struct {
#if __BYTE_ORDER == LITTLE_ENDIAN
        int64_t low;
        int64_t high;
#else
        int64_t high;
        int64_t low;
#endif
    } s;
    struct {
#if __BYTE_ORDER == LITTLE_ENDIAN
        uint64_t low;
        uint64_t high;
#else
        uint64_t high;
        uint64_t low;
#endif
    } u;
};

template <typename T>
inline constexpr bool is_bit64 = false;
template <>
inline constexpr bool is_bit64<int64_t> = true;
template <>
inline constexpr bool is_bit64<uint64_t> = true;

// Generic C++ addition with overflow detection
template <class T, class U, std::enable_if_t<is_bit64<T>, T> = 0, std::enable_if_t<is_bit64<U>, U> = 0>
static inline int asm_add(T x, U y, T& res) {
    if constexpr (std::is_unsigned<T>::value && std::is_unsigned<U>::value) {
        res = x + y;
        // For unsigned: overflow if result < either operand
        return (res < x) ? 1 : 0;
    } else {
        // For signed: use two's complement overflow detection
        res = x + y;
        // Overflow if signs are same but result sign differs
        int overflow = ((x >= 0) == (y >= 0)) && ((x >= 0) != (res >= 0));
        return overflow ? 1 : 0;
    }
}

// Generic C++ multiplication using high/low decomposition
template <class T, class U, std::enable_if_t<is_bit64<T>, T> = 0, std::enable_if_t<is_bit64<U>, U> = 0>
static int asm_mul(T x, U y, Int128Wrapper& res) {
    if constexpr (std::is_unsigned<T>::value && std::is_unsigned<U>::value) {
        // Split into 32-bit halves to avoid overflow
        uint64_t x_low = x & 0xFFFFFFFFULL;
        uint64_t x_high = x >> 32;
        uint64_t y_low = y & 0xFFFFFFFFULL;
        uint64_t y_high = y >> 32;

        // Compute partial products
        uint64_t p0 = x_low * y_low;
        uint64_t p1 = x_low * y_high;
        uint64_t p2 = x_high * y_low;
        uint64_t p3 = x_high * y_high;

        // Combine with carries
        uint64_t mid1 = (p0 >> 32) + (p1 & 0xFFFFFFFFULL);
        uint64_t mid2 = (p0 >> 32) + (p2 & 0xFFFFFFFFULL);

        res.u.low = (p0 & 0xFFFFFFFFULL) | ((mid1 & 0xFFFFFFFFULL) << 32);
        res.u.high = p3 + (p1 >> 32) + (p2 >> 32) + (mid1 >> 32) + (mid2 >> 32);

        // Check for overflow (result > 2^64-1)
        return (res.u.high != 0) ? 1 : 0;
    } else {
        // Signed multiplication
        bool negative = (x < 0) != (y < 0);
        uint64_t abs_x = (x < 0) ? -x : x;
        uint64_t abs_y = (y < 0) ? -y : y;

        Int128Wrapper temp;
        int overflow = asm_mul(abs_x, abs_y, temp);

        if (negative && temp.u128 != 0) {
            // Negate: two's complement
            temp.u128 = ~temp.u128 + 1;
        }

        res.u128 = temp.u128;
        return overflow;
    }
}

// Generic 32x32->64 multiply
static inline int64_t asm_mul32(int32_t x, int32_t y) {
    return static_cast<int64_t>(x) * static_cast<int64_t>(y);
}

// Generic int128 addition with overflow check
static inline bool asm_add_overflow(int128_t x, int128_t y, int128_t* z) {
    *z = x + y;
    // Overflow if adding two positives gives negative or two negatives gives positive
    bool overflow = ((x >= 0) == (y >= 0)) && ((*z >= 0) != (x >= 0));
    return overflow;
}

// Generic int128 subtraction with overflow check
static inline bool asm_sub_overflow(int128_t x, int128_t y, int128_t* z) {
    *z = x - y;
    // Overflow if signs differ and result sign differs from minuend
    bool overflow = ((x >= 0) != (y >= 0)) && ((*z >= 0) != (x >= 0));
    return overflow;
}

// Generic 128x128->256 multiplication (returns overflow flag)
static inline int multi3(const Int128Wrapper& x, const Int128Wrapper& y, Int128Wrapper& res) {
    auto no_zero = (x.u.low | x.u.high) || (y.u.low | y.u.high);
    if (UNLIKELY(!no_zero)) {
        res.u128 = static_cast<uint128_t>(0);
        return 0;
    }

    // Check if high*high would overflow 128 bits
    int overflow = (x.u.high != 0 && y.u.high != 0) ? 1 : 0;

    // Low * Low -> full 128-bit result
    asm_mul(x.u.low, y.u.low, res);

    Int128Wrapper t0, t1;
    // Low * High
    overflow |= asm_mul(x.u.low, y.u.high, t0);
    // High * Low
    overflow |= asm_mul(y.u.low, x.u.high, t1);

    // Add cross terms to high part
    int carry;
    carry = asm_add(res.u.high, t0.u.low, res.u.high);
    overflow |= carry;
    carry = asm_add(res.u.high, t1.u.low, res.u.high);
    overflow |= carry;

    return overflow;
}

static inline int128_t i64_x_i64_produce_i128(int64_t a, int64_t b) {
    Int128Wrapper t;
    asm_mul(a, b, t);
    return t.s128;
}

static inline int64_t i32_x_i32_produce_i64(int32_t a, int32_t b) {
    return asm_mul32(a, b);
}

static inline int multi3(const int128_t& x, const int128_t& y, int128_t& res) {
    // Special case: INT128_MIN * 1 should not be treated as overflow
    if (UNLIKELY((x == std::numeric_limits<int128_t>::min() && y == 1) ||
                 (y == std::numeric_limits<int128_t>::min() && x == 1))) {
        res = std::numeric_limits<int128_t>::min();
        return 0;
    }

    // Sign extraction
    auto sx = x >> 127;
    auto sy = y >> 127;
    // Absolute values
    Int128Wrapper wx = {.s128 = (x ^ sx) - sx};
    Int128Wrapper wy = {.s128 = (y ^ sy) - sy};
    Int128Wrapper wres;
    // Result sign
    sx ^= sy;
    // Multiply absolute values
    auto overflow = multi3(wx, wy, wres);
    // Apply sign
    res = (wres.s128 ^ sx) - sx;
    return overflow;
}

// Generic 128/64->64 division (uses compiler builtins)
static inline uint64_t udiv128by64to64(uint64_t u1, uint64_t u0, uint64_t v, uint64_t* r) {
    // Use compiler builtin for 128/64 division
    unsigned __int128 dividend = (static_cast<unsigned __int128>(u1) << 64) | u0;
    uint64_t quotient = static_cast<uint64_t>(dividend / v);
    *r = static_cast<uint64_t>(dividend % v);
    return quotient;
}

// Generic 128/128->128 division
static inline uint128_t udivmodti4(uint128_t a, uint128_t b, uint128_t* rem) {
    static constexpr unsigned n_utword_bits = sizeof(uint128_t) * CHAR_BIT;

    if (b > a) {
        if (rem != nullptr) *rem = a;
        return 0;
    }

    // Optimized path when divisor fits in 64 bits
    if ((b >> 64) == 0) {
        uint64_t divisor_lo = static_cast<uint64_t>(b);
        uint64_t remainder_hi;

        if ((a >> 64) < divisor_lo) {
            // Result fits in 64 bits
            uint64_t quotient_lo = udiv128by64to64(static_cast<uint64_t>(a >> 64), static_cast<uint64_t>(a), divisor_lo,
                                                   &remainder_hi);
            if (rem != nullptr) *rem = remainder_hi;
            return quotient_lo;
        } else {
            // First get high part remainder
            uint64_t quotient_hi = static_cast<uint64_t>(a >> 64) / divisor_lo;
            uint64_t remainder_hi_partial = static_cast<uint64_t>(a >> 64) % divisor_lo;
            uint64_t quotient_lo =
                    udiv128by64to64(remainder_hi_partial, static_cast<uint64_t>(a), divisor_lo, &remainder_hi);
            if (rem != nullptr) *rem = remainder_hi;
            return (static_cast<uint128_t>(quotient_hi) << 64) | quotient_lo;
        }
    }

    // Full 128-bit division using binary long division
    int shift = __builtin_clzll(b >> 64) - __builtin_clzll(a >> 64);
    uint128_t divisor_shifted = b << shift;
    uint128_t quotient = 0;

    for (; shift >= 0; --shift) {
        quotient <<= 1;
        uint128_t diff = divisor_shifted - a - 1;
        uint128_t mask = (diff >> (n_utword_bits - 1)) & 1;
        quotient |= mask;
        a -= divisor_shifted & ~mask;
        divisor_shifted >>= 1;
    }

    if (rem != nullptr) *rem = a;
    return quotient;
}

// Combined divide and modulo
static inline void divmodti3(int128_t x, int128_t y, int128_t& q, uint128_t& r) {
    int128_t s_x = x >> 127;
    int128_t s_y = y >> 127;
    x = (x ^ s_x) - s_x;
    y = (y ^ s_y) - s_y;
    q = udivmodti4(x, y, &r);
    s_y ^= s_x;
    q = (q ^ s_y) - s_y;
    r = (r ^ s_x) - s_x;
}

} // namespace starrocks
