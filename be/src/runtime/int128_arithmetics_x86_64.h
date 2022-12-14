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

// GCC extended(inline) asm used to optimize decimal arithmetic operations
// on X84_64 architecture.
#if defined(__x86_64__) && defined(__GNUC__)

#include <common/compiler_util.h>

#include <climits>
#include <cstddef>
#include <cstdint>
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
        unt64_t high;
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

// add two 64bit and return overflow flag
template <class T, class U, std::enable_if_t<is_bit64<T>, T> = 0, std::enable_if_t<is_bit64<U>, U> = 0>
static inline int asm_add(T x, U y, T& res) {
    int8_t overflow = 0;
    // clang-format off
    // x and y are two signed int64_t
    // SF|CF == 1 indicates overflow of abs(x) + abs(y);
    // SF^CF == 1 indicates overflow of a + y;
    // in abs(x) + abs(y) operation, both x and y are cast to uint64_t,
    // both uint64_t and int64_t have the same underlying bit layout and
    // add/sub rules, so unsigned modifiers are just used to choose the
    // target execution branch in compile-time.
    if constexpr (std::is_unsigned<T>::value && std::is_unsigned<U>::value) {
        __asm__ __volatile__(
                "mov %[x], %[res]\n\t"
                "add %[y], %[res]\n\t"
                "setc %b[overflow]\n\t"
                "sets %%r8b\n\t"
                "or %%r8b, %b[overflow]"
                : [res] "+r"(res), [overflow] "+r"(overflow)
                : [x] "r"(x), [y] "r"(y)
                : "cc", "r8");
    } else {
        __asm__ __volatile__(
                "mov %[x], %[res]\n\t"
                "add %[y], %[res]"
                : [res] "+r"(res), "=@cco"(overflow)
                : [x] "r"(x), [y] "r"(y)
                : "cc");
    }
    // clang-format on
    return overflow;
}

template <class T, class U, std::enable_if_t<is_bit64<T>, T> = 0, std::enable_if_t<is_bit64<U>, U> = 0>
static int asm_mul(T x, U y, Int128Wrapper& res) {
    int8_t overflow;
    // clang-format off
    if constexpr (std::is_unsigned<T>::value && std::is_unsigned<U>::value) {
        __asm__ __volatile__(
                "mov %[x], %%rax\n\t"
                "mul %[y]\n\t"
                "mov %%rdx, %[high]\n\t"
                "mov %%rax, %[low]"
                : [high] "=r"(res.u.high), [low] "=r"(res.u.low), "=@cco"(overflow)
                : [x] "r"(x), [y] "r"(y)
                : "cc", "rdx", "rax");
    } else {
        __asm__ __volatile__(
                "mov %[x], %%rax\n\t"
                "imul %[y]\n\t"
                "mov %%rdx, %[high]\n\t"
                "mov %%rax, %[low]"
                : [high] "=r"(res.s.high), [low] "=r"(res.s.low), "=@cco"(overflow)
                : [x] "r"(x), [y] "r"(y)
                : "cc", "rdx", "rax");
    }
    // clang-format on
    return overflow;
}

static int64_t asm_mul32(int32_t x, int32_t y) {
    union {
        int64_t i64;
        struct {
#if __BYTE_ORDER == LITTLE_ENDIAN
            int32_t low;
            int32_t high;
#else
            int32_t high;
            int32_t low;
#endif
        } s;
    } z;
    // clang-format off
    __asm__ __volatile__(
            "mov %[x], %%eax\n\t"
            "imul %[y]\n\t"
            "mov %%edx, %[high]\n\t"
            "mov %%eax, %[low]"
            : [high] "=r"(z.s.high), [low] "=r"(z.s.low)
            : [x] "r"(x), [y] "r"(y)
            : "cc", "rax", "rdx");
    // clang-format on
    return z.i64;
}

static inline bool asm_add_overflow(int128_t x, int128_t y, int128_t* z) {
    auto& xw = reinterpret_cast<Int128Wrapper&>(x);
    auto& yw = reinterpret_cast<Int128Wrapper&>(y);
    auto& zw = reinterpret_cast<Int128Wrapper&>(*z);
    int8_t overflow = 0;
    // clang-format off
    __asm__ __volatile__(
            "mov %[xl], %[zl]\n\t"
            "mov %[xh], %[zh]\n\t"
            "add %[yl], %[zl]\n\t"
            "adc %[yh], %[zh]\n\t"
            : [zl] "+r"(zw.s.low), [zh] "+r"(zw.s.high), "=@cco"(overflow)
            : [xl] "r"(xw.s.low), [yl] "r"(yw.s.low), [xh] "r"(xw.s.high), [yh] "r"(yw.s.high)
            : "cc");
    // clang-format on
    return overflow;
}

static inline bool asm_sub_overflow(int128_t x, int128_t y, int128_t* z) {
    auto& xw = reinterpret_cast<Int128Wrapper&>(x);
    auto& yw = reinterpret_cast<Int128Wrapper&>(y);
    auto& zw = reinterpret_cast<Int128Wrapper&>(*z);
    int8_t overflow = 0;
    // clang-format off
    __asm__ __volatile__(
            "mov %[xl], %[zl]\n\t"
            "mov %[xh], %[zh]\n\t"
            "sub %[yl], %[zl]\n\t"
            "sbb %[yh], %[zh]\n\t"
            : [zl] "+r"(zw.s.low), [zh] "+r"(zw.s.high), "=@cco"(overflow)
            : [xl] "r"(xw.s.low), [yl] "r"(yw.s.low), [xh] "r"(xw.s.high), [yh] "r"(yw.s.high)
            : "cc");
    // clang-format on
    return overflow;
}

static inline int multi3(const Int128Wrapper& x, const Int128Wrapper& y, Int128Wrapper& res) {
    auto no_zero = (x.u.low | x.u.high) || (y.u.low | y.u.high);
    if (UNLIKELY(!no_zero)) {
        res.u128 = static_cast<int128_t>(0);
        return 0;
    }
    // x.u.high * y.u.high write into 128..256 bits of result
    int overflow = x.u.high != 0 && y.u.high != 0;

    asm_mul(x.u.low, y.u.low, res);
    Int128Wrapper t0, t1;
    overflow |= asm_mul(x.u.low, y.u.high, t0);
    overflow |= asm_mul(y.u.low, x.u.high, t1);
    overflow |= asm_add(res.u.high, t0.u.low, res.u.high);
    overflow |= asm_add(res.u.high, t1.u.low, res.u.high);
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
    // sgn(x)
    auto sx = x >> 127;
    // sgn(y)
    auto sy = y >> 127;
    // abx(x), abs(y)
    Int128Wrapper wx = {.s128 = (x ^ sx) - sx};
    Int128Wrapper wy = {.s128 = (y ^ sy) - sy};
    Int128Wrapper wres;
    // sgn(x * y)
    sx ^= sy;
    // abx(x) * abs(y)
    auto overflow = multi3(wx, wy, wres);
    // sgn(x * y) and abs(x) * abs(y) produces x * y;
    res = (wres.s128 ^ sx) - sx;
    return overflow;
}

// udiv128by64to64, udivmodti4, divmodti3 origin from llvm-project
// (https://github.com/llvm/llvm-project.git), thanks for its implementation
// of __int128 arithmetic operations.
// udiv128by64to64 and udivmodti4 come from llvm-project/compiler-rt/lib/builtins/udivmodti4.c
static inline uint64_t udiv128by64to64(uint64_t u1, uint64_t u0, uint64_t v, uint64_t* r) {
    uint64_t result;
    // clang-format off
    __asm__("divq %[v]" : "=a"(result), "=d"(*r) : [v] "r"(v), "a"(u0), "d"(u1));
    // clang-format on
    return result;
}

// Effects: if rem != 0, *rem = a % b
// Returns: a / b
static inline uint128_t udivmodti4(uint128_t a, uint128_t b, uint128_t* rem) {
    static constexpr unsigned n_utword_bits = sizeof(uint128_t) * CHAR_BIT;
    Int128Wrapper dividend;
    dividend.u128 = a;
    Int128Wrapper divisor;
    divisor.u128 = b;
    Int128Wrapper quotient;
    Int128Wrapper remainder;
    if (divisor.u128 > dividend.u128) {
        if (rem != nullptr) *rem = dividend.u128;
        return 0;
    }
    // When the divisor fits in 64 bits, we can use an optimized path.
    if (divisor.u.high == 0) {
        remainder.u.high = 0;
        if (dividend.u.high < divisor.u.low) {
            // The result fits in 64 bits.
            quotient.u.low = udiv128by64to64(dividend.u.high, dividend.u.low, divisor.u.low, &remainder.u.low);
            quotient.u.high = 0;
        } else {
            // First, divide with the high part to get the remainder in dividend.s.high.
            // After that dividend.s.high < divisor.s.low.
            quotient.u.high = dividend.u.high / divisor.u.low;
            dividend.u.high = dividend.u.high % divisor.u.low;
            quotient.u.low = udiv128by64to64(dividend.u.high, dividend.u.low, divisor.u.low, &remainder.u.low);
        }
        if (rem != nullptr) *rem = remainder.u128;
        return quotient.u128;
    }
    // 0 <= shift <= 63.
    int32_t shift = __builtin_clzll(divisor.u.high) - __builtin_clzll(dividend.u.high);
    divisor.u128 <<= shift;
    quotient.u.high = 0;
    quotient.u.low = 0;
    for (; shift >= 0; --shift) {
        quotient.u.low <<= 1;
        // Branch free version of.
        // if (dividend.u128 >= divisor.u128)
        // {
        //    dividend.u128 -= divisor.u128;
        //    carry = 1;
        // }
        const int128_t s = (int128_t)(divisor.u128 - dividend.u128 - 1) >> (n_utword_bits - 1);
        quotient.u.low |= s & 1;
        dividend.u128 -= divisor.u128 & s;
        divisor.u128 >>= 1;
    }
    if (rem != nullptr) *rem = dividend.u128;
    return quotient.u128;
}

// int128_t divide(/) and modulo(%) operations translated into
// divti3 and modti3 function calls in both gcc and clang compilers,
// then divti3 and modti3 invoke the same function udivmodti4, so we
// combine divti3 and modti3 into a new function divmodti3, there are
// two benefits:
// 1. compute out quotient and remainder in one udivmodti4 call instead
// of two calls;
// 2. eliminate dynamic linkage resolution and PLT(Procedure Linkage Table)
// trampolining.
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
#endif // defined(__x86_64__) && defined(COMPILER_GCC)
