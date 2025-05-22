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
// on X84_64 architecture for int256_t.
#if defined(__x86_64__) && defined(__GNUC__)

#include <common/compiler_util.h>

#include <climits>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <glog/logging.h>

#include "types/int256.h"

namespace starrocks {

union Int256Wrapper {
    int256_t s256;
    struct {
#if __BYTE_ORDER == LITTLE_ENDIAN
        int64_t low;
        int64_t mid_low;
        int64_t mid_high;
        int64_t high;
#else
        int64_t high;
        int64_t mid_high;
        int64_t mid_low;
        int64_t low;
#endif
    } s;
    struct {
#if __BYTE_ORDER == LITTLE_ENDIAN
        uint64_t low;
        uint64_t mid_low;
        uint64_t mid_high;
        uint64_t high;
#else
        uint64_t high;
        uint64_t mid_high;
        uint64_t mid_low;
        uint64_t low;
#endif
    } u;
};

template <typename T>
inline constexpr bool is_bit256 = std::is_same_v<T, int256_t>;

// add two 256bit and return overflow flag
template <class T, class U>
static inline std::enable_if_t<is_bit256<T> && is_bit256<U>, bool>
asm_add(T x, U y, T& res) {
    bool overflow = false;
    // clang-format off
    // x and y are two signed int256_t
    // SF|CF == 1 indicates overflow of abs(x) + abs(y);
    // SF^CF == 1 indicates overflow of x + y;
    __asm__ __volatile__(
            "movq %[xl], %[resl]\n\t"
            "movq %[xml], %[resml]\n\t"
            "movq %[xmh], %[resmh]\n\t"
            "movq %[xh], %[resh]\n\t"
            "addq %[yl], %[resl]\n\t"
            "adcq %[yml], %[resml]\n\t"
            "adcq %[ymh], %[resmh]\n\t"
            "adcq %[yh], %[resh]\n\t"
            : [resl] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.low),
              [resml] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.mid_low),
              [resmh] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.mid_high),
              [resh] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.high),
              "=@cco"(overflow)
            : [xl] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.low),
              [xml] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.mid_low),
              [xmh] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.mid_high),
              [xh] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.high),
              [yl] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.low),
              [yml] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.mid_low),
              [ymh] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.mid_high),
              [yh] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.high)
            : "cc");
    // clang-format on
    LOG(ERROR) << "overflow:overflow:overflow:overflow:overflow:overflow:overflow:overflow:overflow:|||" << overflow;
    return overflow;
}

// sub two 256bit and return overflow flag
template <class T, class U>
static inline std::enable_if_t<is_bit256<T> && is_bit256<U>, bool>
asm_sub(T x, U y, T& res) {
    bool overflow = false;
    // clang-format off
    __asm__ __volatile__(
            "movq %[xl], %[resl]\n\t"
            "movq %[xml], %[resml]\n\t"
            "movq %[xmh], %[resmh]\n\t"
            "movq %[xh], %[resh]\n\t"
            "subq %[yl], %[resl]\n\t"
            "sbbq %[yml], %[resml]\n\t"
            "sbbq %[ymh], %[resmh]\n\t"
            "sbbq %[yh], %[resh]\n\t"
            : [resl] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.low),
              [resml] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.mid_low),
              [resmh] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.mid_high),
              [resh] "+r"(reinterpret_cast<Int256Wrapper&>(res).s.high),
              "=@cco"(overflow)
            : [xl] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.low),
              [xml] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.mid_low),
              [xmh] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.mid_high),
              [xh] "r"(reinterpret_cast<const Int256Wrapper&>(x).s.high),
              [yl] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.low),
              [yml] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.mid_low),
              [ymh] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.mid_high),
              [yh] "r"(reinterpret_cast<const Int256Wrapper&>(y).s.high)
            : "cc");
    LOG(ERROR) << "overflow:overflow:overflow:overflow:overflow:overflow:overflow:overflow:overflow:|||" << overflow;
    // clang-format on
    return overflow;
}

static inline void asm_mul_64_to_128(uint64_t x, uint64_t y, uint64_t& low, uint64_t& high) {
    // clang-format off
    __asm__ __volatile__(
            "movq %[x], %%rax\n\t"
            "mulq %[y]\n\t"
            "movq %%rax, %[low]\n\t"
            "movq %%rdx, %[high]"
            : [low] "=r"(low), [high] "=r"(high)
            : [x] "r"(x), [y] "r"(y)
            : "cc", "rax", "rdx");
    // clang-format on
}

static inline bool asm_add_256(Int256Wrapper& a, const Int256Wrapper& b) {
    bool carry;
    __asm__ __volatile__(
            "addq %[b_low], %[a_low]\n\t"
            "adcq %[b_mid_low], %[a_mid_low]\n\t"
            "adcq %[b_mid_high], %[a_mid_high]\n\t"
            "adcq %[b_high], %[a_high]\n\t"
            "setc %b[carry]"
            : [a_low] "+r"(a.u.low),
              [a_mid_low] "+r"(a.u.mid_low),
              [a_mid_high] "+r"(a.u.mid_high),
              [a_high] "+r"(a.u.high),
              [carry] "=r"(carry)
            : [b_low] "r"(b.u.low),
              [b_mid_low] "r"(b.u.mid_low),
              [b_mid_high] "r"(b.u.mid_high),
              [b_high] "r"(b.u.high)
            : "cc");
    return carry;
}

static inline void asm_negate_256(Int256Wrapper& x) {
    __asm__ __volatile__(
            "notq %[low]\n\t"
            "notq %[mid_low]\n\t"
            "notq %[mid_high]\n\t"
            "notq %[high]\n\t"
            "addq $1, %[low]\n\t"
            "adcq $0, %[mid_low]\n\t"
            "adcq $0, %[mid_high]\n\t"
            "adcq $0, %[high]"
            : [low] "+r"(x.u.low),
              [mid_low] "+r"(x.u.mid_low),
              [mid_high] "+r"(x.u.mid_high),
              [high] "+r"(x.u.high)
            :
            : "cc");
}

static inline bool asm_mul_unsigned_256(const Int256Wrapper& x, const Int256Wrapper& y, Int256Wrapper& res) {
    uint64_t x_parts[4] = {x.u.low, x.u.mid_low, x.u.mid_high, x.u.high};
    uint64_t y_parts[4] = {y.u.low, y.u.mid_low, y.u.mid_high, y.u.high};

    res.u.low = res.u.mid_low = res.u.mid_high = res.u.high = 0;

    bool overflow = false;

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 4; j++) {
            if (x_parts[i] == 0 || y_parts[j] == 0) continue;

            uint64_t prod_low, prod_high;
            asm_mul_64_to_128(x_parts[i], y_parts[j], prod_low, prod_high);

            int pos = i + j;

            if (pos >= 4) {
                overflow = true;
                continue;
            }

            bool carry = false;
            __asm__ __volatile__(
                    "addq %[val], %[target]\n\t"
                    "setc %b[carry]"
                    : [target] "+m"((&res.u.low)[pos]), [carry] "=r"(carry)
                    : [val] "r"(prod_low)
                    : "cc");

            if (pos + 1 < 4) {
                uint64_t to_add = prod_high + (carry ? 1 : 0);
                carry = false;

                __asm__ __volatile__(
                        "addq %[val], %[target]\n\t"
                        "setc %b[carry]"
                        : [target] "+m"((&res.u.low)[pos + 1]), [carry] "=r"(carry)
                        : [val] "r"(to_add)
                        : "cc");

                int carry_pos = pos + 2;
                while (carry && carry_pos < 4) {
                    __asm__ __volatile__(
                            "addq $1, %[target]\n\t"
                            "setc %b[carry]"
                            : [target] "+m"((&res.u.low)[carry_pos]), [carry] "+r"(carry)
                            :
                            : "cc");
                    carry_pos++;
                }

                if (carry && carry_pos >= 4) {
                    overflow = true;
                }
            } else if (prod_high != 0 || carry) {
                overflow = true;
            }
        }
    }

    return overflow;
}

template <class T, class U>
static inline std::enable_if_t<is_bit256<T> && is_bit256<U>, bool>
asm_mul_signed_256(T x, U y, T& res) {
    auto& x_wrapper = reinterpret_cast<const Int256Wrapper&>(x);
    auto& y_wrapper = reinterpret_cast<const Int256Wrapper&>(y);
    auto& res_wrapper = reinterpret_cast<Int256Wrapper&>(res);

    auto x_is_zero = (x_wrapper.u.low | x_wrapper.u.mid_low | x_wrapper.u.mid_high | x_wrapper.u.high) == 0;
    auto y_is_zero = (y_wrapper.u.low | y_wrapper.u.mid_low | y_wrapper.u.mid_high | y_wrapper.u.high) == 0;

    if (UNLIKELY(x_is_zero || y_is_zero)) {
        res = static_cast<T>(0);
        return false;
    }

    bool x_negative = (x_wrapper.s.high < 0);
    bool y_negative = (y_wrapper.s.high < 0);
    bool result_negative = x_negative ^ y_negative;

    Int256Wrapper abs_x = x_wrapper;
    Int256Wrapper abs_y = y_wrapper;

    if (x_negative) {
        asm_negate_256(abs_x);
    }

    if (y_negative) {
        asm_negate_256(abs_y);
    }

    bool overflow = asm_mul_unsigned_256(abs_x, abs_y, res_wrapper);

    if (result_negative) {
        asm_negate_256(res_wrapper);
    }

    bool sign_overflow = false;
    if (!overflow) {
        bool actual_negative = (res_wrapper.s.high < 0);
        if (actual_negative != result_negative) {
            sign_overflow = true;
        }
    }

    return overflow || sign_overflow;
}

static inline bool mul_256(const int256_t& x, const int256_t& y, int256_t& res) {
    return asm_mul_signed_256(x, y, res);
}

// Optimized comparison functions
template <class T, class U>
static inline std::enable_if_t<is_bit256<T> && is_bit256<U>, bool>
asm_eq(T x, U y) {
    auto& x_wrapper = reinterpret_cast<const Int256Wrapper&>(x);
    auto& y_wrapper = reinterpret_cast<const Int256Wrapper&>(y);

    bool result;
    // clang-format off
    __asm__ __volatile__(
            "cmpq %[yh], %[xh]\n\t"
            "jne 1f\n\t"
            "cmpq %[ymh], %[xmh]\n\t"
            "jne 1f\n\t"
            "cmpq %[yml], %[xml]\n\t"
            "jne 1f\n\t"
            "cmpq %[yl], %[xl]\n\t"
            "1:\n\t"
            "sete %b[result]\n\t"
            : [result] "=r"(result)
            : [xl] "r"(x_wrapper.s.low), [xml] "r"(x_wrapper.s.mid_low),
              [xmh] "r"(x_wrapper.s.mid_high), [xh] "r"(x_wrapper.s.high),
              [yl] "r"(y_wrapper.s.low), [yml] "r"(y_wrapper.s.mid_low),
              [ymh] "r"(y_wrapper.s.mid_high), [yh] "r"(y_wrapper.s.high)
            : "cc");
    // clang-format on

    return result;
}

template <class T, class U>
static inline std::enable_if_t<is_bit256<T> && is_bit256<U>, bool>
asm_lt(T x, U y) {
    auto& x_wrapper = reinterpret_cast<const Int256Wrapper&>(x);
    auto& y_wrapper = reinterpret_cast<const Int256Wrapper&>(y);

    bool result;
    // clang-format off
    __asm__ __volatile__(
            "cmpq %[yh], %[xh]\n\t"
            "jl 2f\n\t"
            "jg 3f\n\t"
            "cmpq %[ymh], %[xmh]\n\t"
            "jb 2f\n\t"
            "ja 3f\n\t"
            "cmpq %[yml], %[xml]\n\t"
            "jb 2f\n\t"
            "ja 3f\n\t"
            "cmpq %[yl], %[xl]\n\t"
            "jmp 4f\n\t"
            "2:\n\t"
            "movb $1, %b[result]\n\t"
            "jmp 5f\n\t"
            "3:\n\t"
            "movb $0, %b[result]\n\t"
            "jmp 5f\n\t"
            "4:\n\t"
            "setb %b[result]\n\t"
            "5:\n\t"
            : [result] "=r"(result)
            : [xl] "r"(x_wrapper.s.low), [xml] "r"(x_wrapper.s.mid_low),
              [xmh] "r"(x_wrapper.s.mid_high), [xh] "r"(x_wrapper.s.high),
              [yl] "r"(y_wrapper.s.low), [yml] "r"(y_wrapper.s.mid_low),
              [ymh] "r"(y_wrapper.s.mid_high), [yh] "r"(y_wrapper.s.high)
            : "cc");
    // clang-format on

    return result;
}

} // namespace starrocks

#endif // defined(__x86_64__) && defined(__GNUC__)
