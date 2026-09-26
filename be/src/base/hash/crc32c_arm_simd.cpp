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

#if defined(__ARM_NEON) && defined(__aarch64__)

#include <arm_neon.h>

#include <cstddef>
#include <cstdint>

#if defined(__clang__)
#define TARGET_CRYPTO __attribute__((target("arch=armv8-a+crc+crypto")))
#elif defined(__GNUC__)
#define TARGET_CRYPTO __attribute__((target("+crypto")))
#else
#define TARGET_CRYPTO
#endif

namespace starrocks::crc32c {

static inline TARGET_CRYPTO uint8x16_t clmul_00(uint8x16_t a, uint8x16_t b) {
    return vreinterpretq_u8_p128(
            vmull_p64(vgetq_lane_p64(vreinterpretq_p64_u8(a), 0), vgetq_lane_p64(vreinterpretq_p64_u8(b), 0)));
}

static inline TARGET_CRYPTO uint8x16_t clmul_11(uint8x16_t a, uint8x16_t b) {
    return vreinterpretq_u8_p128(vmull_high_p64(vreinterpretq_p64_u8(a), vreinterpretq_p64_u8(b)));
}

static inline TARGET_CRYPTO uint8x16_t clmul_01(uint8x16_t a, uint8x16_t b) {
    return vreinterpretq_u8_p128(
            vmull_p64(vgetq_lane_p64(vreinterpretq_p64_u8(a), 0), vgetq_lane_p64(vreinterpretq_p64_u8(b), 1)));
}

TARGET_CRYPTO uint32_t crc32c_pmull_simd(uint32_t crc, const char* buf, size_t len) {
    static const uint64_t __attribute__((aligned((16)))) k1k2[] = {0x0740eef02ULL, 0x09e4addf8ULL};
    static const uint64_t __attribute__((aligned((16)))) k3k4[] = {0x0f20c0dfeULL, 0x14cd00bd6ULL};
    static const uint64_t __attribute__((aligned((16)))) k5k0[] = {0x0dd45aab8ULL, 0x105ec76f0ULL};
    static const uint64_t __attribute__((aligned((16)))) poly[] = {0x105ec76f1ULL, 0x0dea713f1ULL};

    uint8x16_t x0, x1, x2, x3, x4, x5, x6, x7, x8, y5, y6, y7, y8;

    x1 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x00));
    x2 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x10));
    x3 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x20));
    x4 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x30));

    uint32x4_t crc_vec = vsetq_lane_u32(crc, vdupq_n_u32(0), 0);
    x1 = veorq_u8(x1, vreinterpretq_u8_u32(crc_vec));

    x0 = vreinterpretq_u8_u64(vld1q_u64(k1k2));

    buf += 64;
    len -= 64;

    while (len >= 64) {
        x5 = clmul_00(x1, x0);
        x6 = clmul_00(x2, x0);
        x7 = clmul_00(x3, x0);
        x8 = clmul_00(x4, x0);

        x1 = clmul_11(x1, x0);
        x2 = clmul_11(x2, x0);
        x3 = clmul_11(x3, x0);
        x4 = clmul_11(x4, x0);

        y5 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x00));
        y6 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x10));
        y7 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x20));
        y8 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf + 0x30));

        x1 = veorq_u8(x1, x5);
        x2 = veorq_u8(x2, x6);
        x3 = veorq_u8(x3, x7);
        x4 = veorq_u8(x4, x8);

        x1 = veorq_u8(x1, y5);
        x2 = veorq_u8(x2, y6);
        x3 = veorq_u8(x3, y7);
        x4 = veorq_u8(x4, y8);

        buf += 64;
        len -= 64;
    }

    x0 = vreinterpretq_u8_u64(vld1q_u64(k3k4));

    x5 = clmul_00(x1, x0);
    x1 = clmul_11(x1, x0);
    x1 = veorq_u8(x1, x2);
    x1 = veorq_u8(x1, x5);

    x5 = clmul_00(x1, x0);
    x1 = clmul_11(x1, x0);
    x1 = veorq_u8(x1, x3);
    x1 = veorq_u8(x1, x5);

    x5 = clmul_00(x1, x0);
    x1 = clmul_11(x1, x0);
    x1 = veorq_u8(x1, x4);
    x1 = veorq_u8(x1, x5);

    while (len >= 16) {
        x2 = vld1q_u8(reinterpret_cast<const uint8_t*>(buf));
        x5 = clmul_00(x1, x0);
        x1 = clmul_11(x1, x0);
        x1 = veorq_u8(x1, x2);
        x1 = veorq_u8(x1, x5);
        buf += 16;
        len -= 16;
    }

    x2 = clmul_01(x1, x0);
    const uint32_t mask_data[4] = {0xffffffff, 0, 0xffffffff, 0};
    x3 = vreinterpretq_u8_u32(vld1q_u32(mask_data));
    x1 = vextq_u8(x1, vdupq_n_u8(0), 8);
    x1 = veorq_u8(x1, x2);

    x0 = vreinterpretq_u8_u64(vcombine_u64(vld1_u64(k5k0), vdup_n_u64(0)));
    x2 = vextq_u8(x1, vdupq_n_u8(0), 4);
    x1 = vandq_u8(x1, x3);
    x1 = clmul_00(x1, x0);
    x1 = veorq_u8(x1, x2);

    x0 = vreinterpretq_u8_u64(vld1q_u64(poly));
    x2 = vandq_u8(x1, x3);
    x2 = clmul_01(x2, x0);
    x2 = vandq_u8(x2, x3);
    x2 = clmul_00(x2, x0);
    x1 = veorq_u8(x1, x2);

    return vgetq_lane_u32(vreinterpretq_u32_u8(x1), 1);
}

} // namespace starrocks::crc32c

#endif // defined(__ARM_NEON) && defined(__aarch64__)
