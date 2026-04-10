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

// the following code are modified under BSD license here:
// https://github.com/frida/v8-zlib/blob/main/crc32_simd.c
//
// Copyright 2017 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the Chromium source repository LICENSE file.

#if defined(__SSE4_2__) && defined(__PCLMUL__)
#include <x86intrin.h>
#endif

#include <cstdint>

namespace starrocks::crc32c {

#if defined(__SSE4_2__) && defined(__PCLMUL__)
/*
 * crc32c_sse42_simd(): compute the crc32 of the buffer, where the buffer
 * length must be at least 64, and a multiple of 16.
 * SSE4.2+PCLMUL
 */
uint32_t crc32c_sse42_simd(uint32_t crc, const char* buf, size_t len) {
    /*
     * Definitions of the bit-reflected domain constants k1,k2,k3, etc and
     * the CRC32+Barrett polynomials given at the end of the paper.
     */
    static const uint64_t __attribute__((aligned((16)))) k1k2[] = {0x0740eef02, 0x09e4addf8};
    static const uint64_t __attribute__((aligned((16)))) k3k4[] = {0x0f20c0dfe, 0x14cd00bd6};
    static const uint64_t __attribute__((aligned((16)))) k5k0[] = {0x0dd45aab8, 0x105ec76f0};
    static const uint64_t __attribute__((aligned((16)))) poly[] = {0x105ec76f1, 0x0dea713f1};

    __m128i x0, x1, x2, x3, x4, x5, x6, x7, x8, y5, y6, y7, y8;
    /*
     * There's at least one block of 64.
     */
    x1 = _mm_loadu_si128((__m128i*)(buf + 0x00));
    x2 = _mm_loadu_si128((__m128i*)(buf + 0x10));
    x3 = _mm_loadu_si128((__m128i*)(buf + 0x20));
    x4 = _mm_loadu_si128((__m128i*)(buf + 0x30));
    x1 = _mm_xor_si128(x1, _mm_cvtsi32_si128(crc));
    x0 = _mm_load_si128((__m128i*)k1k2);
    buf += 64;
    len -= 64;
    /*
     * Parallel fold blocks of 64, if any.
     */
    while (len >= 64) {
        x5 = _mm_clmulepi64_si128(x1, x0, 0x00);
        x6 = _mm_clmulepi64_si128(x2, x0, 0x00);
        x7 = _mm_clmulepi64_si128(x3, x0, 0x00);
        x8 = _mm_clmulepi64_si128(x4, x0, 0x00);
        x1 = _mm_clmulepi64_si128(x1, x0, 0x11);
        x2 = _mm_clmulepi64_si128(x2, x0, 0x11);
        x3 = _mm_clmulepi64_si128(x3, x0, 0x11);
        x4 = _mm_clmulepi64_si128(x4, x0, 0x11);
        y5 = _mm_loadu_si128((__m128i*)(buf + 0x00));
        y6 = _mm_loadu_si128((__m128i*)(buf + 0x10));
        y7 = _mm_loadu_si128((__m128i*)(buf + 0x20));
        y8 = _mm_loadu_si128((__m128i*)(buf + 0x30));
        x1 = _mm_xor_si128(x1, x5);
        x2 = _mm_xor_si128(x2, x6);
        x3 = _mm_xor_si128(x3, x7);
        x4 = _mm_xor_si128(x4, x8);
        x1 = _mm_xor_si128(x1, y5);
        x2 = _mm_xor_si128(x2, y6);
        x3 = _mm_xor_si128(x3, y7);
        x4 = _mm_xor_si128(x4, y8);
        buf += 64;
        len -= 64;
    }
    /*
     * Fold into 128-bits.
     */
    x0 = _mm_load_si128((__m128i*)k3k4);
    x5 = _mm_clmulepi64_si128(x1, x0, 0x00);
    x1 = _mm_clmulepi64_si128(x1, x0, 0x11);
    x1 = _mm_xor_si128(x1, x2);
    x1 = _mm_xor_si128(x1, x5);
    x5 = _mm_clmulepi64_si128(x1, x0, 0x00);
    x1 = _mm_clmulepi64_si128(x1, x0, 0x11);
    x1 = _mm_xor_si128(x1, x3);
    x1 = _mm_xor_si128(x1, x5);
    x5 = _mm_clmulepi64_si128(x1, x0, 0x00);
    x1 = _mm_clmulepi64_si128(x1, x0, 0x11);
    x1 = _mm_xor_si128(x1, x4);
    x1 = _mm_xor_si128(x1, x5);
    /*
     * Single fold blocks of 16, if any.
     */
    while (len >= 16) {
        x2 = _mm_loadu_si128((__m128i*)buf);
        x5 = _mm_clmulepi64_si128(x1, x0, 0x00);
        x1 = _mm_clmulepi64_si128(x1, x0, 0x11);
        x1 = _mm_xor_si128(x1, x2);
        x1 = _mm_xor_si128(x1, x5);
        buf += 16;
        len -= 16;
    }
    /*
     * Fold 128-bits to 64-bits.
     */
    x2 = _mm_clmulepi64_si128(x1, x0, 0x10);
    x3 = _mm_setr_epi32(~0, 0, ~0, 0);
    x1 = _mm_srli_si128(x1, 8);
    x1 = _mm_xor_si128(x1, x2);
    x0 = _mm_loadl_epi64((__m128i*)k5k0);
    x2 = _mm_srli_si128(x1, 4);
    x1 = _mm_and_si128(x1, x3);
    x1 = _mm_clmulepi64_si128(x1, x0, 0x00);
    x1 = _mm_xor_si128(x1, x2);
    /*
     * Barret reduce to 32-bits.
     */
    x0 = _mm_load_si128((__m128i*)poly);
    x2 = _mm_and_si128(x1, x3);
    x2 = _mm_clmulepi64_si128(x2, x0, 0x10);
    x2 = _mm_and_si128(x2, x3);
    x2 = _mm_clmulepi64_si128(x2, x0, 0x00);
    x1 = _mm_xor_si128(x1, x2);
    /*
     * Return the crc32.
     */
    return _mm_extract_epi32(x1, 1);
}
#endif

} // namespace starrocks::crc32c
