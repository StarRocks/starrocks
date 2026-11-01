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

#include <cstddef>
#include <cstdint>

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace SIMD {

#if defined(__AVX2__)
constexpr int kStringLenSimdWidth = 8;
#elif defined(__ARM_NEON) && defined(__aarch64__)
constexpr int kStringLenSimdWidth = 8;
#else
constexpr int kStringLenSimdWidth = 1;
#endif

constexpr uint32_t kStringLenSimdMask = (1u << kStringLenSimdWidth) - 1;

#if defined(__ARM_NEON) && defined(__aarch64__)
namespace detail {
alignas(16) inline constexpr uint16_t kStringLenBitWeights[8] = {1, 2, 4, 8, 16, 32, 64, 128};

inline uint32_t reduce_8lane_mask(uint32x4_t m0, uint32x4_t m1) {
    uint16x8_t weights = vld1q_u16(kStringLenBitWeights);
    uint16x8_t narrowed = vuzp1q_u16(vreinterpretq_u16_u32(m0), vreinterpretq_u16_u32(m1));
    uint16x8_t masked = vandq_u16(narrowed, weights);
    return vaddvq_u16(masked);
}
} // namespace detail
#endif

inline uint32_t length_eq_mask(const uint32_t* offsets, size_t base, uint32_t target_len) {
#if defined(__AVX2__)
    __m256i off_curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + base));
    __m256i off_next = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + base + 1));
    __m256i lengths = _mm256_sub_epi32(off_next, off_curr);
    __m256i target_vec = _mm256_set1_epi32(target_len);
    __m256i len_match = _mm256_cmpeq_epi32(lengths, target_vec);
    return static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(len_match)));
#elif defined(__ARM_NEON) && defined(__aarch64__)
    uint32x4_t off_curr0 = vld1q_u32(offsets + base);
    uint32x4_t off_next0 = vld1q_u32(offsets + base + 1);
    uint32x4_t lengths0 = vsubq_u32(off_next0, off_curr0);

    uint32x4_t off_curr1 = vld1q_u32(offsets + base + 4);
    uint32x4_t off_next1 = vld1q_u32(offsets + base + 5);
    uint32x4_t lengths1 = vsubq_u32(off_next1, off_curr1);

    uint32x4_t target_vec = vdupq_n_u32(target_len);
    uint32x4_t m0 = vceqq_u32(lengths0, target_vec);
    uint32x4_t m1 = vceqq_u32(lengths1, target_vec);

    return detail::reduce_8lane_mask(m0, m1);
#else
    uint32_t len = offsets[base + 1] - offsets[base];
    return (len == target_len) ? 1u : 0u;
#endif
}

inline uint32_t length_in_range_mask(const uint32_t* offsets, size_t base, uint32_t min_len, uint32_t max_len) {
#if defined(__AVX2__)
    __m256i off_curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + base));
    __m256i off_next = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + base + 1));
    __m256i lengths = _mm256_sub_epi32(off_next, off_curr);
    __m256i min_vec = _mm256_set1_epi32(static_cast<int32_t>(min_len));
    __m256i max_vec = _mm256_set1_epi32(static_cast<int32_t>(max_len));
    // Native unsigned compare via the saturate-and-equal idiom:
    //   lengths >= min  <=>  max_epu32(lengths, min) == lengths
    //   lengths <= max  <=>  min_epu32(lengths, max) == lengths
    // This is unsigned-safe across the full uint32 range, including min_len == 0
    // and max_len == UINT32_MAX (the previous (min-1)/(max+1) trick wrapped).
    __m256i ge_min = _mm256_cmpeq_epi32(_mm256_max_epu32(lengths, min_vec), lengths);
    __m256i le_max = _mm256_cmpeq_epi32(_mm256_min_epu32(lengths, max_vec), lengths);
    __m256i in_range = _mm256_and_si256(ge_min, le_max);
    return static_cast<uint32_t>(_mm256_movemask_ps(_mm256_castsi256_ps(in_range)));
#elif defined(__ARM_NEON) && defined(__aarch64__)
    uint32x4_t off_curr0 = vld1q_u32(offsets + base);
    uint32x4_t off_next0 = vld1q_u32(offsets + base + 1);
    uint32x4_t lengths0 = vsubq_u32(off_next0, off_curr0);

    uint32x4_t off_curr1 = vld1q_u32(offsets + base + 4);
    uint32x4_t off_next1 = vld1q_u32(offsets + base + 5);
    uint32x4_t lengths1 = vsubq_u32(off_next1, off_curr1);

    uint32x4_t min_vec = vdupq_n_u32(min_len);
    uint32x4_t max_vec = vdupq_n_u32(max_len);

    uint32x4_t ge_min0 = vcgeq_u32(lengths0, min_vec);
    uint32x4_t le_max0 = vcleq_u32(lengths0, max_vec);
    uint32x4_t in_range0 = vandq_u32(ge_min0, le_max0);

    uint32x4_t ge_min1 = vcgeq_u32(lengths1, min_vec);
    uint32x4_t le_max1 = vcleq_u32(lengths1, max_vec);
    uint32x4_t in_range1 = vandq_u32(ge_min1, le_max1);

    return detail::reduce_8lane_mask(in_range0, in_range1);
#else
    uint32_t len = offsets[base + 1] - offsets[base];
    return (len >= min_len && len <= max_len) ? 1u : 0u;
#endif
}

} // namespace SIMD
