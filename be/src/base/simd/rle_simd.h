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

#include <algorithm>
#include <cstdint>
#include <limits>

#if defined(__AVX512F__) || defined(__AVX2__) || defined(__SSE4_1__)
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "base/simd/multi_version.h"

namespace starrocks {

// ============================================================================
// SIMD-optimized fill operation for RLE repeated values (int16_t)
// Used for Parquet level decoding (def_level, rep_level are int16_t)
// ============================================================================

#if defined(__AVX512BW__)
MFV_AVX512BW(void simd_fill_int16_avx512(int16_t* __restrict dst, int16_t value, int32_t count) {
    using v32s = __m512i;
    const v32s v_value = _mm512_set1_epi16(value);

    int32_t i = 0;
    // Process 128 elements per iteration (4x unroll of 32-element vectors)
    for (; i + 128 <= count; i += 128) {
        _mm512_storeu_si512((v32s*)(dst + i), v_value);
        _mm512_storeu_si512((v32s*)(dst + i + 32), v_value);
        _mm512_storeu_si512((v32s*)(dst + i + 64), v_value);
        _mm512_storeu_si512((v32s*)(dst + i + 96), v_value);
    }
    // Process 32 elements at a time
    for (; i + 32 <= count; i += 32) {
        _mm512_storeu_si512((v32s*)(dst + i), v_value);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dst[i] = value;
    }
})
#endif

#if defined(__AVX2__)
MFV_AVX2(void simd_fill_int16_avx2(int16_t* __restrict dst, int16_t value, int32_t count) {
    using v16s = __m256i;
    const v16s v_value = _mm256_set1_epi16(value);

    int32_t i = 0;
    // Process 64 elements per iteration (4x unroll of 16-element vectors)
    for (; i + 64 <= count; i += 64) {
        _mm256_storeu_si256((v16s*)(dst + i), v_value);
        _mm256_storeu_si256((v16s*)(dst + i + 16), v_value);
        _mm256_storeu_si256((v16s*)(dst + i + 32), v_value);
        _mm256_storeu_si256((v16s*)(dst + i + 48), v_value);
    }
    // Process 16 elements at a time
    for (; i + 16 <= count; i += 16) {
        _mm256_storeu_si256((v16s*)(dst + i), v_value);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dst[i] = value;
    }
})
#endif

#if defined(__ARM_NEON) && defined(__aarch64__)
inline void simd_fill_int16_neon(int16_t* __restrict dst, int16_t value, int32_t count) {
    const int16x8_t v_value = vdupq_n_s16(value);

    int32_t i = 0;
    // Process 32 elements per iteration (4x unroll)
    for (; i + 32 <= count; i += 32) {
        vst1q_s16(dst + i, v_value);
        vst1q_s16(dst + i + 8, v_value);
        vst1q_s16(dst + i + 16, v_value);
        vst1q_s16(dst + i + 24, v_value);
    }
    // Process 8 elements at a time
    for (; i + 8 <= count; i += 8) {
        vst1q_s16(dst + i, v_value);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dst[i] = value;
    }
}
#endif

MFV_DEFAULT(void simd_fill_int16_default(int16_t* __restrict dst, int16_t value, int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
        dst[i] = value;
    }
})

// Main entry point for int16_t fill
inline void simd_fill_int16(int16_t* __restrict dst, int16_t value, int32_t count) {
#if defined(__AVX512BW__)
    simd_fill_int16_avx512(dst, value, count);
#elif defined(__AVX2__)
    simd_fill_int16_avx2(dst, value, count);
#elif defined(__ARM_NEON) && defined(__aarch64__)
    simd_fill_int16_neon(dst, value, count);
#else
    simd_fill_int16_default(dst, value, count);
#endif
}

// ============================================================================
// SIMD-optimized fill operation for RLE repeated values (int32_t)
// Fills an array with a single repeated value using broadcast + store
// ============================================================================

#if defined(__AVX512F__)
MFV_AVX512F(void simd_fill_int32_avx512(int32_t* __restrict dst, int32_t value, int32_t count) {
    using v16i = __m512i;
    const v16i v_value = _mm512_set1_epi32(value);

    int32_t i = 0;
    // Process 64 elements per iteration (4x unroll of 16-element vectors)
    for (; i + 64 <= count; i += 64) {
        _mm512_storeu_si512((v16i*)(dst + i), v_value);
        _mm512_storeu_si512((v16i*)(dst + i + 16), v_value);
        _mm512_storeu_si512((v16i*)(dst + i + 32), v_value);
        _mm512_storeu_si512((v16i*)(dst + i + 48), v_value);
    }
    // Process 16 elements at a time
    for (; i + 16 <= count; i += 16) {
        _mm512_storeu_si512((v16i*)(dst + i), v_value);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dst[i] = value;
    }
})
#endif

#if defined(__AVX2__)
MFV_AVX2(void simd_fill_int32_avx2(int32_t* __restrict dst, int32_t value, int32_t count) {
    using v8i = __m256i;
    const v8i v_value = _mm256_set1_epi32(value);

    int32_t i = 0;
    // Process 32 elements per iteration (4x unroll of 8-element vectors)
    for (; i + 32 <= count; i += 32) {
        _mm256_storeu_si256((v8i*)(dst + i), v_value);
        _mm256_storeu_si256((v8i*)(dst + i + 8), v_value);
        _mm256_storeu_si256((v8i*)(dst + i + 16), v_value);
        _mm256_storeu_si256((v8i*)(dst + i + 24), v_value);
    }
    // Process 8 elements at a time
    for (; i + 8 <= count; i += 8) {
        _mm256_storeu_si256((v8i*)(dst + i), v_value);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dst[i] = value;
    }
})
#endif

#if defined(__ARM_NEON) && defined(__aarch64__)
inline void simd_fill_int32_neon(int32_t* __restrict dst, int32_t value, int32_t count) {
    const int32x4_t v_value = vdupq_n_s32(value);

    int32_t i = 0;
    // Process 16 elements per iteration (4x unroll)
    for (; i + 16 <= count; i += 16) {
        vst1q_s32(dst + i, v_value);
        vst1q_s32(dst + i + 4, v_value);
        vst1q_s32(dst + i + 8, v_value);
        vst1q_s32(dst + i + 12, v_value);
    }
    // Process 4 elements at a time
    for (; i + 4 <= count; i += 4) {
        vst1q_s32(dst + i, v_value);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dst[i] = value;
    }
}
#endif

MFV_DEFAULT(void simd_fill_int32_default(int32_t* __restrict dst, int32_t value, int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
        dst[i] = value;
    }
})

// Main entry point - dispatches to best available implementation
inline void simd_fill_int32(int32_t* __restrict dst, int32_t value, int32_t count) {
#if defined(__AVX512F__)
    simd_fill_int32_avx512(dst, value, count);
#elif defined(__AVX2__)
    simd_fill_int32_avx2(dst, value, count);
#elif defined(__ARM_NEON) && defined(__aarch64__)
    simd_fill_int32_neon(dst, value, count);
#else
    simd_fill_int32_default(dst, value, count);
#endif
}

// ============================================================================
// SIMD-optimized min/max for dictionary bounds checking
// Returns both min and max values in a single pass
// ============================================================================

#if defined(__AVX512F__)
MFV_AVX512F(void simd_minmax_int32_avx512(const int32_t* __restrict data, int32_t count, int32_t& out_min,
                                          int32_t& out_max) {
    if (count <= 0) {
        out_min = std::numeric_limits<int32_t>::max();
        out_max = std::numeric_limits<int32_t>::min();
        return;
    }

    using v16i = __m512i;
    v16i v_min = _mm512_set1_epi32(std::numeric_limits<int32_t>::max());
    v16i v_max = _mm512_set1_epi32(std::numeric_limits<int32_t>::min());

    int32_t i = 0;
    // Process 16 elements at a time
    for (; i + 16 <= count; i += 16) {
        v16i v_data = _mm512_loadu_si512((const v16i*)(data + i));
        v_min = _mm512_min_epi32(v_min, v_data);
        v_max = _mm512_max_epi32(v_max, v_data);
    }

    // Horizontal reduction
    out_min = _mm512_reduce_min_epi32(v_min);
    out_max = _mm512_reduce_max_epi32(v_max);

    // Process remaining elements
    for (; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
})
#endif

#if defined(__AVX2__)
MFV_AVX2(void simd_minmax_int32_avx2(const int32_t* __restrict data, int32_t count, int32_t& out_min,
                                     int32_t& out_max) {
    if (count <= 0) {
        out_min = std::numeric_limits<int32_t>::max();
        out_max = std::numeric_limits<int32_t>::min();
        return;
    }

    using v8i = __m256i;
    v8i v_min = _mm256_set1_epi32(std::numeric_limits<int32_t>::max());
    v8i v_max = _mm256_set1_epi32(std::numeric_limits<int32_t>::min());

    int32_t i = 0;
    // Process 8 elements at a time
    for (; i + 8 <= count; i += 8) {
        v8i v_data = _mm256_loadu_si256((const v8i*)(data + i));
        v_min = _mm256_min_epi32(v_min, v_data);
        v_max = _mm256_max_epi32(v_max, v_data);
    }

    // Horizontal reduction - AVX2 doesn't have reduce, so we do it manually
    // First, reduce 256-bit to 128-bit
    __m128i min_lo = _mm256_castsi256_si128(v_min);
    __m128i min_hi = _mm256_extracti128_si256(v_min, 1);
    __m128i min_128 = _mm_min_epi32(min_lo, min_hi);

    __m128i max_lo = _mm256_castsi256_si128(v_max);
    __m128i max_hi = _mm256_extracti128_si256(v_max, 1);
    __m128i max_128 = _mm_max_epi32(max_lo, max_hi);

    // Then reduce 128-bit to 64-bit
    min_128 = _mm_min_epi32(min_128, _mm_srli_si128(min_128, 8));
    max_128 = _mm_max_epi32(max_128, _mm_srli_si128(max_128, 8));

    // Finally reduce to 32-bit
    min_128 = _mm_min_epi32(min_128, _mm_srli_si128(min_128, 4));
    max_128 = _mm_max_epi32(max_128, _mm_srli_si128(max_128, 4));

    out_min = _mm_cvtsi128_si32(min_128);
    out_max = _mm_cvtsi128_si32(max_128);

    // Process remaining elements
    for (; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
})
#endif

#if defined(__ARM_NEON) && defined(__aarch64__)
inline void simd_minmax_int32_neon(const int32_t* __restrict data, int32_t count, int32_t& out_min, int32_t& out_max) {
    if (count <= 0) {
        out_min = std::numeric_limits<int32_t>::max();
        out_max = std::numeric_limits<int32_t>::min();
        return;
    }

    int32x4_t v_min = vdupq_n_s32(std::numeric_limits<int32_t>::max());
    int32x4_t v_max = vdupq_n_s32(std::numeric_limits<int32_t>::min());

    int32_t i = 0;
    // Process 4 elements at a time
    for (; i + 4 <= count; i += 4) {
        int32x4_t v_data = vld1q_s32(data + i);
        v_min = vminq_s32(v_min, v_data);
        v_max = vmaxq_s32(v_max, v_data);
    }

    // Horizontal reduction using vminvq_s32/vmaxvq_s32 (aarch64)
    out_min = vminvq_s32(v_min);
    out_max = vmaxvq_s32(v_max);

    // Process remaining elements
    for (; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
}
#endif

MFV_DEFAULT(void simd_minmax_int32_default(const int32_t* __restrict data, int32_t count, int32_t& out_min,
                                           int32_t& out_max) {
    out_min = std::numeric_limits<int32_t>::max();
    out_max = std::numeric_limits<int32_t>::min();
    for (int32_t i = 0; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
})

inline void simd_minmax_int32(const int32_t* __restrict data, int32_t count, int32_t& out_min, int32_t& out_max) {
#if defined(__AVX512F__)
    simd_minmax_int32_avx512(data, count, out_min, out_max);
#elif defined(__AVX2__)
    simd_minmax_int32_avx2(data, count, out_min, out_max);
#elif defined(__ARM_NEON) && defined(__aarch64__)
    simd_minmax_int32_neon(data, count, out_min, out_max);
#else
    simd_minmax_int32_default(data, count, out_min, out_max);
#endif
}

// ============================================================================
// SIMD-optimized dictionary gather for dictionary decoding
// Gathers values from dictionary using indices array
// dest[i] = dict[indices[i]]
// ============================================================================

#if defined(__AVX2__)
MFV_AVX2(void simd_dict_gather_int32_avx2(int32_t* __restrict dest, const int32_t* __restrict dict,
                                          const uint32_t* __restrict indices, int32_t count) {
    using v8i = __m256i;

    int32_t i = 0;
    // Process 8 elements at a time using gather
    for (; i + 8 <= count; i += 8) {
        v8i v_indices = _mm256_loadu_si256((const v8i*)(indices + i));
        v8i v_gathered = _mm256_i32gather_epi32(dict, v_indices, sizeof(int32_t));
        _mm256_storeu_si256((v8i*)(dest + i), v_gathered);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dest[i] = dict[indices[i]];
    }
})
#endif

#if defined(__AVX512F__)
MFV_AVX512F(void simd_dict_gather_int32_avx512(int32_t* __restrict dest, const int32_t* __restrict dict,
                                               const uint32_t* __restrict indices, int32_t count) {
    using v16i = __m512i;

    int32_t i = 0;
    // Process 16 elements at a time using gather
    for (; i + 16 <= count; i += 16) {
        v16i v_indices = _mm512_loadu_si512((const v16i*)(indices + i));
        v16i v_gathered = _mm512_i32gather_epi32(v_indices, dict, sizeof(int32_t));
        _mm512_storeu_si512((v16i*)(dest + i), v_gathered);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dest[i] = dict[indices[i]];
    }
})
#endif

MFV_DEFAULT(void simd_dict_gather_int32_default(int32_t* __restrict dest, const int32_t* __restrict dict,
                                                const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
        dest[i] = dict[indices[i]];
    }
})

inline void simd_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                   const uint32_t* __restrict indices, int32_t count) {
#if defined(__AVX512F__)
    simd_dict_gather_int32_avx512(dest, dict, indices, count);
#elif defined(__AVX2__)
    simd_dict_gather_int32_avx2(dest, dict, indices, count);
#else
    simd_dict_gather_int32_default(dest, dict, indices, count);
#endif
}

// 64-bit version (for int64_t dictionaries)
#if defined(__AVX512F__)
MFV_AVX512F(void simd_dict_gather_int64_avx512(int64_t* __restrict dest, const int64_t* __restrict dict,
                                               const uint32_t* __restrict indices, int32_t count) {
    using v8q = __m512i;

    int32_t i = 0;
    // Process 8 elements at a time (512 bits / 64 bits = 8 elements)
    for (; i + 8 <= count; i += 8) {
        // Load 8 32-bit indices and zero-extend to 64-bit
        __m256i v_indices_32 = _mm256_loadu_si256((const __m256i*)(indices + i));
        v8q v_indices = _mm512_cvtepu32_epi64(v_indices_32);
        v8q v_gathered = _mm512_i64gather_epi64(v_indices, dict, sizeof(int64_t));
        _mm512_storeu_si512((v8q*)(dest + i), v_gathered);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dest[i] = dict[indices[i]];
    }
})
#endif

#if defined(__AVX2__)
MFV_AVX2(void simd_dict_gather_int64_avx2(int64_t* __restrict dest, const int64_t* __restrict dict,
                                          const uint32_t* __restrict indices, int32_t count) {
    using v4q = __m256i;

    int32_t i = 0;
    // Process 4 elements at a time (256 bits / 64 bits = 4 elements)
    for (; i + 4 <= count; i += 4) {
        // Load 4 32-bit indices and zero-extend to 64-bit
        __m128i v_indices_32 = _mm_loadu_si128((const __m128i*)(indices + i));
        v4q v_indices = _mm256_cvtepu32_epi64(v_indices_32);
        v4q v_gathered = _mm256_i64gather_epi64((const long long*)dict, v_indices, sizeof(int64_t));
        _mm256_storeu_si256((v4q*)(dest + i), v_gathered);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dest[i] = dict[indices[i]];
    }
})
#endif

MFV_DEFAULT(void simd_dict_gather_int64_default(int64_t* __restrict dest, const int64_t* __restrict dict,
                                                const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
        dest[i] = dict[indices[i]];
    }
})

inline void simd_dict_gather_int64(int64_t* __restrict dest, const int64_t* __restrict dict,
                                   const uint32_t* __restrict indices, int32_t count) {
#if defined(__AVX512F__)
    simd_dict_gather_int64_avx512(dest, dict, indices, count);
#elif defined(__AVX2__)
    simd_dict_gather_int64_avx2(dest, dict, indices, count);
#else
    simd_dict_gather_int64_default(dest, dict, indices, count);
#endif
}

// Float versions using the int versions (same bit representation)
inline void simd_dict_gather_float(float* __restrict dest, const float* __restrict dict,
                                   const uint32_t* __restrict indices, int32_t count) {
    simd_dict_gather_int32(reinterpret_cast<int32_t*>(dest), reinterpret_cast<const int32_t*>(dict), indices, count);
}

inline void simd_dict_gather_double(double* __restrict dest, const double* __restrict dict,
                                    const uint32_t* __restrict indices, int32_t count) {
    simd_dict_gather_int64(reinterpret_cast<int64_t*>(dest), reinterpret_cast<const int64_t*>(dict), indices, count);
}

// ============================================================================
// SIMD-optimized type widening (sign extension) for Parquet type conversion
// int8 -> int32, int16 -> int32
// ============================================================================

#if defined(__AVX2__)
MFV_AVX2(void simd_widen_int8_to_int32_avx2(int32_t* __restrict dest, const int8_t* __restrict src, int32_t count) {
    int32_t i = 0;
    // Process 8 elements at a time (256 bits / 32 bits = 8 int32s)
    // Load 8 int8s (64 bits), sign-extend to 8 int32s
    for (; i + 8 <= count; i += 8) {
        __m128i v_src = _mm_loadl_epi64((const __m128i*)(src + i)); // load 8 bytes
        __m256i v_dst = _mm256_cvtepi8_epi32(v_src);
        _mm256_storeu_si256((__m256i*)(dest + i), v_dst);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dest[i] = static_cast<int32_t>(src[i]);
    }
})
#endif

MFV_DEFAULT(void simd_widen_int8_to_int32_default(int32_t* __restrict dest, const int8_t* __restrict src,
                                                  int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
        dest[i] = static_cast<int32_t>(src[i]);
    }
})

inline void simd_widen_int8_to_int32(int32_t* __restrict dest, const int8_t* __restrict src, int32_t count) {
#if defined(__AVX2__)
    simd_widen_int8_to_int32_avx2(dest, src, count);
#else
    simd_widen_int8_to_int32_default(dest, src, count);
#endif
}

#if defined(__AVX2__)
MFV_AVX2(void simd_widen_int16_to_int32_avx2(int32_t* __restrict dest, const int16_t* __restrict src, int32_t count) {
    int32_t i = 0;
    // Process 8 elements at a time (256 bits / 32 bits = 8 int32s)
    // Load 8 int16s (128 bits), sign-extend to 8 int32s
    for (; i + 8 <= count; i += 8) {
        __m128i v_src = _mm_loadu_si128((const __m128i*)(src + i));
        __m256i v_dst = _mm256_cvtepi16_epi32(v_src);
        _mm256_storeu_si256((__m256i*)(dest + i), v_dst);
    }
    // Scalar tail
    for (; i < count; ++i) {
        dest[i] = static_cast<int32_t>(src[i]);
    }
})
#endif

MFV_DEFAULT(void simd_widen_int16_to_int32_default(int32_t* __restrict dest, const int16_t* __restrict src,
                                                   int32_t count) {
    for (int32_t i = 0; i < count; ++i) {
        dest[i] = static_cast<int32_t>(src[i]);
    }
})

inline void simd_widen_int16_to_int32(int32_t* __restrict dest, const int16_t* __restrict src, int32_t count) {
#if defined(__AVX2__)
    simd_widen_int16_to_int32_avx2(dest, src, count);
#else
    simd_widen_int16_to_int32_default(dest, src, count);
#endif
}

} // namespace starrocks
