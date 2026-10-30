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

#include "base/simd/rle_simd.h"

#if defined(__AVX512F__) || defined(__AVX2__) || defined(__SSE4_1__)
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "base/simd/multi_version.h"

namespace starrocks {

// ============================================================================
// SIMD min/max for int32 dictionary bounds check
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
    for (; i + 16 <= count; i += 16) {
        v16i v_data = _mm512_loadu_si512((const v16i*)(data + i));
        v_min = _mm512_min_epi32(v_min, v_data);
        v_max = _mm512_max_epi32(v_max, v_data);
    }
    out_min = _mm512_reduce_min_epi32(v_min);
    out_max = _mm512_reduce_max_epi32(v_max);
    for (; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
})
#endif

// The hand-rolled AVX2 reduction was slower than the auto-vectorised scalar
// default by ~15% (the auto-vectoriser uses pmovmskb-free reductions the
// manual code cannot match). NEON was on par with the default. Both removed.
void simd_minmax_int32(const int32_t* __restrict data, int32_t count, int32_t& out_min, int32_t& out_max) {
#if defined(__AVX512F__)
    simd_minmax_int32_avx512(data, count, out_min, out_max);
#else
    simd_minmax_int32_default(data, count, out_min, out_max);
#endif
}

// ============================================================================
// SIMD dictionary gather: dest[i] = dict[indices[i]]
// ============================================================================

#if defined(__AVX512F__)
MFV_AVX512F(void simd_dict_gather_int32_avx512(int32_t* __restrict dest, const int32_t* __restrict dict,
                                               const uint32_t* __restrict indices, int32_t count) {
    using v16i = __m512i;
    int32_t i = 0;
    for (; i + 16 <= count; i += 16) {
        v16i v_indices = _mm512_loadu_si512((const v16i*)(indices + i));
        v16i v_gathered = _mm512_i32gather_epi32(v_indices, dict, sizeof(int32_t));
        _mm512_storeu_si512((v16i*)(dest + i), v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
})
#endif

#if defined(__AVX2__)
MFV_AVX2(void simd_dict_gather_int32_avx2(int32_t* __restrict dest, const int32_t* __restrict dict,
                                          const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_indices = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + i));
        __m256i v_gathered = _mm256_i32gather_epi32(dict, v_indices, sizeof(int32_t));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
})
#endif

// Prefer the AVX2 path: vpgatherdd ymm beats both the AVX-512 vpgatherdd zmm
// and the auto-vectorised scalar on Intel Ice Lake (the wider gather still
// serialises through one gather port at lower AVX-512 clock). On hosts with
// the Intel GDS mitigation applied (some Aliyun Intel SKUs), AVX2 gather is
// throttled; the dispatch should be flipped to _default there.
void simd_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                            const uint32_t* __restrict indices, int32_t count) {
#if defined(__AVX2__)
    simd_dict_gather_int32_avx2(dest, dict, indices, count);
#else
    simd_dict_gather_int32_default(dest, dict, indices, count);
#endif
}

#if defined(__AVX512F__)
MFV_AVX512F(void simd_dict_gather_int64_avx512(int64_t* __restrict dest, const int64_t* __restrict dict,
                                               const uint32_t* __restrict indices, int32_t count) {
    using v8q = __m512i;
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_indices_32 = _mm256_loadu_si256((const __m256i*)(indices + i));
        v8q v_indices = _mm512_cvtepu32_epi64(v_indices_32);
        v8q v_gathered = _mm512_i64gather_epi64(v_indices, dict, sizeof(int64_t));
        _mm512_storeu_si512((v8q*)(dest + i), v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
})
#endif

#if defined(__AVX2__)
MFV_AVX2(void simd_dict_gather_int64_avx2(int64_t* __restrict dest, const int64_t* __restrict dict,
                                          const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    // AVX2 i64 gather takes 4x32-bit indices in xmm.
    for (; i + 4 <= count; i += 4) {
        __m128i v_indices = _mm_loadu_si128(reinterpret_cast<const __m128i*>(indices + i));
        __m256i v_gathered =
                _mm256_i32gather_epi64(reinterpret_cast<const long long*>(dict), v_indices, sizeof(int64_t));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
})
#endif

void simd_dict_gather_int64(int64_t* __restrict dest, const int64_t* __restrict dict,
                            const uint32_t* __restrict indices, int32_t count) {
#if defined(__AVX2__)
    simd_dict_gather_int64_avx2(dest, dict, indices, count);
#else
    simd_dict_gather_int64_default(dest, dict, indices, count);
#endif
}

// Typed float / double gather: punning float*/double* to int32_t*/int64_t* to
// share the integer gather is strict-aliasing UB.
#if defined(__AVX2__)
MFV_AVX2(void simd_dict_gather_float_avx2(float* __restrict dest, const float* __restrict dict,
                                          const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_indices = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + i));
        __m256 v_gathered = _mm256_i32gather_ps(dict, v_indices, sizeof(float));
        _mm256_storeu_ps(dest + i, v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
})

MFV_AVX2(void simd_dict_gather_double_avx2(double* __restrict dest, const double* __restrict dict,
                                           const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m128i v_indices = _mm_loadu_si128(reinterpret_cast<const __m128i*>(indices + i));
        __m256d v_gathered = _mm256_i32gather_pd(dict, v_indices, sizeof(double));
        _mm256_storeu_pd(dest + i, v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
})
#endif

void simd_dict_gather_float(float* __restrict dest, const float* __restrict dict, const uint32_t* __restrict indices,
                            int32_t count) {
#if defined(__AVX2__)
    simd_dict_gather_float_avx2(dest, dict, indices, count);
#else
    simd_dict_gather_float_default(dest, dict, indices, count);
#endif
}

void simd_dict_gather_double(double* __restrict dest, const double* __restrict dict, const uint32_t* __restrict indices,
                             int32_t count) {
#if defined(__AVX2__)
    simd_dict_gather_double_avx2(dest, dict, indices, count);
#else
    simd_dict_gather_double_default(dest, dict, indices, count);
#endif
}

// ============================================================================
// SIMD type widening (sign extension) for Parquet INT8/INT16 -> INT32 reads
// ============================================================================

#if defined(__AVX2__)
MFV_AVX2(void simd_widen_int8_to_int32_avx2(int32_t* __restrict dest, const int8_t* __restrict src, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m128i v_src = _mm_loadl_epi64((const __m128i*)(src + i));
        __m256i v_dst = _mm256_cvtepi8_epi32(v_src);
        _mm256_storeu_si256((__m256i*)(dest + i), v_dst);
    }
    for (; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
})
#endif

// On AVX-512 builds the compiler auto-vectorises the scalar default to a
// 16-lane AVX-512 widen, which matches or beats the hand-written 8-lane AVX2
// path. Opt into the hand-written path only on AVX2-only builds.
void simd_widen_int8_to_int32(int32_t* __restrict dest, const int8_t* __restrict src, int32_t count) {
#if defined(__AVX2__) && !defined(__AVX512F__)
    simd_widen_int8_to_int32_avx2(dest, src, count);
#else
    simd_widen_int8_to_int32_default(dest, src, count);
#endif
}

#if defined(__AVX2__)
MFV_AVX2(void simd_widen_int16_to_int32_avx2(int32_t* __restrict dest, const int16_t* __restrict src, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m128i v_src = _mm_loadu_si128((const __m128i*)(src + i));
        __m256i v_dst = _mm256_cvtepi16_epi32(v_src);
        _mm256_storeu_si256((__m256i*)(dest + i), v_dst);
    }
    for (; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
})
#endif

void simd_widen_int16_to_int32(int32_t* __restrict dest, const int16_t* __restrict src, int32_t count) {
    // See simd_widen_int8_to_int32 above for the rationale.
#if defined(__AVX2__) && !defined(__AVX512F__)
    simd_widen_int16_to_int32_avx2(dest, src, count);
#else
    simd_widen_int16_to_int32_default(dest, src, count);
#endif
}

} // namespace starrocks
