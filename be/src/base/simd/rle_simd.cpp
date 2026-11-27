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

#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace starrocks {

#if defined(__x86_64__)

#define RLE_SIMD_TARGET_AVX2 __attribute__((target("avx2")))
// The 512-bit minmax and widening kernels only emit AVX-512F instructions
// (_mm512_min/max_epi32, _mm512_cvtepi8/16_epi32), so their target attribute
// requires only avx512f (allowing unit tests to exercise them on any AVX-512F CPU).
// Production runtime dispatch below still gates on cpu_supports_fast_avx512()
// (requiring avx512vbmi2) to avoid 512-bit frequency downclocking on Skylake-X/SP
// and Cascade/Cooper Lake.
#define RLE_SIMD_TARGET_AVX512F __attribute__((target("avx512f")))

namespace {
inline bool cpu_supports_fast_avx512() {
    return __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl") &&
           __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("avx512vbmi2");
}
} // namespace

// ============================================================================
// SIMD min/max for int32 dictionary bounds check
// ============================================================================

RLE_SIMD_TARGET_AVX2 void simd_minmax_int32_avx2(const int32_t* __restrict data, int32_t count, int32_t& out_min,
                                                 int32_t& out_max) {
    if (count <= 0) {
        out_min = std::numeric_limits<int32_t>::max();
        out_max = std::numeric_limits<int32_t>::min();
        return;
    }

    __m256i v_min = _mm256_set1_epi32(std::numeric_limits<int32_t>::max());
    __m256i v_max = _mm256_set1_epi32(std::numeric_limits<int32_t>::min());

    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        v_min = _mm256_min_epi32(v_min, v_data);
        v_max = _mm256_max_epi32(v_max, v_data);
    }

    __m128i min128 = _mm_min_epi32(_mm256_castsi256_si128(v_min), _mm256_extracti128_si256(v_min, 1));
    min128 = _mm_min_epi32(min128, _mm_shuffle_epi32(min128, _MM_SHUFFLE(1, 0, 3, 2)));
    min128 = _mm_min_epi32(min128, _mm_shuffle_epi32(min128, _MM_SHUFFLE(2, 3, 0, 1)));
    out_min = _mm_cvtsi128_si32(min128);

    __m128i max128 = _mm_max_epi32(_mm256_castsi256_si128(v_max), _mm256_extracti128_si256(v_max, 1));
    max128 = _mm_max_epi32(max128, _mm_shuffle_epi32(max128, _MM_SHUFFLE(1, 0, 3, 2)));
    max128 = _mm_max_epi32(max128, _mm_shuffle_epi32(max128, _MM_SHUFFLE(2, 3, 0, 1)));
    out_max = _mm_cvtsi128_si32(max128);

    for (; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
}

RLE_SIMD_TARGET_AVX512F void simd_minmax_int32_avx512(const int32_t* __restrict data, int32_t count, int32_t& out_min,
                                                      int32_t& out_max) {
    if (count <= 0) {
        out_min = std::numeric_limits<int32_t>::max();
        out_max = std::numeric_limits<int32_t>::min();
        return;
    }

    __m512i v_min = _mm512_set1_epi32(std::numeric_limits<int32_t>::max());
    __m512i v_max = _mm512_set1_epi32(std::numeric_limits<int32_t>::min());

    int32_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m512i v_data = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(data + i));
        v_min = _mm512_min_epi32(v_min, v_data);
        v_max = _mm512_max_epi32(v_max, v_data);
    }
    out_min = _mm512_reduce_min_epi32(v_min);
    out_max = _mm512_reduce_max_epi32(v_max);
    for (; i < count; ++i) {
        out_min = std::min(out_min, data[i]);
        out_max = std::max(out_max, data[i]);
    }
}
#endif // defined(__x86_64__)

void simd_minmax_int32(const int32_t* __restrict data, int32_t count, int32_t& out_min, int32_t& out_max) {
#if defined(__x86_64__)
#if !defined(ADDRESS_SANITIZER)
    if (cpu_supports_fast_avx512()) {
        simd_minmax_int32_avx512(data, count, out_min, out_max);
        return;
    }
#endif // !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx2")) {
        simd_minmax_int32_avx2(data, count, out_min, out_max);
        return;
    }
#endif // defined(__x86_64__)
    simd_minmax_int32_default(data, count, out_min, out_max);
}

// Prefer the AVX2 gather path on x86_64: vpgatherdd ymm beats both AVX-512
// vpgatherdd zmm and auto-vectorised scalar on Intel Ice Lake (the wider gather
// still serialises through one gather port). On ARM64 (and non-AVX2 x86), the
// 4x-unrolled default hides load-to-use latency across independent local registers.
#if defined(__x86_64__)
RLE_SIMD_TARGET_AVX2 static void simd_dict_gather_int32_avx2(int32_t* __restrict dest, const int32_t* __restrict dict,
                                                             const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_indices = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + i));
        __m256i v_gathered = _mm256_i32gather_epi32(dict, v_indices, sizeof(int32_t));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}
#endif // defined(__x86_64__)

void simd_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                            const uint32_t* __restrict indices, int32_t count) {
#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        simd_dict_gather_int32_avx2(dest, dict, indices, count);
        return;
    }
#endif // defined(__x86_64__)
    simd_dict_gather_int32_default(dest, dict, indices, count);
}

#if defined(__x86_64__)
RLE_SIMD_TARGET_AVX2 static void simd_dict_gather_int64_avx2(int64_t* __restrict dest, const int64_t* __restrict dict,
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
}
#endif // defined(__x86_64__)

void simd_dict_gather_int64(int64_t* __restrict dest, const int64_t* __restrict dict,
                            const uint32_t* __restrict indices, int32_t count) {
#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        simd_dict_gather_int64_avx2(dest, dict, indices, count);
        return;
    }
#endif // defined(__x86_64__)
    simd_dict_gather_int64_default(dest, dict, indices, count);
}

// Typed float / double gather: punning float*/double* to int32_t*/int64_t* to
// share the integer gather is strict-aliasing UB.
#if defined(__x86_64__)
RLE_SIMD_TARGET_AVX2 static void simd_dict_gather_float_avx2(float* __restrict dest, const float* __restrict dict,
                                                             const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_indices = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + i));
        __m256 v_gathered = _mm256_i32gather_ps(dict, v_indices, sizeof(float));
        _mm256_storeu_ps(dest + i, v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}

RLE_SIMD_TARGET_AVX2 static void simd_dict_gather_double_avx2(double* __restrict dest, const double* __restrict dict,
                                                              const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 4 <= count; i += 4) {
        __m128i v_indices = _mm_loadu_si128(reinterpret_cast<const __m128i*>(indices + i));
        __m256d v_gathered = _mm256_i32gather_pd(dict, v_indices, sizeof(double));
        _mm256_storeu_pd(dest + i, v_gathered);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}
#endif // defined(__x86_64__)

void simd_dict_gather_float(float* __restrict dest, const float* __restrict dict, const uint32_t* __restrict indices,
                            int32_t count) {
#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        simd_dict_gather_float_avx2(dest, dict, indices, count);
        return;
    }
#endif // defined(__x86_64__)
    simd_dict_gather_float_default(dest, dict, indices, count);
}

void simd_dict_gather_double(double* __restrict dest, const double* __restrict dict, const uint32_t* __restrict indices,
                             int32_t count) {
#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        simd_dict_gather_double_avx2(dest, dict, indices, count);
        return;
    }
#endif // defined(__x86_64__)
    simd_dict_gather_double_default(dest, dict, indices, count);
}

// ============================================================================
// SIMD type widening (sign extension) for Parquet INT8/INT16 -> INT32 reads
// ============================================================================

#if defined(__x86_64__)
RLE_SIMD_TARGET_AVX512F void simd_widen_int8_to_int32_avx512(int32_t* __restrict dest, const int8_t* __restrict src,
                                                             int32_t count) {
    int32_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m128i v_src = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
        __m512i v_dst = _mm512_cvtepi8_epi32(v_src);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), v_dst);
    }
    for (; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
}

RLE_SIMD_TARGET_AVX2 void simd_widen_int8_to_int32_avx2(int32_t* __restrict dest, const int8_t* __restrict src,
                                                        int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m128i v_src = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(src + i));
        __m256i v_dst = _mm256_cvtepi8_epi32(v_src);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_dst);
    }
    for (; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
}
#endif // defined(__x86_64__)

void simd_widen_int8_to_int32(int32_t* __restrict dest, const int8_t* __restrict src, int32_t count) {
#if defined(__x86_64__)
#if !defined(ADDRESS_SANITIZER)
    if (cpu_supports_fast_avx512()) {
        simd_widen_int8_to_int32_avx512(dest, src, count);
        return;
    }
#endif // !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx2")) {
        simd_widen_int8_to_int32_avx2(dest, src, count);
        return;
    }
#endif // defined(__x86_64__)
    simd_widen_int8_to_int32_default(dest, src, count);
}

#if defined(__x86_64__)
RLE_SIMD_TARGET_AVX512F void simd_widen_int16_to_int32_avx512(int32_t* __restrict dest, const int16_t* __restrict src,
                                                              int32_t count) {
    int32_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m256i v_src = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
        __m512i v_dst = _mm512_cvtepi16_epi32(v_src);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), v_dst);
    }
    for (; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
}

RLE_SIMD_TARGET_AVX2 void simd_widen_int16_to_int32_avx2(int32_t* __restrict dest, const int16_t* __restrict src,
                                                         int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m128i v_src = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
        __m256i v_dst = _mm256_cvtepi16_epi32(v_src);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_dst);
    }
    for (; i < count; ++i) dest[i] = static_cast<int32_t>(src[i]);
}

#undef RLE_SIMD_TARGET_AVX2
#undef RLE_SIMD_TARGET_AVX512F
#endif // defined(__x86_64__)

void simd_widen_int16_to_int32(int32_t* __restrict dest, const int16_t* __restrict src, int32_t count) {
#if defined(__x86_64__)
#if !defined(ADDRESS_SANITIZER)
    if (cpu_supports_fast_avx512()) {
        simd_widen_int16_to_int32_avx512(dest, src, count);
        return;
    }
#endif // !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx2")) {
        simd_widen_int16_to_int32_avx2(dest, src, count);
        return;
    }
#endif // defined(__x86_64__)
    simd_widen_int16_to_int32_default(dest, src, count);
}

} // namespace starrocks
