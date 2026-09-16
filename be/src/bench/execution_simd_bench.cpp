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

// Microbenchmarks for the execution-layer SIMD paths added by PR #73293
// ([Enhancement] Add SIMD optimizations for execution layer).
//
// Covers the one hand-written SIMD path retained after review:
//   * sorting/compare_column.cpp :: compare_integral_column_simd
//       SIMD-packed cmp for an int column against a scalar pivot, updating
//       a cmp_vector that the next sort column will refine. Adaptive: when
//       cmp_vector is mostly zeros (rows still equal at previous columns),
//       SIMD-packed compare; otherwise scalar skip. Gated on !__AVX512F__,
//       where the auto-vectorised scalar fallback already wins.
//
// The PR's other integral loops (local_exchange / bucket_aware_partition XOR,
// chunk_predicate_evaluator and-not, tablet_sink validate_selection &= 0x1) are
// plain scalar loops left to compiler auto-vectorisation, so there is no
// hand-written variant to benchmark against.

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

namespace starrocks {

constexpr size_t kChunk = 4096;

// =====================================================================
// sorting/compare_column.cpp :: compare_integral_column_simd (int8)
// =====================================================================
//
// cmp_vector[i] holds the running compare result for row i (-1 / 0 / +1).
// For a second sort column, we only need to recompute cmp where the
// previous columns already tied (cmp_vector[i] == 0). The adaptive SIMD:
//   * If at least 87.5% of cmp_vector is zero, AVX2-packed compare.
//   * Otherwise scalar skip-non-zeros.
//
// state.range(0) is the percentage of rows still equal (i.e., cmp == 0).

template <typename T>
static inline int8_t scalar_cmp(T lhs, T rhs) {
    return lhs < rhs ? int8_t{-1} : (lhs > rhs ? int8_t{1} : int8_t{0});
}

template <typename T>
static void compare_integral_scalar(int8_t* cmp_vector, const T* lhs, T rhs, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<T>(lhs[i], rhs);
    }
}

template <typename T>
static void compare_integral_simd(int8_t* cmp_vector, const T* lhs, T rhs, size_t n);

// int8 specialization (matches the sizeof(T) == 1 branch in compare_column.cpp).
template <>
void compare_integral_simd<int8_t>(int8_t* cmp_vector, const int8_t* lhs, int8_t rhs, size_t n) {
    size_t i = 0;
    // Mirror the gate in compare_integral_column_simd (compare_column.cpp):
    // on AVX-512 builds the auto-vectorised scalar fallback beats the
    // hand-written AVX2 block-skip path, so we only opt into hand-SIMD
    // on AVX2-only builds.
#if defined(__AVX2__) && !defined(__AVX512F__)
    constexpr size_t kBlock = 32;
    const __m256i zero = _mm256_setzero_si256();
    const __m256i rhs_vec = _mm256_set1_epi8(rhs);
    const __m256i ones = _mm256_set1_epi8(1);
    for (; i + kBlock <= n; i += kBlock) {
        __m256i cmp_bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(cmp_vector + i));
        __m256i zeros = _mm256_cmpeq_epi8(cmp_bytes, zero);
        uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(zeros));
        if (mask != 0xFFFFFFFFu) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int8_t>(lhs[j], rhs);
            }
            continue;
        }
        __m256i lhs_v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + i));
        __m256i gt = _mm256_cmpgt_epi8(lhs_v, rhs_vec);
        __m256i lt = _mm256_cmpgt_epi8(rhs_vec, lhs_v);
        __m256i gt01 = _mm256_and_si256(gt, ones);
        __m256i lt01 = _mm256_and_si256(lt, ones);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(cmp_vector + i), _mm256_sub_epi8(gt01, lt01));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    constexpr size_t kBlock = 16;
    const int8x16_t zero = vdupq_n_s8(0);
    const int8x16_t rhs_vec = vdupq_n_s8(rhs);
    const int8x16_t ones = vdupq_n_s8(1);
    for (; i + kBlock <= n; i += kBlock) {
        int8x16_t cmp_bytes = vld1q_s8(cmp_vector + i);
        uint8x16_t zeros = vceqq_s8(cmp_bytes, zero);
        if (vminvq_u8(zeros) != 0xFF) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int8_t>(lhs[j], rhs);
            }
            continue;
        }
        int8x16_t lhs_v = vld1q_s8(lhs + i);
        int8x16_t gt = vreinterpretq_s8_u8(vcgtq_s8(lhs_v, rhs_vec));
        int8x16_t lt = vreinterpretq_s8_u8(vcltq_s8(lhs_v, rhs_vec));
        int8x16_t gt01 = vandq_s8(gt, ones);
        int8x16_t lt01 = vandq_s8(lt, ones);
        vst1q_s8(cmp_vector + i, vsubq_s8(gt01, lt01));
    }
#endif
    for (; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<int8_t>(lhs[i], rhs);
    }
}

// int32 specialization: cmp_vector is still int8, but lhs is int32.
// Pre-PR scalar/block: load 8-byte cmp slot as uint64, all-zero check.
// SIMD: 8x int32 cmpgt -> int32 +/-1/0, narrow to int8 per lane.
template <>
void compare_integral_simd<int32_t>(int8_t* cmp_vector, const int32_t* lhs, int32_t rhs, size_t n) {
    size_t i = 0;
    // See compare_integral_simd<int8_t> above for the gate rationale.
#if defined(__AVX2__) && !defined(__AVX512F__)
    constexpr size_t kBlock = 8;
    const __m256i rhs_vec = _mm256_set1_epi32(rhs);
    const __m256i ones = _mm256_set1_epi32(1);
    alignas(32) int32_t tmp[8];
    for (; i + kBlock <= n; i += kBlock) {
        uint64_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int32_t>(lhs[j], rhs);
            }
            continue;
        }
        __m256i lhs_v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + i));
        __m256i gt = _mm256_cmpgt_epi32(lhs_v, rhs_vec);
        __m256i lt = _mm256_cmpgt_epi32(rhs_vec, lhs_v);
        __m256i gt01 = _mm256_and_si256(gt, ones);
        __m256i lt01 = _mm256_and_si256(lt, ones);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(tmp), _mm256_sub_epi32(gt01, lt01));
        for (size_t k = 0; k < kBlock; ++k) cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    constexpr size_t kBlock = 4;
    const int32x4_t rhs_vec = vdupq_n_s32(rhs);
    const int32x4_t ones = vdupq_n_s32(1);
    alignas(16) int32_t tmp[4];
    for (; i + kBlock <= n; i += kBlock) {
        uint32_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int32_t>(lhs[j], rhs);
            }
            continue;
        }
        int32x4_t lhs_v = vld1q_s32(lhs + i);
        int32x4_t gt = vreinterpretq_s32_u32(vcgtq_s32(lhs_v, rhs_vec));
        int32x4_t lt = vreinterpretq_s32_u32(vcltq_s32(lhs_v, rhs_vec));
        int32x4_t gt01 = vandq_s32(gt, ones);
        int32x4_t lt01 = vandq_s32(lt, ones);
        vst1q_s32(tmp, vsubq_s32(gt01, lt01));
        for (size_t k = 0; k < kBlock; ++k) cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
    }
#endif
    for (; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<int32_t>(lhs[i], rhs);
    }
}

// int64 specialization: 4-lane AVX2 / 2-lane NEON.
template <>
void compare_integral_simd<int64_t>(int8_t* cmp_vector, const int64_t* lhs, int64_t rhs, size_t n) {
    size_t i = 0;
    // See compare_integral_simd<int8_t> above for the gate rationale.
#if defined(__AVX2__) && !defined(__AVX512F__)
    constexpr size_t kBlock = 4;
    const __m256i rhs_vec = _mm256_set1_epi64x(rhs);
    const __m256i ones = _mm256_set1_epi64x(1);
    alignas(32) int64_t tmp[4];
    for (; i + kBlock <= n; i += kBlock) {
        uint32_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int64_t>(lhs[j], rhs);
            }
            continue;
        }
        __m256i lhs_v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + i));
        __m256i gt = _mm256_cmpgt_epi64(lhs_v, rhs_vec);
        __m256i lt = _mm256_cmpgt_epi64(rhs_vec, lhs_v);
        __m256i gt01 = _mm256_and_si256(gt, ones);
        __m256i lt01 = _mm256_and_si256(lt, ones);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(tmp), _mm256_sub_epi64(gt01, lt01));
        for (size_t k = 0; k < kBlock; ++k) cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    constexpr size_t kBlock = 2;
    const int64x2_t rhs_vec = vdupq_n_s64(rhs);
    const int64x2_t ones = vdupq_n_s64(1);
    alignas(16) int64_t tmp[2];
    for (; i + kBlock <= n; i += kBlock) {
        uint16_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int64_t>(lhs[j], rhs);
            }
            continue;
        }
        int64x2_t lhs_v = vld1q_s64(lhs + i);
        int64x2_t gt = vreinterpretq_s64_u64(vcgtq_s64(lhs_v, rhs_vec));
        int64x2_t lt = vreinterpretq_s64_u64(vcltq_s64(lhs_v, rhs_vec));
        int64x2_t gt01 = vandq_s64(gt, ones);
        int64x2_t lt01 = vandq_s64(lt, ones);
        vst1q_s64(tmp, vsubq_s64(gt01, lt01));
        for (size_t k = 0; k < kBlock; ++k) cmp_vector[i + k] = static_cast<int8_t>(tmp[k]);
    }
#endif
    for (; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<int64_t>(lhs[i], rhs);
    }
}

template <typename T>
static void prepare_compare(std::vector<int8_t>& cmp, std::vector<T>& lhs, int zero_ratio_percent) {
    cmp.resize(kChunk);
    lhs.resize(kChunk);
    std::mt19937_64 rng(0x501234DE);
    std::uniform_int_distribution<int> rd(0, 99);
    std::uniform_int_distribution<int64_t> vd(-1000000, 1000000);
    for (size_t i = 0; i < kChunk; ++i) {
        cmp[i] = (rd(rng) < zero_ratio_percent) ? int8_t{0} : (rd(rng) < 50 ? int8_t{-1} : int8_t{1});
        lhs[i] = static_cast<T>(vd(rng));
    }
}

#define DEFINE_COMPARE_BENCH(TYPE, NAME)                                               \
    static void BM_Compare##NAME##_Scalar(benchmark::State& state) {                   \
        std::vector<int8_t> cmp_init;                                                  \
        std::vector<TYPE> lhs;                                                         \
        prepare_compare<TYPE>(cmp_init, lhs, static_cast<int>(state.range(0)));        \
        for (auto _ : state) {                                                         \
            std::vector<int8_t> cmp = cmp_init;                                        \
            compare_integral_scalar<TYPE>(cmp.data(), lhs.data(), TYPE{0}, kChunk);    \
            benchmark::DoNotOptimize(cmp.data());                                      \
        }                                                                              \
    }                                                                                  \
    static void BM_Compare##NAME##_SIMD(benchmark::State& state) {                     \
        std::vector<int8_t> cmp_init;                                                  \
        std::vector<TYPE> lhs;                                                         \
        prepare_compare<TYPE>(cmp_init, lhs, static_cast<int>(state.range(0)));        \
        for (auto _ : state) {                                                         \
            std::vector<int8_t> cmp = cmp_init;                                        \
            compare_integral_simd<TYPE>(cmp.data(), lhs.data(), TYPE{0}, kChunk);      \
            benchmark::DoNotOptimize(cmp.data());                                      \
        }                                                                              \
    }                                                                                  \
    BENCHMARK(BM_Compare##NAME##_Scalar)->Arg(0)->Arg(50)->Arg(90)->Arg(99)->Arg(100); \
    BENCHMARK(BM_Compare##NAME##_SIMD)->Arg(0)->Arg(50)->Arg(90)->Arg(99)->Arg(100);

DEFINE_COMPARE_BENCH(int8_t, Int8)
DEFINE_COMPARE_BENCH(int32_t, Int32)
DEFINE_COMPARE_BENCH(int64_t, Int64)

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/execution_simd_bench`.
// =====================================================================
