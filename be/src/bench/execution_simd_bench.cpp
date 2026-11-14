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
#if defined(__AVX2__)
    constexpr size_t kBlock = 32;
    const __m256i zero = _mm256_setzero_si256();
    const __m256i rhs_vec = _mm256_set1_epi8(rhs);
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
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(cmp_vector + i), _mm256_sub_epi8(lt, gt));
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
// SIMD: 8x int32 cmpgt -> int32 +/-1/0, narrow to int8 in registers.
template <>
void compare_integral_simd<int32_t>(int8_t* cmp_vector, const int32_t* lhs, int32_t rhs, size_t n) {
    size_t i = 0;
#if defined(__AVX2__)
    constexpr size_t kBlock = 8;
    const __m256i rhs_vec = _mm256_set1_epi32(rhs);
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
        __m256i cmp32 = _mm256_sub_epi32(lt, gt);
        __m128i lo = _mm256_castsi256_si128(cmp32);
        __m128i hi = _mm256_extracti128_si256(cmp32, 1);
        __m128i p16 = _mm_packs_epi32(lo, hi);
        __m128i p8 = _mm_packs_epi16(p16, p16);
        _mm_storel_epi64(reinterpret_cast<__m128i*>(cmp_vector + i), p8);
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    constexpr size_t kBlock = 8;
    const int32x4_t rhs_s = vdupq_n_s32(rhs);
    for (; i + kBlock <= n; i += kBlock) {
        uint64_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int32_t>(lhs[j], rhs);
            }
            continue;
        }
        int32x4_t l0 = vld1q_s32(lhs + i);
        int32x4_t l1 = vld1q_s32(lhs + i + 4);
        uint32x4_t gt0 = vcgtq_s32(l0, rhs_s);
        uint32x4_t gt1 = vcgtq_s32(l1, rhs_s);
        uint32x4_t lt0 = vcgtq_s32(rhs_s, l0);
        uint32x4_t lt1 = vcgtq_s32(rhs_s, l1);
        int8x8_t gt_mask =
                vreinterpret_s8_u8(vmovn_u16(vuzp1q_u16(vreinterpretq_u16_u32(gt0), vreinterpretq_u16_u32(gt1))));
        int8x8_t lt_mask =
                vreinterpret_s8_u8(vmovn_u16(vuzp1q_u16(vreinterpretq_u16_u32(lt0), vreinterpretq_u16_u32(lt1))));
        int8x8_t cmp = vsub_s8(lt_mask, gt_mask);
        vst1_s8(cmp_vector + i, cmp);
    }
#endif
    for (; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<int32_t>(lhs[i], rhs);
    }
}

// int64 specialization: 8-lane AVX2 / 8-lane NEON in-register narrowing.
template <>
void compare_integral_simd<int64_t>(int8_t* cmp_vector, const int64_t* lhs, int64_t rhs, size_t n) {
    size_t i = 0;
#if defined(__AVX2__)
    constexpr size_t kBlock = 8;
    const __m256i perm_idx = _mm256_setr_epi32(0, 2, 4, 6, 0, 2, 4, 6);
    const __m256i rhs_vec = _mm256_set1_epi64x(rhs);
    for (; i + kBlock <= n; i += kBlock) {
        uint64_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int64_t>(lhs[j], rhs);
            }
            continue;
        }
        __m256i lhs0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + i));
        __m256i lhs1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + i + 4));
        __m256i gt0 = _mm256_cmpgt_epi64(lhs0, rhs_vec);
        __m256i lt0 = _mm256_cmpgt_epi64(rhs_vec, lhs0);
        __m256i gt1 = _mm256_cmpgt_epi64(lhs1, rhs_vec);
        __m256i lt1 = _mm256_cmpgt_epi64(rhs_vec, lhs1);
        __m256i cmp64_0 = _mm256_sub_epi64(lt0, gt0);
        __m256i cmp64_1 = _mm256_sub_epi64(lt1, gt1);
        __m128i p0 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(cmp64_0, perm_idx));
        __m128i p1 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(cmp64_1, perm_idx));
        __m128i p16 = _mm_packs_epi32(p0, p1);
        __m128i p8 = _mm_packs_epi16(p16, p16);
        _mm_storel_epi64(reinterpret_cast<__m128i*>(cmp_vector + i), p8);
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    constexpr size_t kBlock = 8;
    // vcgtq_s64 produces all-ones (0xFFFFFFFFFFFFFFFF) for true and 0 for false.
    // Narrowing to int8x8_t yields 0xFF (-1 in two's complement) for true and 0 for false.
    auto narrow_mask_64_to_8 = [](uint64x2_t m0, uint64x2_t m1, uint64x2_t m2, uint64x2_t m3) -> int8x8_t {
        uint32x4_t u01 = vuzp1q_u32(vreinterpretq_u32_u64(m0), vreinterpretq_u32_u64(m1));
        uint32x4_t u23 = vuzp1q_u32(vreinterpretq_u32_u64(m2), vreinterpretq_u32_u64(m3));
        uint16x8_t u0123 = vuzp1q_u16(vreinterpretq_u16_u32(u01), vreinterpretq_u16_u32(u23));
        return vreinterpret_s8_u8(vmovn_u16(u0123));
    };

    const int64x2_t rhs_s = vdupq_n_s64(rhs);
    for (; i + kBlock <= n; i += kBlock) {
        uint64_t block_mask = 0;
        std::memcpy(&block_mask, cmp_vector + i, sizeof(block_mask));
        if (block_mask != 0) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int64_t>(lhs[j], rhs);
            }
            continue;
        }
        int64x2_t l0 = vld1q_s64(lhs + i);
        int64x2_t l1 = vld1q_s64(lhs + i + 2);
        int64x2_t l2 = vld1q_s64(lhs + i + 4);
        int64x2_t l3 = vld1q_s64(lhs + i + 6);
        uint64x2_t gt0 = vcgtq_s64(l0, rhs_s);
        uint64x2_t gt1 = vcgtq_s64(l1, rhs_s);
        uint64x2_t gt2 = vcgtq_s64(l2, rhs_s);
        uint64x2_t gt3 = vcgtq_s64(l3, rhs_s);
        uint64x2_t lt0 = vcgtq_s64(rhs_s, l0);
        uint64x2_t lt1 = vcgtq_s64(rhs_s, l1);
        uint64x2_t lt2 = vcgtq_s64(rhs_s, l2);
        uint64x2_t lt3 = vcgtq_s64(rhs_s, l3);

        int8x8_t gt_mask = narrow_mask_64_to_8(gt0, gt1, gt2, gt3);
        int8x8_t lt_mask = narrow_mask_64_to_8(lt0, lt1, lt2, lt3);
        // True masks are -1 (0xFF) in int8_t, so lt_mask - gt_mask yields -1 (<), +1 (>), or 0 (==).
        int8x8_t cmp = vsub_s8(lt_mask, gt_mask);
        vst1_s8(cmp_vector + i, cmp);
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

#define DEFINE_COMPARE_BENCH(TYPE, NAME)                                                                \
    static void BM_Compare##NAME##_Scalar(benchmark::State& state) {                                    \
        std::vector<int8_t> cmp_init;                                                                   \
        std::vector<TYPE> lhs;                                                                          \
        prepare_compare<TYPE>(cmp_init, lhs, static_cast<int>(state.range(0)));                         \
        for (auto _ : state) {                                                                          \
            std::vector<int8_t> cmp = cmp_init;                                                         \
            compare_integral_scalar<TYPE>(cmp.data(), lhs.data(), TYPE{0}, kChunk);                     \
            benchmark::DoNotOptimize(cmp.data());                                                       \
        }                                                                                               \
    }                                                                                                   \
    static void BM_Compare##NAME##_SIMD(benchmark::State& state) {                                      \
        std::vector<int8_t> cmp_init;                                                                   \
        std::vector<TYPE> lhs;                                                                          \
        prepare_compare<TYPE>(cmp_init, lhs, static_cast<int>(state.range(0)));                         \
        for (auto _ : state) {                                                                          \
            std::vector<int8_t> cmp = cmp_init;                                                         \
            compare_integral_simd<TYPE>(cmp.data(), lhs.data(), TYPE{0}, kChunk);                       \
            benchmark::DoNotOptimize(cmp.data());                                                       \
        }                                                                                               \
    }                                                                                                   \
    BENCHMARK(BM_Compare##NAME##_Scalar)->Arg(0)->Arg(1)->Arg(12)->Arg(50)->Arg(90)->Arg(99)->Arg(100); \
    BENCHMARK(BM_Compare##NAME##_SIMD)->Arg(0)->Arg(1)->Arg(12)->Arg(50)->Arg(90)->Arg(99)->Arg(100);

DEFINE_COMPARE_BENCH(int8_t, Int8)
DEFINE_COMPARE_BENCH(int32_t, Int32)
DEFINE_COMPARE_BENCH(int64_t, Int64)

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/execution_simd_bench`.
// =====================================================================
