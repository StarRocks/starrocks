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

// Microbenchmarks for the predicate-evaluation SIMD paths added by PR #73291
// ([Enhancement] Add SIMD optimizations for predicate evaluation).
//
// Most paths in this PR (selection &= is_null, selection |= is_null,
// selection &= ~is_null in min_max_predicate / column_null_predicate /
// runtime_filter) are the same AND / OR / ANDN bytewise primitives already
// benched by column_ops_simd_bench in PR #73289.  The path *unique* to this
// PR is binary_predicate.cpp's EvalCmpZero kernel: it turns the int8
// per-row compare result (-1 / 0 / +1) into a boolean column according to
// op = EQ / NE / GT / GE / LT / LE.  That is the kernel benched here.

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

namespace starrocks {

enum class CmpOp { EQ, NE, GT, GE, LT, LE };

// Pre-PR scalar kernel: one switch per row.
static void eval_cmp_zero_scalar(const int8_t* src, uint8_t* dst, size_t n, CmpOp op) {
    switch (op) {
    case CmpOp::EQ:
        for (size_t i = 0; i < n; ++i) dst[i] = (src[i] == 0) ? 1 : 0;
        break;
    case CmpOp::NE:
        for (size_t i = 0; i < n; ++i) dst[i] = (src[i] != 0) ? 1 : 0;
        break;
    case CmpOp::GT:
        for (size_t i = 0; i < n; ++i) dst[i] = (src[i] > 0) ? 1 : 0;
        break;
    case CmpOp::GE:
        for (size_t i = 0; i < n; ++i) dst[i] = (src[i] >= 0) ? 1 : 0;
        break;
    case CmpOp::LT:
        for (size_t i = 0; i < n; ++i) dst[i] = (src[i] < 0) ? 1 : 0;
        break;
    case CmpOp::LE:
        for (size_t i = 0; i < n; ++i) dst[i] = (src[i] <= 0) ? 1 : 0;
        break;
    }
}

// Post-PR SIMD kernel (op switched once outside the loop, then a packed
// cmpeq / cmpgt mask is converted from 0xFF to 0x01 via AND-with-1).
static void eval_cmp_zero_simd(const int8_t* src, uint8_t* dst, size_t n, CmpOp op) {
    size_t i = 0;
#ifdef __AVX2__
    const __m256i zero = _mm256_setzero_si256();
    const __m256i one = _mm256_set1_epi8(1);
    switch (op) {
    case CmpOp::EQ:
        for (; i + 32 <= n; i += 32) {
            __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
            __m256i cmp = _mm256_cmpeq_epi8(val, zero);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), _mm256_and_si256(cmp, one));
        }
        break;
    case CmpOp::NE:
        for (; i + 32 <= n; i += 32) {
            __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
            __m256i cmp = _mm256_cmpeq_epi8(val, zero);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), _mm256_andnot_si256(cmp, one));
        }
        break;
    case CmpOp::GT:
        for (; i + 32 <= n; i += 32) {
            __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
            __m256i cmp = _mm256_cmpgt_epi8(val, zero);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), _mm256_and_si256(cmp, one));
        }
        break;
    case CmpOp::GE:
        for (; i + 32 <= n; i += 32) {
            __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
            __m256i cmp = _mm256_cmpgt_epi8(zero, val); // 0 > val == val < 0
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), _mm256_andnot_si256(cmp, one));
        }
        break;
    case CmpOp::LT:
        for (; i + 32 <= n; i += 32) {
            __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
            __m256i cmp = _mm256_cmpgt_epi8(zero, val);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), _mm256_and_si256(cmp, one));
        }
        break;
    case CmpOp::LE:
        for (; i + 32 <= n; i += 32) {
            __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
            __m256i cmp = _mm256_cmpgt_epi8(val, zero);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), _mm256_andnot_si256(cmp, one));
        }
        break;
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    const int8x16_t zero = vdupq_n_s8(0);
    const uint8x16_t one = vdupq_n_u8(1);
    switch (op) {
    case CmpOp::EQ:
        for (; i + 16 <= n; i += 16) {
            int8x16_t val = vld1q_s8(src + i);
            vst1q_u8(dst + i, vandq_u8(vceqq_s8(val, zero), one));
        }
        break;
    case CmpOp::NE:
        for (; i + 16 <= n; i += 16) {
            int8x16_t val = vld1q_s8(src + i);
            vst1q_u8(dst + i, vbicq_u8(one, vceqq_s8(val, zero)));
        }
        break;
    case CmpOp::GT:
        for (; i + 16 <= n; i += 16) {
            int8x16_t val = vld1q_s8(src + i);
            vst1q_u8(dst + i, vandq_u8(vcgtq_s8(val, zero), one));
        }
        break;
    case CmpOp::GE:
        for (; i + 16 <= n; i += 16) {
            int8x16_t val = vld1q_s8(src + i);
            vst1q_u8(dst + i, vandq_u8(vcgeq_s8(val, zero), one));
        }
        break;
    case CmpOp::LT:
        for (; i + 16 <= n; i += 16) {
            int8x16_t val = vld1q_s8(src + i);
            vst1q_u8(dst + i, vandq_u8(vcltq_s8(val, zero), one));
        }
        break;
    case CmpOp::LE:
        for (; i + 16 <= n; i += 16) {
            int8x16_t val = vld1q_s8(src + i);
            vst1q_u8(dst + i, vandq_u8(vcleq_s8(val, zero), one));
        }
        break;
    }
#endif
    // Scalar tail
    for (; i < n; ++i) {
        switch (op) {
        case CmpOp::EQ:
            dst[i] = (src[i] == 0) ? 1 : 0;
            break;
        case CmpOp::NE:
            dst[i] = (src[i] != 0) ? 1 : 0;
            break;
        case CmpOp::GT:
            dst[i] = (src[i] > 0) ? 1 : 0;
            break;
        case CmpOp::GE:
            dst[i] = (src[i] >= 0) ? 1 : 0;
            break;
        case CmpOp::LT:
            dst[i] = (src[i] < 0) ? 1 : 0;
            break;
        case CmpOp::LE:
            dst[i] = (src[i] <= 0) ? 1 : 0;
            break;
        }
    }
}

constexpr size_t kChunk = 4096;

static void prepare(std::vector<int8_t>& src) {
    src.resize(kChunk);
    std::mt19937_64 rng(0xCAFE17C8);
    std::uniform_int_distribution<int> d(-1, 1);
    for (auto& v : src) v = static_cast<int8_t>(d(rng));
}

#define DEFINE_PRED_BENCH(OP)                                                \
    static void BM_CmpZero_##OP##_Scalar(benchmark::State& state) {          \
        std::vector<int8_t> src;                                             \
        std::vector<uint8_t> dst(kChunk);                                    \
        prepare(src);                                                        \
        for (auto _ : state) {                                               \
            eval_cmp_zero_scalar(src.data(), dst.data(), kChunk, CmpOp::OP); \
            benchmark::DoNotOptimize(dst.data());                            \
        }                                                                    \
    }                                                                        \
    static void BM_CmpZero_##OP##_SIMD(benchmark::State& state) {            \
        std::vector<int8_t> src;                                             \
        std::vector<uint8_t> dst(kChunk);                                    \
        prepare(src);                                                        \
        for (auto _ : state) {                                               \
            eval_cmp_zero_simd(src.data(), dst.data(), kChunk, CmpOp::OP);   \
            benchmark::DoNotOptimize(dst.data());                            \
        }                                                                    \
    }                                                                        \
    BENCHMARK(BM_CmpZero_##OP##_Scalar);                                     \
    BENCHMARK(BM_CmpZero_##OP##_SIMD);

DEFINE_PRED_BENCH(EQ)
DEFINE_PRED_BENCH(NE)
DEFINE_PRED_BENCH(GT)
DEFINE_PRED_BENCH(GE)
DEFINE_PRED_BENCH(LT)
DEFINE_PRED_BENCH(LE)

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/predicate_simd_bench`.
// =====================================================================
