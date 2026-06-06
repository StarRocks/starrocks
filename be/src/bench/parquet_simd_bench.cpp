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

// Microbenchmarks for the parquet SIMD paths added by PR #73288
// ([Enhancement] Apply SIMD primitives to parquet encoding/decoding pipeline).
//
// The bulk of the change is covered end-to-end by the existing
// bit_unpack_bench / parquet_dict_decode_bench / parquet_encoding_bench
// targets.  This file focuses on the SIMD paths that are *not* exercised by
// those benches: bloom-filter add/test, parquet null-bitmap counting, and
// the int32->date offset.  Each pair compares the exact pre-PR scalar loop
// against the exact post-PR SIMD path on identical inputs.

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

namespace starrocks {

// =====================================================================
// parquet_block_split_bloom_filter :: add_hash
// =====================================================================
//
// One bloom block is 32 bytes / 8 uint32 lanes; the kernel ORs 8 lanes of
// pre-computed `masks` into the block.
//
// Pre-PR:
//   for (int i = 0; i < 8; ++i) block[i] |= masks[i];
// Post-PR (AVX2):
//   v_block = load; v_masks = load; store(v_block | v_masks).
//
// state.range(0) is the number of inserts in a row -- bigger N amortises
// the load/store overhead and lets the AVX2 path show its win.

static constexpr int kBitsSetPerBlock = 8;
static constexpr int kBytesPerBlock = 32;

static void prepare_bloom(std::vector<uint8_t>& blocks, std::vector<uint32_t>& masks_stream,
                          std::vector<uint64_t>& block_indices, size_t n) {
    blocks.assign(kBytesPerBlock * 1024, 0); // 1024 blocks = 32 KiB filter
    masks_stream.resize(n * kBitsSetPerBlock);
    block_indices.resize(n);
    std::mt19937_64 rng(0xB1007F11);
    std::uniform_int_distribution<uint32_t> md(0, UINT32_MAX);
    std::uniform_int_distribution<uint64_t> bd(0, 1023);
    for (auto& v : masks_stream) v = md(rng);
    for (auto& b : block_indices) b = bd(rng);
}

static void BM_BloomAddHash_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<uint8_t> blocks;
    std::vector<uint32_t> masks_stream;
    std::vector<uint64_t> block_indices;
    prepare_bloom(blocks, masks_stream, block_indices, n);
    for (auto _ : state) {
        for (size_t k = 0; k < n; ++k) {
            auto* block = reinterpret_cast<uint32_t*>(blocks.data() + kBytesPerBlock * block_indices[k]);
            const uint32_t* masks = masks_stream.data() + k * kBitsSetPerBlock;
            for (int i = 0; i < kBitsSetPerBlock; ++i) {
                block[i] |= masks[i];
            }
        }
        benchmark::DoNotOptimize(blocks.data());
    }
}

static void BM_BloomAddHash_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<uint8_t> blocks;
    std::vector<uint32_t> masks_stream;
    std::vector<uint64_t> block_indices;
    prepare_bloom(blocks, masks_stream, block_indices, n);
    for (auto _ : state) {
        for (size_t k = 0; k < n; ++k) {
            auto* block = reinterpret_cast<uint32_t*>(blocks.data() + kBytesPerBlock * block_indices[k]);
            const uint32_t* masks = masks_stream.data() + k * kBitsSetPerBlock;
#ifdef __AVX2__
            __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
            __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(block), _mm256_or_si256(v_block, v_masks));
#else
            for (int i = 0; i < kBitsSetPerBlock; ++i) block[i] |= masks[i];
#endif
        }
        benchmark::DoNotOptimize(blocks.data());
    }
}

BENCHMARK(BM_BloomAddHash_Scalar)->Arg(256)->Arg(1024)->Arg(8192);
BENCHMARK(BM_BloomAddHash_SIMD)->Arg(256)->Arg(1024)->Arg(8192);

// =====================================================================
// parquet_block_split_bloom_filter :: test_hash
// =====================================================================
//
// Pre-PR:
//   for (int i = 0; i < 8; ++i) if ((block[i] & masks[i]) == 0) return false;
//   return true;
// Post-PR (AVX2):
//   v_and = v_block & v_masks; v_zero = cmpeq(v_and, 0);
//   return movemask(v_zero) == 0.
//
// Two regimes:  (a) hash is present everywhere (no early-exit on scalar),
// (b) hash is missing on the first lane (scalar best case).  state.range(0)
// is the index of the lane that has a zero match, or kBitsSetPerBlock for
// "all lanes match".

static bool scalar_test_hash(const uint32_t* block, const uint32_t* masks) {
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        if ((block[i] & masks[i]) == 0) return false;
    }
    return true;
}

static bool simd_test_hash(const uint32_t* block, const uint32_t* masks) {
#ifdef __AVX2__
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    __m256i v_and = _mm256_and_si256(v_block, v_masks);
    __m256i v_zero = _mm256_cmpeq_epi32(v_and, _mm256_setzero_si256());
    return _mm256_movemask_ps(_mm256_castsi256_ps(v_zero)) == 0;
#else
    return scalar_test_hash(block, masks);
#endif
}

static void BM_BloomTestHash_Scalar(benchmark::State& state) {
    alignas(32) uint32_t block[kBitsSetPerBlock];
    alignas(32) uint32_t masks[kBitsSetPerBlock];
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        block[i] = 0xFFFFFFFF;
        masks[i] = 0x1u;
    }
    int zero_lane = static_cast<int>(state.range(0));
    if (zero_lane < kBitsSetPerBlock) block[zero_lane] = 0;
    for (auto _ : state) {
        bool r = scalar_test_hash(block, masks);
        benchmark::DoNotOptimize(r);
    }
}

static void BM_BloomTestHash_SIMD(benchmark::State& state) {
    alignas(32) uint32_t block[kBitsSetPerBlock];
    alignas(32) uint32_t masks[kBitsSetPerBlock];
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        block[i] = 0xFFFFFFFF;
        masks[i] = 0x1u;
    }
    int zero_lane = static_cast<int>(state.range(0));
    if (zero_lane < kBitsSetPerBlock) block[zero_lane] = 0;
    for (auto _ : state) {
        bool r = simd_test_hash(block, masks);
        benchmark::DoNotOptimize(r);
    }
}

BENCHMARK(BM_BloomTestHash_Scalar)->Arg(0)->Arg(4)->Arg(7)->Arg(kBitsSetPerBlock);
BENCHMARK(BM_BloomTestHash_SIMD)->Arg(0)->Arg(4)->Arg(7)->Arg(kBitsSetPerBlock);

// =====================================================================
// stored_column_reader :: count_not_null
// =====================================================================
//
// level_t is int16_t.
// Pre-PR:
//   for (i) if (def_levels[i] == max_def_level) ++count;
// Post-PR:
//   AVX2: 16xint16 cmpeq + movemask + popcount/2
//   NEON: 8xint16 cmpeq + vshrq_n_u16<15> + vaddv_u16

using level_t = int16_t;

static size_t scalar_count_not_null(const level_t* def, size_t n, level_t max) {
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        if (def[i] == max) ++count;
    }
    return count;
}

static size_t simd_count_not_null(const level_t* def, size_t n, level_t max) {
    size_t count = 0;
    size_t i = 0;
#if defined(__AVX2__) && defined(__POPCNT__)
    const __m256i v_max = _mm256_set1_epi16(max);
    for (; i + 16 <= n; i += 16) {
        __m256i v_data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(def + i));
        __m256i v_cmp = _mm256_cmpeq_epi16(v_data, v_max);
        uint32_t mask = _mm256_movemask_epi8(v_cmp);
        count += __builtin_popcount(mask) >> 1;
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    const int16x8_t v_max = vdupq_n_s16(max);
    for (; i + 8 <= n; i += 8) {
        int16x8_t v_data = vld1q_s16(def + i);
        uint16x8_t v_cmp = vceqq_s16(v_data, v_max);
        uint16x8_t v_ones = vshrq_n_u16(v_cmp, 15);
        count += vaddvq_u16(v_ones);
    }
#endif
    for (; i < n; ++i) {
        if (def[i] == max) ++count;
    }
    return count;
}

static void prepare_def_levels(std::vector<level_t>& def, size_t n, int notnull_ratio_percent) {
    def.resize(n);
    std::mt19937_64 rng(0xB055ACE);
    std::uniform_int_distribution<int> d(0, 99);
    for (auto& v : def) v = (d(rng) < notnull_ratio_percent) ? 1 : 0;
}

static void BM_CountNotNull_Scalar(benchmark::State& state) {
    std::vector<level_t> def;
    size_t n = 4096;
    int ratio = static_cast<int>(state.range(0));
    prepare_def_levels(def, n, ratio);
    for (auto _ : state) {
        size_t c = scalar_count_not_null(def.data(), n, level_t{1});
        benchmark::DoNotOptimize(c);
    }
}

static void BM_CountNotNull_SIMD(benchmark::State& state) {
    std::vector<level_t> def;
    size_t n = 4096;
    int ratio = static_cast<int>(state.range(0));
    prepare_def_levels(def, n, ratio);
    for (auto _ : state) {
        size_t c = simd_count_not_null(def.data(), n, level_t{1});
        benchmark::DoNotOptimize(c);
    }
}

BENCHMARK(BM_CountNotNull_Scalar)->Arg(0)->Arg(10)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_CountNotNull_SIMD)->Arg(0)->Arg(10)->Arg(50)->Arg(90)->Arg(100);

// Note: BM_Int32ToDate_* was removed -- scalar broadcast-add is auto-
// vectorised by gcc/clang, microbench showed scalar == SIMD across all sizes.

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/parquet_simd_bench`.
// =====================================================================
