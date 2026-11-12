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
#include <cstring>
#include <numeric>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "base/simd/batch_run_counter.h"
#include "base/simd/simd.h"
#include "base/string/slice.h"

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
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * kBytesPerBlock);
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
#elif defined(__ARM_NEON) && defined(__aarch64__)
            auto* block_bytes = reinterpret_cast<uint8_t*>(block);
            const auto* mask_bytes = reinterpret_cast<const uint8_t*>(masks);
            uint32x4_t block_lo = vreinterpretq_u32_u8(vld1q_u8(block_bytes));
            uint32x4_t block_hi = vreinterpretq_u32_u8(vld1q_u8(block_bytes + 16));
            uint32x4_t mask_lo = vreinterpretq_u32_u8(vld1q_u8(mask_bytes));
            uint32x4_t mask_hi = vreinterpretq_u32_u8(vld1q_u8(mask_bytes + 16));
            vst1q_u8(block_bytes, vreinterpretq_u8_u32(vorrq_u32(block_lo, mask_lo)));
            vst1q_u8(block_bytes + 16, vreinterpretq_u8_u32(vorrq_u32(block_hi, mask_hi)));
#else
            for (int i = 0; i < kBitsSetPerBlock; ++i) block[i] |= masks[i];
#endif
        }
        benchmark::DoNotOptimize(blocks.data());
    }
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * kBytesPerBlock);
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
#elif defined(__ARM_NEON) && defined(__aarch64__)
    const auto* block_bytes = reinterpret_cast<const uint8_t*>(block);
    const auto* mask_bytes = reinterpret_cast<const uint8_t*>(masks);
    uint32x4_t block_lo = vreinterpretq_u32_u8(vld1q_u8(block_bytes));
    uint32x4_t block_hi = vreinterpretq_u32_u8(vld1q_u8(block_bytes + 16));
    uint32x4_t mask_lo = vreinterpretq_u32_u8(vld1q_u8(mask_bytes));
    uint32x4_t mask_hi = vreinterpretq_u32_u8(vld1q_u8(mask_bytes + 16));
    uint32x4_t diff = vorrq_u32(vbicq_u32(mask_lo, block_lo), vbicq_u32(mask_hi, block_hi));
    return vmaxvq_u32(diff) == 0;
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
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * kBytesPerBlock);
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
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * kBytesPerBlock);
}

BENCHMARK(BM_BloomTestHash_Scalar)->Arg(0)->Arg(4)->Arg(7)->Arg(kBitsSetPerBlock);
BENCHMARK(BM_BloomTestHash_SIMD)->Arg(0)->Arg(4)->Arg(7)->Arg(kBitsSetPerBlock);

// =====================================================================
// Full ParquetBlockSplitBloomFilter :: add_hash & test_hash (mask + block)
// =====================================================================

static constexpr uint32_t kBloomSalt[kBitsSetPerBlock] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                                                          0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

static inline void scalar_set_masks(uint32_t key, uint32_t* masks) {
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        masks[i] = key * kBloomSalt[i];
        masks[i] = masks[i] >> 27;
        masks[i] = 0x1U << masks[i];
    }
}

#if defined(__ARM_NEON) && defined(__aarch64__)
static inline void compute_neon_masks(uint32_t key, uint32x4_t* mask_lo, uint32x4_t* mask_hi) {
    static constexpr uint32_t kSaltLo[4] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU};
    static constexpr uint32_t kSaltHi[4] = {0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};
    const uint32x4_t ones = vdupq_n_u32(1);
    int32x4_t shift_lo = vreinterpretq_s32_u32(vshrq_n_u32(vmulq_n_u32(vld1q_u32(kSaltLo), key), 27));
    int32x4_t shift_hi = vreinterpretq_s32_u32(vshrq_n_u32(vmulq_n_u32(vld1q_u32(kSaltHi), key), 27));
    *mask_lo = vshlq_u32(ones, shift_lo);
    *mask_hi = vshlq_u32(ones, shift_hi);
}
#endif

static inline void parquet_bloom_add_hash_scalar(uint8_t* data, uint32_t num_blocks, uint64_t hash) {
    uint32_t block_index = static_cast<uint32_t>(((hash >> 32) * num_blocks) >> 32);
    auto key = static_cast<uint32_t>(hash);
    uint32_t masks[kBitsSetPerBlock];
    scalar_set_masks(key, masks);
    auto* block_offset = reinterpret_cast<uint32_t*>(data + kBytesPerBlock * block_index);
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        block_offset[i] |= masks[i];
    }
}

static inline void parquet_bloom_add_hash_simd(uint8_t* data, uint32_t num_blocks, uint64_t hash) {
    uint32_t block_index = static_cast<uint32_t>(((hash >> 32) * num_blocks) >> 32);
    auto key = static_cast<uint32_t>(hash);
#if defined(__ARM_NEON) && defined(__aarch64__)
    auto* block_bytes = data + kBytesPerBlock * block_index;
    uint32x4_t mask_lo, mask_hi;
    compute_neon_masks(key, &mask_lo, &mask_hi);
    uint32x4_t block_lo = vreinterpretq_u32_u8(vld1q_u8(block_bytes));
    uint32x4_t block_hi = vreinterpretq_u32_u8(vld1q_u8(block_bytes + 16));
    vst1q_u8(block_bytes, vreinterpretq_u8_u32(vorrq_u32(block_lo, mask_lo)));
    vst1q_u8(block_bytes + 16, vreinterpretq_u8_u32(vorrq_u32(block_hi, mask_hi)));
#elif defined(__AVX2__)
    uint32_t masks[kBitsSetPerBlock];
    scalar_set_masks(key, masks);
    auto* block_offset = reinterpret_cast<uint32_t*>(data + kBytesPerBlock * block_index);
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(block_offset), _mm256_or_si256(v_block, v_masks));
#else
    parquet_bloom_add_hash_scalar(data, num_blocks, hash);
#endif
}

static inline bool parquet_bloom_test_hash_scalar(const uint8_t* data, uint32_t num_blocks, uint64_t hash) {
    uint32_t block_index = static_cast<uint32_t>(((hash >> 32) * num_blocks) >> 32);
    auto key = static_cast<uint32_t>(hash);
    uint32_t masks[kBitsSetPerBlock];
    scalar_set_masks(key, masks);
    const auto* block_offset = reinterpret_cast<const uint32_t*>(data + kBytesPerBlock * block_index);
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        if ((block_offset[i] & masks[i]) == 0) {
            return false;
        }
    }
    return true;
}

static inline bool parquet_bloom_test_hash_simd(const uint8_t* data, uint32_t num_blocks, uint64_t hash) {
    uint32_t block_index = static_cast<uint32_t>(((hash >> 32) * num_blocks) >> 32);
    auto key = static_cast<uint32_t>(hash);
#if defined(__ARM_NEON) && defined(__aarch64__)
    const auto* block_bytes = data + kBytesPerBlock * block_index;
    uint32x4_t mask_lo, mask_hi;
    compute_neon_masks(key, &mask_lo, &mask_hi);
    uint32x4_t block_lo = vreinterpretq_u32_u8(vld1q_u8(block_bytes));
    uint32x4_t block_hi = vreinterpretq_u32_u8(vld1q_u8(block_bytes + 16));
    uint32x4_t diff = vorrq_u32(vbicq_u32(mask_lo, block_lo), vbicq_u32(mask_hi, block_hi));
    return vmaxvq_u32(diff) == 0;
#elif defined(__AVX2__)
    uint32_t masks[kBitsSetPerBlock];
    scalar_set_masks(key, masks);
    const auto* block_offset = reinterpret_cast<const uint32_t*>(data + kBytesPerBlock * block_index);
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    return _mm256_testc_si256(v_block, v_masks);
#else
    return parquet_bloom_test_hash_scalar(data, num_blocks, hash);
#endif
}

static void BM_ParquetBloomFilter_AddHash_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    constexpr uint32_t kNumBlocks = 1024;
    std::vector<uint8_t> filter_data(kNumBlocks * kBytesPerBlock, 0);
    std::vector<uint64_t> hashes(n);
    for (size_t i = 0; i < n; ++i) {
        hashes[i] = ((i + 1) * 0x9E3779B97F4A7C15ULL) ^ ((i + 1) << 17);
    }
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            parquet_bloom_add_hash_scalar(filter_data.data(), kNumBlocks, hashes[i]);
        }
        benchmark::DoNotOptimize(filter_data.data());
    }
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * kBytesPerBlock);
}

static void BM_ParquetBloomFilter_AddHash_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    constexpr uint32_t kNumBlocks = 1024;
    std::vector<uint8_t> filter_data(kNumBlocks * kBytesPerBlock, 0);
    std::vector<uint64_t> hashes(n);
    for (size_t i = 0; i < n; ++i) {
        hashes[i] = ((i + 1) * 0x9E3779B97F4A7C15ULL) ^ ((i + 1) << 17);
    }
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) {
            parquet_bloom_add_hash_simd(filter_data.data(), kNumBlocks, hashes[i]);
        }
        benchmark::DoNotOptimize(filter_data.data());
    }
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * kBytesPerBlock);
}

BENCHMARK(BM_ParquetBloomFilter_AddHash_Scalar)->Arg(256)->Arg(1024)->Arg(8192);
BENCHMARK(BM_ParquetBloomFilter_AddHash_SIMD)->Arg(256)->Arg(1024)->Arg(8192);

static void BM_ParquetBloomFilter_TestHash_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    constexpr uint32_t kNumBlocks = 1024;
    std::vector<uint8_t> filter_data(kNumBlocks * kBytesPerBlock, 0);
    std::vector<uint64_t> hashes(n);
    for (size_t i = 0; i < n; ++i) {
        hashes[i] = ((i + 1) * 0x9E3779B97F4A7C15ULL) ^ ((i + 1) << 17);
        parquet_bloom_add_hash_scalar(filter_data.data(), kNumBlocks, hashes[i]);
    }
    for (auto _ : state) {
        size_t hits = 0;
        for (size_t i = 0; i < n; ++i) {
            hits += parquet_bloom_test_hash_scalar(filter_data.data(), kNumBlocks, hashes[i]);
        }
        benchmark::DoNotOptimize(hits);
    }
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * kBytesPerBlock);
}

static void BM_ParquetBloomFilter_TestHash_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    constexpr uint32_t kNumBlocks = 1024;
    std::vector<uint8_t> filter_data(kNumBlocks * kBytesPerBlock, 0);
    std::vector<uint64_t> hashes(n);
    for (size_t i = 0; i < n; ++i) {
        hashes[i] = ((i + 1) * 0x9E3779B97F4A7C15ULL) ^ ((i + 1) << 17);
        parquet_bloom_add_hash_simd(filter_data.data(), kNumBlocks, hashes[i]);
    }
    for (auto _ : state) {
        size_t hits = 0;
        for (size_t i = 0; i < n; ++i) {
            hits += parquet_bloom_test_hash_simd(filter_data.data(), kNumBlocks, hashes[i]);
        }
        benchmark::DoNotOptimize(hits);
    }
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * kBytesPerBlock);
}

BENCHMARK(BM_ParquetBloomFilter_TestHash_Scalar)->Arg(256)->Arg(1024)->Arg(8192);
BENCHMARK(BM_ParquetBloomFilter_TestHash_SIMD)->Arg(256)->Arg(1024)->Arg(8192);

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
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * sizeof(level_t));
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
    state.SetItemsProcessed(state.iterations() * n);
    state.SetBytesProcessed(state.iterations() * n * sizeof(level_t));
}

BENCHMARK(BM_CountNotNull_Scalar)->Arg(0)->Arg(10)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_CountNotNull_SIMD)->Arg(0)->Arg(10)->Arg(50)->Arg(90)->Arg(100);

// =====================================================================
// BatchRunCounter<32> & BatchRunCounter<16>
// =====================================================================

class ScalarBatchRunCounter {
public:
    ScalarBatchRunCounter(const uint8_t* filter, size_t start_offset, size_t length)
            : _filter(filter), _offset(start_offset), _left(length) {}

    BatchCount next_batch() {
        if (_left == 0) {
            return BatchCount(0, 0, true);
        }
        if (_left >= 8) {
            uint64_t partial_value;
            std::memcpy(&partial_value, _filter + _offset, 8);
            _offset += 8;
            _left -= 8;
            if (partial_value == 0) {
                return BatchCount(8, 0, true);
            } else if (partial_value == 0x0101010101010101ULL) {
                return BatchCount(8, 8, true);
            } else {
                return BatchCount(8, 0, false);
            }
        }
        size_t length = _left;
        _offset += _left;
        _left = 0;
        return BatchCount(length, 0, false);
    }

private:
    const uint8_t* _filter = nullptr;
    size_t _offset = 0;
    size_t _left = 0;
};

static void prepare_filter_bytes(std::vector<uint8_t>& filter, size_t n, int selectivity_pct) {
    filter.assign(n, 0);
    if (selectivity_pct == 100) {
        std::fill(filter.begin(), filter.end(), 1);
    } else if (selectivity_pct == 50) {
        for (size_t i = 0; i < n; ++i) {
            if ((i / 64) % 3 == 0) {
                filter[i] = 0;
            } else if ((i / 64) % 3 == 1) {
                filter[i] = 1;
            } else {
                filter[i] = static_cast<uint8_t>(i & 1);
            }
        }
    }
}

template <typename CounterType>
static size_t run_batch_counter_loop(const uint8_t* filter, size_t n) {
    CounterType counter(filter, 0, n);
    size_t set_count = 0;
    size_t offset = 0;
    BatchCount bc = counter.next_batch();
    while (bc.length > 0) {
        if (bc.AllSet()) {
            set_count += bc.length;
        } else if (!bc.NoneSet()) {
            for (size_t i = 0; i < bc.length; ++i) {
                set_count += filter[offset + i];
            }
        }
        offset += bc.length;
        bc = counter.next_batch();
    }
    return set_count;
}

static void BM_BatchRunCounter32_Scalar(benchmark::State& state) {
    constexpr size_t kCount = 4096;
    int selectivity = static_cast<int>(state.range(0));
    std::vector<uint8_t> filter;
    prepare_filter_bytes(filter, kCount, selectivity);
    for (auto _ : state) {
        size_t c = run_batch_counter_loop<ScalarBatchRunCounter>(filter.data(), kCount);
        benchmark::DoNotOptimize(c);
    }
    state.SetItemsProcessed(state.iterations() * kCount);
    state.SetBytesProcessed(state.iterations() * kCount * sizeof(uint8_t));
}

static void BM_BatchRunCounter32_SIMD(benchmark::State& state) {
    constexpr size_t kCount = 4096;
    int selectivity = static_cast<int>(state.range(0));
    std::vector<uint8_t> filter;
    prepare_filter_bytes(filter, kCount, selectivity);
    for (auto _ : state) {
        size_t c = run_batch_counter_loop<BatchRunCounter<32>>(filter.data(), kCount);
        benchmark::DoNotOptimize(c);
    }
    state.SetItemsProcessed(state.iterations() * kCount);
    state.SetBytesProcessed(state.iterations() * kCount * sizeof(uint8_t));
}

static void BM_BatchRunCounter16_Scalar(benchmark::State& state) {
    constexpr size_t kCount = 4096;
    int selectivity = static_cast<int>(state.range(0));
    std::vector<uint8_t> filter;
    prepare_filter_bytes(filter, kCount, selectivity);
    for (auto _ : state) {
        size_t c = run_batch_counter_loop<ScalarBatchRunCounter>(filter.data(), kCount);
        benchmark::DoNotOptimize(c);
    }
    state.SetItemsProcessed(state.iterations() * kCount);
    state.SetBytesProcessed(state.iterations() * kCount * sizeof(uint8_t));
}

static void BM_BatchRunCounter16_SIMD(benchmark::State& state) {
    constexpr size_t kCount = 4096;
    int selectivity = static_cast<int>(state.range(0));
    std::vector<uint8_t> filter;
    prepare_filter_bytes(filter, kCount, selectivity);
    for (auto _ : state) {
        size_t c = run_batch_counter_loop<BatchRunCounter<16>>(filter.data(), kCount);
        benchmark::DoNotOptimize(c);
    }
    state.SetItemsProcessed(state.iterations() * kCount);
    state.SetBytesProcessed(state.iterations() * kCount * sizeof(uint8_t));
}

BENCHMARK(BM_BatchRunCounter32_Scalar)->Arg(0)->Arg(50)->Arg(100);
BENCHMARK(BM_BatchRunCounter32_SIMD)->Arg(0)->Arg(50)->Arg(100);
BENCHMARK(BM_BatchRunCounter16_Scalar)->Arg(0)->Arg(50)->Arg(100);
BENCHMARK(BM_BatchRunCounter16_SIMD)->Arg(0)->Arg(50)->Arg(100);

// =====================================================================
// CacheAwareDictDecoder :: assign_data_with_nulls (sparse < 10% non-null)
// =====================================================================

template <typename DataType>
static void dict_assign_sparse_nulls_scalar(size_t count, const uint8_t* nulls, const DataType* src_data,
                                            DataType* dst_data) {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
        if (!nulls[i]) {
            dst_data[i] = src_data[cnt++];
        }
    }
}

template <typename DataType>
static void dict_assign_sparse_nulls_simd(size_t count, const uint8_t* nulls, const DataType* src_data,
                                          DataType* dst_data) {
    size_t cnt = 0;
    size_t i = 0;
#ifdef __AVX2__
    for (i = 0; i + 32 <= count; i += 32) {
        __m256i loaded = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&nulls[i]));
        int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(loaded, _mm256_setzero_si256()));
        phmap::priv::BitMask<uint32_t, 32> bitmask(mask);
        for (auto idx : bitmask) {
            dst_data[i + idx] = src_data[cnt++];
        }
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    const uint8x16_t zero = vdupq_n_u8(0);
    for (i = 0; i + 16 <= count; i += 16) {
        uint8x16_t loaded = vld1q_u8(&nulls[i]);
        uint64_t mask = SIMD::get_nibble_mask(vceqq_u8(loaded, zero)) & 0x1111111111111111ULL;
        while (mask != 0) {
            uint32_t idx = __builtin_ctzll(mask) >> 2;
            dst_data[i + idx] = src_data[cnt++];
            mask &= (mask - 1);
        }
    }
#endif
    for (; i < count; ++i) {
        if (!nulls[i]) {
            dst_data[i] = src_data[cnt++];
        }
    }
}

static size_t prepare_sparse_nulls(std::vector<uint8_t>& nulls, size_t count, int non_null_pct) {
    nulls.assign(count, 1);
    std::mt19937_64 rng(0x5A17E001ULL + non_null_pct);
    size_t target_non_nulls = (count * static_cast<size_t>(non_null_pct)) / 100;
    size_t placed = 0;
    std::uniform_int_distribution<size_t> dist(0, count - 1);
    while (placed < target_non_nulls) {
        size_t pos = dist(rng);
        if (nulls[pos] != 0) {
            nulls[pos] = 0;
            ++placed;
        }
    }
    return placed;
}

static void BM_DictAssignSparseNulls_Scalar(benchmark::State& state) {
    size_t count = static_cast<size_t>(state.range(0));
    int non_null_pct = static_cast<int>(state.range(1));
    std::vector<uint8_t> nulls;
    size_t num_non_nulls = prepare_sparse_nulls(nulls, count, non_null_pct);
    std::vector<int32_t> src(num_non_nulls, 77);
    std::vector<int32_t> dst(count, 0);
    for (auto _ : state) {
        dict_assign_sparse_nulls_scalar<int32_t>(count, nulls.data(), src.data(), dst.data());
        benchmark::DoNotOptimize(dst.data());
    }
    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * count * sizeof(int32_t));
}

static void BM_DictAssignSparseNulls_SIMD(benchmark::State& state) {
    size_t count = static_cast<size_t>(state.range(0));
    int non_null_pct = static_cast<int>(state.range(1));
    std::vector<uint8_t> nulls;
    size_t num_non_nulls = prepare_sparse_nulls(nulls, count, non_null_pct);
    std::vector<int32_t> src(num_non_nulls, 77);
    std::vector<int32_t> dst(count, 0);
    for (auto _ : state) {
        dict_assign_sparse_nulls_simd<int32_t>(count, nulls.data(), src.data(), dst.data());
        benchmark::DoNotOptimize(dst.data());
    }
    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * count * sizeof(int32_t));
}

BENCHMARK(BM_DictAssignSparseNulls_Scalar)->Args({4096, 1})->Args({4096, 5})->Args({4096, 9});
BENCHMARK(BM_DictAssignSparseNulls_SIMD)->Args({4096, 1})->Args({4096, 5})->Args({4096, 9});

// =====================================================================
// FLBAPlainDecoder :: next_batch(size_t count, uint8_t* dst)
// =====================================================================

static void flba_plain_decode_scalar(const char* data, size_t type_length, size_t count, uint8_t* dst) {
    auto* slices = reinterpret_cast<Slice*>(dst);
    size_t offset = 0;
    for (size_t i = 0; i < count; ++i) {
        slices[i] = Slice(data + offset, type_length);
        offset += type_length;
    }
}

static void flba_plain_decode_simd(const char* data, size_t type_length, size_t count, uint8_t* dst) {
    auto* slices = reinterpret_cast<Slice*>(dst);
    size_t offset = 0;
    size_t i = 0;
#ifdef __AVX2__
    static_assert(sizeof(Slice) == 16);
    __m256i fixed_length = _mm256_set1_epi64x(type_length);
    __m256i inc = _mm256_set1_epi64x(type_length * 4);
    __m256i offsets = _mm256_setr_epi64x(0, type_length, type_length * 2, type_length * 3);
    __m256i cur = _mm256_set1_epi64x(reinterpret_cast<uint64_t>(data + offset));
    cur = _mm256_add_epi64(cur, offsets);
    for (; i + 4 <= count; i += 4) {
        __m256i unpacklo = _mm256_unpacklo_epi64(cur, fixed_length);
        __m256i unpackhi = _mm256_unpackhi_epi64(cur, fixed_length);
        __m256i lo = _mm256_permute2x128_si256(unpacklo, unpackhi, 0x20);
        __m256i hi = _mm256_permute2x128_si256(unpacklo, unpackhi, 0x31);

        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&slices[i]), lo);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(&slices[i + 2]), hi);

        cur = _mm256_add_epi64(cur, inc);
    }
    offset += i * type_length;
#elif defined(__ARM_NEON) && defined(__aarch64__)
    static_assert(sizeof(Slice) == 16);
    const uint64x2_t fixed_length = vdupq_n_u64(type_length);
    const uint64x2_t inc = vdupq_n_u64(type_length * 4);
    const uint64_t base_ptr = reinterpret_cast<uint64_t>(data + offset);
    uint64x2_t cur_lo = vcombine_u64(vcreate_u64(base_ptr), vcreate_u64(base_ptr + type_length));
    uint64x2_t cur_hi = vcombine_u64(vcreate_u64(base_ptr + type_length * 2), vcreate_u64(base_ptr + type_length * 3));
    for (; i + 4 <= count; i += 4) {
        uint64x2_t s0 = vzip1q_u64(cur_lo, fixed_length);
        uint64x2_t s1 = vzip2q_u64(cur_lo, fixed_length);
        uint64x2_t s2 = vzip1q_u64(cur_hi, fixed_length);
        uint64x2_t s3 = vzip2q_u64(cur_hi, fixed_length);

        uint8_t* out = dst + i * sizeof(Slice);
        vst1q_u8(out, vreinterpretq_u8_u64(s0));
        vst1q_u8(out + 16, vreinterpretq_u8_u64(s1));
        vst1q_u8(out + 32, vreinterpretq_u8_u64(s2));
        vst1q_u8(out + 48, vreinterpretq_u8_u64(s3));

        cur_lo = vaddq_u64(cur_lo, inc);
        cur_hi = vaddq_u64(cur_hi, inc);
    }
    offset += i * type_length;
#endif
    for (; i < count; ++i) {
        slices[i] = Slice(data + offset, type_length);
        offset += type_length;
    }
}

static void BM_FLBAPlainDecode_Scalar(benchmark::State& state) {
    size_t count = static_cast<size_t>(state.range(0));
    size_t type_len = static_cast<size_t>(state.range(1));
    std::vector<char> payload(count * type_len, 'x');
    std::vector<Slice> out(count);
    for (auto _ : state) {
        flba_plain_decode_scalar(payload.data(), type_len, count, reinterpret_cast<uint8_t*>(out.data()));
        benchmark::DoNotOptimize(out.data());
    }
    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * count * sizeof(Slice));
}

static void BM_FLBAPlainDecode_SIMD(benchmark::State& state) {
    size_t count = static_cast<size_t>(state.range(0));
    size_t type_len = static_cast<size_t>(state.range(1));
    std::vector<char> payload(count * type_len, 'x');
    std::vector<Slice> out(count);
    for (auto _ : state) {
        flba_plain_decode_simd(payload.data(), type_len, count, reinterpret_cast<uint8_t*>(out.data()));
        benchmark::DoNotOptimize(out.data());
    }
    state.SetItemsProcessed(state.iterations() * count);
    state.SetBytesProcessed(state.iterations() * count * sizeof(Slice));
}

BENCHMARK(BM_FLBAPlainDecode_Scalar)->Args({4096, 8})->Args({4096, 12})->Args({4096, 16});
BENCHMARK(BM_FLBAPlainDecode_SIMD)->Args({4096, 8})->Args({4096, 12})->Args({4096, 16});

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/parquet_simd_bench`.
// =====================================================================
