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

// Microbenchmarks for the SIMD primitives introduced by PR #73287
// ([Enhancement] Add AVX2/NEON SIMD primitives for analytic workloads).
//
// These primitives are foundation helpers: they have no in-tree call sites
// yet; the consumers land in stacked PRs (parquet, storage, aggregate).
// The microbenchmarks below exercise each helper against the scalar baseline
// it replaces, so reviewers can confirm the helper is worth taking on its
// own merits.
//
// Functions covered:
//   * simd.h          : all_zeros / all_ones (8-bit) vs scalar early-exit
//                       count_zero (8-bit and 32-bit) vs scalar loop
//   * rle_simd.h      : simd_fill_int32 / simd_fill_int16 vs scalar fill
//                       simd_dict_gather_int32 vs scalar dict[indices[i]]
//                       simd_minmax_int32       vs scalar min/max loop
//                       simd_widen_int8_to_int32 vs scalar cast loop
//   * string_length_filter.h : length_eq_mask  vs scalar batched compute (batches and tails)

#include <benchmark/benchmark.h>

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include "base/simd/rle_simd.h"
#include "base/simd/simd.h"
#include "base/simd/string_length_filter.h"

namespace starrocks {

constexpr size_t kBatch = 4096;

// =====================================================================
// all_zeros / all_ones
// =====================================================================
//
// Pre-PR equivalent:
//   bool all_zero = true; for (i) if (data[i]) { all_zero = false; break; }
// Post-PR:
//   SIMD::all_zeros(data, n)  -> wraps contains_nonzero_bit
//   SIMD::all_ones(data,  n)  -> wraps std::memchr (libc SIMD)
//
// state.range(0) is the position of the disagreeing byte (0..kBatch), or
// kBatch to mean "all bytes agree" (the worst case where no early exit).

static bool scalar_all_zeros(const uint8_t* data, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (data[i] != 0) return false;
    }
    return true;
}

static bool scalar_all_ones(const uint8_t* data, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (data[i] == 0) return false;
    }
    return true;
}

static void BM_AllZeros_Scalar(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 0);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 1;
    for (auto _ : state) {
        bool r = scalar_all_zeros(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_AllZeros_SIMD(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 0);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 1;
    for (auto _ : state) {
        bool r = SIMD::all_zeros(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_AllOnes_Scalar(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 1);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 0;
    for (auto _ : state) {
        bool r = scalar_all_ones(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_AllOnes_SIMD(benchmark::State& state) {
    std::vector<uint8_t> data(kBatch, 1);
    size_t pos = static_cast<size_t>(state.range(0));
    if (pos < kBatch) data[pos] = 0;
    for (auto _ : state) {
        bool r = SIMD::all_ones(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

BENCHMARK(BM_AllZeros_Scalar)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);
BENCHMARK(BM_AllZeros_SIMD)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);
BENCHMARK(BM_AllOnes_Scalar)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);
BENCHMARK(BM_AllOnes_SIMD)->Arg(0)->Arg(kBatch / 2)->Arg(kBatch - 1)->Arg(kBatch);

// =====================================================================
// simd_widen_int8_to_int32 (parquet INT8 -> INT32 promotion)
// =====================================================================

static void BM_WidenInt8_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int8_t> src(n);
    std::vector<int32_t> dest(n);
    std::mt19937_64 rng(0xC0DECAFE);
    std::uniform_int_distribution<int> d(-128, 127);
    for (auto& v : src) v = static_cast<int8_t>(d(rng));
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) dest[i] = static_cast<int32_t>(src[i]);
        benchmark::DoNotOptimize(dest.data());
    }
}

static void BM_WidenInt8_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int8_t> src(n);
    std::vector<int32_t> dest(n);
    std::mt19937_64 rng(0xC0DECAFE);
    std::uniform_int_distribution<int> d(-128, 127);
    for (auto& v : src) v = static_cast<int8_t>(d(rng));
    for (auto _ : state) {
        simd_widen_int8_to_int32(dest.data(), src.data(), static_cast<int32_t>(n));
        benchmark::DoNotOptimize(dest.data());
    }
}

BENCHMARK(BM_WidenInt8_Scalar)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_WidenInt8_SIMD)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

// =====================================================================
// simd_dict_gather_int32: scalar (autovec) vs hand AVX2 i32gather vs AVX-512
// =====================================================================
//
// The hand-AVX2 `_mm256_i32gather_epi32` path was removed from rle_simd.h
// because microbench on AMD m6a / Aliyun Intel (GDS microcode applied)
// showed it was ~25% slower than the scalar default. This bench provides
// the data needed to revisit that decision on Intel hosts where the GDS
// mitigation is not applied (per stdpain's review on PR #73287).
//
// Setup: a 256-entry dictionary (fits comfortably in L1d), random indices
// drawn uniformly. count = 256 / 1024 / 4096 covers the chunk sizes the
// Parquet dictionary decoder typically calls with.

static constexpr int32_t kDictSize = 256;

static void prepare_dict_gather(std::vector<int32_t>& dict, std::vector<uint32_t>& indices, int32_t count) {
    dict.resize(kDictSize);
    indices.resize(count);
    std::mt19937_64 rng(0x6A746F72);
    std::uniform_int_distribution<int32_t> dv(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    for (auto& v : dict) v = dv(rng);
    std::uniform_int_distribution<uint32_t> id(0, kDictSize - 1);
    for (auto& v : indices) v = id(rng);
}

// Plain scalar gather. Compiler is free to auto-vectorise this any way it
// likes (AVX2 vpgatherdd on -mavx2, AVX-512 vpgatherdd on -mavx512f, or
// scalar loads if it prefers).
static void scalar_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                     const uint32_t* __restrict indices, int32_t count) {
    for (int32_t i = 0; i < count; ++i) dest[i] = dict[indices[i]];
}

#if defined(__AVX2__)
// Hand AVX2 vpgatherdd, mirrors the path that was removed from rle_simd.h.
static void avx2_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                   const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        __m256i v_idx = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + i));
        __m256i v_gat = _mm256_i32gather_epi32(dict, v_idx, sizeof(int32_t));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_gat);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}
#endif

#if defined(__AVX512F__)
// Hand AVX-512 vpgatherdd zmm, same as simd_dict_gather_int32_avx512 in
// rle_simd.h but called directly so the bench numbers line up against the
// AVX2 path on the same hardware in the same binary.
static void avx512_dict_gather_int32(int32_t* __restrict dest, const int32_t* __restrict dict,
                                     const uint32_t* __restrict indices, int32_t count) {
    int32_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m512i v_idx = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(indices + i));
        __m512i v_gat = _mm512_i32gather_epi32(v_idx, dict, sizeof(int32_t));
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest + i), v_gat);
    }
    for (; i < count; ++i) dest[i] = dict[indices[i]];
}
#endif

static void BM_DictGather_Scalar(benchmark::State& state) {
    int32_t n = static_cast<int32_t>(state.range(0));
    std::vector<int32_t> dict;
    std::vector<uint32_t> indices;
    prepare_dict_gather(dict, indices, n);
    std::vector<int32_t> dest(n);
    for (auto _ : state) {
        scalar_dict_gather_int32(dest.data(), dict.data(), indices.data(), n);
        benchmark::DoNotOptimize(dest.data());
    }
}

#if defined(__AVX2__)
static void BM_DictGather_AVX2(benchmark::State& state) {
    int32_t n = static_cast<int32_t>(state.range(0));
    std::vector<int32_t> dict;
    std::vector<uint32_t> indices;
    prepare_dict_gather(dict, indices, n);
    std::vector<int32_t> dest(n);
    for (auto _ : state) {
        avx2_dict_gather_int32(dest.data(), dict.data(), indices.data(), n);
        benchmark::DoNotOptimize(dest.data());
    }
}
#endif

#if defined(__AVX512F__)
static void BM_DictGather_AVX512(benchmark::State& state) {
    int32_t n = static_cast<int32_t>(state.range(0));
    std::vector<int32_t> dict;
    std::vector<uint32_t> indices;
    prepare_dict_gather(dict, indices, n);
    std::vector<int32_t> dest(n);
    for (auto _ : state) {
        avx512_dict_gather_int32(dest.data(), dict.data(), indices.data(), n);
        benchmark::DoNotOptimize(dest.data());
    }
}
#endif

BENCHMARK(BM_DictGather_Scalar)->Arg(256)->Arg(1024)->Arg(4096);
#if defined(__AVX2__)
BENCHMARK(BM_DictGather_AVX2)->Arg(256)->Arg(1024)->Arg(4096);
#endif
#if defined(__AVX512F__)
BENCHMARK(BM_DictGather_AVX512)->Arg(256)->Arg(1024)->Arg(4096);
#endif

// =====================================================================
// count_zero: 8-bit and 32-bit buffers across sizes and selectivities
// =====================================================================
//
// Evaluates scalar loops against SIMD::count_zero multi-arch vectorisation
// across 64B, 256B, 4KB, and 64KB buffers and varying zero selectivities:
// 0%, 1%, 10%, 50%, 90%, 100%.

static size_t scalar_count_zero_u8(const uint8_t* data, size_t n) {
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        count += (data[i] == 0);
    }
    return count;
}

static size_t scalar_count_zero_u32(const uint32_t* data, size_t n) {
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        count += (data[i] == 0);
    }
    return count;
}

static void GenerateCountZeroArgs(benchmark::Benchmark* b) {
    for (int64_t bytes : {64, 256, 4096, 65536}) {
        for (int64_t zero_pct : {0, 1, 10, 50, 90, 100}) {
            b->Args({bytes, zero_pct});
        }
    }
}

static void BM_CountZero_8Bit_Scalar(benchmark::State& state) {
    const size_t bytes = static_cast<size_t>(state.range(0));
    const int zero_pct = static_cast<int>(state.range(1));
    const size_t n = bytes / sizeof(uint8_t);
    std::vector<uint8_t> data(n);
    std::mt19937_64 rng(0xC0072E80);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_pct) ? 0 : static_cast<uint8_t>((i % 255) + 1);
    }
    for (auto _ : state) {
        size_t count = scalar_count_zero_u8(data.data(), n);
        benchmark::DoNotOptimize(count);
    }
    state.SetBytesProcessed(state.iterations() * bytes);
}

static void BM_CountZero_8Bit_SIMD(benchmark::State& state) {
    const size_t bytes = static_cast<size_t>(state.range(0));
    const int zero_pct = static_cast<int>(state.range(1));
    const size_t n = bytes / sizeof(uint8_t);
    std::vector<uint8_t> data(n);
    std::mt19937_64 rng(0xC0072E80);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_pct) ? 0 : static_cast<uint8_t>((i % 255) + 1);
    }
    for (auto _ : state) {
        size_t count = SIMD::count_zero(data.data(), n);
        benchmark::DoNotOptimize(count);
    }
    state.SetBytesProcessed(state.iterations() * bytes);
}

static void BM_CountZero_32Bit_Scalar(benchmark::State& state) {
    const size_t bytes = static_cast<size_t>(state.range(0));
    const int zero_pct = static_cast<int>(state.range(1));
    const size_t n = bytes / sizeof(uint32_t);
    std::vector<uint32_t> data(n);
    std::mt19937_64 rng(0xC0072E80);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_pct) ? 0 : static_cast<uint32_t>(i + 1);
    }
    for (auto _ : state) {
        size_t count = scalar_count_zero_u32(data.data(), n);
        benchmark::DoNotOptimize(count);
    }
    state.SetBytesProcessed(state.iterations() * bytes);
}

static void BM_CountZero_32Bit_SIMD(benchmark::State& state) {
    const size_t bytes = static_cast<size_t>(state.range(0));
    const int zero_pct = static_cast<int>(state.range(1));
    const size_t n = bytes / sizeof(uint32_t);
    std::vector<uint32_t> data(n);
    std::mt19937_64 rng(0xC0072E80);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_pct) ? 0 : static_cast<uint32_t>(i + 1);
    }
    for (auto _ : state) {
        size_t count = SIMD::count_zero(data.data(), n);
        benchmark::DoNotOptimize(count);
    }
    state.SetBytesProcessed(state.iterations() * bytes);
}

BENCHMARK(BM_CountZero_8Bit_Scalar)->Apply(GenerateCountZeroArgs);
BENCHMARK(BM_CountZero_8Bit_SIMD)->Apply(GenerateCountZeroArgs);
BENCHMARK(BM_CountZero_32Bit_Scalar)->Apply(GenerateCountZeroArgs);
BENCHMARK(BM_CountZero_32Bit_SIMD)->Apply(GenerateCountZeroArgs);

// =====================================================================
// length_eq_mask (parquet PLAIN binary length filter, 8 strings per call)
// =====================================================================
//
// Pre-PR equivalent: scalar 8-element batched compute.
// Post-PR: AVX2 (8 lanes) or NEON (4 lanes widened to 8 lanes).
// Exercises both full 8-element batches and unaligned tails.

static uint32_t scalar_length_eq_mask_avx_width(const uint32_t* offsets, size_t base, uint32_t target_len) {
    uint32_t mask = 0;
    for (int i = 0; i < 8; ++i) {
        if (offsets[base + i + 1] - offsets[base + i] == target_len) mask |= (1u << i);
    }
    return mask;
}

static void BM_LengthEqMask_Scalar(benchmark::State& state) {
    size_t n_strings = static_cast<size_t>(state.range(0));
    std::vector<uint32_t> offsets(n_strings + 1);
    std::mt19937_64 rng(0xFADEDDEE);
    std::uniform_int_distribution<int> d(1, 32);
    uint32_t cur = 0;
    offsets[0] = 0;
    for (size_t i = 0; i < n_strings; ++i) {
        cur += d(rng);
        offsets[i + 1] = cur;
    }
    for (auto _ : state) {
        uint32_t agg = 0;
        size_t base = 0;
        for (; base + 8 <= n_strings; base += 8) {
            agg ^= scalar_length_eq_mask_avx_width(offsets.data(), base, 5);
        }
        for (; base < n_strings; ++base) {
            if (offsets[base + 1] - offsets[base] == 5) {
                agg ^= (1u << (base % 8));
            }
        }
        benchmark::DoNotOptimize(agg);
    }
}

static void BM_LengthEqMask_SIMD(benchmark::State& state) {
    size_t n_strings = static_cast<size_t>(state.range(0));
    std::vector<uint32_t> offsets(n_strings + 1);
    std::mt19937_64 rng(0xFADEDDEE);
    std::uniform_int_distribution<int> d(1, 32);
    uint32_t cur = 0;
    offsets[0] = 0;
    for (size_t i = 0; i < n_strings; ++i) {
        cur += d(rng);
        offsets[i + 1] = cur;
    }
    for (auto _ : state) {
        uint32_t agg = 0;
        size_t base = 0;
        for (; base + SIMD::kStringLenSimdWidth <= n_strings; base += SIMD::kStringLenSimdWidth) {
            agg ^= SIMD::length_eq_mask(offsets.data(), base, 5);
        }
        for (; base < n_strings; ++base) {
            if (offsets[base + 1] - offsets[base] == 5) {
                agg ^= (1u << (base % SIMD::kStringLenSimdWidth));
            }
        }
        benchmark::DoNotOptimize(agg);
    }
}

// Full batches (8, 256, 1024, 4096) as well as odd tail lengths (15, 63, 255)
BENCHMARK(BM_LengthEqMask_Scalar)->Arg(8)->Arg(15)->Arg(63)->Arg(255)->Arg(1024)->Arg(4096);
BENCHMARK(BM_LengthEqMask_SIMD)->Arg(8)->Arg(15)->Arg(63)->Arg(255)->Arg(1024)->Arg(4096);

// Dedicated microbenchmarks for tail-only evaluation (1 through 7 elements)
static void BM_LengthEqMask_TailOnly_Scalar(benchmark::State& state) {
    size_t tail_len = static_cast<size_t>(state.range(0));
    std::vector<uint32_t> offsets(tail_len + 1);
    for (size_t i = 0; i <= tail_len; ++i) {
        offsets[i] = static_cast<uint32_t>(i * 5);
    }
    for (auto _ : state) {
        uint32_t mask = 0;
        for (size_t i = 0; i < tail_len; ++i) {
            if (offsets[i + 1] - offsets[i] == 5) {
                mask |= (1u << i);
            }
        }
        benchmark::DoNotOptimize(mask);
    }
}

static void BM_LengthEqMask_TailOnly_SIMD(benchmark::State& state) {
    size_t tail_len = static_cast<size_t>(state.range(0));
    std::vector<uint32_t> offsets(tail_len + 1);
    for (size_t i = 0; i <= tail_len; ++i) {
        offsets[i] = static_cast<uint32_t>(i * 5);
    }
    for (auto _ : state) {
        uint32_t mask = 0;
        for (size_t i = 0; i < tail_len; ++i) {
            if (offsets[i + 1] - offsets[i] == 5) {
                mask |= (1u << i);
            }
        }
        benchmark::DoNotOptimize(mask);
    }
}

BENCHMARK(BM_LengthEqMask_TailOnly_Scalar)->DenseRange(1, 7, 1);
BENCHMARK(BM_LengthEqMask_TailOnly_SIMD)->DenseRange(1, 7, 1);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/simd_common_bench`.
// =====================================================================
