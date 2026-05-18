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
//   * rle_simd.h      : simd_fill_int32 / simd_fill_int16 vs scalar fill
//                       simd_dict_gather_int32 vs scalar dict[indices[i]]
//                       simd_minmax_int32       vs scalar min/max loop
//                       simd_widen_int8_to_int32 vs scalar cast loop
//   * string_length_filter.h : length_eq_mask  vs scalar batched compute

#include <benchmark/benchmark.h>

#include <cstdint>
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
// simd_fill_int32 / simd_fill_int16
// =====================================================================
//
// Pre-PR: scalar for-loop fill (or no helper at all)
// Post-PR: simd_fill_int*  (AVX-512 / AVX2 / NEON)
//
// Sweep over the widths the RLE-level decoder produces.

static void BM_FillInt32_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> dst(n);
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) dst[i] = 0x12345678;
        benchmark::DoNotOptimize(dst.data());
    }
}

static void BM_FillInt32_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> dst(n);
    for (auto _ : state) {
        simd_fill_int32(dst.data(), 0x12345678, static_cast<int32_t>(n));
        benchmark::DoNotOptimize(dst.data());
    }
}

static void BM_FillInt16_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int16_t> dst(n);
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) dst[i] = static_cast<int16_t>(0x1234);
        benchmark::DoNotOptimize(dst.data());
    }
}

static void BM_FillInt16_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int16_t> dst(n);
    for (auto _ : state) {
        simd_fill_int16(dst.data(), static_cast<int16_t>(0x1234), static_cast<int32_t>(n));
        benchmark::DoNotOptimize(dst.data());
    }
}

BENCHMARK(BM_FillInt32_Scalar)->Arg(8)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_FillInt32_SIMD)->Arg(8)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_FillInt16_Scalar)->Arg(8)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_FillInt16_SIMD)->Arg(8)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

// =====================================================================
// simd_dict_gather_int32 (parquet dictionary materialisation)
// =====================================================================
//
// Pre-PR: dest[i] = dict[indices[i]]
// Post-PR: AVX-512/AVX2 gather instruction; scalar fallback.
//
// Dictionary size 1024, indices uniformly sampled.

static constexpr int32_t kDictSize = 1024;

static void prepare_gather(std::vector<int32_t>& dict, std::vector<uint32_t>& indices, std::vector<int32_t>& dest,
                           size_t n) {
    dict.resize(kDictSize);
    indices.resize(n);
    dest.resize(n);
    std::mt19937_64 rng(0xBADC0DE);
    std::uniform_int_distribution<int32_t> vd(0, 1 << 20);
    std::uniform_int_distribution<uint32_t> id(0, kDictSize - 1);
    for (auto& v : dict) v = vd(rng);
    for (auto& i : indices) i = id(rng);
}

static void BM_DictGatherInt32_Scalar(benchmark::State& state) {
    std::vector<int32_t> dict, dest;
    std::vector<uint32_t> indices;
    size_t n = static_cast<size_t>(state.range(0));
    prepare_gather(dict, indices, dest, n);
    for (auto _ : state) {
        for (size_t i = 0; i < n; ++i) dest[i] = dict[indices[i]];
        benchmark::DoNotOptimize(dest.data());
    }
}

static void BM_DictGatherInt32_SIMD(benchmark::State& state) {
    std::vector<int32_t> dict, dest;
    std::vector<uint32_t> indices;
    size_t n = static_cast<size_t>(state.range(0));
    prepare_gather(dict, indices, dest, n);
    for (auto _ : state) {
        simd_dict_gather_int32(dest.data(), dict.data(), indices.data(), static_cast<int32_t>(n));
        benchmark::DoNotOptimize(dest.data());
    }
}

BENCHMARK(BM_DictGatherInt32_Scalar)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_DictGatherInt32_SIMD)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

// =====================================================================
// simd_minmax_int32
// =====================================================================

static void BM_MinMaxInt32_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> data(n);
    std::mt19937_64 rng(0xFEEDFACE);
    std::uniform_int_distribution<int32_t> d(-1000000, 1000000);
    for (auto& v : data) v = d(rng);
    for (auto _ : state) {
        int32_t mn = INT32_MAX, mx = INT32_MIN;
        for (size_t i = 0; i < n; ++i) {
            if (data[i] < mn) mn = data[i];
            if (data[i] > mx) mx = data[i];
        }
        benchmark::DoNotOptimize(mn);
        benchmark::DoNotOptimize(mx);
    }
}

static void BM_MinMaxInt32_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> data(n);
    std::mt19937_64 rng(0xFEEDFACE);
    std::uniform_int_distribution<int32_t> d(-1000000, 1000000);
    for (auto& v : data) v = d(rng);
    for (auto _ : state) {
        int32_t mn = 0, mx = 0;
        simd_minmax_int32(data.data(), static_cast<int32_t>(n), mn, mx);
        benchmark::DoNotOptimize(mn);
        benchmark::DoNotOptimize(mx);
    }
}

BENCHMARK(BM_MinMaxInt32_Scalar)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_MinMaxInt32_SIMD)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096);

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
// length_eq_mask (parquet PLAIN binary length filter, 8 strings per call)
// =====================================================================
//
// Pre-PR equivalent: scalar 8-element batched compute.
// Post-PR: AVX2 (8 lanes) or NEON (4 lanes). The NEON bench loops twice
// per AVX2 call so total work matches.

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
        for (size_t base = 0; base + 8 <= n_strings; base += 8) {
            agg ^= scalar_length_eq_mask_avx_width(offsets.data(), base, 5);
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
        for (size_t base = 0; base + kStringLenSimdWidth <= n_strings; base += kStringLenSimdWidth) {
            agg ^= length_eq_mask(offsets.data(), base, 5);
        }
        benchmark::DoNotOptimize(agg);
    }
}

BENCHMARK(BM_LengthEqMask_Scalar)->Arg(256)->Arg(1024)->Arg(4096);
BENCHMARK(BM_LengthEqMask_SIMD)->Arg(256)->Arg(1024)->Arg(4096);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/simd_common_bench`.
// =====================================================================
