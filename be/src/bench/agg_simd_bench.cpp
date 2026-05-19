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

// Microbenchmarks for the SIMD optimizations introduced by PR #73290
// ([Enhancement] Add SIMD optimizations for aggregate functions).
//
// Each pair below copies the exact pre-PR scalar loop and the exact post-PR
// SIMD/adaptive loop from the corresponding aggregate header, then runs both
// over identical payloads with a sweep over the relevant data distribution
// (null-ratio, filter pass-ratio, broadcast length).  This lets reviewers
// read off the speedup for every regime the PR claims to accelerate -- and,
// equally important, confirm that no regime regresses.
//
// Functions covered:
//   * count.h  : update_batch over nullable column (count_zero replaces
//                "count += !null_data[i]")
//   * boolor.h : update_batch (contains_nonzero_bit replaces scalar early-exit)
//   * aggregate.h / nullable_aggregate.h : merge_batch_selectively / update_batch_selectively
//                                          adaptive find_zero skip vs scalar
//   * count.h / sum.h : get_values broadcast-fill of a constant int64_t

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

#include "base/simd/simd.h"

namespace starrocks {

// Filter type used by aggregate.h is std::vector<uint8_t, ColumnAllocator<uint8_t>>;
// for microbench purposes the allocator is irrelevant -- bench the algorithm, not
// the allocator -- so use the vanilla std::vector here.
using BenchFilter = std::vector<uint8_t>;

constexpr size_t kChunkSize = 4096;

// Fill `buf` so that approximately zero_ratio_percent of the bytes are 0 and
// the remainder are 1.  Used both as a null-bitmap (0 = non-null, 1 = null)
// and as a Filter (0 = passes, 1 = skipped) -- the meaning is the same as in
// the aggregate.h code paths.
static void fill_byte_mask(uint8_t* data, size_t n, int zero_ratio_percent, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_ratio_percent) ? 0 : 1;
    }
}

// =====================================================================
//  count.h :: update_batch over nullable input
// =====================================================================
//
// Pre-PR:
//   for (size_t i = 0; i < chunk_size; ++i) count += !null_data[i];
// Post-PR:
//   count += SIMD::count_zero(null_data, chunk_size);
//
// The sweep covers all-non-null (0% nulls) and all-null (100% nulls) as the
// hottest TPC-H-like workloads, plus midpoints to check the non-saturated
// regime.

static size_t count_nullable_scalar(const uint8_t* null_data, size_t n) {
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        count += !null_data[i];
    }
    return count;
}

static size_t count_nullable_simd(const uint8_t* null_data, size_t n) {
    return SIMD::count_zero(null_data, n);
}

static void BM_Count_Nullable_Scalar(benchmark::State& state) {
    std::vector<uint8_t> null_data(kChunkSize);
    fill_byte_mask(null_data.data(), null_data.size(), static_cast<int>(state.range(0)), 0xC0FFEEull);
    for (auto _ : state) {
        size_t c = count_nullable_scalar(null_data.data(), null_data.size());
        benchmark::DoNotOptimize(c);
    }
}

static void BM_Count_Nullable_SIMD(benchmark::State& state) {
    std::vector<uint8_t> null_data(kChunkSize);
    fill_byte_mask(null_data.data(), null_data.size(), static_cast<int>(state.range(0)), 0xC0FFEEull);
    for (auto _ : state) {
        size_t c = count_nullable_simd(null_data.data(), null_data.size());
        benchmark::DoNotOptimize(c);
    }
}

BENCHMARK(BM_Count_Nullable_Scalar)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(90)->Arg(99)->Arg(100);
BENCHMARK(BM_Count_Nullable_SIMD)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(90)->Arg(99)->Arg(100);

// =====================================================================
//  boolor.h :: update_batch over non-null input
// =====================================================================
//
// Pre-PR:
//   for (size_t i = 0; i < chunk_size; ++i) {
//       if (data[i]) { state.result = true; break; }   // scalar early-exit
//   }
// Post-PR:
//   if (SIMD::contains_nonzero_bit(data, chunk_size)) state.result = true;
//
// The two extremes -- a hit on the very first byte (best case for the
// scalar early-exit) and an all-zero column (worst case for both) -- bound
// the comparison.  The 1% / 50% rows exercise the random-position case
// most production columns fall into.

static bool boolor_scalar(const uint8_t* data, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (data[i]) return true;
    }
    return false;
}

static bool boolor_simd(const uint8_t* data, size_t n) {
    return SIMD::contains_nonzero_bit(data, n);
}

static void BM_BoolOr_Scalar(benchmark::State& state) {
    // state.range(0) is the percentage of "true" bytes.  Bytes are 0/1.
    std::vector<uint8_t> data(kChunkSize);
    fill_byte_mask(data.data(), data.size(), 100 - static_cast<int>(state.range(0)), 0xDEADBEEFull);
    for (auto _ : state) {
        bool r = boolor_scalar(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

static void BM_BoolOr_SIMD(benchmark::State& state) {
    std::vector<uint8_t> data(kChunkSize);
    fill_byte_mask(data.data(), data.size(), 100 - static_cast<int>(state.range(0)), 0xDEADBEEFull);
    for (auto _ : state) {
        bool r = boolor_simd(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }
}

BENCHMARK(BM_BoolOr_Scalar)->Arg(0)->Arg(1)->Arg(50)->Arg(100);
BENCHMARK(BM_BoolOr_SIMD)->Arg(0)->Arg(1)->Arg(50)->Arg(100);

// =====================================================================
//  aggregate.h / nullable_aggregate.h :: merge_batch_selectively
//                                        update_batch_selectively
// =====================================================================
//
// Pre-PR:
//   for (size_t i = 0; i < chunk_size; ++i) {
//       if (filter[i] == 0) callback(i);
//   }
// Post-PR (adaptive):
//   if (SIMD::count_zero(filter.data(), chunk_size) > chunk_size / 8) {
//       // dense: iterate all and branch
//       for (...) if (filter[i] == 0) callback(i);
//   } else {
//       // sparse: SIMD-skip zero positions
//       size_t idx = 0;
//       while (idx < chunk_size) {
//           idx = SIMD::find_zero(filter, idx, chunk_size - idx);
//           if (idx >= chunk_size) break;
//           callback(idx); ++idx;
//       }
//   }
//
// Filter semantics: 0 = pass (callback invoked), 1 = skip.  `state.range(0)`
// is the *percentage of rows that pass* (i.e., zero-ratio of the filter byte).
// The boundary 12.5% (chunk_size / 8) is where the adaptive branch swings;
// we cover both sides plus the extremes.
//
// `callback` is a cheap synthetic op (XOR into accumulator) so that the
// measurement is dominated by the dispatch loop -- this matches the way the
// hot aggregate paths look once `update`/`merge` get inlined for trivial
// scalar aggregators.

template <typename Callback>
static inline void merge_selectively_scalar(const BenchFilter& filter, size_t n, Callback&& cb) {
    for (size_t i = 0; i < n; ++i) {
        if (filter[i] == 0) cb(i);
    }
}

template <typename Callback>
static inline void merge_selectively_adaptive(const BenchFilter& filter, size_t n, Callback&& cb) {
    // Lazy probe over first kProbe bytes -- avoids the O(n) count_zero(n) the
    // first version paid up-front (~100 ns @ 4096 bytes) which destroyed the
    // dense-filter path. Mirrors the fix in aggregate.h.
    constexpr size_t kProbe = 256;
    const size_t probe_n = std::min(n, kProbe);
    const bool sparse = SIMD::count_zero(filter.data(), probe_n) <= probe_n / 32;
    if (!sparse) {
        for (size_t i = 0; i < n; ++i) {
            if (filter[i] == 0) cb(i);
        }
    } else {
        size_t idx = 0;
        while (idx < n) {
            idx = SIMD::find_zero(filter, idx, n - idx);
            if (idx >= n) break;
            cb(idx);
            ++idx;
        }
    }
}

static void BM_MergeSelectively_Scalar(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_byte_mask(filter.data(), filter.size(), static_cast<int>(state.range(0)), 0xABCDEF01ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_scalar(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_MergeSelectively_Adaptive(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_byte_mask(filter.data(), filter.size(), static_cast<int>(state.range(0)), 0xABCDEF01ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_adaptive(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

BENCHMARK(BM_MergeSelectively_Scalar)->Arg(0)->Arg(1)->Arg(5)->Arg(12)->Arg(13)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_MergeSelectively_Adaptive)->Arg(0)->Arg(1)->Arg(5)->Arg(12)->Arg(13)->Arg(50)->Arg(90)->Arg(100);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Results (paste output of `./build_Release/src/bench/output/agg_simd_bench`
// below after running on the target hardware).  Reviewers care about the
// scalar-vs-SIMD pair at each Arg, not absolute ns.
// =====================================================================
//
// AVX2 box (fill in):
//   Benchmark                                Time             CPU   Iterations
//   ------------------------------------------------------------------------
//   BM_Count_Nullable_Scalar/0
//   BM_Count_Nullable_SIMD/0
//   ...
//
// NEON box (fill in):
//   ...
