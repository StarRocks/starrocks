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
// Each section copies the exact pre-PR scalar loop and the exact shipped
// SIMD/adaptive loop from the corresponding aggregate header, then runs both
// over identical payloads with a sweep over the relevant data distribution
// (null-ratio, filter pass-ratio, clustering).  This lets reviewers read off
// the speedup for every regime the PR claims to accelerate -- and, equally
// important, confirm that no regime regresses.
//
// Sections:
//   * count.h  : null-count over a nullable column (count_zero vs the
//                "count += !null_data[i]" reduction in three aliasing shapes)
//   * boolor.h : update_batch (contains_nonzero_bit vs scalar early-exit)
//   * aggregate.h : agg_selective dispatch for the *_batch_selectively flat
//                   loops, incl. clustered filters that fool a prefix probe
//   * nullable_aggregate.h : 32-byte-window null-mask walk of
//                            update_batch_selectively

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <algorithm>
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
//  count.h :: counting non-null rows over a nullable column
// =====================================================================
//
// count.h sums non-null rows with `this->data(state).count += !null_data[i]`,
// where the accumulator is an int64_t reached via a reinterpret_cast from an
// AggDataPtr (uint8_t*) and `null_data` is also a uint8_t*. Three loop shapes
// are compared against SIMD::count_zero to see what the compiler actually emits:
//
//   * _Scalar            local int64_t accumulator -- no aliasing, register-promoted
//   * _ScalarMem         accumulator in memory via a plain pointer; null_data is a
//                        char type so its loads may alias the accumulator store,
//                        which blocks vectorization of the reduction
//   * _ScalarMemRestrict accumulator reached through a __restrict pointer, exactly
//                        as count.h's data(state) does -- tests whether __restrict
//                        survives the reinterpret_cast and re-enables vectorization
//
// noinline keeps each variant a standalone symbol for asm inspection. The sweep
// covers all-non-null (0%) and all-null (100%) plus midpoints.

struct CountState {
    int64_t count;
};

static inline CountState& count_state(uint8_t* __restrict place) {
    return *reinterpret_cast<CountState*>(place);
}
static inline CountState& count_state_aliased(uint8_t* place) {
    return *reinterpret_cast<CountState*>(place);
}

__attribute__((noinline)) static size_t count_nullable_scalar(const uint8_t* null_data, size_t n) {
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        count += !null_data[i];
    }
    return count;
}

__attribute__((noinline)) static void count_nullable_scalar_mem(const uint8_t* null_data, size_t n, uint8_t* state) {
    for (size_t i = 0; i < n; ++i) {
        count_state_aliased(state).count += !null_data[i];
    }
}

__attribute__((noinline)) static void count_nullable_scalar_mem_restrict(const uint8_t* null_data, size_t n,
                                                                         uint8_t* __restrict state) {
    for (size_t i = 0; i < n; ++i) {
        count_state(state).count += !null_data[i];
    }
}

__attribute__((noinline)) static size_t count_nullable_simd(const uint8_t* null_data, size_t n) {
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

static void BM_Count_Nullable_ScalarMem(benchmark::State& state) {
    std::vector<uint8_t> null_data(kChunkSize);
    fill_byte_mask(null_data.data(), null_data.size(), static_cast<int>(state.range(0)), 0xC0FFEEull);
    alignas(16) uint8_t st[sizeof(CountState)];
    for (auto _ : state) {
        count_state_aliased(st).count = 0;
        count_nullable_scalar_mem(null_data.data(), null_data.size(), st);
        benchmark::DoNotOptimize(count_state_aliased(st).count);
    }
}

static void BM_Count_Nullable_ScalarMemRestrict(benchmark::State& state) {
    std::vector<uint8_t> null_data(kChunkSize);
    fill_byte_mask(null_data.data(), null_data.size(), static_cast<int>(state.range(0)), 0xC0FFEEull);
    alignas(16) uint8_t st[sizeof(CountState)];
    for (auto _ : state) {
        count_state(st).count = 0;
        count_nullable_scalar_mem_restrict(null_data.data(), null_data.size(), st);
        benchmark::DoNotOptimize(count_state(st).count);
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
BENCHMARK(BM_Count_Nullable_ScalarMem)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(90)->Arg(99)->Arg(100);
BENCHMARK(BM_Count_Nullable_ScalarMemRestrict)->Arg(0)->Arg(1)->Arg(10)->Arg(50)->Arg(90)->Arg(99)->Arg(100);
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
//  aggregate.h :: agg_selective dispatch for the *_batch_selectively paths
// =====================================================================
//
// Pre-PR baseline:
//   for (size_t i = 0; i < chunk_size; ++i) {
//       if (filter[i] == 0) callback(i);
//   }
//
// Production dispatch (agg_selective::for_each_selected, copied verbatim as
// merge_selectively_production below):
//   * probe four 64-byte windows spread across the filter -- a contiguous
//     prefix probe misreads filters clustered by input order (sorted keys
//     open the chunk with a skipped run);
//   * sparse (<= 1/32 kept in the probe) -> find_zero/memchr skip loop with
//     an escape hatch: once the loop keeps more rows than the probe
//     promised, the remainder is finished with the branch loop;
//   * dense -> the pre-PR branch loop.
//
// merge_selectively_prefix_probe is the rejected first version of the
// dispatch (contiguous 256-byte prefix, no escape), kept so the Clustered
// benchmarks document why the production probe is stratified.
//
// Filter semantics: 0 = pass (callback invoked), 1 = skipped.
// `state.range(0)` is the percentage of rows that pass.  The XOR callback
// models the cheap inlined per-row work of trivial scalar aggregators.

template <typename Callback>
static inline void merge_selectively_scalar(const BenchFilter& filter, size_t n, Callback&& cb) {
    for (size_t i = 0; i < n; ++i) {
        if (filter[i] == 0) cb(i);
    }
}

template <typename Callback>
static inline void merge_selectively_prefix_probe(const BenchFilter& filter, size_t n, Callback&& cb) {
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

// Copy of agg_selective::probe_filter_sparse (aggregate.h).
static inline bool probe_filter_sparse(const uint8_t* filter, size_t n, size_t sparse_divisor) {
    constexpr size_t kProbeWindow = 64;
    constexpr size_t kProbeWindows = 4;
    if (n <= kProbeWindows * kProbeWindow) {
        return SIMD::count_zero(filter, n) <= n / sparse_divisor;
    }
    const size_t starts[kProbeWindows] = {0, n / 3, 2 * n / 3, n - kProbeWindow};
    size_t zeros = 0;
    for (size_t start : starts) {
        zeros += SIMD::count_zero(filter + start, kProbeWindow);
    }
    return zeros <= (kProbeWindows * kProbeWindow) / sparse_divisor;
}

// Copy of agg_selective::for_each_selected (aggregate.h).
template <typename Callback>
static inline void merge_selectively_production(const BenchFilter& filter, size_t n, Callback&& cb) {
    constexpr size_t kSparseDivisor = 32;
    if (!probe_filter_sparse(filter.data(), n, kSparseDivisor)) {
        for (size_t i = 0; i < n; ++i) {
            if (filter[i] == 0) cb(i);
        }
        return;
    }
    const size_t budget = n / kSparseDivisor + 16;
    size_t kept = 0;
    size_t idx = 0;
    while (idx < n) {
        idx = SIMD::find_zero(filter, idx, n - idx);
        if (idx >= n) return;
        cb(idx);
        ++idx;
        if (++kept > budget) break;
    }
    for (size_t i = idx; i < n; ++i) {
        if (filter[i] == 0) cb(i);
    }
}

// First `prefix` bytes are 1 (skipped), the remainder keeps keep_percent of
// rows uniformly -- the run pattern a prefix-only probe cannot see past.
static void fill_skip_prefix_then_uniform(uint8_t* data, size_t n, size_t prefix, int keep_percent, uint64_t seed) {
    const size_t p = std::min(prefix, n);
    std::fill(data, data + p, static_cast<uint8_t>(1));
    fill_byte_mask(data + p, n - p, keep_percent, seed);
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

static void BM_MergeSelectively_PrefixProbe(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_byte_mask(filter.data(), filter.size(), static_cast<int>(state.range(0)), 0xABCDEF01ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_prefix_probe(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_MergeSelectively_Production(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_byte_mask(filter.data(), filter.size(), static_cast<int>(state.range(0)), 0xABCDEF01ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_production(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_MergeSelectivelyClustered_Scalar(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_skip_prefix_then_uniform(filter.data(), filter.size(), 256, static_cast<int>(state.range(0)), 0x0DDBA11ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_scalar(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_MergeSelectivelyClustered_PrefixProbe(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_skip_prefix_then_uniform(filter.data(), filter.size(), 256, static_cast<int>(state.range(0)), 0x0DDBA11ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_prefix_probe(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_MergeSelectivelyClustered_Production(benchmark::State& state) {
    BenchFilter filter(kChunkSize);
    fill_skip_prefix_then_uniform(filter.data(), filter.size(), 256, static_cast<int>(state.range(0)), 0x0DDBA11ull);
    uint64_t acc = 0;
    for (auto _ : state) {
        merge_selectively_production(filter, filter.size(), [&](size_t i) { acc ^= i; });
        benchmark::DoNotOptimize(acc);
    }
}

BENCHMARK(BM_MergeSelectively_Scalar)->Arg(0)->Arg(1)->Arg(5)->Arg(12)->Arg(13)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_MergeSelectively_PrefixProbe)->Arg(0)->Arg(1)->Arg(5)->Arg(12)->Arg(13)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_MergeSelectively_Production)->Arg(0)->Arg(1)->Arg(5)->Arg(12)->Arg(13)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_MergeSelectivelyClustered_Scalar)->Arg(5)->Arg(50)->Arg(100);
BENCHMARK(BM_MergeSelectivelyClustered_PrefixProbe)->Arg(5)->Arg(50)->Arg(100);
BENCHMARK(BM_MergeSelectivelyClustered_Production)->Arg(5)->Arg(50)->Arg(100);

// =====================================================================
//  nullable_aggregate.h :: update_batch_selectively window walk
// =====================================================================
//
// The nullable update path classifies the null mask in 32-byte windows and
// iterates the selected rows inside each window.  Three variants:
//
//   * _Scalar     : scalar `if (!selection[i])` branch per row in each window
//   * _FindZero   : find_zero (memchr) per selected row in each window,
//                   unconditionally
//   * _Production : the shipped dispatch -- probe_filter_sparse(divisor 8)
//                   once per chunk, walk instantiated separately per side
//
// The per-window null classification is identical in all three shapes and is
// left out.  In production every selected row pays a virtual
// nested_function->update call, so the per-row work here goes through a
// base-class pointer that DoNotOptimize keeps opaque (no devirtualization).
//
// Divisor 8, not 32: the heavier per-row payload and the <= 32-byte memchr
// ranges put the scalar/find_zero crossover at ~12% selected (find_zero
// still wins at 10%, loses at 15%); the Arg sweep brackets it.
//
// The probe decision is hoisted and the walk instantiated per side because
// a runtime sparse/dense branch inside the per-window iteration degrades
// the dense-loop codegen by ~1.5x at 10-30% selected.
//
// Arg(0) is the percentage of selected rows (selection[i] == 0).

struct RowUpdater {
    virtual ~RowUpdater() = default;
    virtual void update(uint64_t& acc, size_t i) const = 0;
};

struct XorRowUpdater final : RowUpdater {
    void update(uint64_t& acc, size_t i) const override { acc ^= i; }
};

// Mirrors the AVX2 batch walk in nullable_aggregate.h: full 32-byte windows
// through the strict `<` loop, the rest through the tail call.
template <typename ForEachSelected>
static inline void walk_null_windows(size_t n, ForEachSelected&& for_each_selected) {
    constexpr size_t kWindow = 32;
    size_t offset = 0;
    while (offset + kWindow < n) {
        for_each_selected(offset, offset + kWindow);
        offset += kWindow;
    }
    if (offset < n) {
        for_each_selected(offset, n);
    }
}

__attribute__((noinline)) static void windowed_update_scalar(const BenchFilter& selection, size_t n,
                                                             const RowUpdater* fn, uint64_t& acc) {
    walk_null_windows(n, [&](size_t begin, size_t end) {
        for (size_t i = begin; i < end; ++i) {
            if (!selection[i]) {
                fn->update(acc, i);
            }
        }
    });
}

__attribute__((noinline)) static void windowed_update_find_zero(const BenchFilter& selection, size_t n,
                                                                const RowUpdater* fn, uint64_t& acc) {
    walk_null_windows(n, [&](size_t begin, size_t end) {
        size_t idx = begin;
        while (idx < end) {
            idx = SIMD::find_zero(selection, idx, end - idx);
            if (idx >= end) break;
            fn->update(acc, idx);
            ++idx;
        }
    });
}

__attribute__((noinline)) static void windowed_update_production(const BenchFilter& selection, size_t n,
                                                                 const RowUpdater* fn, uint64_t& acc) {
    if (!probe_filter_sparse(selection.data(), n, 8)) {
        walk_null_windows(n, [&](size_t begin, size_t end) {
            for (size_t i = begin; i < end; ++i) {
                if (!selection[i]) {
                    fn->update(acc, i);
                }
            }
        });
    } else {
        walk_null_windows(n, [&](size_t begin, size_t end) {
            size_t idx = begin;
            while (idx < end) {
                idx = SIMD::find_zero(selection, idx, end - idx);
                if (idx >= end) break;
                fn->update(acc, idx);
                ++idx;
            }
        });
    }
}

static void BM_WindowedUpdate_Scalar(benchmark::State& state) {
    BenchFilter selection(kChunkSize);
    fill_byte_mask(selection.data(), selection.size(), static_cast<int>(state.range(0)), 0x5EEDF00Dull);
    XorRowUpdater impl;
    const RowUpdater* fn = &impl;
    benchmark::DoNotOptimize(fn);
    uint64_t acc = 0;
    for (auto _ : state) {
        windowed_update_scalar(selection, selection.size(), fn, acc);
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_WindowedUpdate_FindZero(benchmark::State& state) {
    BenchFilter selection(kChunkSize);
    fill_byte_mask(selection.data(), selection.size(), static_cast<int>(state.range(0)), 0x5EEDF00Dull);
    XorRowUpdater impl;
    const RowUpdater* fn = &impl;
    benchmark::DoNotOptimize(fn);
    uint64_t acc = 0;
    for (auto _ : state) {
        windowed_update_find_zero(selection, selection.size(), fn, acc);
        benchmark::DoNotOptimize(acc);
    }
}

static void BM_WindowedUpdate_Production(benchmark::State& state) {
    BenchFilter selection(kChunkSize);
    fill_byte_mask(selection.data(), selection.size(), static_cast<int>(state.range(0)), 0x5EEDF00Dull);
    XorRowUpdater impl;
    const RowUpdater* fn = &impl;
    benchmark::DoNotOptimize(fn);
    uint64_t acc = 0;
    for (auto _ : state) {
        windowed_update_production(selection, selection.size(), fn, acc);
        benchmark::DoNotOptimize(acc);
    }
}

BENCHMARK(BM_WindowedUpdate_Scalar)
        ->Arg(1)
        ->Arg(3)
        ->Arg(10)
        ->Arg(15)
        ->Arg(20)
        ->Arg(25)
        ->Arg(30)
        ->Arg(40)
        ->Arg(50)
        ->Arg(90)
        ->Arg(100);
BENCHMARK(BM_WindowedUpdate_FindZero)
        ->Arg(1)
        ->Arg(3)
        ->Arg(10)
        ->Arg(15)
        ->Arg(20)
        ->Arg(25)
        ->Arg(30)
        ->Arg(40)
        ->Arg(50)
        ->Arg(90)
        ->Arg(100);
BENCHMARK(BM_WindowedUpdate_Production)
        ->Arg(1)
        ->Arg(3)
        ->Arg(10)
        ->Arg(15)
        ->Arg(20)
        ->Arg(25)
        ->Arg(30)
        ->Arg(40)
        ->Arg(50)
        ->Arg(90)
        ->Arg(100);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Results: m6i.4xlarge (Ice Lake 8375C), g++ 14.3, -O3 -mavx2 -mpopcnt,
// 4096 rows, --benchmark_repetitions=10 (means shown, CV <= 3%).
// Reviewers care about the variant ratios at each Arg, not absolute ns.
// =====================================================================
//
//   BM_Count_Nullable_Scalar/0                          779 ns
//   BM_Count_Nullable_Scalar/1                          779 ns
//   BM_Count_Nullable_Scalar/10                         779 ns
//   BM_Count_Nullable_Scalar/50                         779 ns
//   BM_Count_Nullable_Scalar/90                         779 ns
//   BM_Count_Nullable_Scalar/99                         779 ns
//   BM_Count_Nullable_Scalar/100                        779 ns
//   BM_Count_Nullable_ScalarMem/0                      1936 ns
//   BM_Count_Nullable_ScalarMem/1                      1936 ns
//   BM_Count_Nullable_ScalarMem/10                     1936 ns
//   BM_Count_Nullable_ScalarMem/50                     1936 ns
//   BM_Count_Nullable_ScalarMem/90                     1936 ns
//   BM_Count_Nullable_ScalarMem/99                     1936 ns
//   BM_Count_Nullable_ScalarMem/100                    1936 ns
//   BM_Count_Nullable_ScalarMemRestrict/0               778 ns
//   BM_Count_Nullable_ScalarMemRestrict/1               778 ns
//   BM_Count_Nullable_ScalarMemRestrict/10              778 ns
//   BM_Count_Nullable_ScalarMemRestrict/50              778 ns
//   BM_Count_Nullable_ScalarMemRestrict/90              778 ns
//   BM_Count_Nullable_ScalarMemRestrict/99              778 ns
//   BM_Count_Nullable_ScalarMemRestrict/100             778 ns
//   BM_Count_Nullable_SIMD/0                            101 ns
//   BM_Count_Nullable_SIMD/1                            101 ns
//   BM_Count_Nullable_SIMD/10                           101 ns
//   BM_Count_Nullable_SIMD/50                           101 ns
//   BM_Count_Nullable_SIMD/90                           101 ns
//   BM_Count_Nullable_SIMD/99                           101 ns
//   BM_Count_Nullable_SIMD/100                          101 ns
//   BM_BoolOr_Scalar/0                                 2052 ns
//   BM_BoolOr_Scalar/1                                  184 ns
//   BM_BoolOr_Scalar/50                               0.578 ns
//   BM_BoolOr_Scalar/100                              0.578 ns
//   BM_BoolOr_SIMD/0                                   57.3 ns
//   BM_BoolOr_SIMD/1                                   5.37 ns
//   BM_BoolOr_SIMD/50                                 0.578 ns
//   BM_BoolOr_SIMD/100                                0.578 ns
//   BM_MergeSelectively_Scalar/0                        777 ns
//   BM_MergeSelectively_Scalar/1                        776 ns
//   BM_MergeSelectively_Scalar/5                        777 ns
//   BM_MergeSelectively_Scalar/12                       777 ns
//   BM_MergeSelectively_Scalar/13                       777 ns
//   BM_MergeSelectively_Scalar/50                       777 ns
//   BM_MergeSelectively_Scalar/90                       777 ns
//   BM_MergeSelectively_Scalar/100                      777 ns
//   BM_MergeSelectively_PrefixProbe/0                  37.7 ns
//   BM_MergeSelectively_PrefixProbe/1                   298 ns
//   BM_MergeSelectively_PrefixProbe/5                   781 ns
//   BM_MergeSelectively_PrefixProbe/12                  781 ns
//   BM_MergeSelectively_PrefixProbe/13                  781 ns
//   BM_MergeSelectively_PrefixProbe/50                  781 ns
//   BM_MergeSelectively_PrefixProbe/90                  781 ns
//   BM_MergeSelectively_PrefixProbe/100                 781 ns
//   BM_MergeSelectively_Production/0                   37.2 ns
//   BM_MergeSelectively_Production/1                    296 ns
//   BM_MergeSelectively_Production/5                    782 ns
//   BM_MergeSelectively_Production/12                   782 ns
//   BM_MergeSelectively_Production/13                   782 ns
//   BM_MergeSelectively_Production/50                   782 ns
//   BM_MergeSelectively_Production/90                   782 ns
//   BM_MergeSelectively_Production/100                  782 ns
//   BM_MergeSelectivelyClustered_Scalar/5               777 ns
//   BM_MergeSelectivelyClustered_Scalar/50              777 ns
//   BM_MergeSelectivelyClustered_Scalar/100             777 ns
//   BM_MergeSelectivelyClustered_PrefixProbe/5         1284 ns
//   BM_MergeSelectivelyClustered_PrefixProbe/50       12315 ns
//   BM_MergeSelectivelyClustered_PrefixProbe/100      24321 ns
//   BM_MergeSelectivelyClustered_Production/5           782 ns
//   BM_MergeSelectivelyClustered_Production/50          782 ns
//   BM_MergeSelectivelyClustered_Production/100         782 ns
//   BM_WindowedUpdate_Scalar/1                         2766 ns
//   BM_WindowedUpdate_Scalar/3                         2723 ns
//   BM_WindowedUpdate_Scalar/10                        2592 ns
//   BM_WindowedUpdate_Scalar/15                        2651 ns
//   BM_WindowedUpdate_Scalar/20                        2731 ns
//   BM_WindowedUpdate_Scalar/25                        2819 ns
//   BM_WindowedUpdate_Scalar/30                        2931 ns
//   BM_WindowedUpdate_Scalar/40                        3350 ns
//   BM_WindowedUpdate_Scalar/50                        4367 ns
//   BM_WindowedUpdate_Scalar/90                        7388 ns
//   BM_WindowedUpdate_Scalar/100                       8468 ns
//   BM_WindowedUpdate_FindZero/1                        539 ns
//   BM_WindowedUpdate_FindZero/3                        929 ns
//   BM_WindowedUpdate_FindZero/10                      2304 ns
//   BM_WindowedUpdate_FindZero/15                      3443 ns
//   BM_WindowedUpdate_FindZero/20                      4863 ns
//   BM_WindowedUpdate_FindZero/25                      6369 ns
//   BM_WindowedUpdate_FindZero/30                      7978 ns
//   BM_WindowedUpdate_FindZero/40                     11497 ns
//   BM_WindowedUpdate_FindZero/50                     15011 ns
//   BM_WindowedUpdate_FindZero/90                     26578 ns
//   BM_WindowedUpdate_FindZero/100                    27572 ns
//   BM_WindowedUpdate_Production/1                      553 ns
//   BM_WindowedUpdate_Production/3                      965 ns
//   BM_WindowedUpdate_Production/10                    2359 ns
//   BM_WindowedUpdate_Production/15                    2819 ns
//   BM_WindowedUpdate_Production/20                    2961 ns
//   BM_WindowedUpdate_Production/25                    3123 ns
//   BM_WindowedUpdate_Production/30                    3298 ns
//   BM_WindowedUpdate_Production/40                    3671 ns
//   BM_WindowedUpdate_Production/50                    4174 ns
//   BM_WindowedUpdate_Production/90                    7800 ns
//   BM_WindowedUpdate_Production/100                   8728 ns
