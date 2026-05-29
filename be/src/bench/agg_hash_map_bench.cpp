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

// Midi-bench comparing phmap::flat_hash_map<int16_t, AggDataPtr> vs
// SmallFixedSizeHashMap<int16_t, AggDataPtr> (a direct 65 537-cell array
// indexed by static_cast<uint16_t>(key)) as the backing of the SMALLINT
// GROUP BY wrapper AggHashMapWithOneNumberKey<TYPE_SMALLINT, ...>.
//
// Driver: production callsite build_hash_map with a production-shaped
// HashTableKeyAllocator (sizeof(int64_t) state matching COUNT(*)).  No FE,
// no scan I/O, no pipeline operator overhead.  Working-set sized so the
// Int16 column (~200 MiB at 100M rows) does not fit in L3.
//
// What is timed in each section:
//   A) build_hash_map only.  Wrapper construction is paused; the array
//      path's 524 KiB upfront zero-fill is NOT in the timed number here.
//      This isolates steady-state hash-insert cost.
//   B) Both BuildOnly and BuildWithCtor companions on a rows sweep.
//      BuildWithCtor includes wrapper construction inside the timed
//      region, so the per-iteration timing carries the 524 KiB upfront
//      cost paid at every Aggregator instantiation.  Comparing the two
//      sections shows the row count beyond which construction stops
//      dominating -- the construction-vs-amortization break-even.
//   C) Construction-only cost (regression guard, microsecond unit).
//   D) Nullable wrapper at null fractions {0%, 10%, 50%}.
//
// density=1 is included as a boundary case (single distinct value after
// partition pruning); it is the array path's best case but NOT the
// headline density we expect upstream readers to focus on.  Mid-density
// (24 hour-of-day, 366 day-of-year, 1000 region-code) is where production
// SMALLINT GROUP BY queries cluster.

#include <benchmark/benchmark.h>

#include <cmath>
#include <memory>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;

// PhmapInt16 mirrors the original phmap<int16_t, AggDataPtr> using-declaration
// verbatim so we measure the actual baseline, not a default-constructed phmap
// with different hash/seed parameters.
template <PhmapSeed seed>
using PhmapInt16 = phmap::flat_hash_map<int16_t, AggDataPtr, StdHashWithSeed<int16_t, seed>>;

using PhmapInt16Wrapper = AggHashMapWithOneNumberKey<TYPE_SMALLINT, PhmapInt16<PhmapSeed1>>;
using ArrayInt16Wrapper =
        AggHashMapWithOneNumberKey<TYPE_SMALLINT, SmallFixedSizeHashMap<int16_t, AggDataPtr, PhmapSeed1>>;

using PhmapInt16NullableWrapper = AggHashMapWithOneNullableNumberKey<TYPE_SMALLINT, PhmapInt16<PhmapSeed1>>;
using ArrayInt16NullableWrapper =
        AggHashMapWithOneNullableNumberKey<TYPE_SMALLINT, SmallFixedSizeHashMap<int16_t, AggDataPtr, PhmapSeed1>>;

enum class Distribution : int {
    Random = 0,
    Sorted = 1,
    Clustered64 = 2, // run-length 64: each value repeats 64 times before the next
    Zipf = 3,        // skewed: ~90% of rows hit ~10% of values (s=1.5)
};

// Production-shaped allocator.  Aggregator uses HashTableKeyAllocator with
// aggregate_key_size set to the COUNT aggregate state size (sizeof(int64_t));
// matching this layout keeps allocation cost realistic across the two
// hash-map backings.
struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    template <typename K>
    AggDataPtr operator()(K /*key*/) {
        return allocator->allocate();
    }
};

// Build (num_rows / kBenchChunkSize) Int16 chunks of kBenchChunkSize rows
// each.  Each chunk is a fresh Int16Column owning its own buffer, so the
// hot loop walks across distinct cache lines and the prefetcher behavior
// matches a streaming-scan scenario rather than an L1-resident replay.
class Int16ChunkStream {
public:
    Int16ChunkStream(int64_t num_rows, int distinct_count, Distribution dist) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        const int range_start = -distinct_count / 2; // straddle signed range to exercise unsigned cast

        // Pre-build a Zipf code table once if needed.
        std::vector<int> zipf_codes;
        if (dist == Distribution::Zipf) {
            zipf_codes = build_zipf_codes(distinct_count);
        }
        std::uniform_int_distribution<int> zipf_pick(0, zipf_codes.empty() ? 0 : zipf_codes.size() - 1);

        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int16Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            switch (dist) {
            case Distribution::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    data[i] = static_cast<int16_t>(range_start + uni(rng));
                }
                break;
            case Distribution::Sorted: {
                std::vector<int> vals(kBenchChunkSize);
                for (int i = 0; i < kBenchChunkSize; ++i) vals[i] = uni(rng);
                std::sort(vals.begin(), vals.end());
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    data[i] = static_cast<int16_t>(range_start + vals[i]);
                }
                break;
            }
            case Distribution::Clustered64: {
                int current = uni(rng);
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    if (i > 0 && i % 64 == 0) current = uni(rng);
                    data[i] = static_cast<int16_t>(range_start + current);
                }
                break;
            }
            case Distribution::Zipf: {
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    data[i] = static_cast<int16_t>(range_start + zipf_codes[zipf_pick(rng)]);
                }
                break;
            }
            }
            _chunks.emplace_back(std::move(col));
        }
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    // Build a 4096-entry sample table over [0, distinct_count) with
    // Zipf(s=1.5) frequencies; sample uniformly from this table to emit
    // skewed codes.  Top ~10% of distinct values get ~80-90% of mass for
    // typical OLAP dimension columns (status, country, segment).
    static std::vector<int> build_zipf_codes(int distinct_count) {
        constexpr int table_size = 4096;
        constexpr double s = 1.5;
        if (distinct_count <= 0) return {};
        // Cumulative weights w_k = 1 / k^s.
        std::vector<double> cum(distinct_count);
        double total = 0.0;
        for (int k = 0; k < distinct_count; ++k) {
            total += 1.0 / std::pow(k + 1, s);
            cum[k] = total;
        }
        std::vector<int> codes(table_size);
        std::mt19937 rng(0xBEEFCAFE);
        std::uniform_real_distribution<double> uni(0.0, total);
        for (int i = 0; i < table_size; ++i) {
            double r = uni(rng);
            int lo = 0, hi = distinct_count - 1;
            while (lo < hi) {
                int mid = (lo + hi) >> 1;
                if (cum[mid] < r)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            codes[i] = lo;
        }
        return codes;
    }

    std::vector<ColumnPtr> _chunks;
};

// Same as Int16ChunkStream but each chunk is wrapped in a NullableColumn.
// null_fraction = 0 produces nullable-column-with-has_null=false (the
// AggHashMapWithOneNullableNumberKey fast path that delegates to the
// non-nullable code), 0.1 / 0.5 produce realistic mixed-null inputs that
// route through compute_agg_through_null_data.
class NullableInt16ChunkStream {
public:
    NullableInt16ChunkStream(int64_t num_rows, int distinct_count, double null_fraction) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        std::uniform_real_distribution<double> coin(0.0, 1.0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        const int range_start = -distinct_count / 2;
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto data_col = Int16Column::create();
            auto null_col = NullColumn::create();
            auto& data = data_col->get_data();
            auto& nulls = null_col->get_data();
            data.resize(kBenchChunkSize);
            nulls.resize(kBenchChunkSize);
            bool any_null = false;
            for (int i = 0; i < kBenchChunkSize; ++i) {
                if (null_fraction > 0 && coin(rng) < null_fraction) {
                    nulls[i] = 1;
                    data[i] = 0;
                    any_null = true;
                } else {
                    nulls[i] = 0;
                    data[i] = static_cast<int16_t>(range_start + uni(rng));
                }
            }
            auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
            nullable->set_has_null(any_null);
            _chunks.emplace_back(std::move(nullable));
        }
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    std::vector<ColumnPtr> _chunks;
};

class BenchSuite {
public:
    void SetUp() {
        config::vector_chunk_size = kBenchChunkSize;
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = kBenchChunkSize;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
        _mem_pool = std::make_unique<MemPool>();
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_hash_map_bench");
        _agg_stat = std::make_unique<AggStatistics>(_runtime_profile.get());
    }

    void TearDown() {
        _agg_stat.reset();
        _runtime_profile.reset();
        _mem_pool.reset();
        _runtime_state.reset();
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<AggStatistics> _agg_stat;
};

// Memory snapshot helper: hash-table byte footprint via dump_bound()
// (uniform across phmap and SmallFixedSizeHashMap) plus mem-pool
// allocated bytes.  Reported as benchmark counters so reviewers can see
// the time/memory trade-off without re-running with a profiler.
template <typename Wrapper>
inline size_t hash_table_bytes(Wrapper* w) {
    return w->hash_map.dump_bound();
}

// ============================================================================
// Section A -- steady-state hash-insert win (build_hash_map only)
// ============================================================================
template <typename Wrapper>
static void BM_Int16BuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int16ChunkStream stream(num_rows, distinct, dist);

    int64_t total_rows = 0;
    size_t final_groups = 0;
    size_t final_ht_bytes = 0;
    size_t final_pool_bytes = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        // Heap-allocated to match production (Aggregator uses std::make_unique
        // for variant pointers).  Stack alloc would hide the 524 KiB upfront
        // cost the array path actually pays in production.
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        // End the iteration paused so wrapper / pool destruction does not
        // bleed into the next iter's measurement window.
        state.PauseTiming();
        final_groups = wrapper->hash_map.size();
        final_ht_bytes = hash_table_bytes(wrapper.get());
        final_pool_bytes = suite._mem_pool->total_allocated_bytes();
        // Observe the last-chunk AggDataPtr targets so build_hash_map
        // results survive DCE.
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = num_rows;
    state.counters["final_groups"] = final_groups;
    state.counters["hash_table_bytes"] = final_ht_bytes;
    state.counters["pool_bytes"] = final_pool_bytes;

    suite.TearDown();
}

// ============================================================================
// Section A (continued) -- build_hash_map + COUNT(*) update
// Same shape as BM_Int16BuildOnly but with a simulated COUNT(*) increment
// per resolved AggDataPtr.  Exposes the total per-row cost (hash-insert
// cost + agg-state-update cost) so the reader can see how the kernel-level
// delta translates to total agg-phase delta.
// ============================================================================
template <typename Wrapper>
static void BM_Int16BuildAndCount(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int16ChunkStream stream(num_rows, distinct, dist);

    int64_t total_rows = 0;
    size_t final_groups = 0;
    size_t final_ht_bytes = 0;
    size_t final_pool_bytes = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            // Simulated CountAggregateFunction::update per row: state is
            // sizeof(int64_t) counter, increment unconditionally.
            for (int i = 0; i < kBenchChunkSize; ++i) {
                ++(*reinterpret_cast<int64_t*>(agg_states[i]));
            }
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_groups = wrapper->hash_map.size();
        final_ht_bytes = hash_table_bytes(wrapper.get());
        final_pool_bytes = suite._mem_pool->total_allocated_bytes();
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += *reinterpret_cast<int64_t*>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = num_rows;
    state.counters["final_groups"] = final_groups;
    state.counters["hash_table_bytes"] = final_ht_bytes;
    state.counters["pool_bytes"] = final_pool_bytes;

    suite.TearDown();
}

// ============================================================================
// Section B -- amortization sweep, ctor-included companion
// Same as BM_Int16BuildOnly but wrapper construction (and the 524 KiB
// zero-fill for the array path) happens INSIDE the timed region.  This
// is the row count at which the upfront cost stops dominating.
// ============================================================================
template <typename Wrapper>
static void BM_Int16BuildWithCtor(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int16ChunkStream stream(num_rows, distinct, dist);

    int64_t total_rows = 0;
    size_t final_ht_bytes = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        // No PauseTiming before construction -- the ctor's 524 KiB
        // zero-fill (array path) or default-capacity table (phmap) is
        // included in the per-iter timing.
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_ht_bytes = hash_table_bytes(wrapper.get());
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
        state.ResumeTiming();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = num_rows;
    state.counters["hash_table_bytes"] = final_ht_bytes;

    suite.TearDown();
}

// ============================================================================
// Section C -- construction-only cost
// SmallFixedSizeHashMap<int16_t> ctor zero-fills the 524 KiB pointer table
// while phmap starts at near-zero capacity; this exposes the upfront cost
// independent of any workload.
// ============================================================================
template <typename Wrapper>
static void BM_Int16Construct(benchmark::State& state) {
    BenchSuite suite;
    suite.SetUp();
    for (auto _ : state) {
        // make_unique allocates on heap matching production -- this exposes
        // the malloc + 524 KiB memset cost the array wrapper pays at every
        // Aggregator instantiation.
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        benchmark::DoNotOptimize(wrapper.get());
        state.PauseTiming();
        wrapper.reset();
        state.ResumeTiming();
    }
    suite.TearDown();
}

// ============================================================================
// Section D -- nullable coverage
// Three null fractions exercise the three branches of compute_agg_states_nullable:
//   null_fraction == 0  -> NullableColumn with has_null=false; fast-path
//                          falls through to compute_agg_states_non_nullable.
//   null_fraction == 0.1 / 0.5 -> NullableColumn with has_null=true;
//                          routes through compute_agg_through_null_data
//                          (the per-row null-bit-aware loop).
// ============================================================================
template <typename Wrapper>
static void BM_Int16NullableBuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    // null_fraction encoded as state.range(2) * 0.01 (so 0, 10, 50 -> 0%, 10%, 50%)
    const double null_fraction = state.range(2) * 0.01;

    BenchSuite suite;
    suite.SetUp();
    NullableInt16ChunkStream stream(num_rows, distinct, null_fraction);

    int64_t total_rows = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["null_pct"] = state.range(2);

    suite.TearDown();
}

// ============================================================================
// Section E -- cx1 (compressed-key int8) vs direct array (int16) for
// SMALLINT GROUP BY when the FE-supplied value range fits in 8 bits.
//
// Today our slice_cx1 guard on TINYINT/BOOL/SMALLINT (aggregator.cpp) means
// we always pick the 65 536-cell int16 direct array, even when the range
// stats say only 8 bits are used.  stdpain raised on #73558 that for int16
// with an 8-bit range, slice_cx1 may actually win: footprint drops from
// 524 KiB to 2 KiB (L1-resident) at the cost of a bitcompress_serialize
// step per row.  This section answers that head-on.
//
// SliceCx1Wrapper is the production wrapper used when slice_cx1 routing
// fires: AggHashMapWithCompressedKeyFixedSize over Int8AggHashMap
// (SmallFixedSizeHashMap<int8_t>).  The wrapper requires bases/offsets/
// used_bits set up before build_hash_map, normally done by Aggregator's
// _build_hash_variant; we mirror that setup explicitly.
// ============================================================================
using SliceCx1Wrapper = AggHashMapWithCompressedKeyFixedSize<Int8AggHashMap<PhmapSeed1>>;

template <typename Wrapper>
static void BM_Int16Cx1BuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int16ChunkStream stream(num_rows, distinct, dist);

    // Same range Int16ChunkStream uses: keys live in
    // [-distinct/2, distinct/2).  delta = distinct - 1, used_bits is the
    // number of bits Aggregator would have computed via get_used_bits.
    const int16_t range_start = static_cast<int16_t>(-distinct / 2);
    const int16_t range_end = static_cast<int16_t>(range_start + distinct - 1);
    const uint16_t delta = static_cast<uint16_t>(static_cast<uint16_t>(range_end) - static_cast<uint16_t>(range_start));
    const int used_bits = delta == 0 ? 0 : (32 - __builtin_clz(static_cast<uint32_t>(delta)));

    int64_t total_rows = 0;
    size_t final_groups = 0;
    size_t final_ht_bytes = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        // Mirror Aggregator::_build_hash_variant's CompressKeyContext setup
        // for a single int16 column: base = min value, offset = 0,
        // used_bits = ceil_log2(range + 1).
        wrapper->bases = std::vector<std::any>{range_start};
        wrapper->offsets = std::vector<int>{0};
        wrapper->used_bits = std::vector<int>{used_bits};
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_groups = wrapper->hash_map.size();
        final_ht_bytes = wrapper->hash_map.dump_bound();
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = num_rows;
    state.counters["final_groups"] = final_groups;
    state.counters["hash_table_bytes"] = final_ht_bytes;
    state.counters["used_bits"] = used_bits;

    suite.TearDown();
}

// ============================================================================
// Args registration
// ============================================================================

// Section A: density x distribution sweep at 100M rows.
// Densities chosen for realistic SMALLINT GROUP BY shapes:
//   1     -> boundary "single distinct after pruning"; array's pathological
//            best case.  Not the headline density.
//   4     -> year bucket
//   24    -> hour-of-day (headline mid density)
//   366   -> day-of-year
//   1000  -> region-code-ish
//   10000 -> ClickHouse fixed_hash_table baseline density
//   65536 -> array's worst-case memory locality (every slot used)
static void RegisterArgs_SectionA(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 100'000'000;
    for (int distinct : {1, 4, 24, 366, 1000, 10000, 65536}) {
        for (int d = 0; d <= 3; ++d) {
            b->Args({kRows, distinct, d});
        }
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

// Section B: amortization sweep at density=24, sorted.  Shows where the
// 524 KiB upfront alloc stops dominating.  Same args used by both
// BM_Int16BuildOnly (ctor paused) and BM_Int16BuildWithCtor (ctor timed)
// so reviewers can read the two side by side.
static void RegisterArgs_SectionB(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    for (int64_t rows : {4'096LL, 100'000LL, 1'000'000LL, 10'000'000LL, 100'000'000LL}) {
        b->Args({rows, 24, /*Sorted*/ 1});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

// Section D: nullable, density=24 hour-of-day shape, sorted, three null
// fractions {0%, 10%, 50%}, smaller row count because nullable codepath is
// inherently slower and we just want to verify no regression vs baseline.
static void RegisterArgs_SectionD(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "null_pct"});
    constexpr int64_t kRows = 50'000'000;
    for (int null_pct : {0, 10, 50}) {
        b->Args({kRows, 24, null_pct});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

// Section E: cx1 vs direct for SMALLINT with 8-bit value range.  All
// distincts <= 256 so slice_cx1 is a legal routing (8 used_bits).
// Random and Sorted cover the prefetch-vs-no-prefetch axis; cx1 has
// prefetch-on under flat_hash_map but no_prefetch under
// SmallFixedSizeHashMap<int8>, so Sorted is the cleanest read.
static void RegisterArgs_SectionE(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 100'000'000;
    for (int distinct : {16, 64, 128, 200, 256}) {
        b->Args({kRows, distinct, /*Sorted*/ 1});
        b->Args({kRows, distinct, /*Random*/ 0});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

BENCHMARK_TEMPLATE(BM_Int16BuildOnly, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_Int16BuildOnly, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_Int16BuildAndCount, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_Int16BuildAndCount, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_Int16BuildOnly, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionB);
BENCHMARK_TEMPLATE(BM_Int16BuildOnly, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionB);

BENCHMARK_TEMPLATE(BM_Int16BuildWithCtor, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionB);
BENCHMARK_TEMPLATE(BM_Int16BuildWithCtor, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionB);

BENCHMARK_TEMPLATE(BM_Int16Construct, PhmapInt16Wrapper)->Unit(benchmark::kMicrosecond);
BENCHMARK_TEMPLATE(BM_Int16Construct, ArrayInt16Wrapper)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Int16NullableBuildOnly, PhmapInt16NullableWrapper)->Apply(RegisterArgs_SectionD);
BENCHMARK_TEMPLATE(BM_Int16NullableBuildOnly, ArrayInt16NullableWrapper)->Apply(RegisterArgs_SectionD);

BENCHMARK_TEMPLATE(BM_Int16BuildOnly, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionE);
BENCHMARK_TEMPLATE(BM_Int16Cx1BuildOnly, SliceCx1Wrapper)->Apply(RegisterArgs_SectionE);

} // namespace starrocks

BENCHMARK_MAIN();
