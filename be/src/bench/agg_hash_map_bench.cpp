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
// Sections:
//   A) Density x distribution sweep at 100M rows; both hash-only and
//      hash + simulated COUNT(*) update.
//   B) Construction-vs-amortization break-even: row count sweep at
//      density=24 sorted, exposes the cell count beyond which the
//      array's 524 KiB upfront alloc stops dominating.
//   C) Construction-only cost regression guard.
//   D) Nullable wrapper at null fractions {0%, 10%, 50%}.

#include <base/testutil/assert.h>
#include <benchmark/benchmark.h>

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
};

// Production-shaped allocator.  Aggregator uses HashTableKeyAllocator with
// aggregate_key_size set to the COUNT aggregate state size (sizeof(int64_t));
// matching this layout keeps allocation cost realistic across the two
// hash-map backings.
struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    AggDataPtr operator()(int16_t /*key*/) { return allocator->allocate(); }
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
            }
            // MutPtr -> ImmutPtr requires rvalue (ImmutPtr(const MutPtr&) is deleted).
            _chunks.emplace_back(std::move(col));
        }
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
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

// ============================================================================
// Section A — main steady-state win (hash insert only)
// ============================================================================
template <typename Wrapper>
static void BM_Int16ColdBuild(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int16ChunkStream stream(num_rows, distinct, dist);

    int64_t total_rows = 0;
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

        // End the iteration paused so wrapper / pool destruction does not
        // bleed into the next iter's measurement window.
        state.PauseTiming();
        wrapper.reset();
        suite._mem_pool->clear();
    }
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["rows_per_iter"] = num_rows;

    suite.TearDown();
}

// ============================================================================
// Section A (continued) — hash insert + COUNT(*) update
// Same shape as BM_Int16ColdBuild but with a simulated COUNT(*) increment
// per resolved AggDataPtr.  Exposes the total per-row cost (hash-insert
// cost + agg-state-update cost) so the reader can see how the kernel-level
// delta translates to total agg-phase delta.
// ============================================================================
template <typename Wrapper>
static void BM_Int16ColdBuildAndCount(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int16ChunkStream stream(num_rows, distinct, dist);

    int64_t total_rows = 0;
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

        state.PauseTiming();
        wrapper.reset();
        suite._mem_pool->clear();
    }
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["rows_per_iter"] = num_rows;

    suite.TearDown();
}

// ============================================================================
// Section C — construction-only cost
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
// Section D — nullable coverage
// Three null fractions exercise the three branches of compute_agg_states_nullable:
//   null_fraction == 0  -> NullableColumn with has_null=false; fast-path
//                          falls through to compute_agg_states_non_nullable.
//   null_fraction == 0.1 / 0.5 -> NullableColumn with has_null=true;
//                          routes through compute_agg_through_null_data
//                          (the per-row null-bit-aware loop).
// ============================================================================
template <typename Wrapper>
static void BM_Int16NullableColdBuild(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    // null_fraction encoded as state.range(2) * 0.01 (so 0, 10, 50 -> 0%, 10%, 50%)
    const double null_fraction = state.range(2) * 0.01;

    BenchSuite suite;
    suite.SetUp();
    NullableInt16ChunkStream stream(num_rows, distinct, null_fraction);

    int64_t total_rows = 0;
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

        state.PauseTiming();
        wrapper.reset();
        suite._mem_pool->clear();
    }
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["null_pct"] = state.range(2);

    suite.TearDown();
}

// ============================================================================
// Args registration
// ============================================================================

// Section A: density x distribution sweep at 100M rows.
// Densities chosen for realistic SMALLINT GROUP BY shapes:
//   1     -> pathological "OneUniqueValue" (single bucket / single slot)
//   4     -> year bucket
//   24    -> hour-of-day
//   366   -> day-of-year
//   1000  -> region-code-ish
//   10000 -> ClickHouse fixed_hash_table baseline density
//   65536 -> array's worst-case memory locality (every slot used)
static void RegisterArgs_SectionA(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 100'000'000;
    for (int distinct : {1, 4, 24, 366, 1000, 10000, 65536}) {
        for (int d = 0; d <= 2; ++d) {
            b->Args({kRows, distinct, d});
        }
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

// Section B: construction-vs-amortization break-even sweep at density=24, sorted.
// Shows where the 524 KiB upfront alloc stops dominating.
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

BENCHMARK_TEMPLATE(BM_Int16ColdBuild, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_Int16ColdBuild, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_Int16ColdBuildAndCount, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_Int16ColdBuildAndCount, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_Int16ColdBuild, PhmapInt16Wrapper)->Apply(RegisterArgs_SectionB);
BENCHMARK_TEMPLATE(BM_Int16ColdBuild, ArrayInt16Wrapper)->Apply(RegisterArgs_SectionB);

BENCHMARK_TEMPLATE(BM_Int16Construct, PhmapInt16Wrapper)->Unit(benchmark::kMicrosecond);
BENCHMARK_TEMPLATE(BM_Int16Construct, ArrayInt16Wrapper)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_Int16NullableColdBuild, PhmapInt16NullableWrapper)->Apply(RegisterArgs_SectionD);
BENCHMARK_TEMPLATE(BM_Int16NullableColdBuild, ArrayInt16NullableWrapper)->Apply(RegisterArgs_SectionD);

} // namespace starrocks

BENCHMARK_MAIN();
