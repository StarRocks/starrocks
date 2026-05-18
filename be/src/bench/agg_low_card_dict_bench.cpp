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

// Midi-bench for the low-cardinality global-dict GROUP BY routing.  When FE
// marks a single bare-SlotRef GROUP BY as dict-encoded (TAggregationNode
// .low_card_dict_group_by_slots), BE routes through AggHashMapWithLowCard
// DictKey<SmallFixedSizeHashMap<uint8_t, AggDataPtr>> instead of
// AggHashMapWithOneNumberKey<TYPE_INT, phmap::flat_hash_map<int32_t, ...>>.
// Both wrappers accept Int32Column input; the dict wrapper narrows to
// uint8_t internally and indexes a 256-cell array directly.  ClickHouse
// parallel: low_cardinality_key8.
//
// Driver: production callsite build_hash_map with a production-shaped
// HashTableKeyAllocator (sizeof(int64_t) state matching COUNT(*)).  Density
// caps at 255 because dict codes only span [0, 255] under
// Config.low_cardinality_threshold = 255; higher densities are blocked at
// FE.  Construction is cheap (256-cell array = ~2 KiB) so Section C is a
// regression guard, not a primary signal.

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

// Baseline: generic Int32 number-key wrapper over phmap.
template <PhmapSeed seed>
using PhmapInt32 = phmap::flat_hash_map<int32_t, AggDataPtr, StdHashWithSeed<int32_t, seed>>;

using PhmapInt32Wrapper = AggHashMapWithOneNumberKey<TYPE_INT, PhmapInt32<PhmapSeed1>>;
using PhmapInt32NullableWrapper = AggHashMapWithOneNullableNumberKey<TYPE_INT, PhmapInt32<PhmapSeed1>>;

// Treatment: AggHashMapWithLowCardDictKey accepts Int32Column input and
// narrows to uint8_t internally; the hash map is a 256-cell direct array
// indexed by static_cast<uint8_t>(code).
using LowCardUInt8Map = SmallFixedSizeHashMap<uint8_t, AggDataPtr, PhmapSeed1>;
using LowCardWrapper = AggHashMapWithOneLowCardDictKey<LowCardUInt8Map>;
using LowCardNullableWrapper = AggHashMapWithOneNullableLowCardDictKey<LowCardUInt8Map>;

enum class Distribution : int {
    Random = 0,
    Sorted = 1,
    Clustered64 = 2,
};

// Production-shaped allocator -- same shape as the SMALLINT bench so the
// two PR's bench results are comparable when read side by side.
struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    template <typename KeyType>
    AggDataPtr operator()(KeyType /*key*/) {
        return allocator->allocate();
    }
};

// Generate (num_rows / kBenchChunkSize) Int32 chunks of kBenchChunkSize rows
// each, each chunk a fresh Int32Column owning its own buffer.  Values are
// FE-generated dict codes in [0, distinct_count - 1] (distinct_count <= 256).
class Int32CodeChunkStream {
public:
    Int32CodeChunkStream(int64_t num_rows, int distinct_count, Distribution dist) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int32Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            switch (dist) {
            case Distribution::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    data[i] = uni(rng);
                }
                break;
            case Distribution::Sorted: {
                std::vector<int> vals(kBenchChunkSize);
                for (int i = 0; i < kBenchChunkSize; ++i) vals[i] = uni(rng);
                std::sort(vals.begin(), vals.end());
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    data[i] = vals[i];
                }
                break;
            }
            case Distribution::Clustered64: {
                int current = uni(rng);
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    if (i > 0 && i % 64 == 0) current = uni(rng);
                    data[i] = current;
                }
                break;
            }
            }
            _chunks.emplace_back(std::move(col));
        }
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    std::vector<ColumnPtr> _chunks;
};

// Same generator wrapped in NullableColumn.
class NullableInt32CodeChunkStream {
public:
    NullableInt32CodeChunkStream(int64_t num_rows, int distinct_count, double null_fraction) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        std::uniform_real_distribution<double> coin(0.0, 1.0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto data_col = Int32Column::create();
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
                    data[i] = uni(rng);
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
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_low_card_dict_bench");
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
static void BM_LowCardColdBuild(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int32CodeChunkStream stream(num_rows, distinct, dist);

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
    state.counters["rows_per_iter"] = num_rows;

    suite.TearDown();
}

// ============================================================================
// Section A (continued) — hash insert + COUNT(*) update
// ============================================================================
template <typename Wrapper>
static void BM_LowCardColdBuildAndCount(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int32CodeChunkStream stream(num_rows, distinct, dist);

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
// LowCardUInt8Map is a 257-cell array (~2 KiB heap); phmap starts at zero
// capacity.  This section just verifies the array path does not regress
// construction cost in the small-table regime.
// ============================================================================
template <typename Wrapper>
static void BM_LowCardConstruct(benchmark::State& state) {
    BenchSuite suite;
    suite.SetUp();
    for (auto _ : state) {
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
// ============================================================================
template <typename Wrapper>
static void BM_LowCardNullableColdBuild(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const double null_fraction = state.range(2) * 0.01;

    BenchSuite suite;
    suite.SetUp();
    NullableInt32CodeChunkStream stream(num_rows, distinct, null_fraction);

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

// Section A: density caps at 255 because Config.low_cardinality_threshold
// = 255 by default; dict codes above 255 are blocked at FE in this PR (see
// PlanFragmentBuilder.visitPhysicalHashAggregate guard).
//   1   -> pathological single value
//   4   -> typical enum bucket
//   24  -> hour-of-day
//   100 -> typical low-card column (status, segment)
//   200 -> deeper realistic GROUP BY low-card range
//   255 -> threshold ceiling, array fills its full 256-cell table
static void RegisterArgs_SectionA(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 100'000'000;
    for (int distinct : {1, 4, 24, 100, 200, 255}) {
        for (int d = 0; d <= 2; ++d) {
            b->Args({kRows, distinct, d});
        }
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

// Section B: construction-vs-amortization break-even at density=24, sorted.
// Array's upfront cost is ~2 KiB so we expect break-even at very small
// row counts; this section confirms it.
static void RegisterArgs_SectionB(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    for (int64_t rows : {4'096LL, 100'000LL, 1'000'000LL, 10'000'000LL, 100'000'000LL}) {
        b->Args({rows, 24, /*Sorted*/ 1});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

// Section D: nullable at density=24, sorted, three null fractions.
static void RegisterArgs_SectionD(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "null_pct"});
    constexpr int64_t kRows = 50'000'000;
    for (int null_pct : {0, 10, 50}) {
        b->Args({kRows, 24, null_pct});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

BENCHMARK_TEMPLATE(BM_LowCardColdBuild, PhmapInt32Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_LowCardColdBuild, LowCardWrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_LowCardColdBuildAndCount, PhmapInt32Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_LowCardColdBuildAndCount, LowCardWrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_LowCardColdBuild, PhmapInt32Wrapper)->Apply(RegisterArgs_SectionB);
BENCHMARK_TEMPLATE(BM_LowCardColdBuild, LowCardWrapper)->Apply(RegisterArgs_SectionB);

BENCHMARK_TEMPLATE(BM_LowCardConstruct, PhmapInt32Wrapper)->Unit(benchmark::kMicrosecond);
BENCHMARK_TEMPLATE(BM_LowCardConstruct, LowCardWrapper)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_LowCardNullableColdBuild, PhmapInt32NullableWrapper)->Apply(RegisterArgs_SectionD);
BENCHMARK_TEMPLATE(BM_LowCardNullableColdBuild, LowCardNullableWrapper)->Apply(RegisterArgs_SectionD);

} // namespace starrocks

BENCHMARK_MAIN();
