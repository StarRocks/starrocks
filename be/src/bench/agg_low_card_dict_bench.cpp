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
// HashTableKeyAllocator (sizeof(int64_t) state matching COUNT(*)).
//
// Density and code-range semantics:
//   * Config.low_cardinality_threshold caps dictionary size at <=255
//     entries; the FE guard rejects marking a slot when threshold > 255.
//     With threshold=255 a dictionary holds up to 255 entries giving
//     codes in [0, 254].  density=255 in the bench produces exactly that
//     worst-case shape (255 distinct codes 0..254 filling the array).
//   * density=256 cannot occur in production with the current guard and
//     is not benched.  If the guard is relaxed in the future, codes 0..255
//     remain a single read of the array (cell 256 is the sentinel slot
//     reserved by SmallFixedSizeHashMap, see fixed_hash_map.h).

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
    Zipf = 3, // skewed: ~90% of mass on top decile (s=1.5)
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

// Build a 4096-entry Zipf code table over [0, distinct_count) so the
// chunk-gen loop just samples a slot in this pre-built table.
static std::vector<int> build_zipf_codes(int distinct_count) {
    constexpr int table_size = 4096;
    constexpr double s = 1.5;
    if (distinct_count <= 0) return {};
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

// Generate (num_rows / kBenchChunkSize) Int32 chunks of kBenchChunkSize rows
// each, each chunk a fresh Int32Column owning its own buffer.  Values are
// FE-generated dict codes in [0, distinct_count - 1] (distinct_count <= 255).
class Int32CodeChunkStream {
public:
    Int32CodeChunkStream(int64_t num_rows, int distinct_count, Distribution dist) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);

        std::vector<int> zipf_codes;
        if (dist == Distribution::Zipf) {
            zipf_codes = build_zipf_codes(distinct_count);
        }
        std::uniform_int_distribution<int> zipf_pick(0, zipf_codes.empty() ? 0 : zipf_codes.size() - 1);

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
            case Distribution::Zipf: {
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    data[i] = zipf_codes[zipf_pick(rng)];
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

template <typename Wrapper>
inline size_t hash_table_bytes(Wrapper* w) {
    return w->hash_map.dump_bound();
}

// ============================================================================
// Section A -- main steady-state win (hash insert only)
// ============================================================================
template <typename Wrapper>
static void BM_LowCardBuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int32CodeChunkStream stream(num_rows, distinct, dist);

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
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_groups = wrapper->hash_map.size();
        final_ht_bytes = hash_table_bytes(wrapper.get());
        final_pool_bytes = suite._mem_pool->total_allocated_bytes();
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
// Section A (continued) -- hash insert + COUNT(*) update
// ============================================================================
template <typename Wrapper>
static void BM_LowCardBuildAndCount(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    Int32CodeChunkStream stream(num_rows, distinct, dist);

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
// Section C -- construction-only cost
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
// Section D -- nullable coverage
// ============================================================================
template <typename Wrapper>
static void BM_LowCardNullableBuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const double null_fraction = state.range(2) * 0.01;

    BenchSuite suite;
    suite.SetUp();
    NullableInt32CodeChunkStream stream(num_rows, distinct, null_fraction);

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
// Args registration
// ============================================================================

// Section A: density caps at 255.  Codes are 0..(distinct-1); at
// distinct=255 codes 0..254 fill the array short by one sentinel slot
// (cell 255 reserved by SmallFixedSizeHashMap).
//   1   -> pathological single value (boundary, not headline)
//   4   -> typical enum bucket
//   24  -> hour-of-day
//   100 -> typical low-card column (status, segment)
//   200 -> deeper realistic GROUP BY low-card range
//   255 -> threshold ceiling, worst-case fill of the array
static void RegisterArgs_SectionA(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 100'000'000;
    for (int distinct : {1, 4, 24, 100, 200, 255}) {
        for (int d = 0; d <= 3; ++d) {
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

BENCHMARK_TEMPLATE(BM_LowCardBuildOnly, PhmapInt32Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_LowCardBuildOnly, LowCardWrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_LowCardBuildAndCount, PhmapInt32Wrapper)->Apply(RegisterArgs_SectionA);
BENCHMARK_TEMPLATE(BM_LowCardBuildAndCount, LowCardWrapper)->Apply(RegisterArgs_SectionA);

BENCHMARK_TEMPLATE(BM_LowCardBuildOnly, PhmapInt32Wrapper)->Apply(RegisterArgs_SectionB);
BENCHMARK_TEMPLATE(BM_LowCardBuildOnly, LowCardWrapper)->Apply(RegisterArgs_SectionB);

BENCHMARK_TEMPLATE(BM_LowCardConstruct, PhmapInt32Wrapper)->Unit(benchmark::kMicrosecond);
BENCHMARK_TEMPLATE(BM_LowCardConstruct, LowCardWrapper)->Unit(benchmark::kMicrosecond);

BENCHMARK_TEMPLATE(BM_LowCardNullableBuildOnly, PhmapInt32NullableWrapper)->Apply(RegisterArgs_SectionD);
BENCHMARK_TEMPLATE(BM_LowCardNullableBuildOnly, LowCardNullableWrapper)->Apply(RegisterArgs_SectionD);

} // namespace starrocks

BENCHMARK_MAIN();
