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

// Bench for the BE config `agg_hash_map_prefetch_dist` and the
// `agg_prefetch_l2_ratio` residency gate.
//
// What this bench measures:
//   Steady-state per-row hash-probe cost on a pre-filled phmap, for a chosen
//   prefetch distance, table size and access pattern -- on BOTH the map
//   (AggHashMapWithOneNumberKey<TYPE_BIGINT>, the GROUP BY path) and the set
//   (AggHashSetOfOneNumberKey<TYPE_BIGINT>, the DISTINCT path).  The table-size
//   axis is reported as resident bucket-array bytes (capacity * slot), the same
//   quantity the runtime model keys on; detected L1/L2/L3 sizes are emitted as
//   counters so the curve can be read against the cache boundaries.
//
// Shape: the timed phase runs at 100% hit rate against a pre-filled table.  An
// earlier 50/50 hot/cold design over-stated cache locality on large random
// tables and let timed misses mutate the table mid-iteration, polluting the
// size-band reading; the split below avoids both:
//   1. Pre-fill phase (NOT timed): drive build_hash_map with `target_size`
//      distinct INT64 keys so the hash table is at the target band.
//   2. Timed phase: drive build_hash_map with chunks whose keys are all
//      drawn from [0, target_size).  Hit rate is 100% (table is fully
//      populated post-prefill); phmap's lazy_emplace short-circuits on
//      hit so no allocations or insertions happen in the timed window.
//      The table stays at `target_size` for the entire run -> the
//      table-size band reading is unpolluted by mid-bench growth.
//
// What this bench does NOT cover (call out for reviewers):
//   - Hit-rate sweep: 100% is the "steady warmed-up table" shape.
//     Lower hit rates require inserts which mutate the table size and
//     conflate the band reading.  If a follow-up wants the cold-probe
//     win specifically, run separately with target_size = 0 and let it
//     grow.
//   - Load-factor sweep: phmap chooses its own load factor; we don't
//     reserve to a specific occupancy.  If the optimal distance per
//     band proves load-factor sensitive, that's a separate bench.
//   - Hardware counters: this is wall-time only.  Use `perf stat -e
//     cache-misses,l1d-load-misses,llc-load-misses` on the binary if
//     you need cycle-level evidence.
//   - Two-level conversion: aggregator promotes a wrapper from
//     one-level to two-level at ~32 MiB (CpuInfo-derived; see
//     aggregator_fwd.h).  Sizes here cap at 8M entries (~ a few MiB
//     bucket array) so the one-level wrapper is exercised throughout.
//
// Decision rule.  Two questions, opposite ends of the size axis:
//   (1) Drop-to-0: is there a resident-bytes band (L1/L2) where distance 0
//       beats 16 by >= 5%?  If yes, the residency gate must zero out prefetch
//       there -- something the constant 16 cannot do.
//   (2) Scale-up: above L3, does the OPTIMAL distance shift monotonically
//       with band by >= 5% over 16?  If not, keep the constant for DRAM too.
// Either claim must be stable across >= 10 repetitions and both access
// patterns (run with --benchmark_repetitions=10
// --benchmark_report_aggregates_only=true).

#include <benchmark/benchmark.h>

#include <memory>
#include <mutex>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;
inline constexpr int64_t kTimedRowsPerCombo = 10'000'000;

template <PhmapSeed seed>
using PhmapInt64 = phmap::flat_hash_map<int64_t, AggDataPtr, StdHashWithSeed<int64_t, seed>>;

using PhmapInt64Wrapper = AggHashMapWithOneNumberKey<TYPE_BIGINT, PhmapInt64<PhmapSeed1>>;

template <PhmapSeed seed>
using PhmapInt64Set = phmap::flat_hash_set<int64_t, StdHashWithSeed<int64_t, seed>>;

using PhmapInt64SetWrapper = AggHashSetOfOneNumberKey<TYPE_BIGINT, PhmapInt64Set<PhmapSeed1>>;

enum class Pattern : int {
    Random = 0,     // uniform random over [0, target_size)
    Clustered64 = 1 // run-length 64 over the same range (consecutive-keys shape)
};

struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    AggDataPtr operator()(int64_t /*key*/) { return allocator->allocate(); }
};

class Int64ChunkStream {
public:
    Int64ChunkStream(int64_t num_rows, int64_t key_universe_size, Pattern p, uint64_t seed)
            : _num_rows(num_rows), _universe(key_universe_size) {
        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<int64_t> uni(0, _universe > 0 ? _universe - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int64Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            switch (p) {
            case Pattern::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) data[i] = uni(rng);
                break;
            case Pattern::Clustered64: {
                int64_t current = uni(rng);
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

    // For the pre-fill phase we want target_size *distinct* keys to populate the
    // table.  Permuted sequential keys cover the universe exactly once across
    // (universe + chunk_size - 1) / chunk_size chunks.
    static std::vector<ColumnPtr> build_distinct_keys(int64_t universe_size) {
        std::vector<int64_t> all(universe_size);
        for (int64_t i = 0; i < universe_size; ++i) all[i] = i;
        std::mt19937_64 rng(0xC0FFEEC0FFEEC0FFULL);
        std::shuffle(all.begin(), all.end(), rng);
        std::vector<ColumnPtr> chunks;
        const int64_t num_chunks = (universe_size + kBenchChunkSize - 1) / kBenchChunkSize;
        chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int64Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            for (int i = 0; i < kBenchChunkSize; ++i) {
                const int64_t idx = c * kBenchChunkSize + i;
                data[i] = idx < universe_size ? all[idx] : all[idx % universe_size];
            }
            chunks.emplace_back(std::move(col));
        }
        return chunks;
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    int64_t _num_rows;
    int64_t _universe;
    std::vector<ColumnPtr> _chunks;
};

class BenchSuite {
public:
    void SetUp() {
        config::vector_chunk_size = kBenchChunkSize;
        // CpuInfo must be initialized before the agg prefetch gate reads L2 size.
        static std::once_flag cpu_once;
        std::call_once(cpu_once, [] { CpuInfo::init(); });
        // Disable the resident<L2 prefetch gate so the bench drives the prefetch
        // distance directly (the gate would otherwise force noprefetch below L2).
        // ratio=0 -> threshold 0 -> prefetch path always taken; distance still
        // fully controls behavior (0 = skipped via the macro guard).
        config::agg_prefetch_l2_ratio = 0;
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = kBenchChunkSize;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
        _mem_pool = std::make_unique<MemPool>();
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_prefetch_bench");
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

// Resident footprint of the bucket array = what actually competes for cache, and
// the exact quantity the runtime gate keys on (capacity * slot bytes).  This is
// NOT dump_bound() (that is the serialize bound) -- report both and read the
// curve against this one.
template <typename Table>
inline size_t resident_bucket_bytes(const Table& t) {
    return t.capacity() * (sizeof(typename Table::value_type) + 1 /* control byte */);
}

// Detected cache sizes are mandatory context: the resident-bytes axis is only
// meaningful relative to where the L1/L2/L3 boundaries fall on this host.
inline void attach_cpu_cache_counters(benchmark::State& state) {
    static std::once_flag once;
    std::call_once(once, [] { CpuInfo::init(); });
    const auto& sizes = CpuInfo::get_cache_sizes();
    const auto at = [&](CpuInfo::CacheLevel lvl) -> double {
        return sizes.size() > static_cast<size_t>(lvl) ? static_cast<double>(sizes[lvl]) : 0.0;
    };
    state.counters["l1_bytes"] = at(CpuInfo::L1_CACHE);
    state.counters["l2_bytes"] = at(CpuInfo::L2_CACHE);
    state.counters["l3_bytes"] = at(CpuInfo::L3_CACHE);
}

// ============================================================================
// Main bench: probe a pre-filled phmap at the given prefetch distance.
// ============================================================================
static void BM_PrefetchProbe(benchmark::State& state) {
    const int32_t distance = static_cast<int32_t>(state.range(0));
    const int64_t target_size = state.range(1);
    const Pattern pattern = static_cast<Pattern>(state.range(2));

    // Knob under test: the function in agg_hash_set.h reads this at chunk
    // start, captures into __prefetch_dist for the loop body.
    config::agg_hash_map_prefetch_dist = distance;

    BenchSuite suite;
    suite.SetUp();

    auto wrapper = std::make_unique<PhmapInt64Wrapper>(kBenchChunkSize, suite._agg_stat.get());
    HashTableKeyAllocator allocator;
    allocator.aggregate_key_size = sizeof(int64_t);
    allocator.pool = suite._mem_pool.get();
    BenchAllocateState alloc{&allocator};
    Buffer<AggDataPtr> agg_states(kBenchChunkSize);

    // 1. Pre-fill phase (NOT timed): one chunk per kBenchChunkSize distinct
    //    keys until the table holds `target_size` entries.
    const auto prefill_chunks = Int64ChunkStream::build_distinct_keys(target_size);
    for (const auto& chunk_col : prefill_chunks) {
        Columns key_columns;
        key_columns.emplace_back(chunk_col);
        wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
    }
    const size_t prefilled = wrapper->hash_map.size();

    // 2. Build the timed chunk stream once outside the loop so column-buffer
    //    construction cost is paid once.
    Int64ChunkStream timed_stream(kTimedRowsPerCombo, target_size, pattern, 0xBEEFCAFE);

    // Touch the buffer once to make sure column pages are resident; otherwise
    // first iteration pays page-fault cost that confounds the distance read.
    for (const auto& chunk_col : timed_stream.chunks()) {
        const auto* col = down_cast<const Int64Column*>(chunk_col.get());
        benchmark::DoNotOptimize(col->immutable_data().data());
    }

    int64_t total_rows = 0;
    for (auto _ : state) {
        for (const auto& chunk_col : timed_stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();
    }

    // Verify the table didn't grow (100% hit rate invariant).
    const size_t post_size = wrapper->hash_map.size();

    state.counters["distance"] = distance;
    state.counters["target_size"] = target_size;
    state.counters["pattern"] = static_cast<int>(pattern);
    state.counters["prefilled"] = prefilled;
    state.counters["post_size"] = post_size;
    state.counters["hash_table_bytes"] = hash_table_bytes(wrapper.get());
    state.counters["resident_bytes"] = resident_bucket_bytes(wrapper->hash_map);
    state.counters["bucket_count"] = wrapper->hash_map.capacity();
    state.counters["rows_per_iter"] = kTimedRowsPerCombo;
    attach_cpu_cache_counters(state);
    state.SetItemsProcessed(total_rows);

    wrapper.reset();
    suite.TearDown();
}

// ============================================================================
// Set variant: DISTINCT path.  Cleaner residency story than the map -- the slot
// array IS the whole footprint, no separate agg-state arena.  Shares the same
// resident<L2 prefetch gate as the map (disabled here via agg_prefetch_l2_ratio=0
// so the raw distance curve is what gets measured, not the gate).
// ============================================================================
static void BM_PrefetchProbeSet(benchmark::State& state) {
    const int32_t distance = static_cast<int32_t>(state.range(0));
    const int64_t target_size = state.range(1);
    const Pattern pattern = static_cast<Pattern>(state.range(2));

    config::agg_hash_map_prefetch_dist = distance;

    BenchSuite suite;
    suite.SetUp();

    auto wrapper = std::make_unique<PhmapInt64SetWrapper>(kBenchChunkSize, suite._agg_stat.get());

    // 1. Pre-fill phase (NOT timed): target_size distinct keys.
    const auto prefill_chunks = Int64ChunkStream::build_distinct_keys(target_size);
    for (const auto& chunk_col : prefill_chunks) {
        Columns key_columns;
        key_columns.emplace_back(chunk_col);
        wrapper->build_hash_set(kBenchChunkSize, key_columns, suite._mem_pool.get());
    }
    const size_t prefilled = wrapper->hash_set.size();

    Int64ChunkStream timed_stream(kTimedRowsPerCombo, target_size, pattern, 0xBEEFCAFE);
    for (const auto& chunk_col : timed_stream.chunks()) {
        const auto* col = down_cast<const Int64Column*>(chunk_col.get());
        benchmark::DoNotOptimize(col->immutable_data().data());
    }

    int64_t total_rows = 0;
    for (auto _ : state) {
        for (const auto& chunk_col : timed_stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_set(kBenchChunkSize, key_columns, suite._mem_pool.get());
            total_rows += kBenchChunkSize;
        }
        benchmark::ClobberMemory();
    }

    const size_t post_size = wrapper->hash_set.size();

    state.counters["distance"] = distance;
    state.counters["target_size"] = target_size;
    state.counters["pattern"] = static_cast<int>(pattern);
    state.counters["prefilled"] = prefilled;
    state.counters["post_size"] = post_size;
    state.counters["resident_bytes"] = resident_bucket_bytes(wrapper->hash_set);
    state.counters["bucket_count"] = wrapper->hash_set.capacity();
    state.counters["rows_per_iter"] = kTimedRowsPerCombo;
    attach_cpu_cache_counters(state);
    state.SetItemsProcessed(total_rows);

    wrapper.reset();
    suite.TearDown();
}

// ============================================================================
// Argument matrix
// ============================================================================
// Distances: 0 (no SW prefetch via guard) + 4 / 8 / 16 / 24 / 32 sweep.
// Sizes (entries): 1Ki .. 8Mi.  The SMALL end is the point of this revision:
//   it spans the L1/L2-resident regime where the drop-to-0 question lives.
//   For the map there is no bucket_count gate, so a few-Ki-entry table runs
//   prefetch=16 today even though it is L1/L2-resident; for the set the gate
//   kicks in at ~8192 buckets, so the small sizes also locate where that gate
//   boundary actually falls.  8Mi caps below the ~32 MiB two-level threshold so
//   the one-level wrapper is exercised throughout; 16Mi+ omitted (never
//   one-level in prod).
// Patterns: Random + Clustered64.  Sorted-by-key is excluded because INT
//   sorted by value is NOT sorted by hash bucket -- it would measure the
//   wrong thing.
static void RegisterArgs(benchmark::Benchmark* b) {
    constexpr int distances[] = {0, 4, 8, 16, 24, 32};
    constexpr int64_t sizes[] = {1024, 2048, 4096, 8192, 16384, 32768, 64 * 1024, 1024 * 1024, 8 * 1024 * 1024};
    constexpr int patterns[] = {static_cast<int>(Pattern::Random), static_cast<int>(Pattern::Clustered64)};
    for (auto d : distances) {
        for (auto s : sizes) {
            for (auto p : patterns) {
                b->Args({d, s, p});
            }
        }
    }
}

BENCHMARK(BM_PrefetchProbe)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_PrefetchProbeSet)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();
