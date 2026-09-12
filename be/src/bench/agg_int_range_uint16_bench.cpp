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

// Bench for the new AggHashMapWithCompressibleInt32Key variant: single
// INT GROUP BY with FE-supplied range fitting in 9-16 bits.
//
// Three paths driven on the same data:
//   A. baseline      -- AggHashMapWithOneNumberKey<TYPE_INT, phmap<int32>>
//                       what BE runs today when FE skips the range
//                       rewrite (no range stats / multi-column / etc.)
//   B. slice_cx4     -- AggHashMapWithCompressedKeyFixedSize<Int32AggHashMap>
//                       what BE runs today when range stats fit in
//                       9-32 bits.  phmap<SliceKey4> + per-row
//                       bitcompress_serialize.
//   C. new variant   -- AggHashMapWithCompressibleInt32Key<RangeUInt16AggHashMap>
//                       this PR.  Inline (value - min) -> uint16
//                       narrowing, 65 536-cell direct array, no
//                       per-row serialization.
//
// Decision rule for shipping: C wins on consecutive-keys / clustered
// workloads at distinct cardinality fitting in uint16 and the
// regression on random / sorted workloads stays within noise.

#include <benchmark/benchmark.h>

#include <any>
#include <cmath>
#include <memory>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;
inline constexpr int64_t kBenchRows = 100'000'000;

template <PhmapSeed seed>
using PhmapInt32 = phmap::flat_hash_map<int32_t, AggDataPtr, StdHashWithSeed<int32_t, seed>>;
using BaselineWrapper = AggHashMapWithOneNumberKey<TYPE_INT, PhmapInt32<PhmapSeed1>>;
using SliceCx4Wrapper = CompressedFixedSize4AggHashMap<PhmapSeed1>;
using DirectUint16Wrapper = CompressibleInt32AggHashMap<PhmapSeed1>;

enum class Distribution : int {
    Random = 0,
    Sorted = 1,
    Clustered64 = 2,
    Zipf = 3,
};

struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    template <typename KeyType>
    AggDataPtr operator()(KeyType /*key*/) {
        return allocator->allocate();
    }
};

class Int32RangeChunkStream {
public:
    Int32RangeChunkStream(int64_t num_rows, int32_t base, int32_t range, Distribution dist) : _base(base) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, range > 0 ? range - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);

        std::vector<int> zipf_codes;
        if (dist == Distribution::Zipf) {
            zipf_codes = build_zipf_codes(range);
        }
        std::uniform_int_distribution<int> zipf_pick(0, zipf_codes.empty() ? 0 : zipf_codes.size() - 1);

        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int32Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            switch (dist) {
            case Distribution::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) data[i] = base + uni(rng);
                break;
            case Distribution::Sorted: {
                std::vector<int> vals(kBenchChunkSize);
                for (int i = 0; i < kBenchChunkSize; ++i) vals[i] = uni(rng);
                std::sort(vals.begin(), vals.end());
                for (int i = 0; i < kBenchChunkSize; ++i) data[i] = base + vals[i];
                break;
            }
            case Distribution::Clustered64: {
                int cur = uni(rng);
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    if (i > 0 && i % 64 == 0) cur = uni(rng);
                    data[i] = base + cur;
                }
                break;
            }
            case Distribution::Zipf:
                for (int i = 0; i < kBenchChunkSize; ++i) data[i] = base + zipf_codes[zipf_pick(rng)];
                break;
            }
            _chunks.emplace_back(std::move(col));
        }
    }

    int32_t base() const { return _base; }
    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    static std::vector<int> build_zipf_codes(int range) {
        constexpr int table_size = 4096;
        constexpr double s = 1.5;
        if (range <= 0) return {};
        std::vector<double> cum(range);
        double total = 0.0;
        for (int k = 0; k < range; ++k) {
            total += 1.0 / std::pow(static_cast<double>(k + 1), s);
            cum[k] = total;
        }
        std::vector<int> codes(table_size);
        std::mt19937 rng(0xBEEFCAFE);
        std::uniform_real_distribution<double> uni(0.0, total);
        for (int i = 0; i < table_size; ++i) {
            double r = uni(rng);
            int lo = 0, hi = range - 1;
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

    int32_t _base;
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
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_int_range_uint16_bench");
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
// A. baseline -- raw phmap<int32> through AggHashMapWithOneNumberKey
// ============================================================================
static void BM_Baseline_Int32_Phmap(benchmark::State& state) {
    const int range = static_cast<int>(state.range(0));
    const Distribution dist = static_cast<Distribution>(state.range(1));
    BenchSuite suite;
    suite.SetUp();
    Int32RangeChunkStream stream(kBenchRows, /*base=*/-500, range, dist);

    int64_t total_rows = 0;
    size_t final_groups = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<BaselineWrapper>(kBenchChunkSize, suite._agg_stat.get());
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
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["range"] = range;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = kBenchRows;
    state.counters["final_groups"] = final_groups;
    suite.TearDown();
}

// ============================================================================
// B. slice_cx4 -- AggHashMapWithCompressedKeyFixedSize<Int32AggHashMap>
//    Current FE-rewrite path for 9-32 bit ranges.  Populates bases /
//    offsets / used_bits as the aggregator would after
//    _try_to_apply_compressed_key_opt succeeds.
// ============================================================================
static void BM_SliceCx4(benchmark::State& state) {
    const int range = static_cast<int>(state.range(0));
    const Distribution dist = static_cast<Distribution>(state.range(1));
    BenchSuite suite;
    suite.SetUp();
    Int32RangeChunkStream stream(kBenchRows, /*base=*/-500, range, dist);

    int64_t total_rows = 0;
    size_t final_groups = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<SliceCx4Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        wrapper->bases.resize(1);
        wrapper->bases[0] = static_cast<int32_t>(stream.base());
        wrapper->offsets = {0};
        // For range up to 16 bits, used_bits is the bit width.  We use 16
        // here as the worst-case slice_cx4 shape -- the real value comes
        // from `get_used_bits` at runtime; the bench is timing the per-row
        // bitcompress cost, not the offset arithmetic.
        wrapper->used_bits = {16};

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
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["range"] = range;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = kBenchRows;
    state.counters["final_groups"] = final_groups;
    suite.TearDown();
}

// ============================================================================
// C. new direct-array variant -- AggHashMapWithCompressibleInt32Key
//    backed by SmallFixedSizeHashMap<uint16_t>.  Inline (value - min)
//    narrowing.  This is what this PR ships behind the routing decision
//    in _try_to_apply_compressed_key_opt.
// ============================================================================
static void BM_DirectArrayUint16(benchmark::State& state) {
    const int range = static_cast<int>(state.range(0));
    const Distribution dist = static_cast<Distribution>(state.range(1));
    BenchSuite suite;
    suite.SetUp();
    Int32RangeChunkStream stream(kBenchRows, /*base=*/-500, range, dist);

    int64_t total_rows = 0;
    size_t final_groups = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<DirectUint16Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        wrapper->set_min(stream.base());
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
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["range"] = range;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = kBenchRows;
    state.counters["final_groups"] = final_groups;
    suite.TearDown();
}

// ============================================================================
// Argument matrix:
//   range: 16 (low end, very dense), 256, 1000, 10000, 65535 (full
//     uint16 range).  All > 8 bits so the new variant fires; the
//     8-bit case is left to the slice_cx1 path.
//   distribution: Random, Sorted, Clustered64, Zipf.
// ============================================================================
static void RegisterArgs(benchmark::internal::Benchmark* b) {
    constexpr int ranges[] = {300, 1000, 10000, 65535};
    constexpr int dists[] = {static_cast<int>(Distribution::Random), static_cast<int>(Distribution::Sorted),
                             static_cast<int>(Distribution::Clustered64), static_cast<int>(Distribution::Zipf)};
    for (int r : ranges) {
        for (int d : dists) {
            b->Args({r, d});
        }
    }
}

BENCHMARK(BM_Baseline_Int32_Phmap)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SliceCx4)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_DirectArrayUint16)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();
