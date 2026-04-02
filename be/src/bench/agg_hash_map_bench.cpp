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

// Benchmark for inline vs non-inline aggregate state in AggHashMap.
// Usage: ./agg_hash_map_bench [--benchmark_filter=...]

#include <benchmark/benchmark.h>

#include <random>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

static constexpr size_t CHUNK_SIZE = 4096;
// state layout for baseline: [int32_t key][padding][int64_t count] = 16 bytes
static constexpr size_t BASELINE_STATE_SIZE = 16;
static constexpr size_t BASELINE_COUNT_OFFSET = 8;

static ColumnPtr generate_key_column(size_t num_rows, size_t num_groups, uint32_t seed = 42) {
    auto column = Int32Column::create();
    column->reserve(num_rows);
    std::mt19937 rng(seed);
    for (size_t i = 0; i < num_rows; i++) {
        column->append(static_cast<int32_t>(rng() % num_groups));
    }
    return column;
}

static void simulate_count_update(const Buffer<AggDataPtr>& states, size_t count, size_t offset) {
    for (size_t i = 0; i < count; i++) {
        (*reinterpret_cast<int64_t*>(states[i] + offset)) += 1;
    }
}

// BM_AggHashMap_Baseline: non-inline (pointer-based) aggregation using HashTableKeyAllocator.
// state.range(0) = num_groups
static void BM_AggHashMap_Baseline(benchmark::State& state) {
    const size_t total_rows = 10'000'000;
    const size_t num_groups = state.range(0);

    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

    auto key_column = generate_key_column(total_rows, num_groups);

    for (auto _ : state) {
        RuntimeProfile profile("bench");
        AggStatistics stat(&profile);
        MapWithKey map(CHUNK_SIZE, &stat);
        MemPool pool;

        HashTableKeyAllocator state_allocator;
        state_allocator.aggregate_key_size = BASELINE_STATE_SIZE;
        state_allocator.pool = &pool;

        auto allocate_func = [&state_allocator](auto & /*key*/) -> AggDataPtr {
            AggDataPtr p = state_allocator.allocate();
            memset(p, 0, BASELINE_STATE_SIZE);
            return p;
        };

        Buffer<AggDataPtr> agg_states(CHUNK_SIZE);

        for (size_t offset = 0; offset < total_rows; offset += CHUNK_SIZE) {
            size_t chunk_rows = std::min(CHUNK_SIZE, total_rows - offset);
            auto chunk_col = key_column->clone_empty();
            chunk_col->append(*key_column, offset, chunk_rows);
            Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};

            agg_states.assign(chunk_rows, nullptr);
            map.build_hash_map(chunk_rows, chunk_cols, &pool, allocate_func, &agg_states);
            simulate_count_update(agg_states, chunk_rows, BASELINE_COUNT_OFFSET);
        }

        benchmark::DoNotOptimize(map.hash_map.size());
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
}

// BM_AggHashMap_Inline: inline aggregate state aggregation.
// state.range(0) = num_groups
static void BM_AggHashMap_Inline(benchmark::State& state) {
    const size_t total_rows = 10'000'000;
    const size_t num_groups = state.range(0);

    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;
    static_assert(MapWithKey::supports_inline_state, "Int32 map should support inline state");

    auto key_column = generate_key_column(total_rows, num_groups);

    for (auto _ : state) {
        RuntimeProfile profile("bench");
        AggStatistics stat(&profile);
        MapWithKey map(CHUNK_SIZE, &stat);
        MemPool pool;

        Buffer<AggDataPtr> agg_states(CHUNK_SIZE);

        for (size_t offset = 0; offset < total_rows; offset += CHUNK_SIZE) {
            size_t chunk_rows = std::min(CHUNK_SIZE, total_rows - offset);
            auto chunk_col = key_column->clone_empty();
            chunk_col->append(*key_column, offset, chunk_rows);
            Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};

            agg_states.assign(chunk_rows, nullptr);
            auto count_update = [](AggDataPtr state, size_t, bool) { (*reinterpret_cast<int64_t*>(state)) += 1; };
            map.template build_hash_map<decltype(count_update)>(chunk_rows, chunk_cols, &pool, NoAllocFunc{},
                                                                &agg_states, count_update);
        }

        benchmark::DoNotOptimize(map.hash_map.size());
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
}

// BM_AggHashMap_FindOnly_Baseline: hash map probe only (pre-populated map, no allocation).
static void BM_AggHashMap_FindOnly_Baseline(benchmark::State& state) {
    const size_t total_rows = 10'000'000;
    const size_t num_groups = 100;

    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

    RuntimeProfile profile("bench");
    AggStatistics stat(&profile);
    MapWithKey map(CHUNK_SIZE, &stat);
    MemPool pool;

    HashTableKeyAllocator state_allocator;
    state_allocator.aggregate_key_size = BASELINE_STATE_SIZE;
    state_allocator.pool = &pool;
    auto allocate_func = [&state_allocator](auto & /*key*/) -> AggDataPtr {
        AggDataPtr p = state_allocator.allocate();
        memset(p, 0, BASELINE_STATE_SIZE);
        return p;
    };

    // Pre-populate with num_groups keys.
    {
        auto seed_col = Int32Column::create();
        for (int k = 0; k < (int)num_groups; k++) seed_col->append(k);
        Columns seed_cols = {seed_col};
        Buffer<AggDataPtr> seed_states(num_groups);
        map.build_hash_map(num_groups, seed_cols, &pool, allocate_func, &seed_states);
    }

    auto key_column = generate_key_column(total_rows, num_groups);
    Buffer<AggDataPtr> agg_states(CHUNK_SIZE);
    Filter not_founds(CHUNK_SIZE);

    for (auto _ : state) {
        for (size_t offset = 0; offset < total_rows; offset += CHUNK_SIZE) {
            size_t chunk_rows = std::min(CHUNK_SIZE, total_rows - offset);
            auto chunk_col = key_column->clone_empty();
            chunk_col->append(*key_column, offset, chunk_rows);
            Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};
            agg_states.assign(chunk_rows, nullptr);
            map.build_hash_map_with_selection(chunk_rows, chunk_cols, &pool, allocate_func, &agg_states, &not_founds);
        }
        benchmark::DoNotOptimize(agg_states[0]);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
}

// BM_AggHashMap_FindOnly_Inline: inline probe only (pre-populated map, no allocation).
static void BM_AggHashMap_FindOnly_Inline(benchmark::State& state) {
    const size_t total_rows = 10'000'000;
    const size_t num_groups = 100;

    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

    RuntimeProfile profile("bench");
    AggStatistics stat(&profile);
    MapWithKey map(CHUNK_SIZE, &stat);
    MemPool pool;

    HashTableKeyAllocator state_allocator;
    state_allocator.aggregate_key_size = BASELINE_STATE_SIZE;
    state_allocator.pool = &pool;
    auto allocate_func = [&state_allocator](auto & /*key*/) -> AggDataPtr {
        AggDataPtr p = state_allocator.allocate();
        memset(p, 0, BASELINE_STATE_SIZE);
        return p;
    };

    // Pre-populate with num_groups keys.
    {
        auto seed_col = Int32Column::create();
        for (int k = 0; k < (int)num_groups; k++) seed_col->append(k);
        Columns seed_cols = {seed_col};
        Buffer<AggDataPtr> seed_states(num_groups);
        map.build_hash_map(num_groups, seed_cols, &pool, allocate_func, &seed_states);
    }

    auto key_column = generate_key_column(total_rows, num_groups);
    Buffer<AggDataPtr> agg_states(CHUNK_SIZE);
    Filter not_founds(CHUNK_SIZE);

    for (auto _ : state) {
        for (size_t offset = 0; offset < total_rows; offset += CHUNK_SIZE) {
            size_t chunk_rows = std::min(CHUNK_SIZE, total_rows - offset);
            auto chunk_col = key_column->clone_empty();
            chunk_col->append(*key_column, offset, chunk_rows);
            Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};
            agg_states.assign(chunk_rows, nullptr);
            auto count_update = [](AggDataPtr state, size_t, bool) { (*reinterpret_cast<int64_t*>(state)) += 1; };
            map.template build_hash_map_with_selection<decltype(count_update)>(
                    chunk_rows, chunk_cols, nullptr, NoAllocFunc{}, &agg_states, &not_founds, count_update);
        }
        benchmark::DoNotOptimize(agg_states[0]);
    }

    state.SetItemsProcessed(state.iterations() * total_rows);
}

BENCHMARK(BM_AggHashMap_Baseline)->Arg(100)->Arg(10'000)->Arg(1'000'000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_AggHashMap_Inline)->Arg(100)->Arg(10'000)->Arg(1'000'000)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_AggHashMap_FindOnly_Baseline)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_AggHashMap_FindOnly_Inline)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();
