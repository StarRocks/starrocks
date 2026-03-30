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

#include <gtest/gtest.h>

#include <chrono>
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

// Benchmark for inline vs non-inline aggregate state in AggHashMap.
// Uses HashTableKeyAllocator (batch allocation) for baseline to match real execution.
class AggHashMapBenchTest : public ::testing::Test {
protected:
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

    // Simulate count(*) update: dereference each AggDataPtr and increment the int64_t at offset.
    static void simulate_count_update(const Buffer<AggDataPtr>& states, size_t count, size_t offset) {
        for (size_t i = 0; i < count; i++) {
            (*reinterpret_cast<int64_t*>(states[i] + offset)) += 1;
        }
    }

    struct BenchResult {
        double build_ms;
        double update_ms;
        double total_ms;
    };

    // Run the non-inline (baseline) benchmark using HashTableKeyAllocator (matches real Aggregator).
    static BenchResult run_baseline(size_t total_rows, size_t num_groups) {
        using HashMap = Int32AggHashMap<PhmapSeed1>;
        using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

        RuntimeProfile profile("bench");
        AggStatistics stat(&profile);
        MapWithKey map(CHUNK_SIZE, &stat);
        MemPool pool;

        // Use HashTableKeyAllocator exactly like the real Aggregator does.
        HashTableKeyAllocator state_allocator;
        state_allocator.aggregate_key_size = BASELINE_STATE_SIZE;
        state_allocator.pool = &pool;

        auto allocate_func = [&state_allocator](auto& key) -> AggDataPtr {
            AggDataPtr p = state_allocator.allocate();
            // Real AllocateState stores key at beginning and calls create() on agg state.
            // For POD count state, create() is just zero-init.
            memset(p, 0, BASELINE_STATE_SIZE);
            return p;
        };

        auto key_column = generate_key_column(total_rows, num_groups);
        Buffer<AggDataPtr> agg_states(CHUNK_SIZE);

        double build_total = 0, update_total = 0;

        for (size_t offset = 0; offset < total_rows; offset += CHUNK_SIZE) {
            size_t chunk_rows = std::min(CHUNK_SIZE, total_rows - offset);
            auto chunk_col = key_column->clone_empty();
            chunk_col->append(*key_column, offset, chunk_rows);
            Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};

            agg_states.assign(chunk_rows, nullptr);

            auto t0 = std::chrono::high_resolution_clock::now();
            map.build_hash_map(chunk_rows, chunk_cols, &pool, allocate_func, &agg_states);
            auto t1 = std::chrono::high_resolution_clock::now();
            simulate_count_update(agg_states, chunk_rows, BASELINE_COUNT_OFFSET);
            auto t2 = std::chrono::high_resolution_clock::now();

            build_total += std::chrono::duration<double, std::milli>(t1 - t0).count();
            update_total += std::chrono::duration<double, std::milli>(t2 - t1).count();
        }

        return {build_total, update_total, build_total + update_total};
    }

    // Run the inline benchmark.
    static BenchResult run_inline(size_t total_rows, size_t num_groups) {
        using HashMap = Int32AggHashMap<PhmapSeed1>;
        using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

        static_assert(MapWithKey::supports_inline_state, "Int32 map should support inline state");

        RuntimeProfile profile("bench");
        AggStatistics stat(&profile);
        MapWithKey map(CHUNK_SIZE, &stat);
        MemPool pool;

        constexpr size_t count_offset = 0; // inline: state is at offset 0

        auto key_column = generate_key_column(total_rows, num_groups);
        Buffer<AggDataPtr> agg_states(CHUNK_SIZE);

        double build_total = 0, update_total = 0;

        for (size_t offset = 0; offset < total_rows; offset += CHUNK_SIZE) {
            size_t chunk_rows = std::min(CHUNK_SIZE, total_rows - offset);
            auto chunk_col = key_column->clone_empty();
            chunk_col->append(*key_column, offset, chunk_rows);
            Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};

            agg_states.assign(chunk_rows, nullptr);

            auto t0 = std::chrono::high_resolution_clock::now();
            map.build_hash_map_inline(chunk_rows, chunk_cols, &pool, &agg_states);
            auto t1 = std::chrono::high_resolution_clock::now();
            simulate_count_update(agg_states, chunk_rows, count_offset);
            auto t2 = std::chrono::high_resolution_clock::now();

            build_total += std::chrono::duration<double, std::milli>(t1 - t0).count();
            update_total += std::chrono::duration<double, std::milli>(t2 - t1).count();
        }

        return {build_total, update_total, build_total + update_total};
    }

    static void print_result(size_t total_rows, size_t num_groups, const BenchResult& baseline,
                             const BenchResult& inlined) {
        std::cout << "=== " << total_rows << " rows, " << num_groups << " groups ===" << std::endl;
        std::cout << "Baseline  - build: " << baseline.build_ms << "ms, update: " << baseline.update_ms
                  << "ms, total: " << baseline.total_ms << "ms" << std::endl;
        std::cout << "Inline    - build: " << inlined.build_ms << "ms, update: " << inlined.update_ms
                  << "ms, total: " << inlined.total_ms << "ms" << std::endl;
        std::cout << "Speedup   - build: " << baseline.build_ms / inlined.build_ms
                  << "x, update: " << baseline.update_ms / inlined.update_ms
                  << "x, total: " << baseline.total_ms / inlined.total_ms << "x" << std::endl;
    }
};

TEST_F(AggHashMapBenchTest, InlineVsBaseline_100Groups) {
    constexpr size_t TOTAL_ROWS = 10'000'000;
    constexpr size_t NUM_GROUPS = 100;
    auto baseline = run_baseline(TOTAL_ROWS, NUM_GROUPS);
    auto inlined = run_inline(TOTAL_ROWS, NUM_GROUPS);
    print_result(TOTAL_ROWS, NUM_GROUPS, baseline, inlined);
}

TEST_F(AggHashMapBenchTest, InlineVsBaseline_10KGroups) {
    constexpr size_t TOTAL_ROWS = 10'000'000;
    constexpr size_t NUM_GROUPS = 10'000;
    auto baseline = run_baseline(TOTAL_ROWS, NUM_GROUPS);
    auto inlined = run_inline(TOTAL_ROWS, NUM_GROUPS);
    print_result(TOTAL_ROWS, NUM_GROUPS, baseline, inlined);
}

TEST_F(AggHashMapBenchTest, InlineVsBaseline_1MGroups) {
    constexpr size_t TOTAL_ROWS = 10'000'000;
    constexpr size_t NUM_GROUPS = 1'000'000;
    auto baseline = run_baseline(TOTAL_ROWS, NUM_GROUPS);
    auto inlined = run_inline(TOTAL_ROWS, NUM_GROUPS);
    print_result(TOTAL_ROWS, NUM_GROUPS, baseline, inlined);
}

// Isolation test: measure ONLY the hash map lookup (find_key) without any allocate_func,
// to separate hash-map probe cost from allocate_func template overhead.
TEST_F(AggHashMapBenchTest, FindOnly_100Groups) {
    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

    constexpr size_t TOTAL_ROWS = 10'000'000;
    constexpr size_t NUM_GROUPS = 100;

    RuntimeProfile profile("bench");
    AggStatistics stat(&profile);
    MapWithKey map(CHUNK_SIZE, &stat);
    MemPool pool;

    // Pre-populate the hash map with 100 groups.
    HashTableKeyAllocator state_allocator;
    state_allocator.aggregate_key_size = BASELINE_STATE_SIZE;
    state_allocator.pool = &pool;
    auto allocate_func = [&state_allocator](auto& key) -> AggDataPtr {
        AggDataPtr p = state_allocator.allocate();
        memset(p, 0, BASELINE_STATE_SIZE);
        return p;
    };
    {
        auto seed_col = Int32Column::create();
        for (int k = 0; k < (int)NUM_GROUPS; k++) seed_col->append(k);
        Columns seed_cols = {seed_col};
        Buffer<AggDataPtr> seed_states(NUM_GROUPS);
        map.build_hash_map(NUM_GROUPS, seed_cols, &pool, allocate_func, &seed_states);
    }

    auto key_column = generate_key_column(TOTAL_ROWS, NUM_GROUPS);
    Buffer<AggDataPtr> agg_states(CHUNK_SIZE);
    Filter not_founds(CHUNK_SIZE);

    // Baseline: build_hash_map_with_selection (uses _find_key, no allocate)
    double baseline_ms = 0;
    for (size_t offset = 0; offset < TOTAL_ROWS; offset += CHUNK_SIZE) {
        size_t chunk_rows = std::min(CHUNK_SIZE, TOTAL_ROWS - offset);
        auto chunk_col = key_column->clone_empty();
        chunk_col->append(*key_column, offset, chunk_rows);
        Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};
        agg_states.assign(chunk_rows, nullptr);

        auto t0 = std::chrono::high_resolution_clock::now();
        map.build_hash_map_with_selection(chunk_rows, chunk_cols, &pool, allocate_func, &agg_states, &not_founds);
        auto t1 = std::chrono::high_resolution_clock::now();
        baseline_ms += std::chrono::duration<double, std::milli>(t1 - t0).count();
    }

    // Inline find: build_hash_map_with_selection_inline (uses _find_key_inline)
    double inline_ms = 0;
    for (size_t offset = 0; offset < TOTAL_ROWS; offset += CHUNK_SIZE) {
        size_t chunk_rows = std::min(CHUNK_SIZE, TOTAL_ROWS - offset);
        auto chunk_col = key_column->clone_empty();
        chunk_col->append(*key_column, offset, chunk_rows);
        Columns chunk_cols = {ColumnPtr(std::move(chunk_col))};
        agg_states.assign(chunk_rows, nullptr);

        auto t0 = std::chrono::high_resolution_clock::now();
        map.build_hash_map_with_selection_inline(chunk_rows, chunk_cols, &agg_states, &not_founds);
        auto t1 = std::chrono::high_resolution_clock::now();
        inline_ms += std::chrono::duration<double, std::milli>(t1 - t0).count();
    }

    std::cout << "=== Find-only, 100 groups, " << TOTAL_ROWS << " rows ===" << std::endl;
    std::cout << "Baseline _find_key:        " << baseline_ms << "ms" << std::endl;
    std::cout << "Inline   _find_key_inline: " << inline_ms << "ms" << std::endl;
    std::cout << "Speedup: " << baseline_ms / inline_ms << "x" << std::endl;
}

// Correctness test: verify inline state produces correct counts.
TEST_F(AggHashMapBenchTest, InlineStateCorrectness) {
    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

    RuntimeProfile profile("test");
    AggStatistics stat(&profile);
    MapWithKey map(CHUNK_SIZE, &stat);
    MemPool pool;

    // Create a small dataset: keys 0-9, each appearing 10 times.
    auto column = Int32Column::create();
    for (int rep = 0; rep < 10; rep++) {
        for (int key = 0; key < 10; key++) {
            column->append(key);
        }
    }
    Columns key_columns = {column};
    Buffer<AggDataPtr> agg_states(100);

    map.build_hash_map_inline(100, key_columns, &pool, &agg_states);

    // Simulate count update at offset 0.
    for (size_t i = 0; i < 100; i++) {
        ASSERT_NE(agg_states[i], nullptr);
        (*reinterpret_cast<int64_t*>(agg_states[i])) += 1;
    }

    // Verify: iterate hash map, each key should have count = 10.
    for (auto it = map.hash_map.begin(); it != map.hash_map.end(); ++it) {
        int64_t count = *reinterpret_cast<int64_t*>(&it->second);
        ASSERT_EQ(count, 10) << "Key " << it->first << " has count " << count << " (expected 10)";
    }
}

} // namespace starrocks
