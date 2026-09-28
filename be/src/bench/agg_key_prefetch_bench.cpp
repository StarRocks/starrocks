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

// Bench for the BE config `agg_hash_map_key_prefetch_dist`: the second prefetch
// stage of the serialized-key agg hash map, which prefetches the key bytes a
// probe is about to compare.
//
// It drives the real SerializedKeyAggHashMap::build_hash_map, with the keys
// persisted and the states allocated from one MemPool as the Aggregator does,
// followed by one pass that updates every row's state (the update_batch that
// follows compute_agg_states in the BE). Each argument pair differs only in
// key_dist (0 = stage off, 8 = the default), so read the pairs side by side.
//
// Axes:
//   groups    distinct keys; with key_len this sets the map's key bytes, which is
//             what the stage's gate keys on (L3 / 2 * agg_prefetch_l2_ratio).
//   key_len   serialized key bytes, ~28 (short) or ~92 (TPC-DS Q67 shape).
//   mode      Hit: a pre-filled map probed with keys drawn from it (phase2-like).
//             AllNew: an empty map fed `groups` distinct keys (high-NDV phase1);
//             the gate turns the stage on mid-build once the keys outgrow it.
//   gated     1: agg_prefetch_l2_ratio = 1, the production gates. 0: ratio 0,
//             both prefetch stages forced on, to show what the gate avoids.
//
// Run with --benchmark_enable_random_interleaving=true --benchmark_repetitions=5
// --benchmark_report_aggregates_only=true pinned to one core (numactl
// --physcpubind=N --localalloc). Interleaving shuffles the on/off runs, so
// neither arm always runs second on warmer pages or a drifting machine load;
// every run also warms up for kWarmUpSeconds first. Compare medians.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"

namespace starrocks {

namespace {

constexpr int kChunkSize = 4096;
// Rows probed per iteration in Hit mode.
constexpr int64_t kHitRows = 1 << 20;
// A few 8-byte aggregate states, like a sum + count.
constexpr size_t kStatePayload = 32;
// Untimed iterations before each run, so both arms start from warm code pages and inputs.
constexpr double kWarmUpSeconds = 0.5;

using Map = SerializedKeyAggHashMap<PhmapSeed1>;
using Key = Map::KeyType;

enum class Mode : int { Hit = 0, AllNew = 1 };

// Builds the state the way AllocateState does: the key in the prefix, the payload after it.
struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(const Key& key) {
        AggDataPtr state = allocator->allocate();
        *reinterpret_cast<Key*>(state) = key;
        memset(state + sizeof(Key), 0, kStatePayload);
        return state;
    }
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
};

// The key of group `id`, `len` bytes: the id's digits, padded. Distinct ids give distinct keys.
std::string key_of(int64_t id, int len) {
    std::string s = std::to_string(id);
    s.resize(std::max<size_t>(len, s.size()), 'k');
    return s;
}

ColumnPtr make_chunk(const std::vector<int64_t>& ids, size_t begin, int len) {
    auto col = BinaryColumn::create();
    const size_t end = std::min(ids.size(), begin + kChunkSize);
    for (size_t i = begin; i < end; ++i) {
        col->append(key_of(ids[i], len));
    }
    return col;
}

std::vector<ColumnPtr> make_chunks(const std::vector<int64_t>& ids, int len) {
    std::vector<ColumnPtr> chunks;
    for (size_t begin = 0; begin < ids.size(); begin += kChunkSize) {
        chunks.emplace_back(make_chunk(ids, begin, len));
    }
    return chunks;
}

std::vector<int64_t> shuffled_ids(int64_t n) {
    std::vector<int64_t> ids(n);
    for (int64_t i = 0; i < n; ++i) ids[i] = i;
    std::mt19937_64 rng(0xC0FFEEC0FFEEC0FFULL);
    std::shuffle(ids.begin(), ids.end(), rng);
    return ids;
}

// One table: its pool, allocator and map, torn down together.
struct Table {
    explicit Table(AggStatistics* stat) : map(kChunkSize, stat) {
        allocator.aggregate_key_size = sizeof(Key) + kStatePayload;
        allocator.pool = &pool;
    }
    MemPool pool;
    HashTableKeyAllocator allocator;
    Map map;
    Buffer<AggDataPtr> agg_states = Buffer<AggDataPtr>(kChunkSize);

    // compute_agg_states, then the per-row state update that update_batch does.
    void feed(const ColumnPtr& chunk) {
        const size_t n = chunk->size();
        Columns key_columns{chunk};
        map.build_hash_map(n, key_columns, &pool, BenchAllocateState{&allocator}, &agg_states);
        for (size_t i = 0; i < n; ++i) {
            *reinterpret_cast<int64_t*>(agg_states[i] + sizeof(Key)) += 1;
        }
    }
};

void attach_counters(benchmark::State& state, const Table& table, int64_t key_len) {
    state.counters["groups_in_map"] = static_cast<double>(table.map.hash_map.size());
    state.counters["key_bytes"] = static_cast<double>(table.map.hash_map.size() * key_len);
    state.counters["gate_bytes"] = static_cast<double>(agg_key_prefetch_min_bytes());
    state.counters["l3_bytes"] = static_cast<double>(CpuInfo::get_l3_cache_size());
}

void set_up(int32_t key_dist, bool gated) {
    static std::once_flag cpu_once;
    std::call_once(cpu_once, [] { CpuInfo::init(); });
    config::vector_chunk_size = kChunkSize;
    config::agg_hash_map_prefetch_dist = 16;
    config::agg_hash_map_key_prefetch_dist = key_dist;
    config::agg_prefetch_l2_ratio = gated ? 1.0 : 0.0;
}

} // namespace

static void BM_SerializedKeyPrefetch(benchmark::State& state) {
    const auto key_dist = static_cast<int32_t>(state.range(0));
    const int64_t groups = state.range(1);
    const auto key_len = static_cast<int>(state.range(2));
    const auto mode = static_cast<Mode>(state.range(3));
    const bool gated = state.range(4) != 0;
    set_up(key_dist, gated);

    RuntimeProfile profile("agg_key_prefetch_bench");
    AggStatistics stat(&profile);
    const std::vector<int64_t> ids = shuffled_ids(groups);
    // The serialized key is the string plus its 4-byte length.
    const int64_t serialized_len = key_len + 4;

    if (mode == Mode::Hit) {
        Table table(&stat);
        for (size_t begin = 0; begin < ids.size(); begin += kChunkSize) {
            table.feed(make_chunk(ids, begin, key_len));
        }
        std::mt19937_64 rng(0xBEEFCAFEULL);
        std::uniform_int_distribution<int64_t> pick(0, groups - 1);
        std::vector<int64_t> probe_ids(kHitRows);
        for (auto& id : probe_ids) id = pick(rng);
        const auto chunks = make_chunks(probe_ids, key_len);
        const size_t prefilled = table.map.hash_map.size();
        int64_t rows = 0;
        for (auto _ : state) {
            for (const auto& chunk : chunks) {
                table.feed(chunk);
            }
            rows += kHitRows;
            benchmark::ClobberMemory();
        }
        if (table.map.hash_map.size() != prefilled) {
            state.SkipWithError("the probe phase inserted keys");
        }
        attach_counters(state, table, serialized_len);
        state.SetItemsProcessed(rows);
    } else {
        const auto chunks = make_chunks(ids, key_len);
        int64_t rows = 0;
        for (auto _ : state) {
            state.PauseTiming();
            auto table = std::make_unique<Table>(&stat);
            state.ResumeTiming();
            for (const auto& chunk : chunks) {
                table->feed(chunk);
            }
            rows += groups;
            benchmark::ClobberMemory();
            state.PauseTiming();
            if (static_cast<int64_t>(table->map.hash_map.size()) != groups) {
                state.SkipWithError("lost or merged a group");
            }
            attach_counters(state, *table, serialized_len);
            table.reset();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(rows);
    }
    state.counters["key_dist"] = key_dist;
    state.counters["gated"] = gated;
}

// key_dist is the innermost axis so each on/off pair is adjacent in the report; run order comes from
// --benchmark_enable_random_interleaving.
static void RegisterArgs(benchmark::Benchmark* b) {
    constexpr int64_t groups[] = {64 * 1024, 128 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024};
    constexpr int64_t key_lens[] = {24, 88};
    for (int gated : {1, 0}) {
        for (int mode : {static_cast<int>(Mode::Hit), static_cast<int>(Mode::AllNew)}) {
            for (auto len : key_lens) {
                for (auto g : groups) {
                    for (int key_dist : {0, 8}) {
                        b->Args({key_dist, g, len, mode, gated});
                    }
                }
            }
        }
    }
}

BENCHMARK(BM_SerializedKeyPrefetch)
        ->ArgNames({"key_dist", "groups", "key_len", "mode", "gated"})
        ->Apply(RegisterArgs)
        ->Unit(benchmark::kMillisecond)
        ->MinWarmUpTime(kWarmUpSeconds)
        ->UseRealTime();

} // namespace starrocks

BENCHMARK_MAIN();
