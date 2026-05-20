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

// Midi-bench for the adaptive consecutive-keys cache on single-Slice
// (AggHashMapWithOneStringKey<SliceAggHashMap>) and serialized multi-column
// (AggHashMapWithSerializedKey<SliceAggHashMap>) GROUP BY paths.
//
// Baseline note: the both_off baseline uses the SAME wrapper type as the
// cache_on combo, with cache force_disable() applied via the FE session
// variable.  This measures "cache gate disabled" but retains the residual
// gate-check cost of the new fields' mere presence (a `bool` load + cmov
// per chunk and a single is_enabled() check per row).  If both_off is at
// parity with pre-PR numbers reported in the PR description, the gate-
// check overhead is below measurement noise.
//
// Out of scope: two-level hash conversion.  Aggregator promotes a wrapper
// from one-level to two-level once size crosses a threshold; the variant
// carry-over preserves cache state across conversion (see
// _carry_over_post_init_state in agg_hash_variant.cpp).  This bench
// measures one-level build only; conversion correctness is covered by UT.
//
// Sections:
//   A) Single-Slice, hash insert and hash+count.  Shapes cover UUID36 at
//      run-length {1, 64, 1024}, UUID36 90% mixed with non-UUID noise,
//      UUID36 run-length-64 with 10% non-UUID injected, 20-byte ascii
//      random, and a high-cardinality UUID36 random shape (1M distinct,
//      10M rows) so reviewers can see cache behavior on grown hash tables.
//   C) Construction-only regression guard.
//   D) Serialized 2-column, hash insert: 4 shapes x 2 combos at 10M rows.

#include <benchmark/benchmark.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
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
inline constexpr int64_t kSectionARows = 30'000'000;
inline constexpr int kDistinctKeys = 1000;
inline constexpr int kHighDistinctKeys = 1'000'000;
inline constexpr int64_t kHighCardRows = 10'000'000;

using OneStringWrapper = AggHashMapWithOneStringKey<SliceAggHashMap<PhmapSeed1>>;
using SerializedWrapper = AggHashMapWithSerializedKey<SliceAggHashMap<PhmapSeed1>>;

enum class DataShape : int {
    UUID36_rl1 = 0,        // 100% UUID36, no consecutive runs (kDistinctKeys distinct, cyclic)
    UUID36_rl64 = 1,       // 100% UUID36, run-length 64
    UUID36_rl1024 = 2,     // 100% UUID36, run-length 1024
    UUID36_mix90 = 3,      // 90% UUID36 + 10% non-UUID, random codes
    UUID36_rl64_mix90 = 4, // run-length-64 UUID36 with 10% non-UUID noise injected randomly
    ascii_random = 5,      // 20-byte ascii, random
    UUID36_high = 6,       // UUID36 random over kHighDistinctKeys (high cardinality)
};

enum class FeatureCombo : int {
    cache_off = 0, // cache force_disable() -- synthetic pre-PR baseline
    cache_on = 1,  // cache enabled (default)
};

enum class SerializedShape : int {
    both_UUID36_rl64 = 0,
    mixed_UUID_ascii = 1,
    both_ascii_random = 2,
    col0_uuid_col1_random = 3,
};

struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    template <typename KeyType>
    AggDataPtr operator()(KeyType /*key*/) {
        return allocator->allocate();
    }
};

inline void format_uuid36(uint32_t code, char* out) {
    static const char hex[] = "0123456789abcdef";
    uint64_t a = code * 0x9E3779B97F4A7C15ULL;
    uint64_t b = (code + 1) * 0xBF58476D1CE4E5B9ULL;
    uint8_t bytes[16];
    for (int i = 0; i < 8; ++i) {
        bytes[i] = static_cast<uint8_t>(a >> (i * 8));
        bytes[i + 8] = static_cast<uint8_t>(b >> (i * 8));
    }
    int o = 0;
    auto emit = [&](int n, bool dash_after) {
        for (int i = 0; i < n; ++i) {
            out[o++] = hex[(bytes[i] >> 4) & 0xF];
            out[o++] = hex[bytes[i] & 0xF];
        }
        if (dash_after) out[o++] = '-';
    };
    emit(4, true);
    emit(2, true);
    for (int i = 6; i < 8; ++i) {
        out[o++] = hex[(bytes[i] >> 4) & 0xF];
        out[o++] = hex[bytes[i] & 0xF];
    }
    out[o++] = '-';
    for (int i = 8; i < 10; ++i) {
        out[o++] = hex[(bytes[i] >> 4) & 0xF];
        out[o++] = hex[bytes[i] & 0xF];
    }
    out[o++] = '-';
    for (int i = 10; i < 16; ++i) {
        out[o++] = hex[(bytes[i] >> 4) & 0xF];
        out[o++] = hex[bytes[i] & 0xF];
    }
}

inline void format_ascii20(uint32_t code, char* out) {
    static const char alpha[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    uint64_t h = code * 0x9E3779B97F4A7C15ULL + 0x12345678;
    for (int i = 0; i < 20; ++i) {
        out[i] = alpha[(h >> (i * 3)) % (sizeof(alpha) - 1)];
        h ^= h << 13;
        h ^= h >> 7;
        h ^= h << 17;
    }
}

class StringChunkStream {
public:
    StringChunkStream(int64_t num_rows, int distinct_count, DataShape shape) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        std::uniform_real_distribution<double> coin(0.0, 1.0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);

        char uuid_buf[36];
        char ascii_buf[20];

        int code_cursor = 0;
        int code_repeat = 0;
        int run_length = 1;
        switch (shape) {
        case DataShape::UUID36_rl1:
            run_length = 1;
            break;
        case DataShape::UUID36_rl64:
        case DataShape::UUID36_rl64_mix90:
            run_length = 64;
            break;
        case DataShape::UUID36_rl1024:
            run_length = 1024;
            break;
        default:
            run_length = 1;
            break;
        }

        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = BinaryColumn::create();
            for (int i = 0; i < kBenchChunkSize; ++i) {
                int code;
                if (shape == DataShape::UUID36_mix90 || shape == DataShape::ascii_random ||
                    shape == DataShape::UUID36_high) {
                    code = uni(rng);
                } else {
                    code = code_cursor;
                    if (++code_repeat >= run_length) {
                        code_repeat = 0;
                        code_cursor = (code_cursor + 1) % distinct_count;
                    }
                }

                if (shape == DataShape::ascii_random) {
                    format_ascii20(code, ascii_buf);
                    col->append(Slice(ascii_buf, 20));
                } else if (shape == DataShape::UUID36_mix90 && coin(rng) >= 0.9) {
                    format_ascii20(code, ascii_buf);
                    col->append(Slice(ascii_buf, 20));
                } else if (shape == DataShape::UUID36_rl64_mix90 && coin(rng) >= 0.9) {
                    format_ascii20(uni(rng), ascii_buf);
                    col->append(Slice(ascii_buf, 20));
                } else {
                    format_uuid36(code, uuid_buf);
                    col->append(Slice(uuid_buf, 36));
                }
            }
            _chunks.emplace_back(std::move(col));
        }
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    std::vector<ColumnPtr> _chunks;
};

class TwoStringChunkStream {
public:
    TwoStringChunkStream(int64_t num_rows, int distinct_count, SerializedShape shape) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct_count > 0 ? distinct_count - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _cols0.reserve(num_chunks);
        _cols1.reserve(num_chunks);

        char uuid_buf[36];
        char ascii_buf[20];

        int code0_cursor = 0;
        int code0_repeat = 0;
        int code1_cursor = 17;
        int code1_repeat = 0;
        constexpr int kRun = 64;

        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col0 = BinaryColumn::create();
            auto col1 = BinaryColumn::create();
            for (int i = 0; i < kBenchChunkSize; ++i) {
                int code0, code1;
                if (shape == SerializedShape::both_UUID36_rl64) {
                    code0 = code0_cursor;
                    if (++code0_repeat >= kRun) {
                        code0_repeat = 0;
                        code0_cursor = (code0_cursor + 1) % distinct_count;
                    }
                    code1 = code1_cursor;
                    if (++code1_repeat >= kRun) {
                        code1_repeat = 0;
                        code1_cursor = (code1_cursor + 1) % distinct_count;
                    }
                } else {
                    code0 = uni(rng);
                    code1 = uni(rng);
                }

                if (shape == SerializedShape::both_ascii_random) {
                    format_ascii20(code0, ascii_buf);
                    col0->append(Slice(ascii_buf, 20));
                } else {
                    format_uuid36(code0, uuid_buf);
                    col0->append(Slice(uuid_buf, 36));
                }

                if (shape == SerializedShape::both_UUID36_rl64) {
                    format_uuid36(code1, uuid_buf);
                    col1->append(Slice(uuid_buf, 36));
                } else {
                    format_ascii20(code1, ascii_buf);
                    col1->append(Slice(ascii_buf, 20));
                }
            }
            _cols0.emplace_back(std::move(col0));
            _cols1.emplace_back(std::move(col1));
        }
    }

    size_t num_chunks() const { return _cols0.size(); }
    const ColumnPtr& col0(size_t i) const { return _cols0[i]; }
    const ColumnPtr& col1(size_t i) const { return _cols1[i]; }

private:
    std::vector<ColumnPtr> _cols0;
    std::vector<ColumnPtr> _cols1;
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
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_consec_keys_cache_bench");
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
inline void apply_combo(Wrapper* wrapper, FeatureCombo combo) {
    if (combo == FeatureCombo::cache_off) {
        wrapper->_consecutive_key_cache.force_disable();
    }
    // cache_on: default state, nothing to do.
}

template <typename Wrapper>
struct MechSnapshot {
    size_t cache_hits = 0;
    size_t cache_misses = 0;
    size_t final_size = 0;
    size_t hash_table_bytes = 0;
    size_t pool_bytes = 0;
};

template <typename Wrapper>
inline MechSnapshot<Wrapper> snapshot(Wrapper* w, MemPool* pool) {
    MechSnapshot<Wrapper> s;
    s.cache_hits = w->get_cache_hits();
    s.cache_misses = w->get_cache_misses();
    s.final_size = w->hash_map.size();
    s.hash_table_bytes = w->hash_map.dump_bound();
    s.pool_bytes = pool ? pool->total_allocated_bytes() : 0;
    return s;
}

template <typename Wrapper>
inline void publish(benchmark::State& state, const MechSnapshot<Wrapper>& s) {
    state.counters["cache_hits"] = s.cache_hits;
    state.counters["cache_misses"] = s.cache_misses;
    state.counters["final_size"] = s.final_size;
    state.counters["hash_table_bytes"] = s.hash_table_bytes;
    state.counters["pool_bytes"] = s.pool_bytes;
}

// ============================================================================
// Section A -- single-Slice wrapper, hash insert only
// ============================================================================
static void BM_OneStringBuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const DataShape shape = static_cast<DataShape>(state.range(1));
    const FeatureCombo combo = static_cast<FeatureCombo>(state.range(2));
    const int distinct = (shape == DataShape::UUID36_high) ? kHighDistinctKeys : kDistinctKeys;

    BenchSuite suite;
    suite.SetUp();
    StringChunkStream stream(num_rows, distinct, shape);

    int64_t total_rows = 0;
    int64_t cum_checksum = 0;
    MechSnapshot<OneStringWrapper> snap;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<OneStringWrapper>(kBenchChunkSize, suite._agg_stat.get());
        apply_combo(wrapper.get(), combo);
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), BenchAllocateState{&allocator},
                                    &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        snap = snapshot(wrapper.get(), suite._mem_pool.get());
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["shape"] = static_cast<int>(shape);
    state.counters["combo"] = static_cast<int>(combo);
    state.counters["distinct"] = distinct;
    state.counters["rows_per_iter"] = num_rows;
    publish(state, snap);

    suite.TearDown();
}

static void BM_OneStringBuildAndCount(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const DataShape shape = static_cast<DataShape>(state.range(1));
    const FeatureCombo combo = static_cast<FeatureCombo>(state.range(2));
    const int distinct = (shape == DataShape::UUID36_high) ? kHighDistinctKeys : kDistinctKeys;

    BenchSuite suite;
    suite.SetUp();
    StringChunkStream stream(num_rows, distinct, shape);

    int64_t total_rows = 0;
    int64_t cum_checksum = 0;
    MechSnapshot<OneStringWrapper> snap;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<OneStringWrapper>(kBenchChunkSize, suite._agg_stat.get());
        apply_combo(wrapper.get(), combo);
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (const auto& chunk_col : stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), BenchAllocateState{&allocator},
                                    &agg_states);
            for (int i = 0; i < kBenchChunkSize; ++i) {
                ++(*reinterpret_cast<int64_t*>(agg_states[i]));
            }
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        snap = snapshot(wrapper.get(), suite._mem_pool.get());
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += *reinterpret_cast<int64_t*>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["shape"] = static_cast<int>(shape);
    state.counters["combo"] = static_cast<int>(combo);
    state.counters["distinct"] = distinct;
    state.counters["rows_per_iter"] = num_rows;
    publish(state, snap);

    suite.TearDown();
}

// ============================================================================
// Section C -- construction-only regression guard
// ============================================================================
static void BM_OneStringConstruct(benchmark::State& state) {
    BenchSuite suite;
    suite.SetUp();
    for (auto _ : state) {
        auto wrapper = std::make_unique<OneStringWrapper>(kBenchChunkSize, suite._agg_stat.get());
        benchmark::DoNotOptimize(wrapper.get());
        state.PauseTiming();
        wrapper.reset();
        state.ResumeTiming();
    }
    suite.TearDown();
}

static void BM_SerializedConstruct(benchmark::State& state) {
    BenchSuite suite;
    suite.SetUp();
    for (auto _ : state) {
        auto wrapper = std::make_unique<SerializedWrapper>(kBenchChunkSize, suite._agg_stat.get());
        benchmark::DoNotOptimize(wrapper.get());
        state.PauseTiming();
        wrapper.reset();
        state.ResumeTiming();
    }
    suite.TearDown();
}

// ============================================================================
// Section D -- serialized multi-column wrapper, hash insert
// ============================================================================
static void BM_SerializedBuildOnly(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const SerializedShape shape = static_cast<SerializedShape>(state.range(1));
    const FeatureCombo combo = static_cast<FeatureCombo>(state.range(2));

    BenchSuite suite;
    suite.SetUp();
    TwoStringChunkStream stream(num_rows, kDistinctKeys, shape);

    int64_t total_rows = 0;
    int64_t cum_checksum = 0;
    MechSnapshot<SerializedWrapper> snap;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<SerializedWrapper>(kBenchChunkSize, suite._agg_stat.get());
        apply_combo(wrapper.get(), combo);
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = sizeof(int64_t);
        allocator.pool = suite._mem_pool.get();
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (size_t c = 0; c < stream.num_chunks(); ++c) {
            Columns key_columns;
            key_columns.emplace_back(stream.col0(c));
            key_columns.emplace_back(stream.col1(c));
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), BenchAllocateState{&allocator},
                                    &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        snap = snapshot(wrapper.get(), suite._mem_pool.get());
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) cs += reinterpret_cast<intptr_t>(agg_states[i]);
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["shape"] = static_cast<int>(shape);
    state.counters["combo"] = static_cast<int>(combo);
    state.counters["rows_per_iter"] = num_rows;
    publish(state, snap);

    suite.TearDown();
}

// ============================================================================
// Args registration
// ============================================================================

static void RegisterArgs_SectionA(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "shape", "combo"});
    for (int shape = 0; shape <= 5; ++shape) {
        for (int combo = 0; combo <= 1; ++combo) {
            b->Args({kSectionARows, shape, combo});
        }
    }
    for (int combo = 0; combo <= 1; ++combo) {
        b->Args({kHighCardRows, static_cast<int>(DataShape::UUID36_high), combo});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

static void RegisterArgs_SectionD(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "shape", "combo"});
    constexpr int64_t kRows = 10'000'000;
    for (int shape = 0; shape <= 3; ++shape) {
        for (int combo = 0; combo <= 1; ++combo) {
            b->Args({kRows, shape, combo});
        }
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

BENCHMARK(BM_OneStringBuildOnly)->Apply(RegisterArgs_SectionA);
BENCHMARK(BM_OneStringBuildAndCount)->Apply(RegisterArgs_SectionA);
BENCHMARK(BM_OneStringConstruct)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SerializedConstruct)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_SerializedBuildOnly)->Apply(RegisterArgs_SectionD);

} // namespace starrocks

BENCHMARK_MAIN();
