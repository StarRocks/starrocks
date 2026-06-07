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

// A/B bench for the inline-agg value specialization of COUNT(*) GROUP BY.
// Methodology follows agg_update_batch_bench.cpp: drive the REAL production
// hot path and A/B two implementations from one source.
//
//   BASELINE (production today): phmap<Key, AggDataPtr> + HashTableKeyAllocator
//     arena.  The real path: build_hash_map resolves an AggDataPtr per row into
//     a state buffer laid out [ key_dup | count(int64) ] (the key is duplicated
//     into the head exactly like Aggregator does, so the build pays the real
//     key-write + create() cost), then the real CountAggregateFunction::
//     update_batch increments the counter through the resolved pointer.
//     Finalize walks the arena and recovers (key, count) from each buffer.
//
//   INLINE (proposed): phmap<Key, int64>.  The slot IS the counter.  Build and
//     update fuse: lazy_emplace(key)->0 then ++slot, no arena, no agg_states[]
//     round-trip, no key duplication, no update_batch dispatch.  Finalize
//     iterates the map directly.
//
// Fairness: the inline probe is gated on the SAME prefetch policy build_hash_map
// uses (arm prefetch only once agg_should_prefetch_table() says the table has
// outgrown L2, distance agg_hash_map_default_prefetch_dist(), 0 = disabled).
// Both paths therefore prefetch identically; the A/B isolates the
// data-structure (slot vs pointer-to-arena), not the prefetch policy.
//
// Per iteration the table is rebuilt fresh and the full chunk stream is
// build+update'd -- the "medium" shape (real inserts + growth + update), which
// is where inline-agg wins on BOTH the build (no arena alloc / key dup) and
// the update (no pointer chase).
//
// Keys: INT (int32), BIGINT (int64), VARCHAR (Slice).  No FE / scan / pipeline.

#include <benchmark/benchmark.h>

#include <cmath>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/binary_column.h"
#include "column/column_hash.h"
#include "column/fixed_length_column.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/count.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kChunk = 4096;
using CountFn = CountAggregateFunction<false>;

enum class Dist : int { Random = 0, Clustered64 = 2, Zipf = 3 };

// ---------------------------------------------------------------------------
// Distribution code generator shared by numeric and slice streams.
// ---------------------------------------------------------------------------
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

static std::vector<std::vector<int>> gen_code_chunks(int64_t num_rows, int distinct, Dist dist) {
    std::mt19937 rng(0xC0FFEE);
    std::uniform_int_distribution<int> uni(0, distinct > 0 ? distinct - 1 : 0);
    const int64_t num_chunks = (num_rows + kChunk - 1) / kChunk;
    std::vector<std::vector<int>> out;
    out.reserve(num_chunks);
    std::vector<int> zipf;
    if (dist == Dist::Zipf) zipf = build_zipf_codes(distinct);
    std::uniform_int_distribution<int> zpick(0, zipf.empty() ? 0 : static_cast<int>(zipf.size()) - 1);
    for (int64_t c = 0; c < num_chunks; ++c) {
        std::vector<int> codes(kChunk);
        switch (dist) {
        case Dist::Random:
            for (int i = 0; i < kChunk; ++i) codes[i] = uni(rng);
            break;
        case Dist::Clustered64: {
            int cur = uni(rng);
            for (int i = 0; i < kChunk; ++i) {
                if (i > 0 && i % 64 == 0) cur = uni(rng);
                codes[i] = cur;
            }
            break;
        }
        case Dist::Zipf:
            for (int i = 0; i < kChunk; ++i) codes[i] = zipf[zpick(rng)];
            break;
        }
        out.emplace_back(std::move(codes));
    }
    return out;
}

template <LogicalType LT>
class NumberKeyStream {
public:
    using ColumnType = RunTimeColumnType<LT>;
    using FieldType = RunTimeCppType<LT>;
    NumberKeyStream(int64_t num_rows, int distinct, Dist dist) {
        auto code_chunks = gen_code_chunks(num_rows, distinct, dist);
        const int range_start = -distinct / 2;
        _chunks.reserve(code_chunks.size());
        for (const auto& codes : code_chunks) {
            auto col = ColumnType::create();
            auto& data = col->get_data();
            data.resize(kChunk);
            for (int i = 0; i < kChunk; ++i) data[i] = static_cast<FieldType>(range_start + codes[i]);
            _chunks.emplace_back(std::move(col));
        }
    }
    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    std::vector<ColumnPtr> _chunks;
};

class SliceKeyStream {
public:
    SliceKeyStream(int64_t num_rows, int distinct, Dist dist) {
        std::vector<std::string> table(distinct > 0 ? distinct : 1);
        for (int k = 0; k < static_cast<int>(table.size()); ++k) {
            std::string s = "key_" + std::to_string(k);
            if (s.size() < 12) s.resize(12, static_cast<char>('a' + (k % 26)));
            table[k] = std::move(s);
        }
        auto code_chunks = gen_code_chunks(num_rows, distinct, dist);
        _chunks.reserve(code_chunks.size());
        for (const auto& codes : code_chunks) {
            auto col = BinaryColumn::create();
            for (int i = 0; i < kChunk; ++i) {
                const std::string& s = table[codes[i]];
                col->append(Slice(s.data(), s.size()));
            }
            _chunks.emplace_back(std::move(col));
        }
    }
    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    std::vector<ColumnPtr> _chunks;
};

class BenchSuite {
public:
    void SetUp() {
        config::vector_chunk_size = kChunk;
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = kChunk;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
        _mem_pool = std::make_unique<MemPool>();
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_inline_accumulator_bench");
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

// Production-shaped arena allocate: write key at head (the dup, like
// aggregator.cpp:70) and run CountAggregateFunction::create() to init the
// counter -- so the baseline pays the real key-write + create cost and finalize
// can recover the key from the buffer head.
template <typename KeyT, size_t CountOff>
struct CountArenaAlloc {
    HashTableKeyAllocator* allocator;
    const CountFn* fn;
    FunctionContext* ctx;
    static constexpr size_t kStateSize = CountOff + sizeof(AggregateCountFunctionState<false>);
    AggDataPtr operator()(std::nullptr_t) {
        AggDataPtr p = allocator->allocate_null_key_data();
        fn->create(ctx, p + CountOff);
        return p;
    }
    AggDataPtr operator()(const KeyT& key) {
        AggDataPtr p = allocator->allocate();
        *reinterpret_cast<KeyT*>(p) = key;
        fn->create(ctx, p + CountOff);
        return p;
    }
};

// ===========================================================================
// Backings.  build_update() processes the whole stream once; finalize() does
// the output pass; groups()/ht_bytes()/pool_bytes() report footprint.
// ===========================================================================

template <LogicalType LT, typename MapT>
struct NumBaseline {
    using Stream = NumberKeyStream<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using FieldType = RunTimeCppType<LT>;
    using KeyT = typename MapT::key_type;
    using Wrapper = AggHashMapWithOneNumberKey<LT, MapT>;
    static constexpr size_t kCountOff = (sizeof(KeyT) + 7) / 8 * 8;
    using Alloc = CountArenaAlloc<KeyT, kCountOff>;

    BenchSuite* suite;
    CountFn fn;
    FunctionContext* ctx;
    std::unique_ptr<Wrapper> wrapper;
    HashTableKeyAllocator allocator;
    Buffer<AggDataPtr> agg_states;

    explicit NumBaseline(BenchSuite* s) : suite(s), ctx(FunctionContext::create_test_context()), agg_states(kChunk) {
        wrapper = std::make_unique<Wrapper>(kChunk, suite->_agg_stat.get());
        allocator.aggregate_key_size = static_cast<int>(Alloc::kStateSize);
        allocator.pool = suite->_mem_pool.get();
    }
    ~NumBaseline() { delete ctx; }

    void build_update(const std::vector<ColumnPtr>& chunks) {
        Alloc alloc{&allocator, &fn, ctx};
        for (const auto& chunk : chunks) {
            Columns key_columns;
            key_columns.emplace_back(chunk);
            wrapper->build_hash_map(kChunk, key_columns, suite->_mem_pool.get(), alloc, &agg_states);
            const Column* cols[1] = {chunk.get()}; // count(*) ignores the column
            fn.update_batch(ctx, kChunk, kCountOff, cols, agg_states.data());
        }
    }
    int64_t finalize() {
        int64_t checksum = 0;
        for (auto it = allocator.begin(); it != allocator.end(); it.next()) {
            AggDataPtr st = it.value();
            FieldType key = *reinterpret_cast<FieldType*>(st);
            int64_t cnt = *reinterpret_cast<int64_t*>(st + kCountOff);
            checksum += static_cast<int64_t>(key) + cnt;
        }
        return checksum;
    }
    size_t groups() const { return wrapper->hash_map.size(); }
    size_t ht_bytes() const { return wrapper->hash_map.dump_bound(); }
    size_t pool_bytes() const { return suite->_mem_pool->total_allocated_bytes(); }
};

template <LogicalType LT>
struct NumInline {
    using Stream = NumberKeyStream<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    using FieldType = RunTimeCppType<LT>;
    using InlineMap = phmap::flat_hash_map<FieldType, int64_t, StdHashWithSeed<FieldType, PhmapSeed1>>;

    BenchSuite* suite;
    InlineMap map;
    std::vector<size_t> hashes;

    explicit NumInline(BenchSuite* s) : suite(s), hashes(kChunk) {}

    void build_update(const std::vector<ColumnPtr>& chunks) {
        const auto hasher = map.hash_function();
        for (const auto& chunk : chunks) {
            const auto* col = down_cast<const ColumnType*>(chunk.get());
            const auto& data = col->get_data();
            // Same gate as build_hash_map: arm prefetch only once the table outgrows L2.
            const size_t dist = agg_hash_map_default_prefetch_dist();
            if (dist != 0 && agg_should_prefetch_table(map)) {
                for (int i = 0; i < kChunk; ++i) hashes[i] = hasher(data[i]);
                size_t pf = dist;
                for (int i = 0; i < kChunk; ++i) {
                    if (pf < static_cast<size_t>(kChunk)) map.prefetch_hash(hashes[pf++]);
                    auto it = map.lazy_emplace_with_hash(data[i], hashes[i],
                                                         [&](const auto& ctor) { ctor(data[i], int64_t(0)); });
                    ++it->second;
                }
            } else {
                for (int i = 0; i < kChunk; ++i) {
                    auto it = map.lazy_emplace(data[i], [&](const auto& ctor) { ctor(data[i], int64_t(0)); });
                    ++it->second;
                }
            }
        }
    }
    int64_t finalize() {
        int64_t checksum = 0;
        for (auto& kv : map) checksum += static_cast<int64_t>(kv.first) + kv.second;
        return checksum;
    }
    size_t groups() const { return map.size(); }
    size_t ht_bytes() const { return map.dump_bound(); }
    size_t pool_bytes() const { return 0; }
};

template <typename MapT>
struct SliceBaseline {
    using Stream = SliceKeyStream;
    using Wrapper = AggHashMapWithOneStringKey<MapT>;
    static constexpr size_t kCountOff = 16; // sizeof(Slice) aligned to 8
    using Alloc = CountArenaAlloc<Slice, kCountOff>;

    BenchSuite* suite;
    CountFn fn;
    FunctionContext* ctx;
    std::unique_ptr<Wrapper> wrapper;
    HashTableKeyAllocator allocator;
    Buffer<AggDataPtr> agg_states;

    explicit SliceBaseline(BenchSuite* s) : suite(s), ctx(FunctionContext::create_test_context()), agg_states(kChunk) {
        wrapper = std::make_unique<Wrapper>(kChunk, suite->_agg_stat.get());
        allocator.aggregate_key_size = static_cast<int>(Alloc::kStateSize);
        allocator.pool = suite->_mem_pool.get();
    }
    ~SliceBaseline() { delete ctx; }

    void build_update(const std::vector<ColumnPtr>& chunks) {
        Alloc alloc{&allocator, &fn, ctx};
        for (const auto& chunk : chunks) {
            Columns key_columns;
            key_columns.emplace_back(chunk);
            wrapper->build_hash_map(kChunk, key_columns, suite->_mem_pool.get(), alloc, &agg_states);
            const Column* cols[1] = {chunk.get()};
            fn.update_batch(ctx, kChunk, kCountOff, cols, agg_states.data());
        }
    }
    int64_t finalize() {
        int64_t checksum = 0;
        for (auto it = allocator.begin(); it != allocator.end(); it.next()) {
            AggDataPtr st = it.value();
            Slice key = *reinterpret_cast<Slice*>(st);
            int64_t cnt = *reinterpret_cast<int64_t*>(st + kCountOff);
            checksum += static_cast<int64_t>(key.size) + cnt;
        }
        return checksum;
    }
    size_t groups() const { return wrapper->hash_map.size(); }
    size_t ht_bytes() const { return wrapper->hash_map.dump_bound(); }
    size_t pool_bytes() const { return suite->_mem_pool->total_allocated_bytes(); }
};

template <typename MapT>
struct SliceInline {
    using Stream = SliceKeyStream;
    using InlineMap = phmap::flat_hash_map<Slice, int64_t, SliceHashWithSeed<PhmapSeed1>, SliceEqual>;

    BenchSuite* suite;
    InlineMap map;
    std::vector<size_t> hashes;

    explicit SliceInline(BenchSuite* s) : suite(s), hashes(kChunk) {}

    void build_update(const std::vector<ColumnPtr>& chunks) {
        const auto hasher = map.hash_function();
        MemPool* pool = suite->_mem_pool.get();
        for (const auto& chunk : chunks) {
            const auto* col = down_cast<const BinaryColumn*>(chunk.get());
            const size_t dist = agg_hash_map_default_prefetch_dist();
            const bool do_pf = dist != 0 && agg_should_prefetch_table(map);
            const auto slices = col->immutable_data();
            for (int i = 0; i < kChunk; ++i) hashes[i] = hasher(slices[i]);
            size_t pf = dist;
            for (int i = 0; i < kChunk; ++i) {
                if (do_pf && pf < static_cast<size_t>(kChunk)) map.prefetch_hash(hashes[pf++]);
                Slice s = slices[i];
                auto emplace = [&](const auto& ctor) {
                    uint8_t* mem = pool->allocate(s.size); // inline still copies the key into the pool
                    memcpy(mem, s.data, s.size);
                    ctor(Slice(reinterpret_cast<char*>(mem), s.size), int64_t(0));
                };
                auto it = map.lazy_emplace_with_hash(s, hashes[i], emplace);
                ++it->second;
            }
        }
    }
    int64_t finalize() {
        int64_t checksum = 0;
        for (auto& kv : map) checksum += static_cast<int64_t>(kv.first.size) + kv.second;
        return checksum;
    }
    size_t groups() const { return map.size(); }
    size_t ht_bytes() const { return map.dump_bound(); }
    size_t pool_bytes() const { return suite->_mem_pool->total_allocated_bytes(); }
};

// ===========================================================================
// v3 width probes: how much a fatter accumulator cell costs.
//   WidthInline<W>: the map value IS a W-byte cell (the Inline16/24/32 candidates);
//   WidthArena<W> : the map value is a pointer, the W-byte state lives in the arena
//                   (what a W-byte multi-aggregate state costs on the general path).
// Per row both touch every 8-byte field once (field0 += 1, field f += key), so a pair
// at equal W differs only in WHERE the state lives.
// ===========================================================================
template <size_t W>
struct WidthCell {
    int64_t v[W / 8];
};

template <size_t W>
struct WidthInline {
    using Stream = NumberKeyStream<TYPE_BIGINT>;
    using ColumnType = RunTimeColumnType<TYPE_BIGINT>;
    using FieldType = RunTimeCppType<TYPE_BIGINT>;
    using InlineMap = phmap::flat_hash_map<FieldType, WidthCell<W>, StdHashWithSeed<FieldType, PhmapSeed1>>;

    BenchSuite* suite;
    InlineMap map;
    std::vector<size_t> hashes;

    explicit WidthInline(BenchSuite* s) : suite(s), hashes(kChunk) {}

    void build_update(const std::vector<ColumnPtr>& chunks) {
        const auto hasher = map.hash_function();
        for (const auto& chunk : chunks) {
            const auto* col = down_cast<const ColumnType*>(chunk.get());
            const auto& data = col->get_data();
            const size_t dist = agg_hash_map_default_prefetch_dist();
            if (dist != 0 && agg_should_prefetch_table(map)) {
                for (int i = 0; i < kChunk; ++i) hashes[i] = hasher(data[i]);
                size_t pf = dist;
                for (int i = 0; i < kChunk; ++i) {
                    if (pf < static_cast<size_t>(kChunk)) map.prefetch_hash(hashes[pf++]);
                    auto it = map.lazy_emplace_with_hash(data[i], hashes[i],
                                                         [&](const auto& ctor) { ctor(data[i], WidthCell<W>{}); });
                    auto& c = it->second;
                    c.v[0] += 1;
                    for (size_t f = 1; f < W / 8; ++f) c.v[f] += data[i];
                }
            } else {
                for (int i = 0; i < kChunk; ++i) {
                    auto it = map.lazy_emplace(data[i], [&](const auto& ctor) { ctor(data[i], WidthCell<W>{}); });
                    auto& c = it->second;
                    c.v[0] += 1;
                    for (size_t f = 1; f < W / 8; ++f) c.v[f] += data[i];
                }
            }
        }
    }
    int64_t finalize() {
        int64_t checksum = 0;
        for (auto& kv : map) checksum += static_cast<int64_t>(kv.first) + kv.second.v[0];
        return checksum;
    }
    size_t groups() const { return map.size(); }
    size_t ht_bytes() const { return map.dump_bound(); }
    size_t pool_bytes() const { return 0; }
};

template <size_t W>
struct WidthArena {
    using Stream = NumberKeyStream<TYPE_BIGINT>;
    using ColumnType = RunTimeColumnType<TYPE_BIGINT>;
    using FieldType = RunTimeCppType<TYPE_BIGINT>;
    using MapT = Int64AggHashMap<PhmapSeed1>;

    BenchSuite* suite;
    MapT map;
    std::vector<size_t> hashes;
    MemPool* pool;

    explicit WidthArena(BenchSuite* s) : suite(s), hashes(kChunk), pool(s->_mem_pool.get()) {}

    void build_update(const std::vector<ColumnPtr>& chunks) {
        const auto hasher = map.hash_function();
        for (const auto& chunk : chunks) {
            const auto* col = down_cast<const ColumnType*>(chunk.get());
            const auto& data = col->get_data();
            const size_t dist = agg_hash_map_default_prefetch_dist();
            if (dist != 0 && agg_should_prefetch_table(map)) {
                for (int i = 0; i < kChunk; ++i) hashes[i] = hasher(data[i]);
                size_t pf = dist;
                for (int i = 0; i < kChunk; ++i) {
                    if (pf < static_cast<size_t>(kChunk)) map.prefetch_hash(hashes[pf++]);
                    auto it = map.lazy_emplace_with_hash(data[i], hashes[i], [&](const auto& ctor) {
                        auto* st = pool->allocate_aligned(W, 8);
                        std::memset(st, 0, W);
                        ctor(data[i], st);
                    });
                    auto* c = reinterpret_cast<int64_t*>(it->second);
                    c[0] += 1;
                    for (size_t f = 1; f < W / 8; ++f) c[f] += data[i];
                }
            } else {
                for (int i = 0; i < kChunk; ++i) {
                    auto it = map.lazy_emplace(data[i], [&](const auto& ctor) {
                        auto* st = pool->allocate_aligned(W, 8);
                        std::memset(st, 0, W);
                        ctor(data[i], st);
                    });
                    auto* c = reinterpret_cast<int64_t*>(it->second);
                    c[0] += 1;
                    for (size_t f = 1; f < W / 8; ++f) c[f] += data[i];
                }
            }
        }
    }
    int64_t finalize() {
        int64_t checksum = 0;
        for (auto& kv : map) checksum += static_cast<int64_t>(kv.first) + reinterpret_cast<int64_t*>(kv.second)[0];
        return checksum;
    }
    size_t groups() const { return map.size(); }
    size_t ht_bytes() const { return map.dump_bound(); }
    size_t pool_bytes() const { return suite->_mem_pool->total_allocated_bytes(); }
};

// ===========================================================================
// Timed sections
// ===========================================================================
template <typename Backing>
static void BM_BuildUpdate(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Dist dist = static_cast<Dist>(state.range(2));
    BenchSuite suite;
    suite.SetUp();
    typename Backing::Stream stream(num_rows, distinct, dist);

    int64_t total_rows = 0, cum = 0;
    size_t groups = 0, ht_bytes = 0, pool_bytes = 0;
    for (auto _ : state) {
        state.PauseTiming();
        Backing b(&suite);
        state.ResumeTiming();
        b.build_update(stream.chunks());
        benchmark::ClobberMemory();
        state.PauseTiming();
        cum += b.finalize();
        groups = b.groups();
        ht_bytes = b.ht_bytes();
        pool_bytes = b.pool_bytes();
        total_rows += static_cast<int64_t>(stream.chunks().size()) * kChunk;
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["final_groups"] = groups;
    state.counters["hash_table_bytes"] = ht_bytes;
    state.counters["pool_bytes"] = pool_bytes;
    state.counters["bytes_per_group"] = groups ? double(ht_bytes + pool_bytes) / groups : 0;
    suite.TearDown();
}

template <typename Backing>
static void BM_Finalize(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Dist dist = static_cast<Dist>(state.range(2));
    BenchSuite suite;
    suite.SetUp();
    typename Backing::Stream stream(num_rows, distinct, dist);

    size_t groups = 0;
    int64_t cum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        Backing b(&suite);
        b.build_update(stream.chunks());
        state.ResumeTiming();
        cum += b.finalize();
        benchmark::ClobberMemory();
        state.PauseTiming();
        groups = b.groups();
        suite._mem_pool->clear();
    }
    benchmark::DoNotOptimize(cum);
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * groups);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["final_groups"] = groups;
    suite.TearDown();
}

// ===========================================================================
using Int32Base = NumBaseline<TYPE_INT, Int32AggHashMap<PhmapSeed1>>;
using Int32Inl = NumInline<TYPE_INT>;
using Int64Base = NumBaseline<TYPE_BIGINT, Int64AggHashMap<PhmapSeed1>>;
using Int64Inl = NumInline<TYPE_BIGINT>;
using SliceBase = SliceBaseline<SliceAggHashMap<PhmapSeed1>>;
using SliceInl = SliceInline<SliceAggHashMap<PhmapSeed1>>;

static void Sweep(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 64'000'000;
    for (int distinct : {1, 64, 1000, 100000, 4000000}) {
        for (int d : {/*Random*/ 0, /*Clustered64*/ 2, /*Zipf*/ 3}) b->Args({kRows, distinct, d});
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}
static void FinSweep(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 64'000'000;
    for (int distinct : {64, 1000, 100000, 4000000}) b->Args({kRows, distinct, /*Random*/ 0});
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

BENCHMARK_TEMPLATE(BM_BuildUpdate, Int32Base)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, Int32Inl)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, Int64Base)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, Int64Inl)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, SliceBase)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, SliceInl)->Apply(Sweep);

// Narrow sweep for the wide-cell crossover question (5..10 aggregate fields):
// decisive cardinalities only, Random distribution.
static void WidthSweep(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist"});
    constexpr int64_t kRows = 64'000'000;
    for (int distinct : {1000, 100000, 4000000}) b->Args({kRows, distinct, 0});
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

using WInl8 = WidthInline<8>;
using WInl16 = WidthInline<16>;
using WInl24 = WidthInline<24>;
using WInl32 = WidthInline<32>;
using WArena8 = WidthArena<8>;
using WArena16 = WidthArena<16>;
using WArena32 = WidthArena<32>;
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl8)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl16)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl24)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl32)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WArena8)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WArena16)->Apply(Sweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WArena32)->Apply(Sweep);

using WInl40 = WidthInline<40>;
using WInl48 = WidthInline<48>;
using WInl64 = WidthInline<64>;
using WInl80 = WidthInline<80>;
using WArena48 = WidthArena<48>;
using WArena64 = WidthArena<64>;
using WArena80 = WidthArena<80>;
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl40)->Apply(WidthSweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl48)->Apply(WidthSweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl64)->Apply(WidthSweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WInl80)->Apply(WidthSweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WArena48)->Apply(WidthSweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WArena64)->Apply(WidthSweep);
BENCHMARK_TEMPLATE(BM_BuildUpdate, WArena80)->Apply(WidthSweep);

BENCHMARK_TEMPLATE(BM_Finalize, Int32Base)->Apply(FinSweep);
BENCHMARK_TEMPLATE(BM_Finalize, Int32Inl)->Apply(FinSweep);
BENCHMARK_TEMPLATE(BM_Finalize, Int64Base)->Apply(FinSweep);
BENCHMARK_TEMPLATE(BM_Finalize, Int64Inl)->Apply(FinSweep);
BENCHMARK_TEMPLATE(BM_Finalize, SliceBase)->Apply(FinSweep);
BENCHMARK_TEMPLATE(BM_Finalize, SliceInl)->Apply(FinSweep);

} // namespace starrocks

// Custom main: the prefetch threshold reads CpuInfo's cache sizes, which are only
// populated by CpuInfo::init() (the daemon does it at startup; BENCHMARK_MAIN would not).
int main(int argc, char** argv) {
    starrocks::CpuInfo::init();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
