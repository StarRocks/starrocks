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

// Bench for the hash-join probe-side prefetch story.
//
// Companion to agg_prefetch_dist_bench.cpp, but the join side is a different
// machine.  There are two independent prefetch mechanisms on probe:
//
//   1. Bucket-resolution prefetch (lookup_init, join_hash_map_method.hpp):
//      a lookahead into `first[]`, present ONLY in LINEAR_CHAINED /
//      LINEAR_CHAINED_SET.  Distance is the BE config hash_map_prefetch_dist
//      (shared with the agg path), gated on first[] spilling L2 via
//      join_probe_prefetch_l2_ratio.  BM_JoinBucketPrefetch sweeps the distance
//      with the gate disabled (ratio 0) to read the raw distance curve.
//
//   2. Chain-walk prefetch (join_hash_map.hpp, PREFETCH_AND_COWAIT): the build
//      `next[]` chain is walked under coroutine interleaving -- N coroutines
//      each prefetch their build row + next link and `co_await`, so one
//      coroutine's miss overlaps another's compute.  This is latency hiding by
//      interleaving, not by a fixed distance; the tunable is the coroutine
//      count `interleaving_group_size`, not a lookahead.  It is gated by
//      `cache_miss_serious()` (join_hash_table_descriptor.h), a STAIRCASE of
//      hard-coded byte thresholds (16/32/64/128 MiB) crossed with chain depth.
//
// Two benches, one harness (a real pre-built JoinHashTable probed to
// completion):
//   - BM_JoinProbe          drives the chain-walk path (#2): coroutine group
//                           size x footprint x depth x hit_rate, on BOTH
//                           chain-walk cost profiles --
//                             LINEAR_CHAINED  (1 prefetch/node)
//                             BUCKET_CHAINED  (2 prefetch/node).
//   - BM_JoinBucketPrefetch drives the bucket-resolution path (#1): the
//                           lookup_init distance, swept via the new config,
//                           on LINEAR_CHAINED with the chain walk minimized.
//
// Two questions, the reason the hard-coded staircase is not trusted:
//
//   (A) Crossover.  For each (footprint, depth, hit_rate), does the coroutine
//       path (group_size > 0) beat the plain walk (group_size = 0), and where?
//       The current gate claims the boundary is at 16/32/64/128 MiB of
//       `probe_bytes` crossed with keys_per_bucket; re-derive the real boundary
//       and express it against CpuInfo L2/L3 instead of magic bytes (the method
//       selector in join_hash_table.cpp already keys on CpuInfo, this gate does
//       not).
//
//   (B) Absolute lookup cost.  ns per probe row and ns per chain node, read by
//       residency band (footprint vs detected L2/L3), for each prefetch config.
//       This is the L2 / L3 / DRAM per-lookup latency the cost model needs.
//       Read the depth = 1 rows for the clean "one bucket access + one chain
//       node" lookup cost; deeper rows show how prefetch amortizes over a chain.
//
// How the gate is forced (we measure what the gate SHOULD decide, not what it
// does): `interleaving_group_size` is set on the query options (0 disables
// coroutines unconditionally; > 0 arms them); after build we overwrite
// `table_items()->cache_miss_serious` so the coroutine path is taken for ALL
// footprints when group_size > 0.  The heuristic's own verdict is captured
// first and emitted as the `heuristic_serious` counter so the truth can be
// diffed against it.
//
// What this bench does NOT cover (call out for reviewers):
//   - Output materialization: probe() gathers the output columns per match, so
//     the absolute ns/row includes that gather.  It is IDENTICAL between the
//     coroutine and plain paths (same matches), so the crossover (A) delta is
//     pure search; the absolute cost (B) is cleanest at depth = 1 / low hit
//     rate where the gather is ~1x.  Use `perf stat -e
//     cache-misses,l2_rqsts.miss,llc-load-misses` for cycle-level evidence.
//   - RANGE_DIRECT_MAPPING / DENSE_*: they have no bucket prefetch and the
//     selector already keeps RANGE_DIRECT_MAPPING L2-resident by construction;
//     their chain walk shares path #2 and can be added as a third method later.
//   - ASOF (coroutines disabled) and the SET variants.

#include <benchmark/benchmark.h>

#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "exec/join/join_hash_table.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;
// Probe rows driven per timed iteration.  Large enough to amortize per-call
// overhead; the build table footprint (not this) is the size axis.
inline constexpr int64_t kTimedProbeRows = 8'000'000;

// Which chain-walk cost profile to exercise.  Forced via the two session flags
// the selector reads (range-direct-mapping + linear-chained opt), so the
// resulting method is deterministic for a single BIGINT key over a contiguous
// universe.
enum class Method : int {
    LinearChained = 0, // range-direct OFF, linear-chained ON -> 1 prefetch/node
    BucketChained = 1, // both OFF (fallback)               -> 2 prefetch/node
};

// ============================================================================
// Descriptor / param plumbing (mirrors join_hash_map_test.cpp, public APIs).
// A single BIGINT join key: probe tuple 0 (slot 0), build tuple 1 (slot 1).
// ============================================================================
class JoinHarness {
public:
    void set_up(Method method, int32_t group_size) {
        config::vector_chunk_size = kBenchChunkSize;
        // Disable the bucket-prefetch L2 gate so the bench drives the distance
        // directly; the gate would otherwise force noprefetch below ratio*L2.
        config::join_probe_prefetch_l2_ratio = 0;
        static std::once_flag cpu_once;
        std::call_once(cpu_once, [] { CpuInfo::init(); });

        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = kBenchChunkSize;
        // Coroutine arming knob.  0 -> plain walk; > 0 -> N interleaved coroutines.
        query_options.__set_interleaving_group_size(group_size);
        // Pin the method by toggling the two selector opts.
        const bool linear = method == Method::LinearChained;
        query_options.__set_enable_hash_join_range_direct_mapping_opt(false);
        query_options.__set_enable_hash_join_linear_chained_opt(linear);
        TQueryGlobals query_globals;
        _state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _state->init_instance_mem_tracker();

        _profile = std::make_shared<RuntimeProfile>("join_probe_prefetch_bench");
        _pool = std::make_unique<ObjectPool>();
        _bigint_type = TypeDescriptor::from_logical_type(TYPE_BIGINT);

        _build_table_param();
    }

    RuntimeState* state() { return _state.get(); }
    const HashTableParam& param() const { return _param; }
    const TypeDescriptor& key_type() const { return _bigint_type; }

private:
    void _add_tuple(TDescriptorTableBuilder* builder) {
        TTupleDescriptorBuilder tuple_builder;
        TSlotDescriptorBuilder slot_builder;
        tuple_builder.add_slot(slot_builder.type(TYPE_BIGINT).column_name("k").column_pos(0).nullable(false).build());
        tuple_builder.build(builder);
    }

    void _build_table_param() {
        TDescriptorTableBuilder row_desc_builder;
        _add_tuple(&row_desc_builder); // tuple 0 -> probe
        _add_tuple(&row_desc_builder); // tuple 1 -> build

        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_state.get(), _pool.get(), row_desc_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        _probe_row_desc = std::make_shared<RowDescriptor>(*tbl, std::vector<TTupleId>{0});
        _build_row_desc = std::make_shared<RowDescriptor>(*tbl, std::vector<TTupleId>{1});

        _param.with_other_conjunct = false;
        _param.join_type = TJoinOp::INNER_JOIN;
        _param.search_ht_timer = ADD_TIMER(_profile, "SearchHashTableTime");
        _param.output_build_column_timer = ADD_TIMER(_profile, "OutputBuildColumnTime");
        _param.output_probe_column_timer = ADD_TIMER(_profile, "OutputProbeColumnTime");
        _param.probe_row_desc = _probe_row_desc.get();
        _param.build_row_desc = _build_row_desc.get();
        _param.probe_output_slots = {0};
        _param.build_output_slots = {1};
        _param.join_keys.emplace_back(JoinKeyDesc{&_bigint_type, false, nullptr});
    }

    std::shared_ptr<RuntimeState> _state;
    std::shared_ptr<RuntimeProfile> _profile;
    std::unique_ptr<ObjectPool> _pool;
    TypeDescriptor _bigint_type;
    std::shared_ptr<RowDescriptor> _probe_row_desc;
    std::shared_ptr<RowDescriptor> _build_row_desc;
    HashTableParam _param;
};

// ============================================================================
// Key streams.
//   build: `num_distinct` keys in [0, num_distinct), each repeated `depth`
//          times -> row_count = num_distinct * depth, keys_per_bucket ~= depth.
//   probe: kBenchChunkSize-row chunks; with probability hit_rate a key is drawn
//          from [0, num_distinct) (a guaranteed hit), else from
//          [num_distinct, 2*num_distinct) (a guaranteed miss -> no chain walk).
// ============================================================================
static ChunkPtr build_chunk_with_depth(int64_t num_distinct, int64_t depth) {
    auto col = Int64Column::create();
    auto& data = col->get_data();
    data.reserve(num_distinct * depth);
    // Interleave the repeats (d outer, key inner) so equal keys are NOT adjacent
    // in build order -- adjacency would make the chain links sequential in
    // `next[]` and understate the random-walk miss cost we are measuring.
    for (int64_t d = 0; d < depth; ++d) {
        for (int64_t k = 0; k < num_distinct; ++k) {
            data.push_back(k);
        }
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(col), 1); // build tuple slot id 1
    return chunk;
}

static std::vector<ChunkPtr> probe_chunks(int64_t total_rows, int64_t num_distinct, int hit_rate_pct, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> hit(0, num_distinct - 1);
    std::uniform_int_distribution<int64_t> miss(num_distinct, 2 * num_distinct - 1);
    std::uniform_int_distribution<int> coin(0, 99);

    const int64_t num_chunks = (total_rows + kBenchChunkSize - 1) / kBenchChunkSize;
    std::vector<ChunkPtr> chunks;
    chunks.reserve(num_chunks);
    for (int64_t c = 0; c < num_chunks; ++c) {
        auto col = Int64Column::create();
        auto& data = col->get_data();
        data.resize(kBenchChunkSize);
        for (int i = 0; i < kBenchChunkSize; ++i) {
            data[i] = coin(rng) < hit_rate_pct ? hit(rng) : miss(rng);
        }
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(col), 0); // probe tuple slot id 0
        chunks.emplace_back(std::move(chunk));
    }
    return chunks;
}

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

// Drive a built table to completion over the probe stream, timed.  has_remain
// stays true while the current probe chunk still has matches to emit (the
// one-to-many overflow protocol, mirrored from SingleHashJoinProberImpl).
static int64_t drive_probe(JoinHashTable& ht, RuntimeState* rs, const std::vector<ChunkPtr>& pchunks,
                           benchmark::State& state) {
    int64_t total = 0;
    for (auto _ : state) {
        for (const auto& pc : pchunks) {
            ChunkPtr probe_chunk = pc;
            Columns key_cols{probe_chunk->columns()[0]};
            bool has_remain = true;
            while (has_remain) {
                ChunkPtr result = std::make_shared<Chunk>();
                auto st = ht.probe(rs, key_cols, &probe_chunk, &result, &has_remain);
                if (!st.ok()) {
                    state.SkipWithError(st.to_string().c_str());
                    return total;
                }
                benchmark::DoNotOptimize(result.get());
            }
            total += kBenchChunkSize;
        }
        benchmark::ClobberMemory();
    }
    return total;
}

// ============================================================================
// Main bench.
// args: {method, num_distinct, depth, hit_rate_pct, group_size}
// ============================================================================
static void BM_JoinProbe(benchmark::State& state) {
    const auto method = static_cast<Method>(state.range(0));
    const int64_t num_distinct = state.range(1);
    const int64_t depth = state.range(2);
    const int hit_rate_pct = static_cast<int>(state.range(3));
    const int32_t group_size = static_cast<int32_t>(state.range(4));

    // Layer-1 bucket prefetch stays at the shipped default here; this BM drives
    // the chain-walk path (Layer-2).  Reset it explicitly so a prior run of
    // BM_JoinBucketPrefetch (same process) cannot leak a swept value in.
    config::hash_map_prefetch_dist = 16;

    JoinHarness harness;
    harness.set_up(method, group_size);

    // 1. Build phase (NOT timed).
    JoinHashTable ht;
    ht.create(harness.param());
    auto bchunk = build_chunk_with_depth(num_distinct, depth);
    Columns build_keys{bchunk->columns()[0]};
    ht.append_chunk(bchunk, build_keys);
    CHECK(ht.build(harness.state()).ok());

    // Capture the heuristic's own verdict, then override it so the coroutine
    // path is taken for ALL footprints when group_size > 0 (we are measuring
    // what the gate SHOULD decide, not what its staircase currently says).
    const bool heuristic_serious = ht.table_items()->ht_cache_miss_serious();
    ht.table_items()->cache_miss_serious = (group_size > 0);

    const std::string method_str = ht.get_hash_map_type();
    const uint32_t row_count = ht.get_row_count();
    const float keys_per_bucket = ht.get_keys_per_bucket();
    const size_t bucket_size = ht.get_bucket_size();

    // 2. Pre-build the probe stream once (column construction paid outside loop).
    auto pchunks = probe_chunks(kTimedProbeRows, num_distinct, hit_rate_pct, 0xBEEFCAFEULL);
    for (const auto& pc : pchunks) {
        const auto* col = down_cast<const Int64Column*>(pc->columns()[0].get());
        benchmark::DoNotOptimize(col->immutable_data().data());
    }

    const int64_t total_probe_rows = drive_probe(ht, harness.state(), pchunks, state);

    // probe_bytes is the exact quantity cache_miss_serious() keys on:
    //   key_bytes (BIGINT build keys) + row_count * sizeof(uint32_t) (next[]).
    const int64_t probe_bytes = static_cast<int64_t>(row_count) * sizeof(int64_t) + row_count * sizeof(uint32_t);
    // The chain walk also randomly touches first[] (bucket_size * 4) -- report
    // it so the true resident footprint can be read alongside the proxy.
    const int64_t first_bytes = static_cast<int64_t>(bucket_size) * sizeof(uint32_t);

    state.counters["method"] = static_cast<int>(method);
    state.counters["num_distinct"] = num_distinct;
    state.counters["depth"] = depth;
    state.counters["hit_rate_pct"] = hit_rate_pct;
    state.counters["group_size"] = group_size;
    state.counters["row_count"] = row_count;
    state.counters["keys_per_bucket"] = keys_per_bucket;
    state.counters["probe_bytes"] = static_cast<double>(probe_bytes);
    state.counters["first_bytes"] = static_cast<double>(first_bytes);
    state.counters["heuristic_serious"] = heuristic_serious ? 1 : 0;
    // ns per chain node walked: only hits walk the chain, ~depth nodes each.
    const double nodes_per_iter = static_cast<double>(kTimedProbeRows) * (hit_rate_pct / 100.0) * depth;
    state.counters["nodes_per_iter"] = nodes_per_iter;
    // method_str surfaced via label so a wrong selector pick is visible -- the
    // selector falls LINEAR_CHAINED back to BUCKET_CHAINED once bucket_size
    // exceeds 0xFFFFFF (~16M rows), so the requested method is not guaranteed.
    state.SetLabel(method_str);
    state.SetItemsProcessed(total_probe_rows);
}

// ============================================================================
// Argument matrix.
// num_distinct picks the footprint band (probe_bytes ~= num_distinct*depth*12):
//   64Ki  @ depth 1 ~= 0.75 MiB  -> ~L2
//   1Mi   @ depth 1 ~= 12 MiB    -> L2..L3
//   8Mi   @ depth 1 ~= 96 MiB    -> >L3 (DRAM)
// depth sweeps the staircase's second axis (keys_per_bucket); depth 1 rows are
// the clean per-lookup cost for the cost model.  group_size 0 = plain-walk
// baseline; > 0 = coroutine counts.  hit_rate 100 isolates the walk; 50 mixes
// in misses (no walk) the way a real probe does.
//
// Two caps keep the matrix sane:
//   - row_count (num_distinct*depth) <= 16Mi bounds per-combo memory (~0.5 GiB)
//     and build time (build is untimed but still runs per combo).
//   - LINEAR_CHAINED is rejected by the selector once bucket_size > 0xFFFFFF
//     (~16M rows), falling back to BUCKET_CHAINED -- faithful to production, and
//     why the deep-DRAM band is only reachable as BUCKET_CHAINED.  The label
//     reports the method actually chosen.
// ============================================================================
static void RegisterArgs(benchmark::Benchmark* b) {
    constexpr int methods[] = {static_cast<int>(Method::LinearChained), static_cast<int>(Method::BucketChained)};
    constexpr int64_t distincts[] = {64 * 1024, 1024 * 1024, 8 * 1024 * 1024};
    constexpr int64_t depths[] = {1, 4, 16};
    constexpr int hit_rates[] = {50, 100};
    constexpr int group_sizes[] = {0, 4, 8, 16, 32};
    constexpr int64_t kMaxRows = 16LL * 1024 * 1024;
    for (auto m : methods) {
        for (auto nd : distincts) {
            for (auto dep : depths) {
                if (nd * dep > kMaxRows) continue;
                for (auto hr : hit_rates) {
                    for (auto gs : group_sizes) {
                        b->Args({m, nd, dep, hr, gs});
                    }
                }
            }
        }
    }
}

BENCHMARK(BM_JoinProbe)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

// ============================================================================
// Layer-1 bench: bucket-resolution prefetch distance (config
// hash_map_prefetch_dist), with the L2 gate disabled (ratio 0 from set_up) so
// the raw distance curve is what gets measured. Isolated from the chain walk:
// LINEAR_CHAINED only (the sole method with this prefetch), group_size 0 (no
// coroutine), depth 1 (one chain node, so lookup_init's bucket access
// dominates), hit_rate 100. The footprint that matters is first[] (bucket_size
// * 4), reported as first_bytes -- reading the distance curve against L1/L2/L3
// is what set the join gate's ratio (the band below which dist 0 beats 16).
// args: {num_distinct, dist}
static void BM_JoinBucketPrefetch(benchmark::State& state) {
    const int64_t num_distinct = state.range(0);
    const int32_t dist = static_cast<int32_t>(state.range(1));

    config::hash_map_prefetch_dist = dist;

    JoinHarness harness;
    harness.set_up(Method::LinearChained, /*group_size=*/0);

    JoinHashTable ht;
    ht.create(harness.param());
    auto bchunk = build_chunk_with_depth(num_distinct, /*depth=*/1);
    Columns build_keys{bchunk->columns()[0]};
    ht.append_chunk(bchunk, build_keys);
    CHECK(ht.build(harness.state()).ok());

    const std::string method_str = ht.get_hash_map_type();
    const uint32_t row_count = ht.get_row_count();
    const size_t bucket_size = ht.get_bucket_size();

    auto pchunks = probe_chunks(kTimedProbeRows, num_distinct, /*hit_rate_pct=*/100, 0xBEEFCAFEULL);
    for (const auto& pc : pchunks) {
        const auto* col = down_cast<const Int64Column*>(pc->columns()[0].get());
        benchmark::DoNotOptimize(col->immutable_data().data());
    }

    const int64_t total_probe_rows = drive_probe(ht, harness.state(), pchunks, state);

    const int64_t first_bytes = static_cast<int64_t>(bucket_size) * sizeof(uint32_t);
    state.counters["dist"] = dist;
    state.counters["num_distinct"] = num_distinct;
    state.counters["row_count"] = row_count;
    state.counters["first_bytes"] = static_cast<double>(first_bytes);
    attach_cpu_cache_counters(state);
    state.SetLabel(method_str);
    state.SetItemsProcessed(total_probe_rows);
}

static void RegisterBucketArgs(benchmark::Benchmark* b) {
    // Small end is the point: first[] L1/L2-resident, where the drop-to-0
    // question lives.  Capped below the LINEAR_CHAINED bucket_size limit.
    constexpr int64_t distincts[] = {4096, 16384, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024};
    constexpr int dists[] = {0, 4, 8, 16, 24, 32};
    for (auto nd : distincts) {
        for (auto d : dists) {
            b->Args({nd, d});
        }
    }
}

BENCHMARK(BM_JoinBucketPrefetch)->Apply(RegisterBucketArgs)->Unit(benchmark::kMillisecond);

// ============================================================================
// Concurrency bench: how coroutine count interacts with cross-driver contention.
// The single-thread sweep says "more coroutines = faster" up to >=32, but each
// coroutine holds an outstanding memory request, so N drivers x group_size all
// compete for the same finite memory-level parallelism / LLC bandwidth.  This
// drives the open question on interleaving_group_size: does the optimum drop as
// the number of concurrent probe drivers per box rises?
//
// One build table is fanned out to num_threads readable clones (each shares the
// built table, has its own probe state -- exactly how the pipeline hands a build
// to N probers), each thread probes its own stream.  Fixed at the winning shape
// (deep chains in DRAM) since that is where coroutines matter.  UseRealTime() so
// the reported time is wall-clock -- items/s is then aggregate throughput across
// all threads, and per-thread throughput = that / num_threads.
inline constexpr int64_t kConcProbeRowsPerThread = 4'000'000;

static void probe_stream_once(JoinHashTable& ht, RuntimeState* rs, const std::vector<ChunkPtr>& pchunks) {
    for (const auto& pc : pchunks) {
        ChunkPtr probe_chunk = pc;
        Columns key_cols{probe_chunk->columns()[0]};
        bool has_remain = true;
        while (has_remain) {
            ChunkPtr result = std::make_shared<Chunk>();
            (void)ht.probe(rs, key_cols, &probe_chunk, &result, &has_remain);
            benchmark::DoNotOptimize(result.get());
        }
    }
}

// args: {group_size, num_threads}
static void BM_JoinProbeConcurrent(benchmark::State& state) {
    const int32_t group_size = static_cast<int32_t>(state.range(0));
    const int num_threads = static_cast<int>(state.range(1));
    const int64_t num_distinct = 1024 * 1024; // x depth 16 = 16Mi rows ~200MB, DRAM
    const int64_t depth = 16;

    JoinHarness harness;
    harness.set_up(Method::BucketChained, group_size);

    JoinHashTable ht;
    ht.create(harness.param());
    auto bchunk = build_chunk_with_depth(num_distinct, depth);
    Columns build_keys{bchunk->columns()[0]};
    ht.append_chunk(bchunk, build_keys);
    CHECK(ht.build(harness.state()).ok());
    ht.table_items()->cache_miss_serious = true; // force coroutine path (shared by all clones)

    // Fan the one build table out to num_threads readable clones + independent streams.
    std::vector<std::vector<ChunkPtr>> streams(num_threads);
    std::vector<JoinHashTable> clones;
    clones.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        streams[t] = probe_chunks(kConcProbeRowsPerThread, num_distinct, /*hit_rate_pct=*/100, 0xBEEFULL + t);
        clones.emplace_back(ht.clone_readable_table());
    }

    int64_t total_rows = 0;
    for (auto _ : state) {
        std::vector<std::thread> workers;
        workers.reserve(num_threads);
        for (int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t] { probe_stream_once(clones[t], harness.state(), streams[t]); });
        }
        for (auto& w : workers) w.join();
        total_rows += static_cast<int64_t>(num_threads) * kConcProbeRowsPerThread;
    }

    state.counters["group_size"] = group_size;
    state.counters["num_threads"] = num_threads;
    state.counters["rows_per_thread_iter"] = kConcProbeRowsPerThread;
    attach_cpu_cache_counters(state);
    state.SetItemsProcessed(total_rows);
    state.SetLabel(ht.get_hash_map_type());
}

static void RegisterConcArgs(benchmark::Benchmark* b) {
    constexpr int group_sizes[] = {0, 8, 16, 32};
    constexpr int threads[] = {1, 2, 4, 8, 16, 32};
    for (auto g : group_sizes) {
        for (auto t : threads) {
            b->Args({g, t});
        }
    }
}

BENCHMARK(BM_JoinProbeConcurrent)->Apply(RegisterConcArgs)->Unit(benchmark::kMillisecond)->UseRealTime();

} // namespace starrocks

BENCHMARK_MAIN();
