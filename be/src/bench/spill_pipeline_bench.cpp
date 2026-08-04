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

// L3 (Spiller-level) spill pipeline micro-benchmark.
//
// Drives the real data-plane chain Spiller::spill -> flush -> restore through a
// SyncExecutor (single-threaded, mirrors the unit test), bypassing pipeline/workgroup
// scheduling. It does NOT measure absolute throughput; it measures the COST BREAKDOWN --
// what fraction of total spill time is serialize (the Amdahl coefficient that bounds the
// payoff of any L1/L2 encoding improvement).
//
// The breakdown is read straight from SpillProcessMetrics' existing timers
// (serialize/write_io/flush/deserialize/mem_table_finalize/...); no custom instrumentation.
//
// Two disk tiers: real disk (DirManager + LogBlockManager container management) and tmpfs
// (/dev/shm, isolates the disk variable -> pure-CPU upper bound). Unordered raw writer
// only (join-build shape); iteration count is fixed to keep on-disk spill data bounded.
//
// NOTE: bench targets are built with -fno-access-control (see ADD_BE_BENCH), so the
// SpillerCaller below may touch Spiller private members exactly as the unit test does.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/hash/crc32c.h"
#include "base/uid_util.h"
#include "bench/spill_bench_data.h"
#include "column/chunk.h"
#include "column/serde/column_array_serde.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/logging.h"
#include "common/runtime_profile.h"
#include "common/statusor.h"
#include "compute_env/spill/codec/codec_selector.h"
#include "compute_env/spill/dir_manager.h"
#include "compute_env/spill/log_block_manager.h"
#include "compute_env/spill/mem_table.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller.hpp"
#include "compute_env/spill/spiller_factory.h"
#include "compute_env/workgroup/scan_task.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks {

using namespace spill_bench;

// Single-threaded synchronous task executor (identical to the unit test's SyncExecutor).
struct SyncExecutor {
    static Status submit(workgroup::ScanTask task) {
        do {
            task.run();
        } while (!task.is_finished());
        return Status::OK();
    }
    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }
};

struct EmptyMemGuard {
    bool scoped_begin() const { return true; }
    void scoped_end() const {}
};

// Reaches into Spiller internals to drive the writer/reader directly, like the unit test.
template <class Writer, class Reader>
struct SpillerCaller {
    SpillerCaller(spill::Spiller* spiller) : _spiller(spiller) {}

    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard) {
        if (_spiller->_chunk_builder.chunk_schema()->empty()) {
            _spiller->_chunk_builder.chunk_schema()->set_schema(chunk);
            RETURN_IF_ERROR(_spiller->_serde->prepare());
        }
        auto writer = _spiller->_writer->as<Writer>();
        return writer->template spill<TaskExecutor>(state, chunk, std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, MemGuard&& guard) {
        auto writer = _spiller->_writer->as<Writer>();
        return writer->template flush<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, MemGuard&& guard) {
        return _spiller->_reader->restore<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, MemGuard&& guard) {
        if (!acquire_once) {
            acquire_once = true;
            RETURN_IF_ERROR(_spiller->_acquire_input_stream(state));
        }
        return _spiller->_reader->trigger_restore<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    bool acquire_once = false;
    spill::Spiller* _spiller;
};

static std::vector<DataSet> pipeline_datasets() {
    // Full decision-tree matrix: this is a baseline of current spill efficiency per data type.
    std::vector<DataSet> v;
    for (const auto& info : all_datasets()) v.push_back(info.id);
    return v;
}

// One column-encoding mode to measure:
//   raw = no column encoding (level 0), v1 = legacy encoding (level 7), v2 = adaptive selector.
struct Combo {
    const char* name;
    bool selector; // spill_enable_codec_selector (v2)
    int level;     // encode_level, ignored when selector is on
};
static const std::vector<Combo>& combos() {
    static const std::vector<Combo> v = {
            {"raw", false, 0},
            {"v1", false, 7},
            {"v2", true, 7},
    };
    return v;
}

// All datasets spill to real local disk (the only tier that matters for ratio + time).
// Override with SPILL_BENCH_BLOCK_ROOT to point at a specific device.
static std::string block_root() {
    const char* env = getenv("SPILL_BENCH_BLOCK_ROOT");
    if (env != nullptr) return env;
    return (std::filesystem::temp_directory_path() / "spill_bench_blocks").string();
}

static double ns(RuntimeProfile::Counter* c) {
    return c != nullptr ? static_cast<double>(c->value()) : 0.0;
}

// Some spill counters are split local/remote by GET_METRICS (write_io_timer, read_io_timer,
// flush_bytes): the parent is never incremented directly, so prefer the local child and fall
// back to the parent. This bench always spills to local disk.
static double ns_local(RuntimeProfile::Counter* local, RuntimeProfile::Counter* parent) {
    if (local != nullptr && local->value() > 0) return static_cast<double>(local->value());
    return parent != nullptr ? static_cast<double>(parent->value()) : 0.0;
}

// Runs one full spill -> flush -> restore cycle with a fresh spiller and a fresh block dir.
// Spills chunks by cycling `chunks` until at least `target_bytes` (raw) has been spilled, so a
// modest distinct pool can drive a large spill volume. Leaves timers populated on `metrics`.
static Status run_cycle(const std::vector<ChunkPtr>& chunks, size_t target_bytes, int encode_level,
                        const std::string& block_dir, RuntimeState* rt, spill::SpillProcessMetrics& metrics,
                        size_t* out_raw_bytes = nullptr) {
    auto fs = FileSystem::Default();
    RETURN_IF_ERROR(fs->create_dir_recursive(block_dir));

    TUniqueId qid = generate_uuid();
    spill::DirManager dir_mgr;
    // init(spill_dir, storage_roots): spill_dir must NOT appear in storage_roots (exact-match
    // exclusion). We have no BE storage roots here, so pass an empty list.
    RETURN_IF_ERROR(dir_mgr.init(block_dir, {}));
    spill::LogBlockManager block_mgr(qid, &dir_mgr);

    spill::SpilledOptions options;
    // Mirror the production defaults: operators fill these from the session variables
    // spill_mem_table_num=2 / spill_mem_table_size=100MB (SessionVariable.java).
    options.mem_table_pool_size = 2;
    options.spill_mem_table_bytes_size = 100 * 1024 * 1024;
    options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    options.encode_level = encode_level;
    options.block_manager = &block_mgr;

    auto factory = spill::make_spilled_factory();
    auto spiller = factory->create(options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    RETURN_IF_ERROR(spiller->prepare(rt));

    size_t spilled = 0;
    size_t i = 0;
    while (spilled < target_bytes && !chunks.empty()) {
        const ChunkPtr& chunk = chunks[i % chunks.size()];
        RETURN_IF_ERROR(caller.spill<SyncExecutor>(rt, chunk, EmptyMemGuard{}));
        RETURN_IF_ERROR(spiller->_spilled_task_status);
        for (const auto& col : chunk->columns()) spilled += col->byte_size();
        ++i;
    }
    RETURN_IF_ERROR(caller.flush<SyncExecutor>(rt, EmptyMemGuard{}));

    RETURN_IF_ERROR(caller.trigger_restore<SyncExecutor>(rt, EmptyMemGuard{}));
    while (true) {
        auto st = caller.restore<SyncExecutor>(rt, EmptyMemGuard{});
        if (st.status().is_end_of_file()) break;
        RETURN_IF_ERROR(st.status());
        RETURN_IF_ERROR(spiller->_spilled_task_status);
        if (st.value() == nullptr) break;
    }
    if (out_raw_bytes != nullptr) *out_raw_bytes = spilled;
    return Status::OK();
}

// The chunk pool is LOADED from the frozen on-disk dataset (spill_bench_datagen v2 output),
// so before/after runs of a spill optimization consume byte-identical inputs. The file's
// frames-crc32c is verified on load. Only the current dataset's pool is kept in memory
// (cases are registered grouped by dataset, so this caches perfectly).
static constexpr uint32_t kFrozenMagic = 0x31425053; // "SPB1"
static constexpr size_t kFrozenHeader = 6 * sizeof(uint32_t);

// Frozen datasets produced by spill_bench_datagen; override with SPILL_BENCH_DATA_DIR.
static std::string frozen_data_dir() {
    const char* env = getenv("SPILL_BENCH_DATA_DIR");
    if (env != nullptr) return env;
    return (std::filesystem::temp_directory_path() / "spill_bench_datasets").string();
}

struct Workload {
    std::vector<ChunkPtr> chunks;
    size_t total_raw = 0;
};

static Status load_frozen(DataSet ds, Workload* out) {
    std::string path = frozen_data_dir() + "/" + dataset_name(ds) + ".spb";
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        return Status::NotFound(fmt::format("frozen dataset not found: {} (run spill_bench_datagen first)", path));
    }
    uint32_t hdr[6];
    in.read(reinterpret_cast<char*>(hdr), sizeof(hdr));
    if (!in.good() || hdr[0] != kFrozenMagic || hdr[1] != 1) {
        return Status::Corruption(fmt::format("bad frozen dataset header: {}", path));
    }
    const uint32_t chunk_count = hdr[2];
    const uint32_t rows_per_chunk = hdr[3];
    const uint32_t expect_crc = hdr[4];

    // schema template (values come from disk; only column types/layout come from the factory)
    GenConfig cfg;
    cfg.num_rows = rows_per_chunk;
    ChunkPtr templ = build_chunk(ds, cfg);

    uint32_t crc = 0;
    std::string buf;
    out->chunks.reserve(chunk_count);
    for (uint32_t c = 0; c < chunk_count; ++c) {
        uint32_t n = 0;
        in.read(reinterpret_cast<char*>(&n), sizeof(n));
        if (!in.good()) return Status::Corruption(fmt::format("truncated frame header, chunk {}: {}", c, path));
        buf.resize(n);
        in.read(buf.data(), n);
        if (!in.good()) return Status::Corruption(fmt::format("truncated frame payload, chunk {}: {}", c, path));
        crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&n), sizeof(n));
        crc = crc32c::Extend(crc, buf.data(), n);

        auto chunk = std::make_shared<Chunk>();
        const auto* cur = reinterpret_cast<const uint8_t*>(buf.data());
        const auto* end = cur + n;
        SlotId slot = 0;
        for (const auto& col : templ->columns()) {
            MutableColumnPtr target = col->clone_empty();
            ASSIGN_OR_RETURN(cur, serde::ColumnArraySerde::deserialize(cur, end, target.get(), false, 0));
            chunk->append_column(std::move(target), slot++);
        }
        for (const auto& col : chunk->columns()) out->total_raw += col->byte_size();
        out->chunks.push_back(std::move(chunk));
    }
    if (crc != expect_crc) {
        return Status::Corruption(
                fmt::format("frozen dataset crc mismatch: {} (got {:08x}, want {:08x})", path, crc, expect_crc));
    }
    return Status::OK();
}

static Workload& get_workload(DataSet ds) {
    // keep only the current dataset resident (~256 MB); cases run grouped by dataset
    static int cached_ds = -1;
    static Workload cached;
    if (cached_ds == static_cast<int>(ds)) return cached;
    cached = Workload{};
    Status st = load_frozen(ds, &cached);
    if (!st.ok()) {
        LOG(FATAL) << "load frozen dataset failed: " << st;
    }
    cached_ds = static_cast<int>(ds);
    return cached;
}

static void BM_SpillPipeline(benchmark::State& state) {
    const DataSet ds = pipeline_datasets()[state.range(0)];
    const Combo& combo = combos()[state.range(1)];

    // Chunk pool cached per dataset. By default each case spills the pool exactly once (target =
    // the dataset's raw size); SPILL_BENCH_TARGET_GB cycles the pool up to a production-scale
    // spill volume instead (per-chunk codec behaviour and ratios are unaffected by the pool
    // repeating -- selection and compression never look across chunks).
    Workload& wl = get_workload(ds);
    const std::vector<ChunkPtr>& chunks = wl.chunks;
    size_t target_bytes = wl.total_raw;
    if (const char* g = getenv("SPILL_BENCH_TARGET_GB"); g != nullptr && atof(g) > 0) {
        target_bytes = static_cast<size_t>(atof(g) * 1024 * 1024 * 1024);
    }

    // Flip the switch for this combo (numeric knobs are set once in main()).
    config::spill_enable_codec_selector = combo.selector;

    RuntimeState rt;
    rt.set_chunk_size(config::vector_chunk_size);
    // Optional: route spill writes through O_DIRECT (production spill_enable_direct_io path) to
    // bypass the page cache and measure true device behaviour.
    if (const char* v = getenv("SPILL_BENCH_DIRECT_IO"); v != nullptr && v[0] == '1') {
        rt._query_options.spill_enable_direct_io = true;
    }

    std::string parent = block_root() + "/" + print_id(generate_uuid());
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);

    // Each timing is collected per iteration and reduced by median (Iterations(N) denoises the
    // per-run disk/scheduling jitter). Ratio is deterministic, so the last pass is representative.
    std::vector<double> v_serialize, v_write, v_flush, v_deser, v_read;
    double raw_bytes = 0, flushed = 0;
    size_t iter = 0;
    for (auto _ : state) {
        RuntimeProfile profile{"spill"};
        std::atomic_int64_t bytes{0};
        spill::SpillProcessMetrics metrics(&profile, &bytes);
        std::string block_dir = parent + "/it" + std::to_string(iter++);
        size_t rb = 0;
        auto st = run_cycle(chunks, target_bytes, combo.level, block_dir, &rt, metrics, &rb);
        if (!st.ok()) {
            state.SkipWithError(std::string(st.message()).c_str());
            break;
        }
        v_serialize.push_back(ns(metrics.serialize_timer));
        // write_io_timer / read_io_timer / flush_bytes are split local/remote by GET_METRICS --
        // the parent is never incremented, so read the local child (this bench spills locally).
        v_write.push_back(ns_local(metrics.local_write_io_timer, metrics.write_io_timer));
        v_flush.push_back(ns(metrics.flush_timer));
        v_deser.push_back(ns(metrics.deserialize_timer));
        v_read.push_back(ns_local(metrics.local_read_io_timer, metrics.read_io_timer));
        raw_bytes = static_cast<double>(rb);
        flushed = ns_local(metrics.local_flush_bytes, metrics.flush_bytes);
        std::error_code rc;
        std::filesystem::remove_all(block_dir, rc);
        benchmark::ClobberMemory();
    }
    std::filesystem::remove_all(parent, ec);

    auto median = [](std::vector<double> v) -> double {
        if (v.empty()) return 0.0;
        std::sort(v.begin(), v.end());
        return v[v.size() / 2];
    };
    // Table 3 (spiller time) = compress(serialize) + write(write_io) + read(read_io) + decompress(deser).
    state.counters["serialize_ns"] = median(v_serialize);
    state.counters["write_io_ns"] = median(v_write);
    state.counters["flush_ns"] = median(v_flush);
    state.counters["read_io_ns"] = median(v_read);
    state.counters["deserialize_ns"] = median(v_deser);
    // Tables 1 & 2 (compression ratio) = on-disk flushed bytes / raw in-memory bytes (lower=better).
    state.counters["raw_bytes"] = raw_bytes;
    state.counters["flush_bytes"] = flushed;
    state.counters["ratio"] = raw_bytes > 0 ? flushed / raw_bytes : 0.0;
}

static void register_all() {
    const auto& datasets = pipeline_datasets();
    const auto& cs = combos();
    // Iterations per case: default 1 (an O_DIRECT pass is IO-dominated and stable); raise via
    // SPILL_BENCH_ITERS for median-denoised buffered runs where per-pass jitter matters.
    int iters = 1;
    if (const char* v = getenv("SPILL_BENCH_ITERS"); v != nullptr && atoi(v) > 0) iters = atoi(v);
    // Full matrix: every dataset x every combo, one process, one pass each.
    for (size_t d = 0; d < datasets.size(); ++d) {
        for (size_t c = 0; c < cs.size(); ++c) {
            std::string name = std::string("BM_Spill/") + dataset_name(datasets[d]) + "/" + cs[c].name;
            benchmark::RegisterBenchmark(name.c_str(), &BM_SpillPipeline)
                    ->Args({static_cast<int64_t>(d), static_cast<int64_t>(c)})
                    ->Iterations(iters)
                    ->Unit(benchmark::kMillisecond);
        }
    }
}

} // namespace starrocks

int main(int argc, char** argv) {
    namespace config = starrocks::config;
    // The bench never calls config::init(), so numeric spill-codec configs would read as 0. Set
    // the ones the selector depends on to their production defaults here (the per-combo on/off
    // switch is flipped inside the benchmark body). SPILL_BENCH_DISK_MBPS overrides the cost
    // model's CPU-vs-bytes exchange rate, which is what a W sweep varies.
    config::spill_codec_disk_bandwidth_mbps = 100;
    if (const char* m = getenv("SPILL_BENCH_DISK_MBPS"); m != nullptr) {
        config::spill_codec_disk_bandwidth_mbps = atof(m);
    }
    // stderr, not stdout: --benchmark_format=json writes the report to stdout.
    fprintf(stderr, "[spill_pipeline_bench] disk=%.0fMB/s\n", (double)config::spill_codec_disk_bandwidth_mbps);

    ::benchmark::Initialize(&argc, argv);
    starrocks::register_all();
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
