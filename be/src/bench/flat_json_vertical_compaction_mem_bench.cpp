// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Purpose
// -------
// Reproduce and measure the high memory usage of lake vertical compaction when
// incompatible Flat JSON inputs are read through JsonMergeIterator. The benchmark
// runs the real storage writer, segment reader, and VerticalCompactionTask; it is
// intended to validate a compaction memory mechanism, not JSON parsing speed.
//
// Two benchmarks compact the same generated rows:
//
//   BM_JsonMergeIterator  stores every input as Flat JSON and forces the production
//                         read path to reconstruct full JSON with JsonMergeIterator.
//   BM_NonFlat            stores the same JSON without Flat JSON and provides the
//                         ScalarColumnIterator control case.
//
// With the default customer-scale input (49 rowsets, 99,470 rows, and a 76,800-byte
// string in remaining JSON), BM_JsonMergeIterator should use substantially more
// compaction memory. A representative run measured about 16.7 GB versus 3.2 GB for
// BM_NonFlat. Exact process RSS depends on the allocator and machine.
//
// Build and run
// -------------
// From the repository root, with an existing Release build:
//
//   cmake --build be/build_Release --target flat_json_vertical_compaction_mem_bench -j 16
//   export STARROCKS_HOME="$PWD"
//   BENCH=be/build_Release/src/bench/output/flat_json_vertical_compaction_mem_bench
//
// Run each mode in a separate process so allocator retention cannot affect the
// comparison:
//
//   "$BENCH" --benchmark_filter=BM_JsonMergeIterator
//   "$BENCH" --benchmark_filter=BM_NonFlat
//
// The default input is intentionally large. Use smaller values for a quick check:
//
//   export SR_FLAT_JSON_BENCH_INPUT_ROWSET_COUNT=4
//   export SR_FLAT_JSON_BENCH_ROWS_PER_INPUT_ROWSET=8
//   export SR_FLAT_JSON_BENCH_REMAINING_JSON_STRING_BYTES=1024
//   "$BENCH" --benchmark_filter=BM_JsonMergeIterator
//   unset SR_FLAT_JSON_BENCH_INPUT_ROWSET_COUNT SR_FLAT_JSON_BENCH_ROWS_PER_INPUT_ROWSET
//   unset SR_FLAT_JSON_BENCH_REMAINING_JSON_STRING_BYTES
//
// To capture jemalloc profiles during compaction:
//
//   export JEMALLOC_CONF='prof:true,prof_active:false,lg_prof_sample:19'
//   export SR_FLAT_JSON_BENCH_HEAP_PROFILE=1
//   export SR_FLAT_JSON_BENCH_PROFILE_DIR="$PWD/flat_json_bench_profile"
//   "$BENCH" --benchmark_filter=BM_JsonMergeIterator
//
// Profiling takes one snapshot every three seconds and keeps only the snapshot
// with the highest compaction memory. The retained file is printed as the
// heap_profile label.
//
// Reading the result
// ------------------
//   input_rowset_count                    Number of input rowsets/segments.
//   total_rows                            Total generated rows.
//   iterator_read_chunk_size              Rows requested from each input iterator.
//   compaction_execution_time_ms           Wall time of VerticalCompactionTask::execute().
//   compaction_peak_memory_bytes          VerticalCompactionTask MemTracker peak.
//   process_rss_peak_bytes                Highest process RSS during compaction.
//   heap_profile_compaction_memory_bytes  Task memory for the retained profile.
//   heap_profile_process_rss_bytes        Process RSS for the retained profile.
//   heap_profile                          Path of the retained profile (result label).
//
// compaction_execution_time_ms isolates the execute() call from task construction
// and sampler startup/shutdown. Google Benchmark's Time and CPU columns cover the
// complete run_compaction call. Heap dumps can slow the compaction itself, so use a
// non-profiled run for timing comparisons; profile-enabled timing is diagnostic only.
//
// The last two counters and the heap_profile label appear only when profiling is
// enabled. Tablet creation and input writing happen before the timed compaction, so
// the reported memory values describe the compaction window rather than data setup.
//
// How the target path is constructed
// ----------------------------------
// A regular row in 48 of the input rowsets looks like this (only some of the 15
// primitive paths are shown):
//
//   {
//     "session_id": "session-id",
//     "org_id": "org-id",
//     "summary": {"objective": "objective"},
//     "covered_start_index": 1,
//     "messages": [{"content": "<76,800 row-specific bytes>"}]
//   }
//
// The Flat JSON writer stores primitive leaves in separate child columns and keeps
// the complex messages array in the remaining-JSON child column:
//
//   extracted children: session_id, org_id, summary.objective,
//                       covered_start_index, ... 11 more paths
//   remaining JSON:     {"messages": [{"content": "<large string>"}]}
//
// The first rowset contains one row with a deliberately different schema:
//
//   {
//     "path_only_in_single_rowset": 0,
//     "messages": [{"content": "<76,800 row-specific bytes>"}]
//   }
//
// Its Flat JSON children are path_only_in_single_rowset plus remaining JSON. The
// remaining-JSON column is payload storage, not an extracted schema path, so the
// extracted-path intersection across all 49 segments is empty:
//
//   no common extracted path
//       -> one JSON root access path with zero children
//       -> ColumnReader::_new_json_iterator sees children().empty()
//       -> JsonMergeIterator reconstructs full JSON from extracted + remaining data
//
// This exercises the expensive production read path without mixing Flat and
// non-Flat input segments. BM_NonFlat writes the identical documents and changes
// only their physical JSON encoding. Runtime checks fail the benchmark if the
// expected storage layout, access path, or iterator type changes.

#include <benchmark/benchmark.h>
#include <cxxabi.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <typeinfo>
#include <utility>
#include <vector>

#include "butil/file_util.h"
#include "cache/datacache.h"
#include "column/chunk.h"
#include "column/column_access_path.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "common/glog_init.h"
#include "common/metrics/process_metrics_registry.h"
#include "common/system/cpu_info.h"
#include "common/system/disk_info.h"
#include "common/system/mem_info.h"
#include "fmt/format.h"
#include "fs/fs_util.h"
#include "jemalloc/jemalloc.h"
#include "module/fs_provider_bootstrap.h"
#include "platform/platform_env.h"
#include "platform/user_function_cache.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/prof/heap_prof.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/compaction_utils.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/options.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/flat_json_config.h"
#include "types/time_types.h"

namespace starrocks::lake {

namespace {

constexpr int64_t kDefaultInputRowsetCount = 49;
constexpr int64_t kDefaultRowsPerInputRowset = 2030;
constexpr int64_t kDefaultRemainingJsonStringBytes = 76800;
constexpr int64_t kMainDocumentExtractedPathCount = 15;
constexpr int64_t kTabletSchemaColumnCount = 6;
constexpr auto kHeapProfileSnapshotInterval = std::chrono::seconds(3);
constexpr auto kMemorySampleInterval = std::chrono::milliseconds(100);

std::atomic<int64_t> g_next_id{1000000};

int64_t next_bench_id() {
    return g_next_id.fetch_add(1, std::memory_order_relaxed);
}

enum class JsonIteratorMode { kJsonMergeIterator, kNonFlat };

const char* iterator_mode_name(JsonIteratorMode iterator_mode) {
    return iterator_mode == JsonIteratorMode::kJsonMergeIterator ? "json_merge_iterator" : "non_flat";
}

struct BenchConfig {
    int64_t input_rowset_count = kDefaultInputRowsetCount;
    int64_t rows_per_input_rowset = kDefaultRowsPerInputRowset;
    int64_t remaining_json_string_bytes = kDefaultRemainingJsonStringBytes;
    bool heap_profile = false;
    std::string profile_dir;

    int64_t total_rows() const { return input_rowset_count * rows_per_input_rowset; }
};

StatusOr<int64_t> read_env_int64(const char* name, int64_t default_value, int64_t min_value, int64_t max_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end = nullptr;
    long long parsed = std::strtoll(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < min_value || parsed > max_value) {
        return Status::InvalidArgument(
                fmt::format("{} must be an integer in [{}, {}], got '{}'", name, min_value, max_value, value));
    }
    return static_cast<int64_t>(parsed);
}

bool read_env_bool(const char* name) {
    const char* value = std::getenv(name);
    return value != nullptr &&
           (std::string_view(value) == "1" || std::string_view(value) == "true" || std::string_view(value) == "TRUE");
}

StatusOr<BenchConfig> read_bench_config() {
    BenchConfig config;
    ASSIGN_OR_RETURN(config.input_rowset_count,
                     read_env_int64("SR_FLAT_JSON_BENCH_INPUT_ROWSET_COUNT", kDefaultInputRowsetCount, 2, 10000));
    ASSIGN_OR_RETURN(config.rows_per_input_rowset, read_env_int64("SR_FLAT_JSON_BENCH_ROWS_PER_INPUT_ROWSET",
                                                                  kDefaultRowsPerInputRowset, 2, INT32_MAX));
    ASSIGN_OR_RETURN(config.remaining_json_string_bytes,
                     read_env_int64("SR_FLAT_JSON_BENCH_REMAINING_JSON_STRING_BYTES", kDefaultRemainingJsonStringBytes,
                                    1, static_cast<int64_t>(kJSONLengthLimit) - 4096));
    if (config.input_rowset_count > std::numeric_limits<int64_t>::max() / config.rows_per_input_rowset) {
        return Status::InvalidArgument("input_rowset_count * rows_per_input_rowset overflows int64");
    }
    config.heap_profile = read_env_bool("SR_FLAT_JSON_BENCH_HEAP_PROFILE");
    const char* profile_dir = std::getenv("SR_FLAT_JSON_BENCH_PROFILE_DIR");
    if (profile_dir != nullptr) {
        config.profile_dir = profile_dir;
    }
    return config;
}

class BenchEnvironment {
public:
    ~BenchEnvironment() {
        if (_storage_engine != nullptr) {
            _storage_engine->stop();
            delete _storage_engine;
        }
        if (_data_cache_initialized) {
            DataCache::GetInstance()->destroy();
        }
        tls_thread_status.set_mem_tracker(nullptr);
        if (_runtime_env_initialized) {
            RuntimeEnv::GetInstance()->stop();
        }
        if (_platform_env_initialized) {
            PlatformEnv::GetInstance()->destroy();
        }
        if (_logging_initialized) {
            shutdown_logging();
        }
        if (!_storage_root.value().empty()) {
            (void)butil::DeleteFile(_storage_root, true);
        }
    }

    Status init(const char* argv0);

private:
    butil::FilePath _storage_root;
    StorageEngine* _storage_engine = nullptr;
    bool _logging_initialized = false;
    bool _platform_env_initialized = false;
    bool _runtime_env_initialized = false;
    bool _data_cache_initialized = false;
};

Status BenchEnvironment::init(const char* argv0) {
    const char* starrocks_home = std::getenv("STARROCKS_HOME");
    if (starrocks_home == nullptr) {
        return Status::InvalidArgument("STARROCKS_HOME must point to the BE runtime directory");
    }

    if (!butil::CreateNewTempDirectory("flat_json_compaction_bench_", &_storage_root)) {
        return Status::IOError("failed to create benchmark storage root");
    }
    if (std::getenv("UDF_RUNTIME_DIR") == nullptr) {
        const auto udf_runtime_dir = _storage_root.Append("udf-runtime");
        if (!butil::CreateDirectory(udf_runtime_dir)) {
            return Status::IOError(fmt::format("failed to create UDF runtime directory {}", udf_runtime_dir.value()));
        }
        if (setenv("UDF_RUNTIME_DIR", udf_runtime_dir.value().c_str(), 0) != 0) {
            return Status::IOError(fmt::format("failed to set UDF_RUNTIME_DIR: {}", std::strerror(errno)));
        }
    }

    const std::string conf_file = fmt::format("{}/conf/be_test.conf", starrocks_home);
    if (!config::init(conf_file.c_str())) {
        return Status::InternalError(fmt::format("failed to load {}", conf_file));
    }

    butil::FilePath spill_path = _storage_root.Append("spill");
    if (!butil::CreateDirectory(spill_path)) {
        return Status::IOError(fmt::format("failed to create spill directory {}", spill_path.value()));
    }

    config::storage_root_path = _storage_root.value();
    config::spill_local_storage_dir = spill_path.value();
    config::enable_event_based_compaction_framework = false;
    config::disable_storage_page_cache = true;
    config::datacache_enable = false;
    config::enable_load_segment_parallel = false;
    config::storage_flood_stage_left_capacity_bytes = 10 * 1024 * 1024;
    const char* log_dir = std::getenv("LOG_DIR");
    config::sys_log_dir = log_dir != nullptr ? log_dir : _storage_root.Append("log").value();
    RETURN_IF_ERROR(fs::create_directories(config::sys_log_dir));

    FLAGS_alsologtostderr = true;
    init_glog(argv0, true);
    _logging_initialized = true;
    RETURN_IF_ERROR(fs::install_builtin_file_system_providers());
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    RETURN_IF_ERROR(UserFunctionCache::instance()->init(config::user_function_dir));
    date::init_date_cache();

    std::vector<StorePath> paths{StorePath(config::storage_root_path)};
    // Metric singletons keep registry back-pointers, so the process registry must outlive shutdown.
    static auto* process_metrics_registry = new ProcessMetricsRegistry("starrocks_be");
    PlatformEnvOptions platform_env_options;
    platform_env_options.metrics = process_metrics_registry->root_registry();
    platform_env_options.store_paths = paths;
    RETURN_IF_ERROR(PlatformEnv::GetInstance()->init(std::move(platform_env_options)));
    _platform_env_initialized = true;

    auto* runtime_env = RuntimeEnv::GetInstance();
    RETURN_IF_ERROR(runtime_env->init(process_metrics_registry->root_registry()));
    _runtime_env_initialized = true;

    std::vector<std::string> cache_storage_root_paths;
    cache_storage_root_paths.reserve(paths.size());
    for (const auto& path : paths) {
        cache_storage_root_paths.emplace_back(path.path);
    }
    DataCacheInitOptions cache_init_options;
    cache_init_options.storage_root_paths = std::move(cache_storage_root_paths);
    cache_init_options.metrics = process_metrics_registry->root_registry();
    cache_init_options.process_mem_limit = runtime_env->process_mem_limit();
    cache_init_options.process_mem_tracker = runtime_env->process_mem_tracker();
    auto* data_cache = DataCache::GetInstance();
    data_cache->set_mem_trackers(runtime_env->datacache_mem_tracker(), runtime_env->page_cache_mem_tracker());
    RETURN_IF_ERROR(data_cache->init(cache_init_options));
    _data_cache_initialized = true;

    EngineOptions options;
    options.store_paths = paths;
    options.compaction_mem_tracker = runtime_env->compaction_mem_tracker();
    options.update_mem_tracker = runtime_env->update_mem_tracker();
    options.table_metrics_mgr = process_metrics_registry->table_metrics_mgr();
    RETURN_IF_ERROR(StorageEngine::open(options, &_storage_engine));
    return Status::OK();
}

class LakeHarness {
public:
    Status init(JsonIteratorMode iterator_mode) {
        _root = lake::join_path(config::storage_root_path,
                                fmt::format("flat_json_vertical_compaction_mem_bench_{}_{}_{}", getpid(),
                                            iterator_mode_name(iterator_mode), next_bench_id()));
        RETURN_IF_ERROR(fs::create_directories(lake::join_path(_root, lake::kSegmentDirectoryName)));
        RETURN_IF_ERROR(fs::create_directories(lake::join_path(_root, lake::kMetadataDirectoryName)));
        RETURN_IF_ERROR(fs::create_directories(lake::join_path(_root, lake::kTxnLogDirectoryName)));

        _parent_tracker = std::make_unique<MemTracker>(-1);
        _mem_tracker = std::make_unique<MemTracker>(-1, "flat-json-bench", _parent_tracker.get());
        _location_provider = std::make_shared<FixedLocationProvider>(_root);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 512 * 1024 * 1024,
                                                          PlatformEnv::GetInstance()->store_path_registry());
        return Status::OK();
    }

    ~LakeHarness() {
        _tablet_manager.reset();
        _update_manager.reset();
        if (!_root.empty()) {
            (void)fs::remove_all(_root);
        }
    }

    TabletManager* tablet_manager() const { return _tablet_manager.get(); }

private:
    std::string _root;
    std::unique_ptr<MemTracker> _parent_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<LocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
};

std::shared_ptr<TabletMetadata> make_tablet_metadata() {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_bench_id());
    metadata->set_version(1);
    metadata->set_cumulative_point(0);
    metadata->set_next_rowset_id(1);

    auto* schema = metadata->mutable_schema();
    schema->set_id(next_bench_id());
    schema->set_keys_type(DUP_KEYS);
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);

    auto add_column = [&](const std::string& name, const std::string& type, bool is_key, int32_t unique_id) {
        auto* column = schema->add_column();
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(false);
        column->set_unique_id(unique_id);
        if (!is_key) {
            column->set_aggregation("NONE");
        }
    };

    add_column("id", "BIGINT", true, 0);
    add_column("document", "JSON", false, 1);
    add_column("value0", "BIGINT", false, 2);
    add_column("value1", "BIGINT", false, 3);
    add_column("value2", "BIGINT", false, 4);
    add_column("value3", "BIGINT", false, 5);
    schema->set_next_column_unique_id(kTabletSchemaColumnCount);
    return metadata;
}

void set_tablet_flat_json_enabled(TabletMetadata* metadata, bool enabled) {
    FlatJsonConfig flat_json_config;
    flat_json_config.set_flat_json_enabled(enabled);
    flat_json_config.to_pb(metadata->mutable_flat_json_config());
}

uint64_t next_splitmix64(uint64_t* state) {
    uint64_t value = (*state += 0x9e3779b97f4a7c15ULL);
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

void append_deterministic_remaining_json_string(std::string* json, int64_t remaining_json_string_bytes,
                                                int64_t row_id) {
    // Printable bytes make the generated value valid JSON without escaping. Mapping two
    // problematic characters keeps quotes and backslashes out while preserving enough
    // entropy that segment compression cannot collapse the string like repeated
    // 'x' data.
    uint64_t state = static_cast<uint64_t>(row_id) ^ 0xd1b54a32d192ed03ULL;
    for (int64_t offset = 0; offset < remaining_json_string_bytes;) {
        uint64_t random = next_splitmix64(&state);
        for (int byte = 0; byte < 8 && offset < remaining_json_string_bytes; ++byte, ++offset) {
            char value = static_cast<char>(33 + ((random >> (byte * 8)) & 0xff) % 94);
            if (value == '"' || value == '\\') {
                ++value;
            }
            json->push_back(value);
        }
    }
}

const std::string& main_document_small_fields() {
    static const std::string fields =
            R"("session_id":"session-id","org_id":"org-id","user_identity_id":"user-id",)"
            R"("agent_id":"agent-id","prompt_caps_sig":"prompt-signature",)"
            R"("_id":{"$oid":"object-id"},"prompt_built_at":{"$date":"2026-08-05T00:00:00Z"},)"
            R"("updated_at":{"$date":"2026-08-05T00:00:00Z"},)"
            R"("last_turn_at":{"$date":"2026-08-05T00:00:00Z"},)"
            R"("summary_requested_at":{"$date":"2026-08-05T00:00:00Z"},)"
            R"("summary":{"objective":"objective","updated_at":{"$date":"2026-08-05T00:00:00Z"}},)"
            R"("covered_start_index":1,"last_covered_index":2,"last_prompt_tokens":3)";
    return fields;
}

StatusOr<JsonValue> make_json_value(int64_t remaining_json_string_bytes, int64_t row_id, bool use_single_rowset_path) {
    std::string json;
    json.reserve(remaining_json_string_bytes + 512);
    json.push_back('{');
    if (use_single_rowset_path) {
        json.append("\"path_only_in_single_rowset\":");
        json.append(std::to_string(row_id));
    } else {
        json.append(main_document_small_fields());
    }
    json.append(",\"messages\":[{\"content\":\"");
    append_deterministic_remaining_json_string(&json, remaining_json_string_bytes, row_id);
    json.append("\"}]}");
    return JsonValue::parse(json);
}

StatusOr<Chunk> make_chunk(const std::shared_ptr<Schema>& chunk_schema, const BenchConfig& bench_config,
                           int64_t first_row_id, int64_t rows, bool use_single_rowset_path) {
    auto ids = Int64Column::create();
    auto documents = JsonColumn::create();
    auto value0 = Int64Column::create();
    auto value1 = Int64Column::create();
    auto value2 = Int64Column::create();
    auto value3 = Int64Column::create();

    std::vector<int64_t> id_values(rows);
    std::vector<int64_t> value_column_values(rows);
    for (int64_t i = 0; i < rows; ++i) {
        const int64_t row_id = first_row_id + i;
        id_values[i] = row_id;
        value_column_values[i] = row_id;
        ASSIGN_OR_RETURN(auto json_value,
                         make_json_value(bench_config.remaining_json_string_bytes, row_id, use_single_rowset_path));
        documents->append(json_value);
    }
    ids->append_numbers(id_values.data(), id_values.size() * sizeof(int64_t));
    value0->append_numbers(value_column_values.data(), value_column_values.size() * sizeof(int64_t));
    value1->append_numbers(value_column_values.data(), value_column_values.size() * sizeof(int64_t));
    value2->append_numbers(value_column_values.data(), value_column_values.size() * sizeof(int64_t));
    value3->append_numbers(value_column_values.data(), value_column_values.size() * sizeof(int64_t));

    Columns columns{std::move(ids),    std::move(documents), std::move(value0),
                    std::move(value1), std::move(value2),    std::move(value3)};
    return Chunk(std::move(columns), chunk_schema);
}

struct PreparedTablet {
    std::unique_ptr<LakeHarness> harness;
    std::shared_ptr<TabletMetadata> metadata;
    std::shared_ptr<const TabletSchema> tablet_schema;
    std::vector<std::shared_ptr<Rowset>> rowsets;
    int64_t total_rows = 0;
};

int64_t row_count_for_input_rowset(const BenchConfig& bench_config, int64_t input_rowset_index) {
    // The first rowset has only one row and a Flat JSON path absent from every
    // regular rowset, leaving compaction with no common extracted path.
    if (input_rowset_index == 0) {
        return 1;
    }
    if (input_rowset_index == bench_config.input_rowset_count - 1) {
        return 2 * bench_config.rows_per_input_rowset - 1;
    }
    return bench_config.rows_per_input_rowset;
}

StatusOr<PreparedTablet> prepare_tablet(JsonIteratorMode iterator_mode, const BenchConfig& bench_config) {
    PreparedTablet prepared;
    prepared.harness = std::make_unique<LakeHarness>();
    RETURN_IF_ERROR(prepared.harness->init(iterator_mode));

    prepared.metadata = make_tablet_metadata();
    prepared.tablet_schema = TabletSchema::create(prepared.metadata->schema());
    if (prepared.tablet_schema->num_columns() != kTabletSchemaColumnCount ||
        prepared.tablet_schema->sort_key_idxes().size() != 1) {
        return Status::InternalError("benchmark tablet schema is not the required six-column/one-key shape");
    }
    auto chunk_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(prepared.tablet_schema));

    auto* tablet_manager = prepared.harness->tablet_manager();
    set_tablet_flat_json_enabled(prepared.metadata.get(), iterator_mode == JsonIteratorMode::kJsonMergeIterator);
    RETURN_IF_ERROR(tablet_manager->put_tablet_metadata(*prepared.metadata));
    VersionedTablet tablet(tablet_manager, prepared.metadata);

    int64_t next_row_id = 0;
    for (int64_t input_rowset_index = 0; input_rowset_index < bench_config.input_rowset_count; ++input_rowset_index) {
        ASSIGN_OR_RETURN(auto writer, tablet.new_writer(kHorizontal, next_bench_id()));
        RETURN_IF_ERROR(writer->open());
        const int64_t row_count = row_count_for_input_rowset(bench_config, input_rowset_index);
        const bool use_single_rowset_path = row_count == 1;
        ASSIGN_OR_RETURN(auto chunk,
                         make_chunk(chunk_schema, bench_config, next_row_id, row_count, use_single_rowset_path));
        next_row_id += row_count;
        prepared.total_rows += row_count;
        RETURN_IF_ERROR(writer->write(chunk));
        RETURN_IF_ERROR(writer->finish());

        if (writer->segments().size() != 1) {
            writer->close();
            return Status::InternalError(fmt::format("input rowset {} produced {} segments instead of one",
                                                     input_rowset_index, writer->segments().size()));
        }
        const auto segment = writer->segments()[0];
        const auto num_rows = writer->num_rows();
        const auto data_size = writer->data_size();
        writer->close();

        auto* rowset = prepared.metadata->add_rowsets();
        rowset->set_id(input_rowset_index + 1);
        rowset->set_overlapped(false);
        rowset->set_num_rows(num_rows);
        rowset->set_data_size(data_size);
        rowset->set_next_compaction_offset(0);
        segment.to_proto(0, rowset->add_segment_metas());
    }

    prepared.metadata->set_next_rowset_id(bench_config.input_rowset_count + 1);
    prepared.metadata->set_version(2);
    RETURN_IF_ERROR(tablet_manager->put_tablet_metadata(*prepared.metadata));

    VersionedTablet final_tablet(tablet_manager, prepared.metadata);
    prepared.rowsets = final_tablet.get_rowsets();
    if (prepared.rowsets.size() != bench_config.input_rowset_count) {
        return Status::InternalError(fmt::format("prepared {} rowsets, expected {}", prepared.rowsets.size(),
                                                 bench_config.input_rowset_count));
    }
    for (const auto& rowset : prepared.rowsets) {
        if (rowset->is_overlapped() || rowset->num_segments() != 1) {
            return Status::InternalError("every benchmark rowset must be non-overlapping with exactly one segment");
        }
    }
    return prepared;
}

int64_t column_reader_tree_footprint_bytes(const ColumnReader* reader) {
    int64_t result = reader->total_mem_footprint();
    if (reader->sub_readers() != nullptr) {
        for (const auto& child : *reader->sub_readers()) {
            result += column_reader_tree_footprint_bytes(child.get());
        }
    }
    return result;
}

std::string dynamic_type_name(const ColumnIterator& iterator) {
    int status = 0;
    std::unique_ptr<char, decltype(&std::free)> demangled(
            abi::__cxa_demangle(typeid(iterator).name(), nullptr, nullptr, &status), &std::free);
    return status == 0 && demangled != nullptr ? demangled.get() : typeid(iterator).name();
}

struct SegmentInspection {
    int64_t segments = 0;
    int64_t single_row_segments = 0;
    int64_t flat_segments = 0;
    int64_t flat_segments_with_remaining_json = 0;
    int64_t json_merge_iterators = 0;
    int64_t json_root_reader_footprint_bytes = 0;
    int64_t json_reader_tree_footprint_bytes = 0;
    int64_t column_group_root_reader_footprint_bytes = 0;
    int64_t min_flat_json_child_reader_count = std::numeric_limits<int64_t>::max();
    int64_t max_flat_json_child_reader_count = 0;
    int32_t iterator_read_chunk_size = 0;
};

StatusOr<SegmentInspection> inspect_segments(const PreparedTablet& prepared) {
    SegmentInspection inspection;
    LakeIOOptions io_options{.fill_data_cache = false,
                             .buffer_size = config::lake_compaction_stream_buffer_size_bytes,
                             .fill_metadata_cache = true};
    const auto& json_tablet_column = prepared.tablet_schema->column(1);
    ASSIGN_OR_RETURN(auto empty_compaction_access_path,
                     ColumnAccessPath::create(TAccessPathType::ROOT, std::string(json_tablet_column.name()), 1));
    empty_compaction_access_path->set_from_compaction(true);

    for (const auto& rowset : prepared.rowsets) {
        ASSIGN_OR_RETURN(auto segments, rowset->segments(io_options));
        inspection.segments += segments.size();
        for (const auto& segment : segments) {
            if (segment->num_rows() == 1) {
                ++inspection.single_row_segments;
            }
            for (uint32_t column_index = 1; column_index < kTabletSchemaColumnCount; ++column_index) {
                const auto uid = prepared.tablet_schema->column(column_index).unique_id();
                const auto* reader = segment->column_with_uid(uid);
                if (reader != nullptr) {
                    inspection.column_group_root_reader_footprint_bytes += reader->total_mem_footprint();
                }
            }

            const auto* json_reader = segment->column_with_uid(json_tablet_column.unique_id());
            if (json_reader == nullptr) {
                return Status::InternalError("JSON column reader is missing from an input segment");
            }
            inspection.json_root_reader_footprint_bytes += json_reader->total_mem_footprint();
            inspection.json_reader_tree_footprint_bytes += column_reader_tree_footprint_bytes(json_reader);

            const bool is_flat = json_reader->sub_readers() != nullptr && !json_reader->sub_readers()->empty();
            if (is_flat) {
                ++inspection.flat_segments;
                if (json_reader->has_remain_json()) {
                    ++inspection.flat_segments_with_remaining_json;
                }
                inspection.min_flat_json_child_reader_count = std::min<int64_t>(
                        inspection.min_flat_json_child_reader_count, json_reader->sub_readers()->size());
                inspection.max_flat_json_child_reader_count = std::max<int64_t>(
                        inspection.max_flat_json_child_reader_count, json_reader->sub_readers()->size());
                // Segment intentionally exposes readers as const, while iterator creation lazily
                // initializes reader indexes. Production SegmentIterator performs the same mutable
                // operation through its owned reader. The empty root mirrors the access path that
                // lake compaction derives for these mutually exclusive Flat JSON schemas; the task's
                // actual root and child counts are checked independently after execution.
                auto* mutable_json_reader = const_cast<ColumnReader*>(json_reader);
                ASSIGN_OR_RETURN(auto iterator, mutable_json_reader->new_iterator(empty_compaction_access_path.get(),
                                                                                  &json_tablet_column));
                if (dynamic_type_name(*iterator).find("JsonMergeIterator") != std::string::npos) {
                    ++inspection.json_merge_iterators;
                }
            }
        }
    }

    inspection.iterator_read_chunk_size = CompactionUtils::get_read_chunk_size(
            config::compaction_memory_limit_per_worker, config::lake_compaction_chunk_size, prepared.total_rows,
            inspection.column_group_root_reader_footprint_bytes, inspection.segments);
    if (inspection.min_flat_json_child_reader_count == std::numeric_limits<int64_t>::max()) {
        inspection.min_flat_json_child_reader_count = 0;
    }
    return inspection;
}

class ObservableVerticalCompactionTask final : public VerticalCompactionTask {
public:
    using VerticalCompactionTask::VerticalCompactionTask;

    MemTracker* task_mem_tracker() const { return _mem_tracker.get(); }
    int64_t task_peak_bytes() const { return _mem_tracker->peak_consumption(); }
    size_t access_path_count() const { return _column_access_paths.size(); }
    size_t access_path_child_count() const {
        size_t child_count = 0;
        for (const auto& access_path : _column_access_paths) {
            child_count += access_path->children().size();
        }
        return child_count;
    }
};

int64_t current_rss_bytes() {
    std::ifstream statm("/proc/self/statm");
    int64_t pages = 0;
    int64_t resident_pages = 0;
    if (!(statm >> pages >> resident_pages)) {
        return -1;
    }
    return resident_pages * sysconf(_SC_PAGESIZE);
}

Status enable_heap_profile() {
    bool configured = false;
    size_t configured_size = sizeof(configured);
    if (je_mallctl("config.prof", &configured, &configured_size, nullptr, 0) != 0 || !configured) {
        return Status::NotSupported("the linked jemalloc was built without profiling support");
    }

    bool enable = true;
    const int global_result = je_mallctl("prof.active", nullptr, nullptr, &enable, sizeof(enable));
    const int thread_init_result = je_mallctl("prof.thread_active_init", nullptr, nullptr, &enable, sizeof(enable));
    const int current_thread_result = je_mallctl("thread.prof.active", nullptr, nullptr, &enable, sizeof(enable));
    bool active = false;
    size_t active_size = sizeof(active);
    const int read_result = je_mallctl("prof.active", &active, &active_size, nullptr, 0);
    if (global_result != 0 || thread_init_result != 0 || current_thread_result != 0 || read_result != 0 || !active) {
        return Status::InternalError(fmt::format(
                "jemalloc profiling could not be activated: global={}, thread_init={}, current_thread={}, read={}, "
                "active={}",
                global_result, thread_init_result, current_thread_result, read_result, active));
    }
    return Status::OK();
}

struct CompactionMeasurement {
    double execution_time_ms = 0;
    int64_t task_peak_bytes = 0;
    int64_t process_rss_peak_bytes = 0;
    int64_t heap_profile_compaction_memory_bytes = 0;
    int64_t heap_profile_process_rss_bytes = 0;
    size_t access_path_count = 0;
    size_t access_path_child_count = 0;
    std::string heap_profile_path;
};

StatusOr<CompactionMeasurement> run_compaction(const BenchConfig& bench_config, const PreparedTablet& prepared) {
    VersionedTablet tablet(prepared.harness->tablet_manager(), prepared.metadata);
    CompactionTaskContext context(next_bench_id(), prepared.metadata->id(), prepared.metadata->version(), false, true,
                                  nullptr);
    ObservableVerticalCompactionTask task(tablet, prepared.rowsets, &context, prepared.tablet_schema);

    CompactionMeasurement measurement;
    std::string heap_profile_path;
    int64_t heap_profile_compaction_memory_bytes = 0;
    int64_t heap_profile_process_rss_bytes = 0;

    if (bench_config.heap_profile) {
        if (!bench_config.profile_dir.empty()) {
            config::pprof_profile_dir = bench_config.profile_dir;
        }
        RETURN_IF_ERROR(fs::create_directories(config::pprof_profile_dir));
        RETURN_IF_ERROR(enable_heap_profile());
    }

    std::atomic<bool> stop_sampler{false};
    std::atomic<int64_t> process_rss_peak_bytes{current_rss_bytes()};
    std::thread sampler([&] {
        auto next_heap_profile_snapshot = std::chrono::steady_clock::now() + kHeapProfileSnapshotInterval;

        while (!stop_sampler.load(std::memory_order_acquire)) {
            const int64_t tracker_bytes = task.task_mem_tracker()->consumption();
            const int64_t rss_bytes = current_rss_bytes();
            const auto now = std::chrono::steady_clock::now();
            process_rss_peak_bytes.store(std::max(process_rss_peak_bytes.load(std::memory_order_relaxed), rss_bytes),
                                         std::memory_order_relaxed);

            if (bench_config.heap_profile && now >= next_heap_profile_snapshot) {
                auto new_profile_path = HeapProf::getInstance().snapshot();
                if (!new_profile_path.empty()) {
                    if (heap_profile_path.empty() || tracker_bytes > heap_profile_compaction_memory_bytes) {
                        if (!heap_profile_path.empty()) {
                            (void)fs::remove(heap_profile_path);
                        }
                        heap_profile_path = std::move(new_profile_path);
                        heap_profile_compaction_memory_bytes = tracker_bytes;
                        heap_profile_process_rss_bytes = rss_bytes;
                    } else {
                        (void)fs::remove(new_profile_path);
                    }
                }
                next_heap_profile_snapshot = std::chrono::steady_clock::now() + kHeapProfileSnapshotInterval;
            }
            std::this_thread::sleep_for(kMemorySampleInterval);
        }
    });

    const auto execution_start = std::chrono::steady_clock::now();
    const auto compaction_status = task.execute(CompactionTask::kNoCancelFn);
    const auto execution_end = std::chrono::steady_clock::now();
    stop_sampler.store(true, std::memory_order_release);
    sampler.join();
    if (bench_config.heap_profile) {
        HeapProf::getInstance().disable_prof();
    }
    RETURN_IF_ERROR(compaction_status);

    measurement.execution_time_ms = std::chrono::duration<double, std::milli>(execution_end - execution_start).count();
    measurement.task_peak_bytes = task.task_peak_bytes();
    measurement.process_rss_peak_bytes = process_rss_peak_bytes.load(std::memory_order_relaxed);
    measurement.heap_profile_compaction_memory_bytes = heap_profile_compaction_memory_bytes;
    measurement.heap_profile_process_rss_bytes = heap_profile_process_rss_bytes;
    measurement.access_path_count = task.access_path_count();
    measurement.access_path_child_count = task.access_path_child_count();
    measurement.heap_profile_path = std::move(heap_profile_path);
    if (bench_config.heap_profile && measurement.heap_profile_path.empty()) {
        return Status::InternalError(fmt::format("automatic heap profile snapshot was not captured; task peak was {}",
                                                 measurement.task_peak_bytes));
    }
    return measurement;
}

Status validate_observations(JsonIteratorMode iterator_mode, const BenchConfig& bench_config,
                             const PreparedTablet& prepared, const SegmentInspection& inspection,
                             const CompactionMeasurement& measurement) {
    const bool expected_flat_json_enabled = iterator_mode == JsonIteratorMode::kJsonMergeIterator;
    if (!prepared.metadata->has_flat_json_config() ||
        prepared.metadata->flat_json_config().flat_json_enable() != expected_flat_json_enabled) {
        return Status::InternalError("tablet metadata does not contain the expected Flat JSON configuration");
    }
    if (prepared.total_rows != bench_config.total_rows() || inspection.segments != bench_config.input_rowset_count) {
        return Status::InternalError("prepared row/segment count does not match benchmark configuration");
    }
    if (inspection.single_row_segments != 1) {
        return Status::InternalError(
                fmt::format("expected one single-row input segment, observed {}", inspection.single_row_segments));
    }
    if (measurement.task_peak_bytes <= 0) {
        return Status::InternalError("compaction did not produce a positive peak-memory measurement");
    }
    if (measurement.execution_time_ms <= 0) {
        return Status::InternalError("compaction did not produce a positive execution-time measurement");
    }
    if (measurement.process_rss_peak_bytes <= 0) {
        return Status::InternalError("compaction did not produce a positive process RSS peak");
    }
    if (bench_config.heap_profile &&
        (measurement.heap_profile_compaction_memory_bytes <= 0 || measurement.heap_profile_process_rss_bytes <= 0)) {
        return Status::InternalError("heap profile did not record compaction memory and process RSS");
    }
    if (iterator_mode == JsonIteratorMode::kJsonMergeIterator) {
        if (inspection.flat_segments != bench_config.input_rowset_count) {
            return Status::InternalError(fmt::format("expected {} Flat segments, observed {}",
                                                     bench_config.input_rowset_count, inspection.flat_segments));
        }
        if (inspection.json_merge_iterators != inspection.flat_segments) {
            return Status::InternalError(fmt::format("expected {} JsonMergeIterators, observed {}",
                                                     inspection.flat_segments, inspection.json_merge_iterators));
        }
        if (measurement.access_path_count != 1 || measurement.access_path_child_count != 0) {
            return Status::InternalError(
                    fmt::format("expected one root compaction access path with no children, observed {} roots and {} "
                                "children",
                                measurement.access_path_count, measurement.access_path_child_count));
        }
        if (inspection.flat_segments_with_remaining_json != inspection.flat_segments) {
            return Status::InternalError(
                    fmt::format("expected all {} Flat segments to contain remaining JSON, but {} "
                                "did",
                                inspection.flat_segments, inspection.flat_segments_with_remaining_json));
        }
        if (inspection.min_flat_json_child_reader_count != 2 ||
            inspection.max_flat_json_child_reader_count != kMainDocumentExtractedPathCount + 1) {
            return Status::InternalError(fmt::format(
                    "expected 2 Flat JSON child readers in the single-row segment and {} in regular segments; "
                    "observed min={} and max={}",
                    kMainDocumentExtractedPathCount + 1, inspection.min_flat_json_child_reader_count,
                    inspection.max_flat_json_child_reader_count));
        }
        if (inspection.json_reader_tree_footprint_bytes <= inspection.json_root_reader_footprint_bytes) {
            return Status::InternalError(
                    "Flat JSON reader-tree footprint is not larger than its root-reader footprint");
        }
    } else {
        if (inspection.flat_segments != 0 || inspection.json_merge_iterators != 0) {
            return Status::InternalError("non-Flat mode unexpectedly contains Flat segments or JsonMergeIterators");
        }
        if (inspection.json_root_reader_footprint_bytes <= 0) {
            return Status::InternalError("non-Flat JSON root-reader footprint is not positive");
        }
    }
    return Status::OK();
}

void publish_counters(benchmark::State& state, const BenchConfig& bench_config, const PreparedTablet& prepared,
                      const SegmentInspection& inspection, const CompactionMeasurement& measurement) {
    state.counters["input_rowset_count"] = static_cast<double>(bench_config.input_rowset_count);
    state.counters["total_rows"] = static_cast<double>(prepared.total_rows);
    state.counters["iterator_read_chunk_size"] = static_cast<double>(inspection.iterator_read_chunk_size);
    state.counters["compaction_execution_time_ms"] = measurement.execution_time_ms;
    state.counters["compaction_peak_memory_bytes"] = static_cast<double>(measurement.task_peak_bytes);
    state.counters["process_rss_peak_bytes"] = static_cast<double>(measurement.process_rss_peak_bytes);
    if (!measurement.heap_profile_path.empty()) {
        state.counters["heap_profile_compaction_memory_bytes"] =
                static_cast<double>(measurement.heap_profile_compaction_memory_bytes);
        state.counters["heap_profile_process_rss_bytes"] =
                static_cast<double>(measurement.heap_profile_process_rss_bytes);
        state.SetLabel(fmt::format("heap_profile={}", measurement.heap_profile_path));
    }
}

void run_benchmark(benchmark::State& state, JsonIteratorMode iterator_mode) {
    auto config_or = read_bench_config();
    if (!config_or.ok()) {
        state.SkipWithError(config_or.status().to_string().c_str());
        return;
    }
    const auto bench_config = std::move(config_or).value();

    auto prepared_or = prepare_tablet(iterator_mode, bench_config);
    if (!prepared_or.ok()) {
        state.SkipWithError(prepared_or.status().to_string().c_str());
        return;
    }
    auto prepared = std::move(prepared_or).value();

    auto inspection_or = inspect_segments(prepared);
    if (!inspection_or.ok()) {
        state.SkipWithError(inspection_or.status().to_string().c_str());
        return;
    }
    const auto inspection = std::move(inspection_or).value();

    CompactionMeasurement measurement;
    for (auto _ : state) {
        auto measurement_or = run_compaction(bench_config, prepared);
        if (!measurement_or.ok()) {
            state.SkipWithError(measurement_or.status().to_string().c_str());
            return;
        }
        measurement = std::move(measurement_or).value();
    }

    auto validation = validate_observations(iterator_mode, bench_config, prepared, inspection, measurement);
    if (!validation.ok()) {
        state.SkipWithError(validation.to_string().c_str());
        return;
    }
    publish_counters(state, bench_config, prepared, inspection, measurement);
}

void BM_JsonMergeIterator(benchmark::State& state) {
    run_benchmark(state, JsonIteratorMode::kJsonMergeIterator);
}

void BM_NonFlat(benchmark::State& state) {
    run_benchmark(state, JsonIteratorMode::kNonFlat);
}

BENCHMARK(BM_JsonMergeIterator)->Iterations(1)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NonFlat)->Iterations(1)->Unit(benchmark::kMillisecond);

} // namespace

} // namespace starrocks::lake

int main(int argc, char** argv) {
    starrocks::lake::BenchEnvironment environment;
    auto status = environment.init(argv[0]);
    if (!status.ok()) {
        std::cerr << status << std::endl;
        return 1;
    }
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
