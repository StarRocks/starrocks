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

#include "connector/hive/paimon/paimon_global_index_scanner.h"

#include <fmt/format.h>
#include <paimon/executor.h>
#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/utils/range.h>
#include <paimon/utils/row_range_index.h>

#include <chrono>
#include <map>
#include <optional>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_global_index_evaluator.h"
#include "connector/hive/paimon/paimon_global_index_schema_guard.h"
#include "connector/hive/paimon/tracked_paimon_memory_pool.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

namespace starrocks {

namespace {

constexpr int32_t kProtocolVersion = 3;
constexpr std::string_view kIndexResultColumn = "index_result";
constexpr std::string_view kArgsColumn = "args";

Status paimon_error(std::string_view operation, const paimon::Status& status) {
    return Status::InternalError(fmt::format("Paimon Global Index {} failed: {}", operation, status.ToString()));
}

Status check_query_state(RuntimeState* runtime_state) {
    RETURN_IF_CANCELLED(runtime_state);
    RETURN_IF_ERROR(runtime_state->check_query_state("Paimon Global Index scan"));
    return runtime_state->check_mem_limit("Paimon Global Index scan");
}

Status lock_scan(RuntimeState* runtime_state, std::unique_lock<std::timed_mutex>* lock) {
    while (!lock->try_lock_for(std::chrono::milliseconds(50))) {
        RETURN_IF_ERROR(check_query_state(runtime_state));
    }
    return check_query_state(runtime_state);
}

int64_t monotonic_nanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

void update_paimon_io_profile(RuntimeProfile* profile, const PaimonFileSystemStats::Snapshot& stats) {
    constexpr const char* kSection = "PaimonGlobalIndexFileSystem";
    ADD_COUNTER(profile, kSection, TUnit::NONE);
    auto update = [&](const char* name, TUnit::type unit, int64_t value) {
        auto* counter = ADD_CHILD_COUNTER(profile, name, unit, kSection);
        COUNTER_UPDATE(counter, value);
    };
    update("AppIOCount", TUnit::UNIT, stats.app_io_count());
    update("AppIOBytes", TUnit::BYTES, stats.app_io_bytes());
    update("AppIOTime", TUnit::TIME_NS, stats.app_io_ns());
    update("FileSystemIOCount", TUnit::UNIT, stats.fs_io_count);
    update("FileSystemIOBytes", TUnit::BYTES, stats.fs_io_bytes);
    update("FileSystemIOTime", TUnit::TIME_NS, stats.fs_io_ns);
}

auto cache_identity(const DataCacheOptions& options) {
    return std::make_tuple(options.enable_datacache, options.enable_cache_select, options.enable_populate_datacache,
                           options.enable_datacache_async_populate_mode, options.enable_datacache_io_adaptor,
                           options.modification_time, options.datacache_evict_probability, options.datacache_priority,
                           options.datacache_ttl_seconds);
}

void add_io_delta(PaimonFileSystemStats::Snapshot* total, const PaimonFileSystemStats::Snapshot& before,
                  const PaimonFileSystemStats::Snapshot& after) {
    total->sequential_read_count += after.sequential_read_count - before.sequential_read_count;
    total->sequential_read_bytes += after.sequential_read_bytes - before.sequential_read_bytes;
    total->sequential_read_ns += after.sequential_read_ns - before.sequential_read_ns;
    total->positional_read_count += after.positional_read_count - before.positional_read_count;
    total->positional_read_bytes += after.positional_read_bytes - before.positional_read_bytes;
    total->positional_read_ns += after.positional_read_ns - before.positional_read_ns;
    total->async_read_count += after.async_read_count - before.async_read_count;
    total->async_read_bytes += after.async_read_bytes - before.async_read_bytes;
    total->async_read_ns += after.async_read_ns - before.async_read_ns;
    total->fs_io_count += after.fs_io_count - before.fs_io_count;
    total->fs_io_bytes += after.fs_io_bytes - before.fs_io_bytes;
    total->fs_io_ns += after.fs_io_ns - before.fs_io_ns;
}

} // namespace

// Only active operations borrow RuntimeState. Clearing the final lease waits for
// any in-flight cancellation check; late SDK callbacks then see an inactive lease,
// never a pointer to a destroyed scanner/fragment. Shards keep their own readers.
struct PaimonGlobalIndexSharedState {
    struct Operation {
        Status start(RuntimeState* state) {
            std::lock_guard lock(mutex);
            if (active != 0 && runtime_state != state) {
                return Status::InvalidArgument("Paimon Global Index operations belong to different fragments");
            }
            runtime_state = state;
            ++active;
            return Status::OK();
        }
        void finish() {
            std::lock_guard lock(mutex);
            if (--active == 0) {
                runtime_state = nullptr;
            }
        }
        paimon::Status check() {
            std::lock_guard lock(mutex);
            if (runtime_state == nullptr) {
                return paimon::Status::Cancelled("Paimon Global Index operation has finished");
            }
            auto status = check_query_state(runtime_state);
            return status.ok() ? paimon::Status::OK() : paimon::Status::Cancelled(status.to_string());
        }
        std::mutex mutex;
        RuntimeState* runtime_state = nullptr;
        size_t active = 0;
    };
    std::shared_ptr<MemTracker> tracker;
    std::shared_ptr<FileSystem> file_system_owner;
    TUniqueId query_id;
    TUniqueId fragment_id;
    std::string request;
    DataCacheOptions options;
    std::shared_ptr<Operation> operation;
    std::shared_ptr<paimon::MemoryPool> memory_pool;
    std::shared_ptr<PaimonFileSystem> file_system;
    std::shared_ptr<PaimonGlobalIndexSchemaGuard> schema_guard;
    std::shared_ptr<paimon::GlobalIndexScan> scan;
    std::mutex profile_mutex;
    PaimonFileSystemStats::Snapshot reported_io;
};

Status PaimonGlobalIndexScanner::do_init(RuntimeState*, const HdfsScannerContext& scanner_ctx) {
    _emitted = false;
    _scan_context = scanner_ctx.paimon_global_index_context != nullptr
                            ? scanner_ctx.paimon_global_index_context
                            : std::make_shared<PaimonGlobalIndexScanContext>();
    return Status::OK();
}

Status PaimonGlobalIndexScanner::do_open(RuntimeState* runtime_state) {
    const int64_t start_ns = monotonic_nanos();
    DeferOp record_time([&] { _open_ns += monotonic_nanos() - start_ns; });
    RETURN_IF_ERROR(check_query_state(runtime_state));
    RETURN_IF_ERROR(_parse_request());
    if (_scanner_ctx->fs == nullptr) {
        return Status::InternalError("Paimon Global Index scanner has no StarRocks file system");
    }

    std::unique_lock lock(_scan_context->mutex, std::defer_lock);
    RETURN_IF_ERROR(lock_scan(runtime_state, &lock));
    const auto& scan_range = _scanner_ctx->scan_range->paimon_global_index_scan_range;
    auto shared = std::static_pointer_cast<PaimonGlobalIndexSharedState>(_scan_context->state);
    if (shared != nullptr) {
        if (shared->query_id != runtime_state->query_id() ||
            shared->fragment_id != runtime_state->fragment_instance_id() ||
            shared->tracker != runtime_state->query_mem_tracker_ptr() || shared->request != scan_range.query_json ||
            cache_identity(shared->options) != cache_identity(_scanner_ctx->datacache_options)) {
            return Status::InvalidArgument("Incompatible Paimon Global Index scan reuse identity");
        }
        _shared_state = shared;
        _memory_pool = shared->memory_pool;
        _paimon_file_system = shared->file_system;
        _schema_guard = shared->schema_guard;
        _global_index_scan = shared->scan;
        return Status::OK();
    }
    if (_scanner_ctx->paimon_global_index_context != nullptr &&
        _scanner_ctx->paimon_global_index_file_system == nullptr) {
        return Status::InternalError("Shared Paimon Global Index scan requires an owned file system");
    }
    const auto query_tracker = runtime_state->query_mem_tracker_ptr();
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(query_tracker.get());
    shared = std::allocate_shared<PaimonGlobalIndexSharedState>(
            PaimonQueryAllocator<PaimonGlobalIndexSharedState>(query_tracker));
    shared->tracker = query_tracker;
    shared->operation = std::allocate_shared<PaimonGlobalIndexSharedState::Operation>(
            PaimonQueryAllocator<PaimonGlobalIndexSharedState::Operation>(query_tracker));
    shared->file_system_owner = _scanner_ctx->paimon_global_index_file_system;
    shared->query_id = runtime_state->query_id();
    shared->fragment_id = runtime_state->fragment_instance_id();
    shared->request = scan_range.query_json;
    shared->options = _scanner_ctx->datacache_options;
    RETURN_IF_ERROR(shared->operation->start(runtime_state));
    DeferOp finish_operation([&] {
        shared->operation->finish();
        if (_paimon_file_system != nullptr) {
            _io_stats = _paimon_file_system->get_stats();
        }
    });
    _memory_pool = std::allocate_shared<TrackedPaimonMemoryPool>(
            PaimonQueryAllocator<TrackedPaimonMemoryPool>(query_tracker), query_tracker.get());
    shared->memory_pool = _memory_pool;
    _paimon_file_system = std::allocate_shared<PaimonFileSystem>(
            PaimonQueryAllocator<PaimonFileSystem>(query_tracker), _scanner_ctx->fs, _scanner_ctx->datacache_options,
            [operation = shared->operation, owner = shared->file_system_owner] {
                // Input streams retain this callback, so their underlying FS outlives
                // both the first scanner and the provider if SDK cleanup is delayed.
                (void)owner;
                return operation->check();
            },
            query_tracker);
    shared->file_system = _paimon_file_system;
    auto schema_guard = std::allocate_shared<PaimonGlobalIndexSchemaGuard>(
            PaimonQueryAllocator<PaimonGlobalIndexSchemaGuard>(query_tracker), _paimon_file_system,
            scan_range.table_path, _request["schemaId"].GetInt64(), _request["schemaTimeMillis"].GetInt64());
#ifdef BE_TEST
    auto scan = _scan_factory_for_test ? _scan_factory_for_test()
                                       : paimon::GlobalIndexScan::Create(
                                                 scan_range.table_path, std::optional<int64_t>(scan_range.snapshot_id),
                                                 std::optional<std::vector<std::map<std::string, std::string>>>{},
                                                 std::map<std::string, std::string>(), schema_guard,
                                                 paimon::GetGlobalDefaultExecutor(), _memory_pool);
#else
    auto scan = paimon::GlobalIndexScan::Create(scan_range.table_path, std::optional<int64_t>(scan_range.snapshot_id),
                                                std::optional<std::vector<std::map<std::string, std::string>>>{},
                                                std::map<std::string, std::string>(), schema_guard,
                                                paimon::GetGlobalDefaultExecutor(), _memory_pool);
#endif
    RETURN_IF_ERROR(check_query_state(runtime_state));
    if (!scan.ok()) {
        return paimon_error("scan creation", scan.status());
    }
#ifdef BE_TEST
    if (!_scan_factory_for_test && !schema_guard->schema_validated()) {
#else
    if (!schema_guard->schema_validated()) {
#endif
        return Status::InternalError("Paimon Global Index did not validate its schema");
    }
    _global_index_scan = std::move(scan).value();
    _schema_guard = std::move(schema_guard);
    shared->schema_guard = _schema_guard;
    shared->scan = _global_index_scan;
    _shared_state = shared;
    _scan_context->state = shared;
    return Status::OK();
}

Status PaimonGlobalIndexScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_emitted) {
        return Status::EndOfFile("Paimon Global Index shard is exhausted");
    }

    RETURN_IF_ERROR(check_query_state(runtime_state));
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(runtime_state->query_mem_tracker_ptr().get());
    if (_shared_state != nullptr) {
        RETURN_IF_ERROR(_shared_state->operation->start(runtime_state));
    }
    DeferOp finish_operation([&] {
        if (_shared_state != nullptr) {
            _shared_state->operation->finish();
        }
    });

    const int64_t evaluate_start_ns = monotonic_nanos();
    auto evaluated = _evaluate(runtime_state);
    _evaluate_ns = monotonic_nanos() - evaluate_start_ns;
    RETURN_IF_ERROR(check_query_state(runtime_state));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexResult> result, std::move(evaluated));

    // Serialize materializes the lazy bitmap. The file-system callback stops subsequent I/O;
    // SDK CPU-only traversal remains cooperative only at these serialization boundaries.
    const int64_t serialize_start_ns = monotonic_nanos();
    auto serialized = paimon::GlobalIndexResult::Serialize(result, _memory_pool);
    _serialize_ns = monotonic_nanos() - serialize_start_ns;
    RETURN_IF_ERROR(check_query_state(runtime_state));
    if (!serialized.ok()) {
        return paimon_error("result serialization", serialized.status());
    }
    auto bytes = std::move(serialized).value();
    _serialized_bytes = static_cast<int64_t>(bytes->size());

    const std::string& query_json = _scanner_ctx->scan_range->paimon_global_index_scan_range.query_json;
    for (const SlotDescriptor* slot : _scanner_ctx->materialize_slots) {
        MutableColumnPtr column = ColumnHelper::create_column(slot->type(), true);
        if (slot->col_name() == kIndexResultColumn) {
            column->append_datum(Datum(Slice(bytes->data(), bytes->size())));
        } else if (slot->col_name() == kArgsColumn) {
            column->append_datum(Datum(Slice(query_json)));
        } else {
            column->append_default();
        }
        (*chunk)->append_or_update_column(std::move(column), slot->id());
    }
    (*chunk)->set_num_rows(1);
    _emitted = true;
    return Status::OK();
}

void PaimonGlobalIndexScanner::do_close(RuntimeState*) noexcept {
    _global_index_scan.reset();
    _paimon_file_system.reset();
    _memory_pool.reset();
    _schema_guard.reset();
    _shared_state.reset();
    _scan_context.reset();
}

void PaimonGlobalIndexScanner::do_update_counter(HdfsScannerProfile* profile) {
    if (profile->runtime_profile == nullptr) {
        return;
    }
    constexpr const char* kSection = "PaimonGlobalIndex";
    ADD_COUNTER(profile->runtime_profile, kSection, TUnit::NONE);
    auto update = [&](const char* name, TUnit::type unit, int64_t value) {
        auto* counter = ADD_CHILD_COUNTER(profile->runtime_profile, name, unit, kSection);
        COUNTER_UPDATE(counter, value);
    };
    update("OpenTime", TUnit::TIME_NS, _open_ns);
    update("EvaluateTime", TUnit::TIME_NS, _evaluate_ns);
    update("SerializeTime", TUnit::TIME_NS, _serialize_ns);
    update("SerializedBytes", TUnit::BYTES, _serialized_bytes);

    if (_shared_state != nullptr) {
        // Shards may evaluate concurrently. Drain shared counters once, instead of
        // attributing overlapping before/after snapshots to every scanner.
        std::lock_guard lock(_shared_state->profile_mutex);
        auto current = _paimon_file_system->get_stats();
        PaimonFileSystemStats::Snapshot delta;
        add_io_delta(&delta, _shared_state->reported_io, current);
        _shared_state->reported_io = current;
        update_paimon_io_profile(profile->runtime_profile, delta);
    } else {
        update_paimon_io_profile(profile->runtime_profile, _io_stats);
    }
}

int64_t PaimonGlobalIndexScanner::estimated_mem_usage() const {
    // The shared pool measures all concurrently active shards, not one source.
    // Zero means a per-source estimate is unavailable: the adaptive I/O limiter
    // keeps its existing estimate, while the query tracker still accounts every
    // allocation. Reporting the shared peak here multiplies it by driver count.
    if (_scanner_ctx != nullptr && _scanner_ctx->paimon_global_index_context != nullptr) {
        return 0;
    }
    return _memory_pool == nullptr ? 0 : static_cast<int64_t>(_memory_pool->MaxMemoryUsage());
}

Status PaimonGlobalIndexScanner::_parse_request() {
    const auto& scan_range = _scanner_ctx->scan_range->paimon_global_index_scan_range;
    return _parse_request(scan_range, &_request);
}

Status PaimonGlobalIndexScanner::_parse_request(const TPaimonGlobalIndexScanRange& scan_range,
                                                rapidjson::Document* request) {
    if (!scan_range.__isset.protocol_version || !scan_range.__isset.shard_id || !scan_range.__isset.range_from ||
        !scan_range.__isset.range_to || !scan_range.__isset.query_json || !scan_range.__isset.table_path ||
        !scan_range.__isset.snapshot_id) {
        return Status::InvalidArgument("Incomplete Paimon Global Index scan range");
    }
    if (scan_range.protocol_version != kProtocolVersion) {
        return Status::InvalidArgument(
                fmt::format("Unsupported Paimon Global Index protocol version {}", scan_range.protocol_version));
    }
    if (scan_range.shard_id < 0 || scan_range.snapshot_id < 0 || scan_range.table_path.empty() ||
        scan_range.range_from < 0 || scan_range.range_to < scan_range.range_from) {
        return Status::InvalidArgument(
                fmt::format("Invalid Paimon Global Index shard [{}, {}]", scan_range.range_from, scan_range.range_to));
    }
    if (request->Parse(scan_range.query_json.data(), scan_range.query_json.size()).HasParseError() ||
        !request->IsObject()) {
        return Status::InvalidArgument("Invalid Paimon Global Index request JSON");
    }
    if (!request->HasMember("version") || !(*request)["version"].IsInt() ||
        (*request)["version"].GetInt() != scan_range.protocol_version || !request->HasMember("snapshotId") ||
        !(*request)["snapshotId"].IsInt64() || (*request)["snapshotId"].GetInt64() != scan_range.snapshot_id ||
        !request->HasMember("schemaId") || !(*request)["schemaId"].IsInt64() || (*request)["schemaId"].GetInt64() < 0 ||
        !request->HasMember("schemaTimeMillis") || !(*request)["schemaTimeMillis"].IsInt64() ||
        (*request)["schemaTimeMillis"].GetInt64() < 0 || !request->HasMember("predicate") ||
        !(*request)["predicate"].IsObject() || !request->HasMember("indexes") || !(*request)["indexes"].IsObject() ||
        (*request)["indexes"].ObjectEmpty() || !request->HasMember("tablePath") ||
        !(*request)["tablePath"].IsString() ||
        std::string_view((*request)["tablePath"].GetString()) != scan_range.table_path ||
        !request->HasMember("branch") || !(*request)["branch"].IsString() ||
        std::string_view((*request)["branch"].GetString()) != "main") {
        return Status::InvalidArgument("Inconsistent Paimon Global Index request envelope");
    }
    for (const auto& member : (*request)["indexes"].GetObject()) {
        const auto& binding = member.value;
        if (member.name.GetStringLength() == 0 || !binding.IsObject() || !binding.HasMember("fieldId") ||
            !binding["fieldId"].IsInt() || binding["fieldId"].GetInt() < 0 || !binding.HasMember("provider") ||
            !binding["provider"].IsString() || std::string_view(binding["provider"].GetString()) != "btree") {
            return Status::InvalidArgument("Invalid Paimon Global Index field binding");
        }
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexScanner::_evaluate(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(check_query_state(runtime_state));
    const auto& scan_range = _scanner_ctx->scan_range->paimon_global_index_scan_range;
    std::vector<paimon::Range> ranges = {paimon::Range(scan_range.range_from, scan_range.range_to)};
    auto row_range = paimon::RowRangeIndex::Create(ranges);
    if (!row_range.ok()) {
        return paimon_error("row-range creation", row_range.status());
    }
    std::optional<paimon::RowRangeIndex> row_range_index(std::move(row_range).value());

    const rapidjson::Value& indexes = _request["indexes"];
    auto reader_getter = [&](std::string_view column_name) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
        auto member = indexes.FindMember(
                rapidjson::StringRef(column_name.data(), static_cast<rapidjson::SizeType>(column_name.size())));
        if (member == indexes.MemberEnd()) {
            return Status::InvalidArgument(
                    fmt::format("No validated Paimon Global Index selected for column '{}'", column_name));
        }
        const int32_t field_id = member->value["fieldId"].GetInt();
        RETURN_IF_ERROR(check_query_state(runtime_state));
        // GlobalIndexScan's initialization is shared; each call creates its own
        // readers. The SDK does not promise concurrent CreateReaders calls.
        std::unique_lock lock(_scan_context->mutex, std::defer_lock);
        RETURN_IF_ERROR(lock_scan(runtime_state, &lock));
        auto readers = _global_index_scan->CreateReaders(field_id, row_range_index);
        RETURN_IF_ERROR(check_query_state(runtime_state));
        if (!readers.ok()) {
            return paimon_error(fmt::format("reader creation for field {}", field_id), readers.status());
        }
        // FE validated that BTree is the only provider for this field at this snapshot.
        // The field-ID API wraps its files in a reader whose type is "union", not "btree".
        if (readers.value().size() != 1 || readers.value().front() == nullptr) {
            return Status::InternalError(
                    fmt::format("Validated Paimon BTree index is missing or ambiguous for field {} at snapshot {}",
                                field_id, scan_range.snapshot_id));
        }
        return readers.value().front();
    };

    auto type_getter = [&](std::string_view column_name) -> StatusOr<std::string> {
#ifdef BE_TEST
        if (_field_type_getter_for_test) {
            return _field_type_getter_for_test(column_name);
        }
#endif
        auto member = indexes.FindMember(
                rapidjson::StringRef(column_name.data(), static_cast<rapidjson::SizeType>(column_name.size())));
        if (member == indexes.MemberEnd() || _schema_guard == nullptr) {
            return Status::InvalidArgument("Missing Paimon Global Index schema field binding");
        }
        return _schema_guard->field_type(member->value["fieldId"].GetInt(), column_name);
    };
    PaimonGlobalIndexEvaluator evaluator(std::move(reader_getter), std::move(type_getter));
    auto evaluated = evaluator.evaluate(_request["predicate"]);
    RETURN_IF_ERROR(check_query_state(runtime_state));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexResult> result, std::move(evaluated));
    if (result == nullptr) {
        return Status::InternalError("Paimon Global Index evaluator returned a null result");
    }
    // Reader ranges prune index files, but may still return rows outside this shard.
    // Clip before serialization so intersected per-column file boundaries do not
    // duplicate candidate rows across distributed shards.
    auto clipped = result->And(paimon::BitmapGlobalIndexResult::FromRanges(ranges));
    if (!clipped.ok()) {
        return paimon_error("shard result clipping", clipped.status());
    }
    return std::move(clipped).value();
}

} // namespace starrocks
