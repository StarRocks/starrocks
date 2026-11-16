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
#include <paimon/global_index/bitmap_global_index_result.h>
#include <paimon/global_index/global_indexer.h>
#include <paimon/global_index/global_indexer_factory.h>
#include <paimon/global_index/io/global_index_file_reader.h>
#include <paimon/utils/range.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_global_index_evaluator.h"
#include "connector/hive/paimon/paimon_global_index_reader.h"
#include "connector/hive/paimon/tracked_paimon_memory_pool.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

namespace starrocks {
namespace {

using paimon_global_index::arrow_key_format;
using paimon_global_index::valid_btree_metadata;
using paimon_global_index::SegmentedIndexReader;

constexpr int32_t kProtocolVersion = 5;
constexpr std::string_view kIndexResultColumn = "index_result";

Status paimon_error(std::string_view operation, const paimon::Status& status) {
    return Status::InternalError(fmt::format("Paimon Global Index {} failed: {}", operation, status.ToString()));
}

Status check_query_state(RuntimeState* runtime_state) {
    RETURN_IF_CANCELLED(runtime_state);
    RETURN_IF_ERROR(runtime_state->check_query_state("Paimon Global Index scan"));
    return runtime_state->check_mem_limit("Paimon Global Index scan");
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

// A range-local adapter. Only files selected by FE can be opened; it never
// discovers a snapshot, schema or manifest through the SDK.
class IndexFileReader final : public paimon::GlobalIndexFileReader {
public:
    using Open = std::function<paimon::Result<std::unique_ptr<paimon::InputStream>>(const std::string&)>;
    explicit IndexFileReader(Open open) : _open(std::move(open)) {}
    paimon::Result<std::unique_ptr<paimon::InputStream>> GetInputStream(const std::string& path) const override {
        return _open(path);
    }

private:
    Open _open;
};

} // namespace

// Stream callbacks may retain this lease, but never the scanner or RuntimeState.
// File opens hold the lease until Open returns, so finishing an operation also
// waits for in-flight opens before the range's ObjectPool can release its FS.
// Already opened streams own their files and cancel late reads through check().
struct PaimonGlobalIndexOperation {
    void start(RuntimeState* state) {
        std::lock_guard lock(mutex);
        runtime_state = state;
    }
    void finish() {
        std::lock_guard lock(mutex);
        runtime_state = nullptr;
    }
    paimon::Status check() {
        std::lock_guard lock(mutex);
        if (runtime_state == nullptr) return paimon::Status::Cancelled("Paimon Global Index operation has finished");
        auto status = check_query_state(runtime_state);
        return status.ok() ? paimon::Status::OK() : paimon::Status::Cancelled(status.to_string());
    }
    template <class Open>
    paimon::Result<std::unique_ptr<paimon::InputStream>> open(Open&& open_file) {
        std::lock_guard lock(mutex);
        if (runtime_state == nullptr) return paimon::Status::Cancelled("Paimon Global Index operation has finished");
        return open_file();
    }
    // Open invokes the adapter's query-state callback on the same thread.
    std::recursive_mutex mutex;
    RuntimeState* runtime_state = nullptr;
};

Status PaimonGlobalIndexScanner::do_init(RuntimeState*, const HdfsScannerContext&) {
    _emitted = false;
    return Status::OK();
}

Status PaimonGlobalIndexScanner::do_open(RuntimeState* runtime_state) {
    const int64_t start_ns = monotonic_nanos();
    DeferOp record_time([&] { _open_ns += monotonic_nanos() - start_ns; });
    RETURN_IF_ERROR(check_query_state(runtime_state));
    RETURN_IF_ERROR(_parse_request());
    if (_scanner_ctx->fs == nullptr) return Status::InternalError("Paimon Global Index scanner has no file system");
    const auto tracker = runtime_state->query_mem_tracker_ptr();
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker.get());
    _operation =
            std::allocate_shared<PaimonGlobalIndexOperation>(PaimonQueryAllocator<PaimonGlobalIndexOperation>(tracker));
    _operation->start(runtime_state);
    DeferOp finish([&] { _operation->finish(); });
    _memory_pool = std::allocate_shared<TrackedPaimonMemoryPool>(PaimonQueryAllocator<TrackedPaimonMemoryPool>(tracker),
                                                                 tracker.get());
    _paimon_file_system = std::allocate_shared<PaimonFileSystem>(
            PaimonQueryAllocator<PaimonFileSystem>(tracker), _scanner_ctx->fs, _scanner_ctx->datacache_options,
            [operation = _operation] { return operation->check(); }, tracker);
    auto file_reader = std::allocate_shared<IndexFileReader>(
            PaimonQueryAllocator<IndexFileReader>(tracker),
            [operation = _operation, fs = _paimon_file_system](const std::string& path) {
                return operation->open([&] { return fs->Open(path); });
            });
    const auto& files = _scanner_ctx->scan_range->paimon_global_index_scan_range.index_files;
    using Group = std::tuple<int32_t, int64_t, int64_t>;
    std::map<Group, std::vector<paimon::GlobalIndexIOMeta>> groups;
    for (const auto& file : files) {
        auto [it, inserted] = _field_types.emplace(file.field_id, file.field_type);
        if (!inserted && it->second != file.field_type) {
            return Status::InvalidArgument("Inconsistent Paimon Global Index key types");
        }
        auto metadata = std::allocate_shared<paimon::Bytes>(PaimonQueryAllocator<paimon::Bytes>(tracker), file.metadata,
                                                            _memory_pool.get());
        groups[{file.field_id, file.row_range_from, file.row_range_to}].emplace_back(file.path, file.file_size,
                                                                                     metadata);
    }
    for (const auto& member : _request["indexes"].GetObject()) {
        const int32_t field_id = member.value["fieldId"].GetInt();
        auto composite =
                std::allocate_shared<SegmentedIndexReader>(PaimonQueryAllocator<SegmentedIndexReader>(tracker));
        auto indexer = paimon::GlobalIndexerFactory::Get("btree", {});
        if (!indexer.ok()) return paimon_error("indexer creation", indexer.status());
        if (indexer.value() == nullptr) return Status::NotSupported("Paimon BTree indexer is unavailable");
        for (const auto& [group, metas] : groups) {
            if (std::get<0>(group) != field_id) continue;
            RETURN_IF_ERROR(check_query_state(runtime_state));
            auto reader = paimon_global_index::create_segment_reader(*indexer.value(), member.name.GetString(),
                                                                     _field_types.at(field_id), file_reader, metas,
                                                                     _memory_pool);
            if (!reader.ok()) return paimon_error("file reader creation", reader.status());
            if (reader.value() == nullptr) return Status::InternalError("Paimon BTree file reader is unavailable");
            composite->add(std::move(reader).value(), std::get<1>(group));
        }
        _readers.emplace(field_id, std::move(composite));
    }
    return check_query_state(runtime_state);
}

Status PaimonGlobalIndexScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_emitted) {
        return Status::EndOfFile("Paimon Global Index shard is exhausted");
    }

    RETURN_IF_ERROR(check_query_state(runtime_state));
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(runtime_state->query_mem_tracker_ptr().get());
    _operation->start(runtime_state);
    DeferOp finish_operation([&] { _operation->finish(); });

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
    if (!_request.HasMember("resultBytesLimit") || !_request["resultBytesLimit"].IsInt64() ||
        _request["resultBytesLimit"].GetInt64() <= 0 ||
        _serialized_bytes > std::min<int64_t>(1024 * 1024, _request["resultBytesLimit"].GetInt64())) {
        return Status::MemoryLimitExceeded("Paimon Global Index result exceeds the FE collection budget");
    }

    for (const SlotDescriptor* slot : _scanner_ctx->materialize_slots) {
        MutableColumnPtr column = ColumnHelper::create_column(slot->type(), true);
        if (slot->col_name() == "shard_id") {
            column->append_datum(
                    Datum(static_cast<int64_t>(_scanner_ctx->scan_range->paimon_global_index_scan_range.shard_id)));
        } else if (slot->col_name() == kIndexResultColumn) {
            column->append_datum(Datum(Slice(bytes->data(), bytes->size())));
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
    if (_operation != nullptr) _operation->finish();
    _readers.clear();
    if (_paimon_file_system != nullptr) _io_stats = _paimon_file_system->get_stats();
    _paimon_file_system.reset();
    _memory_pool.reset();
    _operation.reset();
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

    update_paimon_io_profile(profile->runtime_profile,
                             _paimon_file_system != nullptr ? _paimon_file_system->get_stats() : _io_stats);
}

int64_t PaimonGlobalIndexScanner::estimated_mem_usage() const {
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
        !scan_range.__isset.snapshot_id || !scan_range.__isset.index_files || scan_range.index_files.empty()) {
        return Status::InvalidArgument("Incomplete Paimon Global Index scan range");
    }
    if (scan_range.protocol_version != kProtocolVersion) {
        return Status::InvalidArgument(
                fmt::format("Unsupported Paimon Global Index protocol version {}", scan_range.protocol_version));
    }
    // Paimon adds one to the inclusive upper bound when creating a bitmap.
    if (scan_range.shard_id < 0 || scan_range.snapshot_id < 0 || scan_range.table_path.empty() ||
        scan_range.range_from < 0 || scan_range.range_to < scan_range.range_from ||
        scan_range.range_to == std::numeric_limits<int64_t>::max()) {
        return Status::InvalidArgument(
                fmt::format("Invalid Paimon Global Index shard [{}, {}]", scan_range.range_from, scan_range.range_to));
    }
    if (scan_range.query_json.size() > 64 * 1024 || scan_range.index_files.size() > 65536) {
        return Status::InvalidArgument("Oversized Paimon Global Index task");
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
    if (!request->HasMember("resultBytesLimit") || !(*request)["resultBytesLimit"].IsInt64() ||
        (*request)["resultBytesLimit"].GetInt64() <= 0 || (*request)["resultBytesLimit"].GetInt64() > 1024 * 1024) {
        return Status::InvalidArgument("Invalid Paimon Global Index result limit");
    }
    for (const auto& member : (*request)["indexes"].GetObject()) {
        const auto& binding = member.value;
        if (member.name.GetStringLength() == 0 || !binding.IsObject() || !binding.HasMember("fieldId") ||
            !binding["fieldId"].IsInt() || binding["fieldId"].GetInt() < 0 || !binding.HasMember("provider") ||
            !binding["provider"].IsString() || std::string_view(binding["provider"].GetString()) != "btree") {
            return Status::InvalidArgument("Invalid Paimon Global Index field binding");
        }
    }
    size_t payload_bytes = scan_range.query_json.size() + 128;
    std::set<int32_t> fields;
    for (const auto& file : scan_range.index_files) {
        if (!file.__isset.field_id || !file.__isset.field_type || !file.__isset.path || !file.__isset.file_size ||
            !file.__isset.row_range_from || !file.__isset.row_range_to || !file.__isset.metadata || file.field_id < 0 ||
            file.path.empty() || file.file_size <= 0 || file.row_range_from < 0 ||
            file.row_range_to < file.row_range_from || file.row_range_to == std::numeric_limits<int64_t>::max() ||
            file.row_range_from > scan_range.range_to || file.row_range_to < scan_range.range_from ||
            arrow_key_format(file.field_type) == nullptr || !valid_btree_metadata(file.metadata)) {
            return Status::InvalidArgument("Invalid Paimon Global Index file descriptor");
        }
        payload_bytes += file.path.size() + file.metadata.size() + file.field_type.size() + 128;
        if (payload_bytes > 1024 * 1024) {
            return Status::InvalidArgument("Oversized Paimon Global Index task");
        }
        fields.insert(file.field_id);
    }
    for (const auto& member : (*request)["indexes"].GetObject()) {
        if (!fields.contains(member.value["fieldId"].GetInt())) {
            return Status::InvalidArgument("Missing Paimon Global Index files for a predicate column");
        }
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<paimon::GlobalIndexResult>> PaimonGlobalIndexScanner::_evaluate(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(check_query_state(runtime_state));
    const auto& scan_range = _scanner_ctx->scan_range->paimon_global_index_scan_range;
    std::vector<paimon::Range> ranges = {paimon::Range(scan_range.range_from, scan_range.range_to)};
    const rapidjson::Value& indexes = _request["indexes"];
    auto reader_getter = [&](std::string_view column_name) -> StatusOr<std::shared_ptr<paimon::GlobalIndexReader>> {
        auto member = indexes.FindMember(rapidjson::StringRef(column_name.data(), column_name.size()));
        if (member == indexes.MemberEnd()) return Status::InvalidArgument("Missing Paimon Global Index column binding");
        auto reader = _readers.find(member->value["fieldId"].GetInt());
        if (reader == _readers.end()) return Status::InvalidArgument("Missing Paimon Global Index file reader");
        RETURN_IF_ERROR(check_query_state(runtime_state));
        return reader->second;
    };
    auto type_getter = [&](std::string_view column_name) -> StatusOr<std::string> {
        auto member = indexes.FindMember(rapidjson::StringRef(column_name.data(), column_name.size()));
        if (member == indexes.MemberEnd()) return Status::InvalidArgument("Missing Paimon Global Index column binding");
        return _field_types.at(member->value["fieldId"].GetInt());
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
