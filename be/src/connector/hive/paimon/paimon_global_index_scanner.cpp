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
#include <paimon/utils/range.h>
#include <paimon/utils/row_range_index.h>

#include <chrono>
#include <map>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_global_index_evaluator.h"
#include "connector/hive/paimon/tracked_paimon_memory_pool.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

namespace starrocks {
namespace {

constexpr int32_t kProtocolVersion = 1;
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

int64_t monotonic_nanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

std::string paimon_index_type(std::string_view semantic_type) {
    if (semantic_type == "range") {
        return "btree";
    }
    return {};
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

} // namespace

Status PaimonGlobalIndexScanner::do_init(RuntimeState*, const HdfsScannerContext&) {
    _emitted = false;
    return Status::OK();
}

Status PaimonGlobalIndexScanner::do_open(RuntimeState* runtime_state) {
    const int64_t start_ns = monotonic_nanos();
    RETURN_IF_ERROR(check_query_state(runtime_state));
    RETURN_IF_ERROR(_parse_request());
    if (_scanner_ctx->fs == nullptr) {
        return Status::InternalError("Paimon Global Index scanner has no StarRocks file system");
    }

    _memory_pool = std::make_shared<TrackedPaimonMemoryPool>(runtime_state->query_mem_tracker_ptr().get());
    _paimon_file_system = std::make_shared<PaimonFileSystem>(_scanner_ctx->fs, _scanner_ctx->datacache_options);

    const auto& scan_range = _scanner_ctx->scan_range->paimon_global_index_scan_range;
#ifdef BE_TEST
    auto scan = _scan_factory_for_test ? _scan_factory_for_test()
                                       : paimon::GlobalIndexScan::Create(
                                                 scan_range.table_path, std::optional<int64_t>(scan_range.snapshot_id),
                                                 std::optional<std::vector<std::map<std::string, std::string>>>{},
                                                 std::map<std::string, std::string>(), _paimon_file_system,
                                                 paimon::GetGlobalDefaultExecutor(), _memory_pool);
#else
    auto scan = paimon::GlobalIndexScan::Create(scan_range.table_path, std::optional<int64_t>(scan_range.snapshot_id),
                                                std::optional<std::vector<std::map<std::string, std::string>>>{},
                                                std::map<std::string, std::string>(), _paimon_file_system,
                                                paimon::GetGlobalDefaultExecutor(), _memory_pool);
#endif
    if (!scan.ok()) {
        return paimon_error("scan creation", scan.status());
    }
    _global_index_scan = std::move(scan).value();
    _open_ns = monotonic_nanos() - start_ns;
    return Status::OK();
}

Status PaimonGlobalIndexScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_emitted) {
        return Status::EndOfFile("Paimon Global Index shard is exhausted");
    }

    const int64_t evaluate_start_ns = monotonic_nanos();
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexResult> result, _evaluate(runtime_state));
    _evaluate_ns = monotonic_nanos() - evaluate_start_ns;

    const int64_t serialize_start_ns = monotonic_nanos();
    auto serialized = paimon::GlobalIndexResult::Serialize(result, _memory_pool);
    if (!serialized.ok()) {
        return paimon_error("result serialization", serialized.status());
    }
    auto bytes = std::move(serialized).value();
    _serialized_bytes = static_cast<int64_t>(bytes->size());
    _serialize_ns = monotonic_nanos() - serialize_start_ns;

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

    if (_paimon_file_system != nullptr) {
        update_paimon_io_profile(profile->runtime_profile, _paimon_file_system->get_stats());
    }
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
        !scan_range.__isset.snapshot_id) {
        return Status::InvalidArgument("Incomplete Paimon Global Index scan range");
    }
    if (scan_range.protocol_version != kProtocolVersion) {
        return Status::InvalidArgument(
                fmt::format("Unsupported Paimon Global Index protocol version {}", scan_range.protocol_version));
    }
    if (scan_range.range_from < 0 || scan_range.range_to < scan_range.range_from) {
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
        !request->HasMember("predicate") || !(*request)["predicate"].IsObject() || !request->HasMember("indexes") ||
        !(*request)["indexes"].IsObject()) {
        return Status::InvalidArgument("Inconsistent Paimon Global Index request envelope");
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
        if (member == indexes.MemberEnd() || !member->value.IsString()) {
            return Status::InvalidArgument(
                    fmt::format("No validated Paimon Global Index selected for column '{}'", column_name));
        }
        std::string index_type = paimon_index_type(member->value.GetString());
        if (index_type.empty()) {
            return Status::InvalidArgument(fmt::format("Unsupported Paimon Global Index type '{}' for column '{}'",
                                                       member->value.GetString(), column_name));
        }
        auto reader = _global_index_scan->CreateReader(std::string(column_name), index_type, row_range_index);
        if (!reader.ok()) {
            return paimon_error(fmt::format("reader creation for column '{}'", column_name), reader.status());
        }
        if (reader.value() == nullptr) {
            return Status::InternalError(
                    fmt::format("Validated Paimon Global Index '{}' disappeared for column '{}' at snapshot {}",
                                index_type, column_name, scan_range.snapshot_id));
        }
        return std::move(reader).value();
    };

    PaimonGlobalIndexEvaluator evaluator(std::move(reader_getter));
    ASSIGN_OR_RETURN(std::shared_ptr<paimon::GlobalIndexResult> result, evaluator.evaluate(_request["predicate"]));
    RETURN_IF_ERROR(check_query_state(runtime_state));
    if (result == nullptr) {
        return Status::InternalError("Paimon Global Index evaluator returned a null result");
    }
    return result;
}

} // namespace starrocks
