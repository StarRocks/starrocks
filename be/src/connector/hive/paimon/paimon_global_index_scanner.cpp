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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <map>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_global_index_evaluator.h"
#include "connector/hive/paimon/paimon_query_allocator.h"
#include "connector/hive/paimon/tracked_paimon_memory_pool.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"

namespace starrocks {
namespace {

constexpr int32_t kPredicateProtocolVersion = 1;
constexpr int32_t kTopNProtocolVersion = 2;
constexpr std::string_view kIndexResultColumn = "index_result";
constexpr std::string_view kArgsColumn = "args";
constexpr std::string_view kRowIdColumn = "row_id";
constexpr std::string_view kScoreColumn = "score";
constexpr std::string_view kTopNKind = "top_n";

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
    if (semantic_type == "vector") {
        return "lumina";
    }
    return {};
}

bool is_supported_vector_direction(const rapidjson::Value& score_expression, bool ascending) {
    if (!score_expression.IsObject() || !score_expression.HasMember("o") || !score_expression["o"].IsString() ||
        std::string_view(score_expression["o"].GetString(), score_expression["o"].GetStringLength()) != "ca" ||
        !score_expression.HasMember("f") || !score_expression["f"].IsString() || !score_expression.HasMember("a") ||
        !score_expression["a"].IsArray() || score_expression["a"].Size() != 2) {
        return false;
    }
    std::string_view function(score_expression["f"].GetString(), score_expression["f"].GetStringLength());
    if (function == "approx_l2_distance") {
        return ascending;
    }
    return !ascending && (function == "approx_cosine_similarity" || function == "approx_inner_product");
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
    _open_ns = 0;
    _evaluate_ns = 0;
    _serialize_ns = 0;
    _serialized_bytes = 0;
    _scored_rows = 0;
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
    const auto query_tracker = runtime_state->query_mem_tracker_ptr();
    _paimon_file_system =
            std::allocate_shared<PaimonFileSystem>(PaimonQueryAllocator<PaimonFileSystem>(query_tracker),
                                                   _scanner_ctx->fs, _scanner_ctx->datacache_options, query_tracker);

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

    const auto& scan_range = _scanner_ctx->scan_range->paimon_global_index_scan_range;
    const bool top_n = scan_range.protocol_version == kTopNProtocolVersion;
    const std::string& query_json = _scanner_ctx->scan_range->paimon_global_index_scan_range.query_json;
    if (top_n) {
        auto scored = std::dynamic_pointer_cast<paimon::ScoredGlobalIndexResult>(result);
        if (scored == nullptr) {
            return Status::InternalError("Paimon Vector TopN returned a non-scored result");
        }
        auto iterator_result = scored->CreateScoredIterator();
        if (!iterator_result.ok()) {
            return paimon_error("scored result iteration", iterator_result.status());
        }
        auto iterator = std::move(iterator_result).value();
        if (iterator == nullptr) {
            return Status::InternalError("Paimon Vector TopN returned a null scored iterator");
        }
        std::vector<int64_t> row_ids;
        std::vector<float> scores;
        const int32_t limit = _request["localLimit"].GetInt();
        row_ids.reserve(std::min<int32_t>(limit, 4096));
        scores.reserve(std::min<int32_t>(limit, 4096));
        while (iterator->HasNext() && row_ids.size() < static_cast<size_t>(limit)) {
            if ((row_ids.size() & 1023) == 0) {
                RETURN_IF_ERROR(check_query_state(runtime_state));
            }
            auto [row_id, score] = iterator->NextWithScore();
            if (row_id < scan_range.range_from || row_id > scan_range.range_to) {
                return Status::InternalError(fmt::format("Paimon Vector TopN returned row id {} outside shard [{}, {}]",
                                                         row_id, scan_range.range_from, scan_range.range_to));
            }
            score = _normalize_score(_request["scoreExpression"], score);
            if (!std::isfinite(score)) {
                return Status::InternalError(
                        fmt::format("Paimon Vector TopN returned a non-finite score for row id {}", row_id));
            }
            row_ids.push_back(row_id);
            scores.push_back(score);
        }
        if (iterator->HasNext()) {
            return Status::InternalError(
                    fmt::format("Paimon Vector TopN returned more than the requested {} candidates", limit));
        }

        const size_t num_rows = row_ids.size();
        for (const SlotDescriptor* slot : _scanner_ctx->materialize_slots) {
            MutableColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            if (slot->col_name() == kRowIdColumn) {
                for (int64_t row_id : row_ids) {
                    column->append_datum(Datum(row_id));
                }
            } else if (slot->col_name() == kScoreColumn) {
                for (float score : scores) {
                    column->append_datum(Datum(score));
                }
            } else if (slot->col_name() == kArgsColumn) {
                for (size_t i = 0; i < num_rows; ++i) {
                    column->append_datum(Datum(Slice(query_json)));
                }
            } else {
                column->append_default(num_rows);
            }
            (*chunk)->append_or_update_column(std::move(column), slot->id());
        }
        (*chunk)->set_num_rows(num_rows);
        _scored_rows = static_cast<int64_t>(num_rows);
        _emitted = true;
        return Status::OK();
    }

    const int64_t serialize_start_ns = monotonic_nanos();
    auto serialized = paimon::GlobalIndexResult::Serialize(result, _memory_pool);
    if (!serialized.ok()) {
        return paimon_error("result serialization", serialized.status());
    }
    auto bytes = std::move(serialized).value();
    _serialized_bytes = static_cast<int64_t>(bytes->size());
    _serialize_ns = monotonic_nanos() - serialize_start_ns;

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
    update("ScoredRows", TUnit::UNIT, _scored_rows);

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
    if (scan_range.protocol_version != kPredicateProtocolVersion &&
        scan_range.protocol_version != kTopNProtocolVersion) {
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
        !request->HasMember("indexes") || !(*request)["indexes"].IsObject() ||
        (*request)["indexes"].MemberCount() == 0) {
        return Status::InvalidArgument("Inconsistent Paimon Global Index request envelope");
    }
    if (scan_range.protocol_version == kPredicateProtocolVersion) {
        if (!request->HasMember("predicate") || !(*request)["predicate"].IsObject() || request->HasMember("kind") ||
            request->HasMember("scoreExpression") || request->HasMember("localLimit") ||
            request->HasMember("ascending")) {
            return Status::InvalidArgument("Invalid Paimon predicate request envelope");
        }
        return Status::OK();
    }

    const rapidjson::Value& indexes = (*request)["indexes"];
    if (indexes.MemberCount() != 1 || !indexes.MemberBegin()->value.IsString() ||
        std::string_view(indexes.MemberBegin()->value.GetString(), indexes.MemberBegin()->value.GetStringLength()) !=
                "vector" ||
        request->HasMember("predicate") || !request->HasMember("kind") || !(*request)["kind"].IsString() ||
        std::string_view((*request)["kind"].GetString(), (*request)["kind"].GetStringLength()) != kTopNKind ||
        !request->HasMember("scoreExpression") || !(*request)["scoreExpression"].IsObject() ||
        !request->HasMember("localLimit") || !(*request)["localLimit"].IsInt() ||
        (*request)["localLimit"].GetInt() <= 0 || !request->HasMember("ascending") ||
        !(*request)["ascending"].IsBool() ||
        !is_supported_vector_direction((*request)["scoreExpression"], (*request)["ascending"].GetBool())) {
        return Status::InvalidArgument("Invalid Paimon Vector TopN request envelope");
    }
    return Status::OK();
}

float PaimonGlobalIndexScanner::_normalize_score(const rapidjson::Value& score_expression, float score) {
    std::string_view function(score_expression["f"].GetString(), score_expression["f"].GetStringLength());
    // Lumina exposes cosine distance (1 - cosine similarity), while StarRocks orders
    // approx_cosine_similarity itself. Normalize before the cross-shard global merge.
    return function == "approx_cosine_similarity" ? 1.0f - score : score;
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
    std::shared_ptr<paimon::GlobalIndexResult> result;
    if (scan_range.protocol_version == kTopNProtocolVersion) {
        ASSIGN_OR_RETURN(result,
                         evaluator.evaluate_top_n(_request["scoreExpression"], _request["localLimit"].GetInt()));
    } else {
        ASSIGN_OR_RETURN(result, evaluator.evaluate(_request["predicate"]));
    }
    RETURN_IF_ERROR(check_query_state(runtime_state));
    if (result == nullptr) {
        return Status::InternalError("Paimon Global Index evaluator returned a null result");
    }
    return result;
}

} // namespace starrocks
