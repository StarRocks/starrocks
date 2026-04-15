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

#include "runtime/rejected_record_writer.h"

#include <filesystem>
#include <fstream>

#include "agent/master_info.h"
#include "base/uid_util.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "common/logging.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

// Best-effort extraction of a single cell as a JSON-safe string. Used when
// building the `raw_record` object. We intentionally do not try to preserve
// numeric vs string distinction here -- replay uses
// `CAST(raw_record->>'col' AS <type>)` anyway, and always-string values
// keep this writer free of ExprCore dependencies (RuntimeCore is not
// allowed to take those per the BE module boundary manifest).
std::string cell_to_string(const Column& col, size_t row_idx) {
    if (col.is_null(row_idx)) {
        return {};
    }
    if (col.is_binary()) {
        // BinaryColumn::debug_item() wraps values in single quotes, which
        // would be embedded into the JSON string literal. Go through
        // get_slice() so the value appears exactly as it was loaded.
        const auto* binary = down_cast<const BinaryColumn*>(&col);
        Slice slice = binary->get_slice(row_idx);
        return {slice.data, slice.size};
    }
    // For numeric / decimal / date / json / nested columns, debug_item()
    // produces a reasonable literal representation that replay queries can
    // CAST back to the target type.
    return col.debug_item(row_idx);
}

// Serialize a rapidjson value to a std::string.
std::string to_json_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    value.Accept(writer);
    return {buf.GetString(), buf.GetSize()};
}

// JSON-encode a key/value pair onto `obj`. `value` is added as a string if
// `is_null` is false, or as JSON null otherwise. We always copy strings so
// the caller does not have to worry about slice lifetime.
void add_member(rapidjson::Value& obj, rapidjson::Document::AllocatorType& alloc, const std::string& key,
                const std::string& value, bool is_null) {
    rapidjson::Value k(key.c_str(), static_cast<rapidjson::SizeType>(key.size()), alloc);
    if (is_null) {
        obj.AddMember(k, rapidjson::Value(rapidjson::kNullType), alloc);
    } else {
        rapidjson::Value v(value.c_str(), static_cast<rapidjson::SizeType>(value.size()), alloc);
        obj.AddMember(k, v, alloc);
    }
}

// Add a JSON-typed member: if `json_text` is valid JSON parse it and embed,
// otherwise fall back to a JSON null. We parse here so the final line is
// one well-formed JSON object rather than a JSON-of-JSON-strings that the
// sync daemon would have to re-parse.
void add_parsed_json(rapidjson::Document& doc, rapidjson::Document::AllocatorType& alloc, const char* key,
                     const std::string& json_text) {
    if (json_text.empty()) {
        doc.AddMember(rapidjson::Value(key, alloc), rapidjson::Value(rapidjson::kNullType), alloc);
        return;
    }
    rapidjson::Document parsed;
    if (parsed.Parse(json_text.c_str(), json_text.size()).HasParseError()) {
        doc.AddMember(rapidjson::Value(key, alloc), rapidjson::Value(rapidjson::kNullType), alloc);
        return;
    }
    // Move the parsed subtree into `doc`, copying strings into `alloc` so the
    // subtree outlives its own allocator.
    rapidjson::Value moved(parsed, alloc);
    doc.AddMember(rapidjson::Value(key, alloc), moved, alloc);
}

// Positional column name used when `col_names` is empty or mismatched --
// surfacing `col_0`, `col_1`, ... rather than silently dropping the row.
std::string positional_name(size_t idx) {
    return "col_" + std::to_string(idx);
}

std::string pick_name(const std::vector<std::string>& col_names, size_t idx) {
    if (idx < col_names.size() && !col_names[idx].empty()) {
        return col_names[idx];
    }
    return positional_name(idx);
}

// Convert TQueryOptions.load_job_type to the stable string values the
// `_statistics_.rejected_records.load_type` column's comment enumerates.
// Returns "UNKNOWN" only when the query is genuinely not a load (e.g.
// an internal SELECT that happened to set log_rejected_record_num) or
// the field was not set by the FE.
std::string resolve_load_type(const TQueryOptions& query_options) {
    if (!query_options.__isset.load_job_type) {
        return "UNKNOWN";
    }
    switch (query_options.load_job_type) {
    case TLoadJobType::BROKER:
        return "BROKER_LOAD";
    case TLoadJobType::SPARK:
        return "SPARK_LOAD";
    case TLoadJobType::INSERT_QUERY:
    case TLoadJobType::INSERT_VALUES:
        return "INSERT";
    case TLoadJobType::STREAM_LOAD:
        return "STREAM_LOAD";
    case TLoadJobType::ROUTINE_LOAD:
        return "ROUTINE_LOAD";
    default:
        return "UNKNOWN";
    }
}

} // namespace

RejectedRecordWriter::RejectedRecordWriter(RuntimeState* state) : _state(state) {
    DCHECK(state != nullptr);
}

RejectedRecordWriter::~RejectedRecordWriter() = default;

void RejectedRecordWriter::flush() {
    // No-op: every append_serialized call now open-writes-closes the
    // backing file, so nothing is held in the writer's own buffers.
    // Kept for API compatibility with callers that still call flush()
    // defensively at fragment teardown.
}

const std::string& RejectedRecordWriter::resolve_file_path() {
    // Caller holds _file_lock.
    if (_path_resolved) {
        return _file_path;
    }
    if (_path_resolve_failed) {
        return _file_path; // empty
    }

    DCHECK(_state != nullptr);
    ExecEnv* env = _state->exec_env();
    if (env == nullptr || env->load_path_mgr() == nullptr) {
        _path_resolve_failed = true;
        return _file_path;
    }

    // Reuse the rejected-record directory layout (store-path-relative so it
    // lives on the same mount that the legacy retention policy already
    // sweeps). `.jsonl` suffix lets the sync daemon filter files picked up
    // by this writer from anything else that might coexist under the
    // rejected-record root.
    std::string base = env->load_path_mgr()->get_load_rejected_record_absolute_path(
            /*rejected_record_dir=*/"", _state->db(), _state->load_label(), _state->load_job_id(),
            _state->fragment_instance_id());
    _file_path = base + ".jsonl";

    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(_file_path).parent_path(), ec);
    if (ec) {
        LOG(WARNING) << "RejectedRecordWriter: failed to create parent directory for " << _file_path << ": "
                     << ec.message();
        _file_path.clear();
        _path_resolve_failed = true;
        return _file_path;
    }
    _path_resolved = true;
    VLOG(1) << "RejectedRecordWriter path resolved " << _file_path;
    return _file_path;
}

void RejectedRecordWriter::append_from_chunk(const Chunk& chunk, size_t row_idx,
                                             const std::vector<std::string>& col_names,
                                             const std::string& error_code, const std::string& error_message,
                                             const std::string& error_column, const std::string& source_info) {
    if (row_idx >= chunk.num_rows()) {
        return;
    }
    std::string raw_record = build_json_from_chunk(chunk, row_idx, col_names);
    append_serialized(raw_record, error_code, error_message, error_column, source_info);
}

void RejectedRecordWriter::append_from_slices(const std::vector<Slice>& col_values,
                                              const std::vector<std::string>& col_names,
                                              const std::string& error_code, const std::string& error_message,
                                              const std::string& error_column, const std::string& source_info) {
    std::string raw_record = build_json_from_slices(col_values, col_names);
    append_serialized(raw_record, error_code, error_message, error_column, source_info);
}

void RejectedRecordWriter::append_raw(const std::string& raw_text, const std::string& error_code,
                                      const std::string& error_message, const std::string& error_column,
                                      const std::string& source_info) {
    std::string raw_record = build_json_from_raw(raw_text);
    append_serialized(raw_record, error_code, error_message, error_column, source_info);
}

void RejectedRecordWriter::append_serialized(const std::string& raw_record_json, const std::string& error_code,
                                             const std::string& error_message, const std::string& error_column,
                                             const std::string& source_info) {
    // Single enforcement point for the user-configured cap
    // (`log_rejected_record_num`). Entry paths that already check
    // enable_log_rejected_record() before calling still pass through
    // here cheaply; paths that forget to check (e.g. Parquet's
    // ArrowConvertContext, which previously only observed its own
    // hard-coded MAX_ERROR_MESSAGE_COUNTER for error-log throttling)
    // now honour the user's limit automatically.
    if (!_state->enable_log_rejected_record()) {
        return;
    }

    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    // Populate every column the DDL declares. Keys must match
    // `_statistics_.rejected_records` column names so merge-commit
    // Stream Load maps them positionally. Stringy fields go through
    // rapidjson so embedded quotes / backslashes are escaped uniformly.

    // Identity -- UUID generated here makes at-least-once sync retries
    // dedup into a single row via the system table's primary key.
    const std::string record_id = generate_uuid_string();
    doc.AddMember("id",
                  rapidjson::Value(record_id.c_str(), static_cast<rapidjson::SizeType>(record_id.size()), alloc),
                  alloc);

    // Target. target_database is set early in OlapTableSink::init() via
    // RuntimeState::set_db; target_table alongside via set_table_name.
    // Scanner-phase rejections inherit whichever value the sink installed.
    // If neither is set (pure SELECT rejection path, unusual) these stay
    // empty strings and the FE treats them as nulls.
    const std::string& db_name = _state->db();
    const std::string& tbl_name = _state->table_name();
    doc.AddMember("target_database",
                  rapidjson::Value(db_name.c_str(), static_cast<rapidjson::SizeType>(db_name.size()), alloc), alloc);
    doc.AddMember("target_table",
                  rapidjson::Value(tbl_name.c_str(), static_cast<rapidjson::SizeType>(tbl_name.size()), alloc), alloc);

    // Load context.
    const std::string& label = _state->load_label();
    const int64_t txn_id = _state->load_job_id();
    std::string load_type = resolve_load_type(_state->query_options());
    doc.AddMember("load_label",
                  rapidjson::Value(label.c_str(), static_cast<rapidjson::SizeType>(label.size()), alloc), alloc);
    doc.AddMember("load_type",
                  rapidjson::Value(load_type.c_str(), static_cast<rapidjson::SizeType>(load_type.size()), alloc),
                  alloc);
    doc.AddMember("txn_id", rapidjson::Value(txn_id), alloc);
    // user_name: RuntimeState::_user is only populated on query paths
    // that explicitly stash the submitting user (StreamLoad's internal
    // executor does; Broker/Routine Load and INSERT fragments do not
    // plumb the identity down to the BE plan fragment at all today).
    // Writing an empty string would make consumers think the load had
    // no user, which is actively misleading; leave the JSON key absent
    // so Stream Load writes NULL and operators can recover the user
    // by joining _statistics_.rejected_records.load_label against
    // information_schema.loads.user when needed.
    const std::string& user_name = _state->user();
    if (!user_name.empty()) {
        doc.AddMember("user_name",
                      rapidjson::Value(user_name.c_str(), static_cast<rapidjson::SizeType>(user_name.size()), alloc),
                      alloc);
    }

    // Error context.
    doc.AddMember("error_code",
                  rapidjson::Value(error_code.c_str(), static_cast<rapidjson::SizeType>(error_code.size()), alloc),
                  alloc);
    doc.AddMember("error_message",
                  rapidjson::Value(error_message.c_str(), static_cast<rapidjson::SizeType>(error_message.size()),
                                   alloc),
                  alloc);
    doc.AddMember("error_column",
                  rapidjson::Value(error_column.c_str(), static_cast<rapidjson::SizeType>(error_column.size()), alloc),
                  alloc);

    // Payload.
    add_parsed_json(doc, alloc, "raw_record", raw_record_json);
    add_parsed_json(doc, alloc, "source_info", source_info);

    // Diagnostics.
    std::optional<int64_t> backend_id = get_backend_id();
    if (backend_id.has_value()) {
        doc.AddMember("backend_id", rapidjson::Value(*backend_id), alloc);
    }
    // else: leave the column absent; FE Stream Load will treat it as NULL.

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);

    std::lock_guard<std::mutex> lock(_file_lock);
    const std::string& path = resolve_file_path();
    if (path.empty()) {
        return;
    }
    // Append mode + explicit close-at-end. The sync daemon's processing
    // loop atomically renames files it claims for a flush, so a concurrent
    // rename between this ofstream's open() and close() is fine: we write
    // to whatever inode the name now points at (new file the writer
    // implicitly created for the post-rename path), and the daemon has
    // already captured the previous inode under its claimed name. In
    // the other direction, the daemon reads and removes only files
    // suffixed with .syncing.<uuid>, never the live .jsonl path the
    // writer touches here.
    std::ofstream out(path, std::ios::out | std::ios::app);
    if (!out.is_open()) {
        LOG(WARNING) << "RejectedRecordWriter: failed to open " << path << " for append";
        return;
    }
    out.write(buf.GetString(), buf.GetSize());
    out.put('\n');
    out.close();

    // Single counter-increment site for the entire feature. All paths
    // that eventually reach the writer -- legacy
    // RuntimeStateHelper::append_rejected_record_to_file, ORC's
    // capture_rejected_rows_before_filter, and the Parquet
    // ArrowConvertContext::report_error_message -- go through
    // append_serialized, so enable_log_rejected_record()'s cap now
    // applies symmetrically. Counting here rather than in the entry
    // methods means callers that build up a record and then fail at
    // file open (best-effort no-op, see ensure_file_open) don't
    // inflate the counter past the usable writes.
    _state->note_rejected_record();
}

std::string RejectedRecordWriter::build_json_from_chunk(const Chunk& chunk, size_t row_idx,
                                                        const std::vector<std::string>& col_names) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    const size_t num_cols = chunk.num_columns();
    for (size_t i = 0; i < num_cols; ++i) {
        const auto& col_ptr = chunk.get_column_by_index(i);
        if (col_ptr == nullptr) {
            continue;
        }
        const Column& col = *col_ptr;
        bool is_null = col.is_null(row_idx);
        std::string value = is_null ? std::string() : cell_to_string(col, row_idx);
        add_member(doc, alloc, pick_name(col_names, i), value, is_null);
    }
    return to_json_string(doc);
}

std::string RejectedRecordWriter::build_json_from_slices(const std::vector<Slice>& col_values,
                                                         const std::vector<std::string>& col_names) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    for (size_t i = 0; i < col_values.size(); ++i) {
        const Slice& slice = col_values[i];
        std::string value(slice.data, slice.size);
        add_member(doc, alloc, pick_name(col_names, i), value, false);
    }
    return to_json_string(doc);
}

std::string RejectedRecordWriter::build_json_from_raw(const std::string& raw_text) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    add_member(doc, alloc, "_raw", raw_text, false);
    return to_json_string(doc);
}

} // namespace starrocks
