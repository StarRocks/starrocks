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

#include "base/uid_util.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "common/logging.h"
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

} // namespace

RejectedRecordWriter::RejectedRecordWriter(RuntimeState* state) : _state(state) {
    DCHECK(state != nullptr);
}

RejectedRecordWriter::~RejectedRecordWriter() {
    flush();
    std::lock_guard<std::mutex> lock(_file_lock);
    if (_file && _file->is_open()) {
        _file->close();
    }
}

void RejectedRecordWriter::flush() {
    std::lock_guard<std::mutex> lock(_file_lock);
    if (_file && _file->is_open()) {
        _file->flush();
    }
}

bool RejectedRecordWriter::ensure_file_open() {
    // Caller holds _file_lock.
    if (_file && _file->is_open()) {
        return true;
    }
    if (_open_failed) {
        return false;
    }

    DCHECK(_state != nullptr);
    ExecEnv* env = _state->exec_env();
    if (env == nullptr || env->load_path_mgr() == nullptr) {
        _open_failed = true;
        return false;
    }

    // Reuse the rejected-record directory layout (store-path-relative so it
    // lives on the same mount as the legacy reject file and is cleaned up
    // by the same retention policy). We append `.jsonl` so the Phase 3 sync
    // daemon can scan only the new JSON Lines files and coexist with the
    // legacy tab-delimited ones until all call sites migrate.
    std::string base = env->load_path_mgr()->get_load_rejected_record_absolute_path(
            /*rejected_record_dir=*/"", _state->db(), _state->load_label(), _state->load_job_id(),
            _state->fragment_instance_id());
    _file_path = base + ".jsonl";

    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(_file_path).parent_path(), ec);
    if (ec) {
        LOG(WARNING) << "RejectedRecordWriter: failed to create parent directory for " << _file_path << ": "
                     << ec.message();
        _open_failed = true;
        return false;
    }

    _file = std::make_unique<std::ofstream>(_file_path, std::ios::out | std::ios::app);
    if (!_file->is_open()) {
        LOG(WARNING) << "RejectedRecordWriter: failed to open " << _file_path;
        _open_failed = true;
        _file.reset();
        return false;
    }
    VLOG(1) << "RejectedRecordWriter opened " << _file_path;
    return true;
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
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    // Assemble the top-level record. Keys match the FE system table
    // (_statistics_.rejected_records) column names so the Phase 3 sync
    // daemon can do a column-name-indexed Stream Load.
    const std::string record_id = generate_uuid_string();
    rapidjson::Value id_v(record_id.c_str(), static_cast<rapidjson::SizeType>(record_id.size()), alloc);
    doc.AddMember("id", id_v, alloc);

    const std::string& db_name = _state->db();
    const std::string& label = _state->load_label();
    const int64_t txn_id = _state->load_job_id();

    doc.AddMember("target_database",
                  rapidjson::Value(db_name.c_str(), static_cast<rapidjson::SizeType>(db_name.size()), alloc), alloc);
    // target_table and load_type are filled in by the Phase 3 sync daemon
    // from the load-job metadata; the Scanner/Sink rejection paths do not
    // universally have the target-table name on hand.
    doc.AddMember("target_table", rapidjson::Value("", 0, alloc), alloc);
    doc.AddMember("load_label",
                  rapidjson::Value(label.c_str(), static_cast<rapidjson::SizeType>(label.size()), alloc), alloc);
    doc.AddMember("load_type", rapidjson::Value("UNKNOWN", 7, alloc), alloc);
    doc.AddMember("txn_id", rapidjson::Value(txn_id), alloc);

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

    add_parsed_json(doc, alloc, "raw_record", raw_record_json);
    add_parsed_json(doc, alloc, "source_info", source_info);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);

    std::lock_guard<std::mutex> lock(_file_lock);
    if (!ensure_file_open()) {
        return;
    }
    _file->write(buf.GetString(), buf.GetSize());
    _file->put('\n');
    _records_written.fetch_add(1, std::memory_order_relaxed);
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
