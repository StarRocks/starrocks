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

#include "exec/lance/lance_native_reader.h"

#include <fmt/format.h>

#include <charconv>
#include <system_error>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_scan_io_fwd.h"
#include "common/statusor.h"
#include "exec/lance/lance_rs_ffi.h"
#include "exec/file_scanner/parquet_scanner.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

constexpr int SR_LANCE_NEXT_EOF = 0;
constexpr int SR_LANCE_NEXT_BATCH = 1;
constexpr int SR_LANCE_ERROR = -1;
constexpr const char* LANCE_DISTANCE_COLUMN = "_distance";
constexpr const char* LANCE_VECTOR_COLUMN_PARAM = "lance.vector_column";
constexpr const char* LANCE_VECTOR_METRIC_PARAM = "lance.metric_type";
constexpr const char* LANCE_NPROBES_PARAM = "lance.nprobes";
constexpr const char* LANCE_REFINE_FACTOR_PARAM = "lance.refine_factor";
constexpr const char* LANCE_EF_PARAM = "lance.ef";
constexpr const char* LANCE_QUERY_PARALLELISM_PARAM = "lance.query_parallelism";
constexpr int32_t LANCE_QUERY_PARALLELISM_UNSET = -2;

SrLanceString to_lance_string(const std::string& value) {
    return SrLanceString{value.data(), value.size()};
}

Status lance_error_status(const std::string& prefix, char* error) {
    std::string message = error == nullptr ? "unknown error" : error;
    if (error != nullptr) {
        sr_lance_free_error(error);
    }
    return Status::InternalError(fmt::format("{}: {}", prefix, message));
}

bool is_lance_vector_distance_column(const HdfsScannerParams& params, const std::string& column_name) {
    return params.table_specific.use_lance_vector_search &&
           column_name == params.table_specific.lance_vector_search_options.vector_distance_column_name;
}

std::shared_ptr<arrow::Array> get_lance_arrow_array(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                    const HdfsScannerParams& params, const std::string& column_name) {
    if (is_lance_vector_distance_column(params, column_name)) {
        return batch->GetColumnByName(LANCE_DISTANCE_COLUMN);
    }
    return batch->GetColumnByName(column_name);
}

bool needs_lance_distance_to_similarity_conversion(const HdfsScannerParams& params) {
    if (!params.table_specific.use_lance_vector_search) {
        return false;
    }
    const auto& query_params = params.table_specific.lance_vector_search_options.query_params;
    auto it = query_params.find(LANCE_VECTOR_METRIC_PARAM);
    if (it == query_params.end()) {
        return false;
    }
    return it->second == "dot" || it->second == "inner_product";
}

StatusOr<std::shared_ptr<arrow::Array>> convert_lance_distance_to_similarity(
        const std::shared_ptr<arrow::Array>& array) {
    if (array->type_id() != arrow::Type::FLOAT) {
        return Status::InternalError(
                fmt::format("Lance vector distance column must be FLOAT, got {}", array->type()->ToString()));
    }
    auto distance_array = std::static_pointer_cast<arrow::FloatArray>(array);
    arrow::FloatBuilder builder;
    auto status = builder.Reserve(distance_array->length());
    if (!status.ok()) {
        return Status::InternalError(
                fmt::format("Failed to reserve Lance vector similarity column: {}", status.ToString()));
    }
    for (int64_t i = 0; i < distance_array->length(); ++i) {
        if (distance_array->IsNull(i)) {
            status = builder.AppendNull();
        } else {
            status = builder.Append(1.0f - distance_array->Value(i));
        }
        if (!status.ok()) {
            return Status::InternalError(
                    fmt::format("Failed to append Lance vector similarity value: {}", status.ToString()));
        }
    }
    std::shared_ptr<arrow::Array> output;
    status = builder.Finish(&output);
    if (!status.ok()) {
        return Status::InternalError(
                fmt::format("Failed to build Lance vector similarity column: {}", status.ToString()));
    }
    return output;
}

Status convert_lance_vector_distance_column(const HdfsScannerParams& params,
                                            std::shared_ptr<arrow::RecordBatch>* batch) {
    if (!needs_lance_distance_to_similarity_conversion(params)) {
        return Status::OK();
    }
    int distance_column_index = (*batch)->schema()->GetFieldIndex(LANCE_DISTANCE_COLUMN);
    if (distance_column_index < 0) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto similarity_array,
                     convert_lance_distance_to_similarity((*batch)->column(distance_column_index)));
    auto columns = (*batch)->columns();
    columns[distance_column_index] = std::move(similarity_array);
    *batch = arrow::RecordBatch::Make((*batch)->schema(), (*batch)->num_rows(), std::move(columns));
    return Status::OK();
}

StatusOr<std::string> get_required_query_param(const TVectorSearchOptions& options, const std::string& key) {
    auto it = options.query_params.find(key);
    if (it == options.query_params.end() || it->second.empty()) {
        return Status::InternalError(fmt::format("Missing Lance vector search query parameter {}", key));
    }
    return it->second;
}

StatusOr<int32_t> parse_optional_positive_i32_param(const TVectorSearchOptions& options, const std::string& key) {
    auto it = options.query_params.find(key);
    if (it == options.query_params.end() || it->second.empty()) {
        return -1;
    }
    int32_t value = -1;
    const auto& raw = it->second;
    auto result = std::from_chars(raw.data(), raw.data() + raw.size(), value);
    if (result.ec != std::errc() || result.ptr != raw.data() + raw.size() || value <= 0) {
        return Status::InternalError(
                fmt::format("Invalid positive integer value for Lance vector parameter {}: {}", key, raw));
    }
    return value;
}

StatusOr<int32_t> parse_optional_query_parallelism_param(const TVectorSearchOptions& options, const std::string& key) {
    auto it = options.query_params.find(key);
    if (it == options.query_params.end() || it->second.empty()) {
        return LANCE_QUERY_PARALLELISM_UNSET;
    }
    const auto& raw = it->second;
    int32_t value = LANCE_QUERY_PARALLELISM_UNSET;
    auto result = std::from_chars(raw.data(), raw.data() + raw.size(), value);
    if (result.ec != std::errc() || result.ptr != raw.data() + raw.size() || value < -1) {
        return Status::InternalError(
                fmt::format("Invalid query parallelism value for Lance vector parameter {}: {}", key, raw));
    }
    return value;
}

} // namespace

LanceNativeReader::LanceNativeReader(const HdfsScannerParams& scanner_params, const HdfsScannerContext& scanner_ctx,
                                     RuntimeState* state, HdfsScanStats* app_stats)
        : _scanner_params(scanner_params),
          _scanner_ctx(scanner_ctx),
          _state(state),
          _app_stats(app_stats),
          _max_chunk_size(state->chunk_size() ? state->chunk_size() : 4096) {
    _init_read_fields();
    _cast_exprs.assign(_scanner_ctx.materialized_columns.size(), nullptr);
    _conv_ctx.state = state;
}

LanceNativeReader::~LanceNativeReader() {
    close();
}

Status LanceNativeReader::open() {
    SCOPED_RAW_TIMER(&_app_stats->reader_init_ns);
    RETURN_IF_ERROR(_open_reader());
    _chunk_filter.reserve(0);
    _batch_start_idx = 0;
    _chunk_start_idx = 0;
    _scanner_eof = false;
    _read_chunk_initialized = false;
    _read_chunk = std::make_shared<Chunk>();
    return Status::OK();
}

Status LanceNativeReader::_open_reader() {
    std::vector<SrLanceString> fields;
    fields.reserve(_field_names.size());
    std::vector<std::string> projected_field_names;
    projected_field_names.reserve(_field_names.size());
    for (const auto& name : _field_names) {
        if (is_lance_vector_distance_column(_scanner_params, name)) {
            continue;
        }
        projected_field_names.emplace_back(name);
    }
    for (const auto& name : projected_field_names) {
        fields.emplace_back(to_lance_string(name));
    }

    std::vector<SrLanceStringPair> storage_options;
    storage_options.reserve(_scanner_params.table_specific.lance_storage_options.size());
    for (const auto& [key, value] : _scanner_params.table_specific.lance_storage_options) {
        storage_options.push_back({to_lance_string(key), to_lance_string(value)});
    }

    SrLanceVectorOptions vector_options;
    const SrLanceVectorOptions* vector_options_ptr = nullptr;
    std::string vector_column;
    std::string metric_type;
    std::vector<SrLanceString> query_vector;
    std::vector<SrLanceString> index_segment_uuids;
    if (_scanner_params.table_specific.use_lance_vector_search) {
        if (_scanner_params.table_specific.lance_vector_search_options.vector_limit_k <= 0) {
            return Status::InternalError("Invalid Lance vector search limit");
        }
        if (_scanner_params.table_specific.lance_vector_search_options.query_vector.empty()) {
            return Status::InternalError("Lance vector search query vector must not be empty");
        }
        if (_scanner_params.table_specific.lance_index_segment_uuids.empty()) {
            return Status::InternalError("Lance vector search requires at least one index segment");
        }

        ASSIGN_OR_RETURN(vector_column,
                         get_required_query_param(_scanner_params.table_specific.lance_vector_search_options,
                                                  LANCE_VECTOR_COLUMN_PARAM));
        ASSIGN_OR_RETURN(metric_type,
                         get_required_query_param(_scanner_params.table_specific.lance_vector_search_options,
                                                  LANCE_VECTOR_METRIC_PARAM));
        query_vector.reserve(_scanner_params.table_specific.lance_vector_search_options.query_vector.size());
        for (const auto& value : _scanner_params.table_specific.lance_vector_search_options.query_vector) {
            query_vector.emplace_back(to_lance_string(value));
        }
        index_segment_uuids.reserve(_scanner_params.table_specific.lance_index_segment_uuids.size());
        for (const auto& value : _scanner_params.table_specific.lance_index_segment_uuids) {
            index_segment_uuids.emplace_back(to_lance_string(value));
        }

        ASSIGN_OR_RETURN(int32_t nprobes,
                         parse_optional_positive_i32_param(
                                 _scanner_params.table_specific.lance_vector_search_options, LANCE_NPROBES_PARAM));
        ASSIGN_OR_RETURN(int32_t refine_factor,
                         parse_optional_positive_i32_param(_scanner_params.table_specific.lance_vector_search_options,
                                                           LANCE_REFINE_FACTOR_PARAM));
        ASSIGN_OR_RETURN(int32_t ef,
                         parse_optional_positive_i32_param(
                                 _scanner_params.table_specific.lance_vector_search_options, LANCE_EF_PARAM));
        ASSIGN_OR_RETURN(int32_t query_parallelism,
                         parse_optional_query_parallelism_param(
                                 _scanner_params.table_specific.lance_vector_search_options,
                                 LANCE_QUERY_PARALLELISM_PARAM));

        vector_options = SrLanceVectorOptions{to_lance_string(vector_column),
                                              to_lance_string(metric_type),
                                              query_vector.data(),
                                              query_vector.size(),
                                              _scanner_params.table_specific.lance_vector_search_options.vector_limit_k,
                                              index_segment_uuids.data(),
                                              index_segment_uuids.size(),
                                              nprobes,
                                              refine_factor,
                                              ef,
                                              query_parallelism};
        vector_options_ptr = &vector_options;
    }

    char* error = nullptr;
    int result = sr_lance_reader_open(
            to_lance_string(_scanner_params.table_specific.lance_dataset_uri),
            _scanner_params.table_specific.lance_fragment_id, fields.data(), fields.size(), _max_chunk_size,
            storage_options.data(), storage_options.size(), vector_options_ptr, config::lance_index_cache_size_bytes,
            config::lance_metadata_cache_size_bytes, &_reader, &error);
    if (result == SR_LANCE_ERROR || _reader == nullptr) {
        return lance_error_status(
                fmt::format("Failed to open Lance native reader for {}",
                            _scanner_params.table_specific.lance_dataset_uri),
                error);
    }
    return Status::OK();
}

StatusOr<size_t> LanceNativeReader::get_next(ChunkPtr* chunk) {
    if (_read_chunk != nullptr) {
        _read_chunk->reset();
    }
    _chunk_filter.clear();

    if (_batch_is_exhausted()) {
        while (true) {
            Status status = _next_batch();
            if (_scanner_eof) {
                return status;
            }
            if (status.ok()) {
                break;
            }
            return status;
        }
    }

    RETURN_IF_ERROR(_ensure_read_chunk());

    while (!_scanner_eof) {
        RETURN_IF_ERROR(_append_batch_to_read_chunk());
        if (_chunk_is_full()) {
            break;
        }
        auto status = _next_batch();
        if (status.ok()) {
            RETURN_IF_ERROR(_ensure_read_chunk());
            continue;
        }
        if (!status.is_end_of_file()) {
            return status;
        }
        if (_chunk_start_idx > 0) {
            break;
        }
        return status;
    }

    *chunk = _read_chunk->clone_empty_with_slot(_max_chunk_size);
    ASSIGN_OR_RETURN(size_t row_count, _fill_dst_chunk(chunk));
    _chunk_start_idx = 0;
    return row_count;
}

Status LanceNativeReader::_next_batch() {
    SCOPED_RAW_TIMER(&_app_stats->column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats->io_ns);
    _app_stats->io_count += 1;
    _batch_start_idx = 0;

    ArrowArray c_array;
    ArrowSchema c_schema;
    int64_t rows = 0;
    char* error = nullptr;
    int result = sr_lance_reader_next(_reader, &c_array, &c_schema, &rows, &error);
    if (result == SR_LANCE_NEXT_EOF) {
        _scanner_eof = true;
        return Status::EndOfFile("no data");
    }
    if (result == SR_LANCE_ERROR) {
        return lance_error_status(
                fmt::format("Failed to read next Lance batch from {}",
                            _scanner_params.table_specific.lance_dataset_uri),
                error);
    }
    if (result != SR_LANCE_NEXT_BATCH) {
        return Status::InternalError(fmt::format("Unexpected Lance native reader result code {}", result));
    }

    auto arrow_batch_result = arrow::ImportRecordBatch(&c_array, &c_schema);
    if (UNLIKELY(!arrow_batch_result.ok())) {
        return Status::InternalError(
                fmt::format("Arrow ImportRecordBatch: {}", arrow_batch_result.status().ToString()));
    }
    _arrow_batch = arrow_batch_result.ValueOrDie();
    RETURN_IF_ERROR(convert_lance_vector_distance_column(_scanner_params, &_arrow_batch));
    return Status::OK();
}

Status LanceNativeReader::_ensure_read_chunk() {
    if (_read_chunk_initialized) {
        return Status::OK();
    }
    if (_arrow_batch == nullptr) {
        return Status::InternalError("Cannot initialize Lance read chunk without an Arrow batch");
    }

    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        auto array = get_lance_arrow_array(_arrow_batch, _scanner_params, slot_desc->col_name());
        if (array == nullptr) {
            return Status::InternalError(
                    fmt::format("Cannot find Lance column {} in Arrow batch", slot_desc->col_name()));
        }
        ColumnPtr column;
        RETURN_IF_ERROR(ParquetScanner::new_column(array->type().get(), slot_desc, &column, _conv_funcs[i].get(),
                                                   &_cast_exprs[i], _pool, true));
        column->reserve(_max_chunk_size);
        _read_chunk->append_column(column, slot_desc->id());
    }
    _read_chunk_initialized = true;
    return Status::OK();
}

Status LanceNativeReader::_append_batch_to_read_chunk() {
    SCOPED_RAW_TIMER(&_app_stats->column_convert_ns);
    size_t remaining_chunk = static_cast<size_t>(_max_chunk_size - _chunk_start_idx);
    size_t remaining_batch = static_cast<size_t>(_arrow_batch->num_rows() - _batch_start_idx);
    size_t num_elements = std::min(remaining_chunk, remaining_batch);
    _chunk_filter.resize(_chunk_filter.size() + num_elements, 1);
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.current_slot = slot_desc;
        auto array = get_lance_arrow_array(_arrow_batch, _scanner_params, slot_desc->col_name());
        if (array == nullptr) {
            return Status::InternalError(
                    fmt::format("Cannot find Lance column {} in Arrow batch", slot_desc->col_name()));
        }
        auto& column = _read_chunk->get_column_by_slot_id(slot_desc->id());
        RETURN_IF_ERROR(ParquetScanner::convert_array_to_column(_conv_funcs[i].get(), num_elements, array.get(), column,
                                                                _batch_start_idx, _chunk_start_idx, &_chunk_filter,
                                                                &_conv_ctx));
    }
    _chunk_start_idx += num_elements;
    _batch_start_idx += num_elements;
    _app_stats->raw_rows_read += num_elements;
    return Status::OK();
}

StatusOr<size_t> LanceNativeReader::_fill_dst_chunk(ChunkPtr* dst) {
    if (_scanner_ctx.materialized_columns.empty()) {
        *dst = std::make_shared<Chunk>();
        return _chunk_start_idx;
    }

    auto num_rows = _read_chunk->filter(_chunk_filter);
    _app_stats->late_materialize_skip_rows += _chunk_start_idx - num_rows;
    SCOPED_RAW_TIMER(&_app_stats->cast_chunk_ns);
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, _read_chunk.get()));
        column = ColumnHelper::unfold_const_column(slot_desc->type(), _read_chunk->num_rows(), column);
        (*dst)->get_column_by_slot_id(slot_desc->id())->swap_column(*column);
    }
    return num_rows;
}

void LanceNativeReader::_init_read_fields() {
    const size_t num_materialized_columns = _scanner_ctx.materialized_columns.size();
    _field_names.reserve(num_materialized_columns);
    _conv_funcs.reserve(num_materialized_columns);
    for (const auto& materialized_column : _scanner_ctx.materialized_columns) {
        _field_names.emplace_back(materialized_column.name());
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
}

void LanceNativeReader::close() {
    if (_reader != nullptr) {
        sr_lance_reader_close(_reader);
        _reader = nullptr;
    }
}

bool LanceNativeReader::_chunk_is_full() const {
    return _chunk_start_idx >= _max_chunk_size;
}

bool LanceNativeReader::_batch_is_exhausted() const {
    return _scanner_eof || _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

} // namespace starrocks
