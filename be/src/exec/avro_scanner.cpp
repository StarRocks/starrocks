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

#include "exec/avro_scanner.h"

#include <fmt/format.h>
#include <ryu/ryu.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/avro_scanner.h"
#include "exec/json_parser.h"
#include "exec/json_scanner.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/json_functions.h"
#include "formats/avro/nullable_column.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/types.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "libserdes/serdes-avro.h"
#ifdef __cplusplus
}
#endif

void replaceAll(std::string& str, const std::string& from, const std::string& to) {
    if (from.empty()) return;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}

namespace starrocks {

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _serdes(nullptr),
          _closed(false) {}

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter, const std::string schema_text)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _schema_text(schema_text),
          _closed(false) {}

AvroScanner::~AvroScanner() {
#if BE_TEST
    avro_file_reader_close(_dbreader);
#else
    if (_serdes != nullptr) {
        serdes_destroy(_serdes);
    }
#endif
}

// Previously, when parsing avro through JsonScanner, we used.*. to handle union data types,
// but for the new implementation, we no longer need this pattern. For example, for the following
// avro schema:
// {
//     "name": "raw_log",
//     "type": ["null", {
//         "fields": [{
//             "name": "id",
//             "type": "string"
//         }, {
//             "name": "data",
//             "type": "string"
//         }],
//         "name": "logs",
//         "type": "record"
//     }]
// }
// If you want to select the id field, for the new implementation jsonpath can be written as $.id instead
// of $.*.id. To ensure forward compatibility, we find the.*. pattern in the new implementation and replace
// it with '.'. In addition, there is another case where the union is at the end of the path, where the pattern
// is.*, in which case.* is removed.
std::string AvroScanner::preprocess_jsonpaths(std::string jsonpaths) {
    replaceAll(jsonpaths, ".*.", ".");
    replaceAll(jsonpaths, ".*", "");
    return jsonpaths;
}

Status AvroScanner::open() {
    if (_scan_range.ranges.size() == 0) {
        return Status::EndOfFile("EOF of reading protobuf file");
    }
    const TBrokerRangeDesc& range_desc = _scan_range.ranges[0];
#if BE_TEST
    if (avro_file_reader(range_desc.path.c_str(), &_dbreader)) {
        auto err_msg = "Error opening file: " + std::string(avro_strerror());
        return Status::InternalError(err_msg);
    }
#endif

    RETURN_IF_ERROR(FileScanner::open());
    RETURN_IF_ERROR(_construct_avro_types());
    RETURN_IF_ERROR(_construct_cast_exprs());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
#ifndef BE_TEST
    if (_serdes == nullptr) {
        std::string confluent_schema_registry_url;
        if (!_scan_range.params.__isset.confluent_schema_registry_url) {
            return Status::InternalError("'confluent_schema_registry_url' not set");
        } else {
            confluent_schema_registry_url = _scan_range.params.confluent_schema_registry_url;
        }

        serdes_conf_t* sconf =
                serdes_conf_new(NULL, 0, "schema.registry.url", confluent_schema_registry_url.c_str(), NULL);
        _serdes = serdes_new(sconf, _err_buf, sizeof(_err_buf));
        if (!_serdes) {
            LOG(ERROR) << "failed to create serdes handle: " << _err_buf;
            return Status::InternalError("failed to create serdes handle");
        }
    }
#endif
    if (range_desc.__isset.jsonpaths) {
        std::string jsonpaths = preprocess_jsonpaths(range_desc.jsonpaths);
        RETURN_IF_ERROR(JsonScanner::parse_json_paths(jsonpaths, &_json_paths));
    }
    Status st = create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &_file);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create sequential files: " << st.to_string();
        return st;
    }

    for (const auto& desc : _src_slot_descriptors) {
        if (desc == nullptr) {
            continue;
        }
        _slot_desc_dict.emplace(desc->col_name(), desc);
    }
    _init_data_idx_to_slot_once = false;
    return Status::OK();
}

void AvroScanner::_materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        AdaptiveNullableColumn* adaptive_column =
                down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

Status AvroScanner::_create_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    *chunk = std::make_shared<Chunk>();
    size_t slot_size = _src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        auto column = ColumnHelper::create_column(_avro_types[column_pos], true, false, 0, true);
        (*chunk)->append_column(column, slot_desc->id());
    }

    return Status::OK();
}

void AvroScanner::_report_error(const std::string& line, const std::string& err_msg) {
    _state->append_error_msg_to_file(line, err_msg);
}

Status AvroScanner::_construct_row(const avro_value_t& avro_value, Chunk* chunk) {
    size_t slot_size = _src_slot_descriptors.size();
    size_t jsonpath_size = _json_paths.size();
    for (size_t i = 0; i < slot_size; i++) {
        if (_src_slot_descriptors[i] == nullptr) {
            continue;
        }
        auto column = down_cast<NullableColumn*>(chunk->get_column_by_slot_id(_src_slot_descriptors[i]->id()).get());
        if (UNLIKELY(i >= jsonpath_size)) {
            column->append_nulls(1);
            continue;
        }
        avro_value_t output_value;
        auto st = _extract_field(avro_value, _json_paths[i], &output_value);
        if (LIKELY(st.ok())) {
            RETURN_IF_ERROR(_construct_column(output_value, column, _src_slot_descriptors[i]->type(),
                                              _src_slot_descriptors[i]->col_name()));
        } else if (st.is_not_found()) {
            column->append_nulls(1);
        } else {
            return st;
        }
    }
    return Status::OK();
}

Status AvroScanner::_parse_avro(Chunk* chunk, const std::shared_ptr<SequentialFile>& file) {
    const int capacity = _state->chunk_size();
    DCHECK_EQ(0, chunk->num_rows());
    for (size_t num_rows = chunk->num_rows(); num_rows < capacity; /**/) {
        avro_value_t avro_value;
#ifdef BE_TEST
        // In general, we want to test component injection schemastr.
        avro_schema_error_t error;
        avro_schema_t schema = NULL;
        int result = avro_schema_from_json(_schema_text.c_str(), _schema_text.size(), &schema, &error);
        if (result != 0) {
            auto err_msg = "parse schema from json error: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        avro_value_iface_t* iface = avro_generic_class_from_schema(schema);
        if (avro_generic_value_new(iface, &avro_value)) {
            auto err_msg = "Cannot allocate new value instance: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        DeferOp avro_deleter([&] {
            avro_schema_decref(schema);
            avro_value_iface_decref(iface);
            avro_value_decref(&avro_value);
        });
        result = avro_file_reader_read_value(_dbreader, &avro_value);
        if (result != 0) {
            auto err_msg = "read avro value error: " + std::string(avro_strerror());
            return Status::EndOfFile(err_msg);
        }

        char* avro_as_json = nullptr;
        result = avro_value_to_json(&avro_value, 1, &avro_as_json);
        if (result != 0) {
            auto err_msg = "Unable to read value: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        free(avro_as_json);
#else
        const uint8_t* data{};
        size_t length = 0;
        auto* stream_file = down_cast<StreamLoadPipeInputStream*>(file->stream().get());
        {
            SCOPED_RAW_TIMER(&_counter->file_read_ns);
            ASSIGN_OR_RETURN(_parser_buf, stream_file->pipe()->read());
        }
        data = reinterpret_cast<uint8_t*>(_parser_buf->ptr);
        length = _parser_buf->remaining();
        serdes_schema_t* schema;
        serdes_err_t err =
                serdes_deserialize_avro(_serdes, &avro_value, &schema, data, length, _err_buf, sizeof(_err_buf));
        if (err) {
            auto err_msg = "serdes deserialize avro failed: " + std::string(_err_buf);
            LOG(ERROR) << err_msg;
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", err_msg);
            return Status::InternalError("serdes deserialize avro failed");
        }
        DeferOp op([&] { avro_value_decref(&avro_value); });
#endif
        size_t chunk_row_num = chunk->num_rows();
        Status st = Status::OK();
        if (!_json_paths.empty()) {
            st = _construct_row(avro_value, chunk);
        } else {
            if (!_init_data_idx_to_slot_once) {
                size_t element_count;
                if (UNLIKELY(avro_value_get_size(&avro_value, &element_count) != 0)) {
                    auto err_msg = "Cannot get record size: " + std::string(avro_strerror());
                    return Status::InternalError(err_msg);
                }
                _data_idx_to_slot.assign(element_count, SlotInfo());
                for (size_t i = 0; i < element_count; i++) {
                    const char* field_name;
                    avro_value_t element_value;
                    if (UNLIKELY(avro_value_get_by_index(&avro_value, i, &element_value, &field_name) != 0)) {
                        auto err_msg = "Cannot get value by index: " + std::string(avro_strerror());
                        return Status::InternalError(err_msg);
                    }
                    _data_idx_to_fieldname.push_back(std::string(field_name));
                }

                _init_data_idx_to_slot_once = true;
            }
            st = _construct_row_without_jsonpath(avro_value, chunk);
        }
        if (!st.ok()) {
            if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
                // We would continue to construct row even if error is returned,
                // hence the number of error appended to the file should be limited.
                _state->append_error_msg_to_file("", st.to_string());
                LOG(WARNING) << "failed to construct row: " << st;
            }
            // Before continuing to process other rows, we need to first clean the fail parsed row.
            chunk->set_num_rows(chunk_row_num);
            return st;
        }
        num_rows++;
    }
    return Status::OK();
}

Status AvroScanner::_construct_row_without_jsonpath(const avro_value_t& avro_value, Chunk* chunk) {
    _found_columns.assign(chunk->num_columns(), false);
    size_t element_count = _data_idx_to_fieldname.size();
    avro_value_t element_value;
    for (size_t i = 0; i < element_count; i++) {
        if (UNLIKELY(avro_value_get_by_index(&avro_value, i, &element_value, NULL) != 0)) {
            auto err_msg = "Cannot get value by index: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        SlotInfo& slot_info = _data_idx_to_slot[i];
        if (slot_info.id_ > -1) {
            int column_index = chunk->get_index_by_slot_id(slot_info.id_);
            _found_columns[column_index] = true;
        } else if (slot_info.id_ == -1) {
            continue;
        } else if (UNLIKELY(slot_info.id_ < -1)) {
            const std::string& key = _data_idx_to_fieldname[i];
            // look up key in the slot dict.
            auto itr = _slot_desc_dict.find(key);
            if (itr == _slot_desc_dict.end()) {
                slot_info.id_ = -1;
                continue;
            }
            auto slot_desc = itr->second;
            slot_info.id_ = slot_desc->id();
            slot_info.type_ = slot_desc->type();
            slot_info.key_ = key;
            int column_index = chunk->get_index_by_slot_id(slot_info.id_);
            _found_columns[column_index] = true;
        }

        auto& column = chunk->get_column_by_slot_id(slot_info.id_);
        // We should expand the union type.
        avro_value_t* cur_value = &element_value;
        if (UNLIKELY(avro_value_get_type(cur_value) == AVRO_UNION)) {
            avro_value_t branch;
            RETURN_IF_ERROR(_handle_union(cur_value, &branch));
            *cur_value = branch;
        }
        // construct column with value.
        RETURN_IF_ERROR(_construct_column(*cur_value, column.get(), slot_info.type_, slot_info.key_));
    }

    for (int i = 0; i < _found_columns.size(); i++) {
        if (UNLIKELY(!_found_columns[i])) {
            auto& column = chunk->get_column_by_index(i);
            column->append_nulls(1);
        }
    }
    return Status::OK();
}

StatusOr<ChunkPtr> AvroScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    ChunkPtr src_chunk;
    RETURN_IF_ERROR(_create_src_chunk(&src_chunk));
    const int chunk_capacity = _state->chunk_size();
    src_chunk->reserve(chunk_capacity);
    src_chunk->set_num_rows(0);
    Status st = _parse_avro(src_chunk.get(), _file);
    if (!st.ok() && !st.is_time_out() && !st.is_end_of_file()) {
        return st;
    }
    if (src_chunk->num_rows() == 0) {
        if (st.is_end_of_file()) {
            return Status::EndOfFile("EOF of reading avro file, nothing read");
        } else if (st.is_time_out()) {
            // if timeout happens at the beginning of reading src_chunk, we return the error state
            // else we will _materialize the lines read before timeout and return ok()
            return st;
        }
    }
    _materialize_src_chunk_adaptive_nullable_column(src_chunk);
    ASSIGN_OR_RETURN(auto cast_chunk, _cast_chunk(src_chunk));
    return materialize(src_chunk, cast_chunk);
}

StatusOr<ChunkPtr> AvroScanner::_cast_chunk(const starrocks::ChunkPtr& src_chunk) {
    SCOPED_RAW_TIMER(&_counter->cast_chunk_ns);
    ChunkPtr cast_chunk = std::make_shared<Chunk>();

    size_t slot_size = _src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot = _src_slot_descriptors[column_pos];
        if (slot == nullptr) {
            continue;
        }

        ASSIGN_OR_RETURN(ColumnPtr col, _cast_exprs[column_pos]->evaluate_checked(nullptr, src_chunk.get()));
        col = ColumnHelper::unfold_const_column(slot->type(), src_chunk->num_rows(), col);
        cast_chunk->append_column(std::move(col), slot->id());
    }

    return cast_chunk;
}

Status AvroScanner::_construct_cast_exprs() {
    size_t slot_size = _src_slot_descriptors.size();
    _cast_exprs.resize(slot_size);
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }

        auto& from_type = _avro_types[column_pos];
        auto& to_type = slot_desc->type();
        Expr* slot = _pool.add(new ColumnRef(slot_desc));

        if (to_type.is_assignable(from_type)) {
            _cast_exprs[column_pos] = slot;
            continue;
        }

        VLOG(3) << strings::Substitute("The field name($0) cast STARROCKS($1) to STARROCKS($2).", slot_desc->col_name(),
                                       from_type.debug_string(), to_type.debug_string());

        Expr* cast = VectorizedCastExprFactory::from_type(from_type, to_type, slot, &_pool);

        if (cast == nullptr) {
            return Status::InternalError(strings::Substitute("Not support cast $0 to $1.", from_type.debug_string(),
                                                             to_type.debug_string()));
        }

        _cast_exprs[column_pos] = cast;
    }

    return Status::OK();
}

Status AvroScanner::_construct_avro_types() {
    size_t slot_size = _src_slot_descriptors.size();
    _avro_types.resize(slot_size);
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }

        switch (slot_desc->type().type) {
        case TYPE_ARRAY: {
            TypeDescriptor json_type(TYPE_ARRAY);
            TypeDescriptor* child_type = &json_type;

            const TypeDescriptor* slot_type = &(slot_desc->type().children[0]);
            while (slot_type->type == TYPE_ARRAY) {
                slot_type = &(slot_type->children[0]);

                child_type->children.emplace_back(TYPE_ARRAY);
                child_type = &(child_type->children[0]);
            }

            // the json lib don't support get_int128_t(), so we load with BinaryColumn and then convert to LargeIntColumn
            if (slot_type->type == TYPE_FLOAT || slot_type->type == TYPE_DOUBLE || slot_type->type == TYPE_BIGINT ||
                slot_type->type == TYPE_INT || slot_type->type == TYPE_SMALLINT || slot_type->type == TYPE_TINYINT) {
                // Treat these types as what they are.
                child_type->children.emplace_back(slot_type->type);
            } else if (slot_type->type == TYPE_VARCHAR) {
                auto varchar_type = TypeDescriptor::create_varchar_type(slot_type->len);
                child_type->children.emplace_back(varchar_type);
            } else if (slot_type->type == TYPE_CHAR) {
                auto char_type = TypeDescriptor::create_char_type(slot_type->len);
                child_type->children.emplace_back(char_type);
            } else if (slot_type->type == TYPE_JSON) {
                child_type->children.emplace_back(TypeDescriptor::create_json_type());
            } else {
                // Treat other types as VARCHAR.
                auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
                child_type->children.emplace_back(varchar_type);
            }

            _avro_types[column_pos] = std::move(json_type);
            break;
        }

        // Treat these types as what they are.
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
        case TYPE_BIGINT:
        case TYPE_INT:
        case TYPE_BOOLEAN:
        case TYPE_SMALLINT:
        case TYPE_TINYINT: {
            _avro_types[column_pos] = TypeDescriptor{slot_desc->type().type};
            break;
        }

        case TYPE_CHAR: {
            auto char_type = TypeDescriptor::create_char_type(slot_desc->type().len);
            _avro_types[column_pos] = std::move(char_type);
            break;
        }

        case TYPE_VARCHAR: {
            auto varchar_type = TypeDescriptor::create_varchar_type(slot_desc->type().len);
            _avro_types[column_pos] = std::move(varchar_type);
            break;
        }

        case TYPE_JSON: {
            _avro_types[column_pos] = TypeDescriptor::create_json_type();
            break;
        }

        // Treat other types as VARCHAR.
        default: {
            auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            _avro_types[column_pos] = std::move(varchar_type);
            break;
        }
        }
    }
    return Status::OK();
}

void AvroScanner::close() {
    if (!_closed) {
        _file.reset();
        _closed = true;
    }
    FileScanner::close();
}

Status AvroScanner::_get_array_element(const avro_value_t* cur_value, size_t idx, avro_value_t* element) {
    size_t element_count;
    if (avro_value_get_size(cur_value, &element_count) != 0) {
        auto err_msg = "Cannot get avro array size: " + std::string(avro_strerror());
        return Status::InternalError(err_msg);
    }
    if (idx >= element_count) {
        auto err_msg = "array idx greater than the avro array max size";
        return Status::InternalError(err_msg);
    }
    if (avro_value_get_by_index(cur_value, idx, element, nullptr) != 0) {
        auto err_msg = "Cannot get avro value from array: " + std::string(avro_strerror());
        return Status::InternalError(err_msg);
    }
    return Status::OK();
}

bool construct_path_from_str(std::string path_str, std::vector<AvroPath>& paths) {
    return false;
}

// This function handles the union data type, and the reason for using loops is that there is a nested case:
//    [
//         null,
//         [
//             null,
//             [
//                 null,
//                 "string"
//             ]
//         ]
//     ]
// The whole logic is to return the deepest data, which could be null or some other data type.
Status AvroScanner::_handle_union(const avro_value_t* input_value, avro_value_t* branch) {
    while (avro_value_get_type(input_value) == AVRO_UNION) {
        int disc;
        if (avro_value_get_discriminant(input_value, &disc) != 0) {
            auto err_msg = "Cannot get union discriminan: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        if (avro_value_set_branch(input_value, disc, branch) != 0) {
            auto err_msg = "Cannot set union branch: " + std::string(avro_strerror());
            return Status::InternalError(err_msg);
        }
        if (avro_value_get_type(branch) == AVRO_NULL) {
            return Status::OK();
        }
        input_value = branch;
    }
    return Status::OK();
}

// This function extracts the corresponding field data from avro data according to
// the corresponding path of each column, where:
// input_value: input param
// paths: input param
// output_value: output param
Status AvroScanner::_extract_field(const avro_value_t& input_value, const std::vector<AvroPath>& paths,
                                   avro_value_t* output_value) {
    avro_value_t cur_value = input_value;

    // Select the entire data
    if (UNLIKELY(paths.size() == 1 && paths[0].key == "$" && paths[0].idx == -1)) {
        // Remove union
        if (avro_value_get_type(&cur_value) == AVRO_UNION) {
            avro_value_t branch;
            RETURN_IF_ERROR(_handle_union(&cur_value, &branch));
            cur_value = branch;
        }
        *output_value = cur_value;
        return Status::OK();
    }

    // The starting point for our progression is an array, and we cannot use avro_value_get_by_name
    // to get the value of the next field. We want the path pattern to look like this:
    // {
    //     key == "$"
    //     idx = 0,1,2,3....
    // }
    if (UNLIKELY(avro_value_get_type(&cur_value) == AVRO_ARRAY)) {
        if (paths[0].key != "$" || paths[0].idx < 0) {
            auto err_msg = "The avro root type is an array, and you should select a specific array element.";
            return Status::InternalError(err_msg);
        }
        avro_value_t element;
        RETURN_IF_ERROR(_get_array_element(&cur_value, paths[0].idx, &element));
        cur_value = element;
    }

    // paths[0] should be $, skip it
    for (int i = 1; i < paths.size(); i++) {
        // The union type is used to provide nullable semantics. For scenarios where AVRO imports,
        // AVRO's union type can only be of this pattern:
        // {
        //      null,
        //      Other Type
        // }
        // For each iteration, we first determine if the current avro type is union. If it is union,
        // we continue to determine if it is null. If so, we stop the progression and return.
        // If it is not null, the corresponding value is extracted and the progress continues.
        if (UNLIKELY(avro_value_get_type(&cur_value) == AVRO_UNION)) {
            avro_value_t branch;
            RETURN_IF_ERROR(_handle_union(&cur_value, &branch));
            cur_value = branch;
            if (avro_value_get_type(&branch) == AVRO_NULL) {
                *output_value = cur_value;
                return Status::OK();
            }
        }

        if (UNLIKELY(avro_value_get_type(&cur_value) != AVRO_RECORD)) {
            if (i == paths.size() - 1) {
                break;
            } else {
                auto err_msg = "A non-record type was found during avro parsing. Please check the path you specified";
                return Status::InternalError(err_msg);
            }
        }
        avro_value_t next_value;
        if (LIKELY(avro_value_get_by_name(&cur_value, paths[i].key.c_str(), &next_value, nullptr) == 0)) {
            // For each path, we first need to determine whether the path has an array element operation
            cur_value = next_value;
            if (UNLIKELY(paths[i].idx != -1)) {
                // In this case, you need to remove the union:
                // $.event_params[2]
                // {
                //   "name": "event_params",
                //   "type": ["null", {
                //      "items": "string",
                //      "type": "array"
                //    }]
                // }
                if (avro_value_get_type(&cur_value) == AVRO_UNION) {
                    avro_value_t branch;
                    RETURN_IF_ERROR(_handle_union(&cur_value, &branch));
                    cur_value = branch;
                    if (avro_value_get_type(&branch) == AVRO_NULL) {
                        *output_value = cur_value;
                        return Status::OK();
                    }
                }
                // cur_value must be an array type, otherwise it is invalid
                if (avro_value_get_type(&cur_value) != AVRO_ARRAY) {
                    auto err_msg =
                            "A non-array type was found during avro parsing. Please check the path you specified";
                    return Status::InternalError(err_msg);
                }
                avro_value_t element;
                RETURN_IF_ERROR(_get_array_element(&cur_value, paths[0].idx, &element));
                cur_value = element;
            }
        } else {
            // If no field can be found, end the parsing of the row and return not found.
            auto msg = strings::Substitute("Cannot get field: $0. err msg: $1.", paths[i].key, avro_strerror());
            return Status::NotFound(msg);
        }
    }
    // Remove union
    if (UNLIKELY(avro_value_get_type(&cur_value) == AVRO_UNION)) {
        avro_value_t branch;
        RETURN_IF_ERROR(_handle_union(&cur_value, &branch));
        cur_value = branch;
    }
    *output_value = cur_value;
    return Status::OK();
}

Status AvroScanner::_construct_column(const avro_value_t& input_value, Column* column, const TypeDescriptor& type_desc,
                                      const std::string& col_name) {
    return add_adaptive_nullable_column(column, type_desc, col_name, input_value, !_strict_mode);
}

} // namespace starrocks