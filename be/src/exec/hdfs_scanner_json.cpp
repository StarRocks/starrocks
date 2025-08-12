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

#include "exec/hdfs_scanner_json.h"
#include "util/compression/compression_utils.h"
#include "util/simdjson_util.h"
#include "formats/json/nullable_column.h"

namespace starrocks {
HdfsScannerJsonReader::HdfsScannerJsonReader(RandomAccessFile* file, size_t file_length,
                                             std::vector<SlotDescriptor*> slot_descs,
                                             std::vector<TypeDescriptor> type_descs)
{
    _file = file;
    _offset = 0;
    _remain_length = file_length;
    _file_length = file_length;
    _slot_descs = std::move(slot_descs);
    _type_descs = std::move(type_descs);

    int index = 0;
    for (size_t i = 0; i < _slot_descs.size(); i++) {
        const auto& desc = _slot_descs[i];
        if (desc == nullptr) {
            continue;
        }
        index++;
        _slot_desc_dict.emplace(desc->col_name(), desc);
        _type_desc_dict.emplace(desc->col_name(), _type_descs[i]);
    }
}

Status HdfsScannerJsonReader::init() {
    ASSIGN_OR_RETURN(_buffer, ByteBuffer::allocate_with_tracker(_init_buf_size));
    _buffer->limit = 0;

    //_parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
    return Status::OK();
}

Status HdfsScannerJsonReader::_read_and_parse_json() {
    RETURN_IF_ERROR(_read_file_stream());
    _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
    _empty_parser = false;
    LOG(ERROR) << "LXH: READ AND PARSE: " << _buffer->pos << ":" << _buffer->limit << ":" << _buffer->capacity;
    return _parser->parse(_buffer->ptr, _buffer->limit, _buffer->capacity);
}

Status HdfsScannerJsonReader::_read_file_stream() {
    if (_buffer->pos > 0) {
        _buffer->move_to_front();
    }

    size_t try_read_size = _buffer->try_read_size();
    LOG(ERROR) << "LXH: READ_FILE_STREAM: " << _buffer->pos << ":" << _buffer->limit << ":" << try_read_size;
    ASSIGN_OR_RETURN(int64_t read_size, _file->read(_buffer->limit_ptr(), try_read_size));
    if (read_size == 0) {
        return Status::EndOfFile("");
    } else {
        _buffer->read(read_size);
        return Status::OK();
    }
}

Status HdfsScannerJsonReader::_construct_column(simdjson::ondemand::value& value, Column* column,
                                                const TypeDescriptor& type_desc, const std::string& col_name) {
    return add_nullable_column(column, type_desc, col_name, &value, true);
}

Status HdfsScannerJsonReader::_construct_row_without_jsonpath(simdjson::ondemand::object* row,
                                                              Chunk* chunk) {
    _parsed_columns.assign(chunk->num_columns(), false);
    faststring buffer;
    try {
        uint32_t key_index = 0;
        int tmp_i = 0;
        for (auto field : *row) {
            //LOG(ERROR) << "LXH: CONSTRUCT: " << tmp_i;
            tmp_i++;
            int column_index;
            std::string_view key = field_unescaped_key_safe(field, &buffer);
            if (_prev_parsed_position.size() > key_index && _prev_parsed_position[key_index].key == key) {
                column_index = _prev_parsed_position[key_index].column_index;
                if (column_index < 0) {
                    key_index++;
                    continue;
                }
            } else {
                auto itr = _slot_desc_dict.find(key);
                if (itr == _slot_desc_dict.end()) {
                    if (_prev_parsed_position.size() <= key_index) {
                        _prev_parsed_position.emplace_back(key);
                    } else {
                        _prev_parsed_position[key_index].key = key;
                        _prev_parsed_position[key_index].column_index = -1;
                    }
                    key_index++;
                    continue;
                }

                auto slot_desc = itr->second;
                auto type_desc = _type_desc_dict[key];

                column_index = chunk->get_index_by_slot_id(slot_desc->id());
                if (_prev_parsed_position.size() <= key_index) {
                    _prev_parsed_position.emplace_back(key, column_index, type_desc);
                } else {
                    _prev_parsed_position[key_index].key = key;
                    _prev_parsed_position[key_index].column_index = column_index;
                    _prev_parsed_position[key_index].type = type_desc;
                }
            }

            DCHECK(column_index >= 0);
            if (_parsed_columns[column_index]) {
                key_index++;
                continue;
            } else {
                _parsed_columns[column_index] = true;
            }
            auto& column = chunk->get_column_by_index(column_index);
            simdjson::ondemand::value val = field.value();

            RETURN_IF_ERROR(_construct_column(val, column.get(), _prev_parsed_position[key_index].type,
                                              _prev_parsed_position[key_index].key));

            key_index++;
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("construct row in object order failed, error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    for (int i = 0; i < chunk->num_columns(); i++) {
        if (!_parsed_columns[i]) {
            auto& column = chunk->get_column_by_index(i);
            column->append_nulls(1);
        }
    }
    return Status::OK();
}

Status HdfsScannerJsonReader::_read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read) {
    simdjson::ondemand::object row;
    while (*rows_read < rows_to_read) {
        auto st = _parser->get_current(&row);
        if (!st.ok()) {
            LOG(ERROR) << "LXH: EOF" << st;
            return st;
        }
        //LOG(ERROR) << "LXH: LEFT_1: " << _parser->left_bytes_string(4096);
        //LOG(ERROR) << "LXH: LEFT_2: " << _parser->left_bytes();
        st = _construct_row_without_jsonpath(&row, chunk);
        if (!st.ok()) {
            LOG(ERROR) << "LXH: construct row failed: " << st;
            return st;
        }
        ++(*rows_read);
        st = _parser->advance();
        //LOG(ERROR) << "LXH: LEFT_3: " << _parser->left_bytes_string(4096);
        //LOG(ERROR) << "LXH: LEFT_4: " << _parser->left_bytes();
        if (!st.ok()) {
            LOG(ERROR) << "LXH: ADVANCE: " << st;
            return st;
        }
    }

    return Status::OK();
}

Status HdfsScannerJsonReader::next_record(Chunk* chunk, int32_t rows_to_read) {
    int32_t rows_read = 0;
    while (rows_read < rows_to_read) {
        if (_empty_parser) {
            LOG(ERROR) << "LXH: EMPTY PARSER";
            Status st = _read_and_parse_json();
            if (!st.ok()) {
                LOG(ERROR) << "LXH: read_and_parse_json failed: " << st;
                return st;
            }
            _empty_parser = false;
        }

        //LOG(ERROR) << "LXH: READ ROWS";
        Status st = _read_rows(chunk, rows_to_read, &rows_read);
        if (st.is_end_of_file()) {
            size_t left_bytes = _parser->left_bytes();
            _buffer->pos = _buffer->limit - left_bytes;
            _empty_parser = true;
        } else if (!st.ok()) {
            return st;
        }
    }

    //LOG(INFO) << "LXH: CHUNK: " << (*chunk).num_rows();

    return Status::OK();
}

Status HdfsJsonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    const TTextFileDesc& text_file_desc = _scanner_params.scan_range->text_file_desc;
    RETURN_IF_ERROR(_setup_compression_type(text_file_desc));
    return Status::OK();
}

static TypeDescriptor construct_json_type(const TypeDescriptor& src_type) {
    switch (src_type.type) {
    case TYPE_ARRAY: {
        TypeDescriptor json_type(TYPE_ARRAY);
        const auto& child_type = src_type.children[0];
        json_type.children.emplace_back(construct_json_type(child_type));
        return json_type;
    }
    case TYPE_STRUCT: {
        TypeDescriptor json_type(TYPE_STRUCT);
        json_type.field_names = src_type.field_names;
        for (auto& child_type : src_type.children) {
            json_type.children.emplace_back(construct_json_type(child_type));
        }
        return json_type;
    }
    case TYPE_MAP: {
        TypeDescriptor json_type(TYPE_MAP);
        const auto& key_type = src_type.children[0];
        const auto& value_type = src_type.children[1];
        json_type.children.emplace_back(construct_json_type(key_type));
        json_type.children.emplace_back(construct_json_type(value_type));
        return json_type;
    }
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_BIGINT:
    case TYPE_INT:
    case TYPE_SMALLINT:
    case TYPE_TINYINT:
    case TYPE_BOOLEAN:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_JSON: {
        return src_type;
    }
    default:
        // Treat other types as VARCHAR.
        return TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    }
}

Status HdfsJsonScanner::_construct_json_types() {
    size_t slot_size = _scanner_ctx.slot_descs.size();
    _json_types.resize(slot_size);
    for (int column_pos = 0; column_pos < slot_size; column_pos++) {
        auto slot_desc = _scanner_ctx.slot_descs[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }
        _json_types[column_pos] = construct_json_type(slot_desc->type());
    }

    for (size_t i = 0; i < _json_types.size(); i++) {
        LOG(ERROR) << "LXH: CONSTRUCT JSON: " << i << ":" << _json_types[i].type;
    }
    return Status::OK();
}

Status HdfsJsonScanner::do_open(RuntimeState* runtime_state) {
    if (_no_data) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_construct_json_types());
    RETURN_IF_ERROR(open_random_access_file());

    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    Status st = _create_csv_reader();
    if (st.is_end_of_file()) {
        _no_data = true;
        return Status::OK();
    } else if (!st.ok()) {
        return st;
    }
    RETURN_IF_ERROR(_reader->init());

    // update materialized columns.
    {
        std::unordered_set<std::string> names;
        for (const auto& column : _scanner_ctx.materialized_columns) {
            if (column.name() == "___count___") {
                continue;
            }
            names.insert(column.name());
        }
        RETURN_IF_ERROR(_scanner_ctx.update_materialized_columns(names));
        int i = 0;
        for (auto kv : _scanner_ctx.materialized_columns) {
            LOG(INFO) << "LXH: MATE: " << i << ":" << kv.name() << ":" << kv.slot_type().debug_string();
            i++;
        }
    }

    RETURN_IF_ERROR(_build_hive_column_name_2_index());

    for (const auto& column : _scanner_ctx.materialized_columns) {
        // converter
    }

    LOG(INFO) << "LXH: HdfsJsonScanner OPEN SUCCESS";

    return Status::OK();
}

Status HdfsJsonScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_no_data) {
        return Status::EndOfFile("");
    }
    CHECK(chunk != nullptr);

    //LOG(ERROR) << "LXH: START DO_GET_NEXT";
    Status st = _reader->next_record(chunk->get(), runtime_state->chunk_size());
    if (!st.ok()) {
        if (st.is_end_of_file()) {
            _no_data = true;
            return Status::OK();
        } else {
            return st;
        }
    }
    //LOG(ERROR) << "LXH: END DO_GET_NEXT";

    return Status::OK();
}

Status HdfsJsonScanner::_parse_json(int chunk_size, ChunkPtr* chunk) {
    DCHECK_EQ(0, (*chunk)->num_rows());
    int num_columns = (*chunk)->num_columns();
    _column_raw_ptrs.resize(num_columns);
    for (int i = 0; i < num_columns; i++) {
        _column_raw_ptrs[i] = (*chunk)->get_column_by_index(i).get();
        _column_raw_ptrs[i]->reserve(chunk_size);
    }
    LOG(INFO) << "LXH: NUM_COLUMNS: " << num_columns;

    return Status::OK();
}

Status HdfsJsonScanner::_create_csv_reader() {
    const THdfsScanRange* scan_range = _scanner_ctx.scan_range;
    if (_compression_type != NO_COMPRESSION) {
        auto file_size = static_cast<size_t>(-1);
        _reader = std::make_shared<HdfsScannerJsonReader>(_file.get(), file_size, _scanner_ctx.slot_descs,
                                                          _json_types);
        LOG(INFO) << "LXH: JSON_READER_1: " << _file->filename() << ":" << file_size;
    } else {
        _reader = std::make_shared<HdfsScannerJsonReader>(_file.get(), scan_range->file_length,
                                                          _scanner_ctx.slot_descs, _json_types);
        LOG(INFO) << "LXH: JSON_READER_2: " << _file->filename() << ":" << scan_range->file_length;
    }

    return Status::OK();
}

Status HdfsJsonScanner::_build_hive_column_name_2_index() {
    LOG(ERROR) << "LXH: COLUMN_NAMEs: " << _scanner_ctx.hive_column_names->size();
    if (_scanner_ctx.hive_column_names->empty()) {
        _materialize_slots_index_2_csv_column_index.resize(_scanner_ctx.materialized_columns.size());
        for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
            _materialize_slots_index_2_csv_column_index[i] = i;
        }
        return Status::OK();
    }

    const bool case_sensitive = _scanner_ctx.case_sensitive;
    std::unordered_map<std::string, size_t> formatted_hive_column_name_2_index;

    for (size_t i = 0; i < _scanner_ctx.hive_column_names->size(); i++) {
        const std::string& name = (*_scanner_ctx.hive_column_names)[i];
        const std::string formatted_column_name = _scanner_ctx.formatted_name(name);
        formatted_hive_column_name_2_index.emplace(formatted_column_name, i);
        LOG(ERROR) << "LXH: HIVE: " << i << ":" << name << ":" << formatted_column_name;
    }

    _materialize_slots_index_2_csv_column_index.resize(_scanner_ctx.materialized_columns.size());
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
        const auto& column = _scanner_ctx.materialized_columns[i];
        const std::string formatted_slot_name = column.formatted_name(case_sensitive);
        const auto& it = formatted_hive_column_name_2_index.find(formatted_slot_name);
        if (it == formatted_hive_column_name_2_index.end()) {
            return Status::InternalError("Can not get index of column name: " + formatted_slot_name);
        }
        _materialize_slots_index_2_csv_column_index[i] = it->second;
    }
    return Status::OK();
}

Status HdfsJsonScanner::_setup_compression_type(const TTextFileDesc& text_file_desc) {
    CompressionTypePB compression_type;
    if (text_file_desc.__isset.compression_type) {
        compression_type = CompressionUtils::to_compression_pb(text_file_desc.compression_type);
        LOG(INFO) << "LXH: C_1: " << compression_type;
    } else {
        compression_type = get_compression_type_from_path(_scanner_params.path);
        LOG(INFO) << "LXH: C_2: " << compression_type;
    }
    if (compression_type != UNKNOWN_COMPRESSION) {
        _compression_type = compression_type;
    } else {
        _compression_type = NO_COMPRESSION;
    }
    if (_compression_type != NO_COMPRESSION && _scanner_params.scan_range->offset != 0) {
        _no_data = true;
    }
    return Status::OK();
}

} // namespace starrocks
