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

#include "exec/json_scanner.h"

#include <fmt/format.h>
#include <ryu/ryu.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/json_parser.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/json_functions.h"
#include "formats/json/nullable_column.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"

namespace starrocks {

const int64_t MAX_ERROR_LOG_LENGTH = 64;

JsonScanner::JsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _next_range(0),
          _max_chunk_size(state->chunk_size()),
          _cur_file_reader(nullptr),
          _cur_file_eof(true) {}

JsonScanner::~JsonScanner() = default;

Status JsonScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    RETURN_IF_ERROR(_construct_json_types());
    RETURN_IF_ERROR(_construct_cast_exprs());

    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }

    const TBrokerRangeDesc& range = _scan_range.ranges[0];

    if (range.__isset.jsonpaths) {
        RETURN_IF_ERROR(parse_json_paths(range.jsonpaths, &_json_paths));
    }
    if (range.__isset.json_root) {
        JsonFunctions::parse_json_paths(range.json_root, &_root_paths);
    }
    if (range.__isset.strip_outer_array) {
        _strip_outer_array = range.strip_outer_array;
    }

    return Status::OK();
}

StatusOr<ChunkPtr> JsonScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    ChunkPtr src_chunk;
    RETURN_IF_ERROR(_create_src_chunk(&src_chunk));

    if (_cur_file_eof) {
        RETURN_IF_ERROR(_open_next_reader());
        _cur_file_eof = false;
    }

    Status status;
    try {
        status = _cur_file_reader->read_chunk(src_chunk.get(), _max_chunk_size);
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = "Unrecognized json format, stop json loader.";
        LOG(WARNING) << err_msg;
        return Status::DataQualityError(err_msg);
    }
    if (!status.ok()) {
        if (status.is_end_of_file()) {
            _cur_file_eof = true;
        } else if (!status.is_time_out()) {
            return status;
        }
    }

    if (src_chunk->num_rows() == 0) {
        if (status.is_end_of_file()) {
            return Status::EndOfFile("EOF of reading json file, nothing read");
        } else if (status.is_time_out()) {
            // if timeout happens at the beginning of reading src_chunk, we return the error state
            // else we will _materialize the lines read before timeout and return ok()
            return status;
        }
    }
    _materialize_src_chunk_adaptive_nullable_column(src_chunk);
    ASSIGN_OR_RETURN(auto cast_chunk, _cast_chunk(src_chunk));
    return materialize(src_chunk, cast_chunk);
}

void JsonScanner::close() {
    FileScanner::close();
}

Status JsonScanner::_construct_json_types() {
    size_t slot_size = _src_slot_descriptors.size();
    _json_types.resize(slot_size);
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

            _json_types[column_pos] = std::move(json_type);
            break;
        }

        // Treat these types as what they are.
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
        case TYPE_BIGINT:
        case TYPE_INT:
        case TYPE_SMALLINT:
        case TYPE_TINYINT: {
            _json_types[column_pos] = TypeDescriptor{slot_desc->type().type};
            break;
        }

        case TYPE_CHAR: {
            auto char_type = TypeDescriptor::create_char_type(slot_desc->type().len);
            _json_types[column_pos] = std::move(char_type);
            break;
        }

        case TYPE_VARCHAR: {
            auto varchar_type = TypeDescriptor::create_varchar_type(slot_desc->type().len);
            _json_types[column_pos] = std::move(varchar_type);
            break;
        }

        case TYPE_JSON: {
            _json_types[column_pos] = TypeDescriptor::create_json_type();
            break;
        }

        // Treat other types as VARCHAR.
        default: {
            auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            _json_types[column_pos] = std::move(varchar_type);
            break;
        }
        }
    }
    return Status::OK();
}

Status JsonScanner::_construct_cast_exprs() {
    size_t slot_size = _src_slot_descriptors.size();
    _cast_exprs.resize(slot_size);
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }

        auto& from_type = _json_types[column_pos];
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

Status JsonScanner::parse_json_paths(const std::string& jsonpath, std::vector<std::vector<SimpleJsonPath>>* path_vecs) {
    try {
        simdjson::dom::parser parser;
        simdjson::dom::element elem = parser.parse(jsonpath.c_str(), jsonpath.length());

        simdjson::dom::array paths = elem.get_array();

        for (const auto& path : paths) {
            if (!path.is_string()) {
                return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
            }

            std::vector<SimpleJsonPath> parsed_paths;
            const char* cstr = path.get_c_str();

            JsonFunctions::parse_json_paths(std::string(cstr), &parsed_paths);
            path_vecs->emplace_back(std::move(parsed_paths));
        }
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg =
                strings::Substitute("Invalid json path: $0, error: $1", jsonpath, simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status JsonScanner::_create_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    *chunk = std::make_shared<Chunk>();
    size_t slot_size = _src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];

        if (slot_desc == nullptr) {
            continue;
        }

        // The columns in source chunk are all in AdaptiveNullableColumn type;
        auto col = ColumnHelper::create_column(_json_types[column_pos], true, false, 0, true);
        (*chunk)->append_column(col, slot_desc->id());
    }

    return Status::OK();
}

void JsonScanner::_materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        AdaptiveNullableColumn* adaptive_column =
                down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

Status JsonScanner::_open_next_reader() {
    if (_next_range >= _scan_range.ranges.size()) {
        return Status::EndOfFile("EOF of reading json file");
    }
    std::shared_ptr<SequentialFile> file;
    const TBrokerRangeDesc& range_desc = _scan_range.ranges[_next_range];
    Status st = create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &file);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create sequential files: " << st.to_string();
        return st;
    }
    _cur_file_reader = std::make_unique<JsonReader>(_state, _counter, this, file, _strict_mode, _src_slot_descriptors);
    RETURN_IF_ERROR(_cur_file_reader->open());
    _next_range++;
    return Status::OK();
}

StatusOr<ChunkPtr> JsonScanner::_cast_chunk(const starrocks::ChunkPtr& src_chunk) {
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

JsonReader::JsonReader(starrocks::RuntimeState* state, starrocks::ScannerCounter* counter, JsonScanner* scanner,
                       std::shared_ptr<SequentialFile> file, bool strict_mode, std::vector<SlotDescriptor*> slot_descs)
        : _state(state),
          _counter(counter),
          _scanner(scanner),
          _strict_mode(strict_mode),
          _file(std::move(file)),
          _slot_descs(std::move(slot_descs)),
          _op_col_index(-1) {
#if BE_TEST
    raw::RawVector<char> buf(_buf_size);
    std::swap(buf, _buf);
#endif
    int index = 0;
    for (const auto& desc : _slot_descs) {
        if (desc == nullptr) {
            continue;
        }
        if (UNLIKELY(desc->col_name() == "__op")) {
            _op_col_index = index;
        }
        index++;
        _slot_desc_dict.emplace(desc->col_name(), desc);
    }
}

Status JsonReader::open() {
    RETURN_IF_ERROR(_read_and_parse_json());
    _empty_parser = false;
    _closed = false;
    return Status::OK();
}

JsonReader::~JsonReader() {
    close();
}

Status JsonReader::close() {
    if (_closed) {
        return Status::OK();
    }
    _file.reset();
    _closed = true;
    return Status::OK();
}

/**
 * Case 1 : Json without JsonPath
 * For example:
 *  [{"colunm1":"value1", "colunm2":10}, {"colunm1":"value2", "colunm2":30}]
 * Result:
 *      column1    column2
 *      ------------------
 *      value1     10
 *      value2     30
 *
 * Case 2 : Json with JsonPath
 * {
 *   "RECORDS":[
 *      {"column1":"value1","column2":"10"},
 *      {"column1":"value2","column2":"30"},
 *   ]
 * }
 * JsonRoot = "$.RECORDS"
 * JsonPaths = "[$.column1, $.column2]"
 * Result:
 *      column1    column2
 *      ------------------
 *      value1     10
 *      value2     30
 */
Status JsonReader::read_chunk(Chunk* chunk, int32_t rows_to_read) {
    int32_t rows_read = 0;
    while (rows_read < rows_to_read) {
        if (_empty_parser) {
            auto st = _read_and_parse_json();
            if (!st.ok()) {
                if (st.is_end_of_file()) {
                    // all data has been exhausted.
                    return st;
                }
                if (st.is_time_out()) {
                    // read time out
                    return st;
                }
                // Parse error.
                _counter->num_rows_filtered++;
                _state->append_error_msg_to_file("", st.to_string());
                return st;
            }
            _empty_parser = false;
        }

        Status st;
        // Eliminates virtual function call.
        if (!_scanner->_root_paths.empty()) {
            // With json root set, expand the outer array automatically.
            // The strip_outer_array determines whether to expand the sub-array of json root.
            if (_scanner->_strip_outer_array) {
                // Expand outer array automatically according to _is_ndjson.
                if (_is_ndjson) {
                    st = _read_rows<ExpandedJsonDocumentStreamParserWithRoot>(chunk, rows_to_read, &rows_read);
                } else {
                    st = _read_rows<ExpandedJsonArrayParserWithRoot>(chunk, rows_to_read, &rows_read);
                }
            } else {
                if (_is_ndjson) {
                    st = _read_rows<JsonDocumentStreamParserWithRoot>(chunk, rows_to_read, &rows_read);
                } else {
                    st = _read_rows<JsonArrayParserWithRoot>(chunk, rows_to_read, &rows_read);
                }
            }
        } else {
            // Without json root set, the strip_outer_array determines whether to expand outer array.
            if (_scanner->_strip_outer_array) {
                st = _read_rows<JsonArrayParser>(chunk, rows_to_read, &rows_read);
            } else {
                st = _read_rows<JsonDocumentStreamParser>(chunk, rows_to_read, &rows_read);
            }
        }

        if (st.is_end_of_file()) {
            // the parser is exhausted.
            _empty_parser = true;
        } else if (!st.ok()) {
            return st;
        }
    }

    return Status::OK();
}

template <typename ParserType>
Status JsonReader::_read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read) {
    simdjson::ondemand::object row;
    auto parser = down_cast<ParserType*>(_parser.get());

    while (*rows_read < rows_to_read) {
        auto st = parser->get_current(&row);
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                return st;
            }
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file(
                    fmt::format("parser current location: {}", parser->left_bytes_string(MAX_ERROR_LOG_LENGTH)),
                    st.to_string());
            return st;
        }
        size_t chunk_row_num = chunk->num_rows();
        st = _construct_row(&row, chunk);
        if (!st.ok()) {
            if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
                // We would continue to construct row even if error is returned,
                // hence the number of error appended to the file should be limited.
                std::string_view sv;
                (void)!row.raw_json().get(sv);
                _state->append_error_msg_to_file(std::string(sv.data(), sv.size()), st.to_string());
                LOG(WARNING) << "failed to construct row: " << st;
            }
            if (_state->enable_log_rejected_record()) {
                std::string_view sv;
                (void)!row.raw_json().get(sv);
                _state->append_rejected_record_to_file(std::string(sv.data(), sv.size()), st.to_string(),
                                                       _file->filename());
            }
            // Before continuing to process other rows, we need to first clean the fail parsed row.
            chunk->set_num_rows(chunk_row_num);
        }
        ++(*rows_read);

        st = parser->advance();
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                return st;
            }
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", st.to_string());
            return st;
        }
    }
    return Status::OK();
}

Status JsonReader::_construct_row_without_jsonpath(simdjson::ondemand::object* row, Chunk* chunk) {
    _parsed_columns.assign(chunk->num_columns(), false);

    try {
        uint32_t key_index = 0;
        for (auto field : *row) {
            int column_index;
            std::string_view key = field.unescaped_key();

            // _prev_parsed_position records the chunk column index for each key of previous parsed json object.
            // For example, if previous json object is
            // {
            //      'a':1,
            //      'b':2,
            // }
            // and key 'a' refers to the 1st column of chunk, key 'b' refers to the 2nd column of chunk,
            // then _prev_parsed_position is [ {'a', 1, int}, {'b', 2, int}].
            // At this time, suppose the next parsed json object is
            // {
            //      'a':10,
            //      'b':15,
            //      'c':25
            // }
            // through the _prev_parsed_position, we can know that the column index for 'a' is 1, and the column
            // index for 'b' is 2. Since previous parsed json object doesn't contain 'c', key 'c' 's column index
            // needs to be searched from the _slot_desc_dict, and if the key 'c' refers to the 3rd column of chunk,
            // then we will update the _prev_parsed_position to be [{'a', 1, int}, {'b', 2, int}, {'c', 3, int}].
            if (LIKELY(_prev_parsed_position.size() > key_index && _prev_parsed_position[key_index].key == key)) {
                // obtain column_index from previous parsed position
                column_index = _prev_parsed_position[key_index].column_index;
                if (column_index < 0) {
                    // column_index < 0 means key is not in the slot dict, and we will skip this field
                    key_index++;
                    continue;
                }
            } else {
                // look up key in the slot dict.
                auto itr = _slot_desc_dict.find(key);
                if (itr == _slot_desc_dict.end()) {
                    // parsed key of the json object is not in the slot dict, and we will skip this field
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

                // update the prev parsed position
                column_index = chunk->get_index_by_slot_id(slot_desc->id());
                if (_prev_parsed_position.size() <= key_index) {
                    _prev_parsed_position.emplace_back(key, column_index, slot_desc->type());
                } else {
                    _prev_parsed_position[key_index].key = key;
                    _prev_parsed_position[key_index].column_index = column_index;
                    _prev_parsed_position[key_index].type = slot_desc->type();
                }
            }

            DCHECK(column_index >= 0);
            _parsed_columns[column_index] = true;
            auto& column = chunk->get_column_by_index(column_index);
            simdjson::ondemand::value val = field.value();

            // construct column with value.
            RETURN_IF_ERROR(_construct_column(val, column.get(), _prev_parsed_position[key_index].type,
                                              _prev_parsed_position[key_index].key));

            key_index++;
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("construct row in object order failed, error: $0",
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }

    // append null to the column without data.
    for (int i = 0; i < chunk->num_columns(); i++) {
        if (!_parsed_columns[i]) {
            auto& column = chunk->get_column_by_index(i);
            if (UNLIKELY(i == _op_col_index)) {
                // special treatment for __op column, fill default value '0' rather than null
                if (column->is_binary()) {
                    std::ignore = column->append_strings(std::vector{Slice{"0"}});
                } else {
                    column->append_datum(Datum((uint8_t)0));
                }
            } else {
                column->append_nulls(1);
            }
        }
    }
    return Status::OK();
}

Status JsonReader::_construct_row_with_jsonpath(simdjson::ondemand::object* row, Chunk* chunk) {
    size_t slot_size = _slot_descs.size();
    size_t jsonpath_size = _scanner->_json_paths.size();
    for (size_t i = 0; i < slot_size; i++) {
        if (_slot_descs[i] == nullptr) {
            continue;
        }
        const char* column_name = _slot_descs[i]->col_name().c_str();

        // The columns in JsonReader's chunk are all in NullableColumn type;
        auto column = down_cast<NullableColumn*>(chunk->get_column_by_slot_id(_slot_descs[i]->id()).get());
        if (i >= jsonpath_size) {
            if (strcmp(column_name, "__op") == 0) {
                // special treatment for __op column, fill default value '0' rather than null
                if (column->is_binary()) {
                    column->append_strings(std::vector{Slice{"0"}});
                } else {
                    column->append_datum(Datum((uint8_t)0));
                }
            } else {
                column->append_nulls(1);
            }
            continue;
        }

        // NOTE
        // Why not process this syntax in extract_from_object?
        // simdjson's api is limited, which coult not convert ondemand::object to ondemand::value.
        // As a workaround, extract procedure is duplicated, for both ondemand::object and ondemand::value
        // TODO(mofei) make it more elegant
        if (_scanner->_json_paths[i].size() == 1 && _scanner->_json_paths[i][0].key == "$") {
            // add_nullable_column may invoke a for-range iterating to the row.
            // If the for-range iterating is invoked after field access, or a second for-range iterating is invoked,
            // it would get an error "Objects and arrays can only be iterated when they are first encountered",
            // Hence, resetting the row object is necessary here.
            row->reset();
            RETURN_IF_ERROR(add_adaptive_nullable_column_by_json_object(
                    column, _slot_descs[i]->type(), _slot_descs[i]->col_name(), row, !_strict_mode));
        } else {
            simdjson::ondemand::value val;
            auto st = JsonFunctions::extract_from_object(*row, _scanner->_json_paths[i], &val);
            if (st.ok()) {
                RETURN_IF_ERROR(_construct_column(val, column, _slot_descs[i]->type(), _slot_descs[i]->col_name()));
            } else if (st.is_not_found()) {
                if (strcmp(column_name, "__op") == 0) {
                    // special treatment for __op column, fill default value '0' rather than null
                    if (column->is_binary()) {
                        column->append_strings(std::vector{Slice{"0"}});
                    } else {
                        column->append_datum(Datum((uint8_t)0));
                    }
                } else {
                    column->append_nulls(1);
                }
            } else {
                return st;
            }
        }
    }
    return Status::OK();
}

Status JsonReader::_construct_row(simdjson::ondemand::object* row, Chunk* chunk) {
    if (_scanner->_json_paths.empty()) return _construct_row_without_jsonpath(row, chunk);

    return _construct_row_with_jsonpath(row, chunk);
}

// read one json string from file read and parse it to json doc.
Status JsonReader::_read_and_parse_json() {
    uint8_t* data{};
    size_t length = 0;

#ifdef BE_TEST

    [[maybe_unused]] size_t message_size = 0;
    ASSIGN_OR_RETURN(auto nread, _file->read(_buf.data(), _buf_size));
    if (nread == 0) {
        return Status::EndOfFile("EOF of reading file");
    }

    data = reinterpret_cast<uint8_t*>(_buf.data());
    length = nread;

#else
    // TODO: Remove the down_cast, should not rely on the specific implementation.
    auto* stream_file = down_cast<StreamLoadPipeInputStream*>(_file->stream().get());
    {
        SCOPED_RAW_TIMER(&_counter->file_read_ns);
        ASSIGN_OR_RETURN(_parser_buf, stream_file->pipe()->read());

        _state->update_num_bytes_scan_from_source(_parser_buf->remaining());

        if (_parser_buf->capacity < _parser_buf->remaining() + simdjson::SIMDJSON_PADDING) {
            // For efficiency reasons, simdjson requires a string with a few bytes (simdjson::SIMDJSON_PADDING) at the end.
            // Hence, a re-allocation is needed if the space is not enough.
            auto buf = ByteBuffer::allocate(_parser_buf->remaining() + simdjson::SIMDJSON_PADDING);
            buf->put_bytes(_parser_buf->ptr, _parser_buf->remaining());
            buf->flip();
            std::swap(buf, _parser_buf);
        }
    }

    data = reinterpret_cast<uint8_t*>(_parser_buf->ptr);
    length = _parser_buf->remaining();

#endif

    // Check the content formart accroding to the first non-space character.
    // Treat json string started with '{' as ndjson.
    // Treat json string started with '[' as json array.
    for (size_t i = 0; i < length; ++i) {
        // Skip spaces at the string head.
        if (data[i] == ' ' || data[i] == '\t' || data[i] == '\r' || data[i] == '\n') continue;

        if (data[i] == '[') {
            break;
        } else if (data[i] == '{') {
            _is_ndjson = true;
            break;
        } else {
            LOG(WARNING) << "illegal json started with [" << data[i] << "]";
            return Status::DataQualityError(fmt::format("illegal json started with {}", data[i]));
        }
    }

    if (!_scanner->_root_paths.empty()) {
        // With json root set, expand the outer array automatically.
        // The strip_outer_array determines whether to expand the sub-array of json root.
        if (_scanner->_strip_outer_array) {
            // Expand outer array automatically according to _is_ndjson.
            if (_is_ndjson) {
                _parser = std::make_unique<ExpandedJsonDocumentStreamParserWithRoot>(&_simdjson_parser,
                                                                                     _scanner->_root_paths);
            } else {
                _parser = std::make_unique<ExpandedJsonArrayParserWithRoot>(&_simdjson_parser, _scanner->_root_paths);
            }
        } else {
            if (_is_ndjson) {
                _parser = std::make_unique<JsonDocumentStreamParserWithRoot>(&_simdjson_parser, _scanner->_root_paths);
            } else {
                _parser = std::make_unique<JsonArrayParserWithRoot>(&_simdjson_parser, _scanner->_root_paths);
            }
        }
    } else {
        // Without json root set, the strip_outer_array determines whether to expand outer array.
        if (_scanner->_strip_outer_array) {
            _parser = std::make_unique<JsonArrayParser>(&_simdjson_parser);
        } else {
            _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
        }
    }

    _empty_parser = false;
    return _parser->parse(data, length, length + simdjson::SIMDJSON_PADDING);
}

// _construct_column constructs column based on no value.
Status JsonReader::_construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                                     const std::string& col_name) {
    return add_adaptive_nullable_column(column, type_desc, col_name, &value, !_strict_mode);
}

} // namespace starrocks
