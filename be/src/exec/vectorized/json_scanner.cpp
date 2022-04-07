// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/json_scanner.h"

#include <fmt/format.h>
#include <ryu/ryu.h>

#include <algorithm>
#include <sstream>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "env/env.h"
#include "exec/vectorized/json_parser.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/json_functions.h"
#include "formats/json/nullable_column.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

const int64_t MAX_ERROR_LINES_IN_FILE = 50;

JsonScanner::JsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _next_range(0),
          _max_chunk_size(state->chunk_size()),
          _cur_file_reader(nullptr),
          _cur_file_eof(true) {}

JsonScanner::~JsonScanner() {
    close();
}

Status JsonScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    RETURN_IF_ERROR(_construct_json_types());
    RETURN_IF_ERROR(_construct_cast_exprs());

    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }

    const TBrokerRangeDesc& range = _scan_range.ranges[0];

    if (range.__isset.jsonpaths && range.__isset.json_root) {
        return Status::InvalidArgument("json path and json root cannot be both set");
    }

    if (range.__isset.jsonpaths) {
        RETURN_IF_ERROR(_parse_json_paths(range.jsonpaths, &_json_paths));
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
    Status status = _cur_file_reader->read_chunk(src_chunk.get(), _max_chunk_size);
    if (status.is_end_of_file()) {
        _cur_file_eof = true;
    }

    if (src_chunk->num_rows() == 0) {
        return Status::EndOfFile("EOF of reading json file, nothing read");
    }
    auto cast_chunk = _cast_chunk(src_chunk);
    return materialize(src_chunk, cast_chunk);
}

void JsonScanner::close() {}

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

            if (slot_type->type == TYPE_FLOAT || slot_type->type == TYPE_DOUBLE || slot_type->type == TYPE_BIGINT ||
                slot_type->type == TYPE_BIGINT || slot_type->type == TYPE_INT || slot_type->type == TYPE_SMALLINT ||
                slot_type->type == TYPE_TINYINT) {
                // Treat these types as what they are.
                child_type->children.emplace_back(slot_type->type);

            } else if (slot_type->type == TYPE_VARCHAR) {
                auto varchar_type = TypeDescriptor::create_varchar_type(slot_type->len);
                child_type->children.emplace_back(varchar_type);

            } else if (slot_type->type == TYPE_CHAR) {
                auto char_type = TypeDescriptor::create_char_type(slot_type->len);
                child_type->children.emplace_back(char_type);

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

Status JsonScanner::_parse_json_paths(const std::string& jsonpath,
                                      std::vector<std::vector<SimpleJsonPath>>* path_vecs) {
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

        // The columns in source chunk are all in NullableColumn type;
        auto col = ColumnHelper::create_column(_json_types[column_pos], true);
        (*chunk)->append_column(col, slot_desc->id());
    }

    return Status::OK();
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

ChunkPtr JsonScanner::_cast_chunk(const starrocks::vectorized::ChunkPtr& src_chunk) {
    SCOPED_RAW_TIMER(&_counter->cast_chunk_ns);
    ChunkPtr cast_chunk = std::make_shared<Chunk>();

    size_t slot_size = _src_slot_descriptors.size();
    for (int column_pos = 0; column_pos < slot_size; ++column_pos) {
        auto slot = _src_slot_descriptors[column_pos];
        if (slot == nullptr) {
            continue;
        }

        ColumnPtr col = _cast_exprs[column_pos]->evaluate(nullptr, src_chunk.get());
        col = ColumnHelper::unfold_const_column(slot->type(), src_chunk->num_rows(), col);
        cast_chunk->append_column(std::move(col), slot->id());
    }

    return cast_chunk;
}

JsonReader::JsonReader(starrocks::RuntimeState* state, starrocks::vectorized::ScannerCounter* counter,
                       JsonScanner* scanner, std::shared_ptr<SequentialFile> file, bool strict_mode,
                       const std::vector<SlotDescriptor*>& slot_descs)
        : _state(state),
          _counter(counter),
          _scanner(scanner),
          _strict_mode(strict_mode),
          _file(std::move(file)),
          _slot_descs(slot_descs) {
#if BE_TEST
    raw::RawVector<char> buf(_buf_size);
    std::swap(buf, _buf);
#endif
}

Status JsonReader::open() {
    RETURN_IF_ERROR(_read_and_parse_json());
    _empty_parser = false;

    if (_scanner->_json_paths.empty() && _scanner->_root_paths.empty()) {
        // Since the iterating order may be different with json field when the json path/root is set,
        // reorder the column descriptor as json field, when the json path and json root is empty.
        _reorder_column();
    }

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
    if (_empty_parser) {
        auto st = _read_and_parse_json();
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                return st;
            }
            // Parse error.
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", st.to_string());
            return st;
        }
    }

    // Eliminates virtual function call.
    if (!_scanner->_root_paths.empty()) {
        // With json root set, expand the outer array automatically.
        // The strip_outer_array determines whether to expand the sub-array of json root.
        if (_scanner->_strip_outer_array) {
            // Expand outer array automatically according to _is_ndjson.
            if (_is_ndjson) {
                return _read_chunk<ExpandedJsonDocumentStreamParserWithRoot>(chunk, rows_to_read);
            } else {
                return _read_chunk<ExpandedJsonArrayParserWithRoot>(chunk, rows_to_read);
            }
        } else {
            if (_is_ndjson) {
                return _read_chunk<JsonDocumentStreamParserWithRoot>(chunk, rows_to_read);
            } else {
                return _read_chunk<JsonArrayParserWithRoot>(chunk, rows_to_read);
            }
        }
    } else {
        // Without json root set, the strip_outer_array determines whether to expand outer array.
        if (_scanner->_strip_outer_array) {
            return _read_chunk<JsonArrayParser>(chunk, rows_to_read);
        } else {
            return _read_chunk<JsonDocumentStreamParser>(chunk, rows_to_read);
        }
    }
}

template <typename ParserType>
Status JsonReader::_read_chunk(Chunk* chunk, int32_t rows_to_read) {
    simdjson::ondemand::object row;

    auto parser = down_cast<ParserType*>(_parser.get());

    for (int32_t n = 0; n < rows_to_read; n++) {
        auto st = parser->get_current(&row);
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                _empty_parser = true;
                return Status::OK();
            }
            chunk->set_num_rows(n);
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", st.to_string());
            return st;
        }

        st = _construct_row(&row, chunk, _slot_descs);
        if (!st.ok()) {
            chunk->set_num_rows(n);
            if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
                // We would continue to construct row even if error is returned,
                // hence the number of error appended to the file should be limited.
                std::string_view sv;
                (void)!row.raw_json().get(sv);
                _state->append_error_msg_to_file(std::string(sv.data(), sv.size()), st.to_string());
                LOG(WARNING) << "failed to construct row: " << st;
            }
        }

        st = parser->advance();
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                _empty_parser = true;
                return Status::OK();
            }
            chunk->set_num_rows(n);
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", st.to_string());
            return st;
        }
    }
    return Status::OK();
}

Status JsonReader::_construct_row(simdjson::ondemand::object* row, Chunk* chunk,
                                  const std::vector<SlotDescriptor*>& slot_descs) {
    if (_scanner->_json_paths.empty()) {
        // No json path.

        for (SlotDescriptor* slot_desc : slot_descs) {
            if (slot_desc == nullptr) {
                continue;
            }

            // The columns in JsonReader's chunk are all in NullableColumn type;
            auto column = chunk->get_column_by_slot_id(slot_desc->id());
            const auto& col_name = slot_desc->col_name();

            try {
                simdjson::ondemand::value val = row->find_field_unordered(col_name);
                RETURN_IF_ERROR(_construct_column(val, column.get(), slot_desc->type(), slot_desc->col_name()));
            } catch (simdjson::simdjson_error& e) {
                if (col_name == "__op") {
                    // special treatment for __op column, fill default value '0' rather than null
                    if (column->is_binary()) {
                        column->append_strings(std::vector{Slice{"0"}});
                    } else {
                        column->append_datum(Datum((uint8_t)0));
                    }
                } else {
                    // Column name not found, fill column with null.
                    column->append_nulls(1);
                }
                continue;
            }
        }
        return Status::OK();
    } else {
        // With json path.

        size_t slot_size = slot_descs.size();
        size_t jsonpath_size = _scanner->_json_paths.size();
        for (size_t i = 0; i < slot_size; i++) {
            if (slot_descs[i] == nullptr) {
                continue;
            }
            const char* column_name = slot_descs[i]->col_name().c_str();

            // The columns in JsonReader's chunk are all in NullableColumn type;
            auto column = down_cast<NullableColumn*>(chunk->get_column_by_slot_id(slot_descs[i]->id()).get());
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

            simdjson::ondemand::value val;
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
                RETURN_IF_ERROR(add_nullable_column(column, slot_descs[i]->type(), slot_descs[i]->col_name(), row,
                                                    !_strict_mode));
            } else if (!JsonFunctions::extract_from_object(*row, _scanner->_json_paths[i], &val).ok()) {
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
                RETURN_IF_ERROR(_construct_column(val, column, slot_descs[i]->type(), slot_descs[i]->col_name()));
            }
        }
        return Status::OK();
    }
}

// Try to reorder the slot_descs as the key order in json document.
// Nothing would be done if got any error.
void JsonReader::_reorder_column() {
    // Build slot_desc_dict.
    std::unordered_map<std::string, SlotDescriptor*> slot_desc_dict;
    for (const auto& desc : _slot_descs) {
        if (desc == nullptr) {
            continue;
        }
        slot_desc_dict.emplace(desc->col_name(), desc);
    }

    std::vector<SlotDescriptor*> ordered_slot_descs;
    ordered_slot_descs.reserve(_slot_descs.size());

    // get the first row of json.
    simdjson::ondemand::object obj;
    if (!_parser->get_current(&obj).ok()) return;

    std::ostringstream oss;
    simdjson::ondemand::raw_json_string json_str;

    // Sort the column in the slot_descs as the key order in json document.
    for (auto field : obj) {
        try {
            json_str = field.key();
        } catch (simdjson::simdjson_error& e) {
            // Nothing would be done if got any error.
            return;
        }

        oss << json_str;
        auto key = oss.str();
        oss.str("");

        // Find the SlotDescriptor with the json document key.
        // Duplicated key in json would be skipped since the key has been erased before.
        auto itr = slot_desc_dict.find(key);

        // Swap the SlotDescriptor to the expected index.
        if (itr != slot_desc_dict.end()) {
            ordered_slot_descs.push_back(itr->second);
            // Erase the key from the dict.
            slot_desc_dict.erase(itr);
        }
    }

    // Append left key(s) in the dict to the ordered_slot_descs;
    for (const auto& kv : slot_desc_dict) {
        ordered_slot_descs.push_back(kv.second);
    }

    std::swap(ordered_slot_descs, _slot_descs);

    return;
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

    StreamPipeSequentialFile* stream_file = reinterpret_cast<StreamPipeSequentialFile*>(_file.get());
    // For efficiency reasons, simdjson requires a string with a few bytes (simdjson::SIMDJSON_PADDING) at the end.
    RETURN_IF_ERROR(stream_file->read_one_message(&_json_binary_ptr, &length, simdjson::SIMDJSON_PADDING));
    if (length == 0) {
        return Status::EndOfFile("EOF of reading file");
    }

    data = _json_binary_ptr.get();

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
            return Status::EndOfFile("illegal json started with " + data[i]);
        }
    }

    if (!_scanner->_root_paths.empty()) {
        // With json root set, expand the outer array automatically.
        // The strip_outer_array determines whether to expand the sub-array of json root.
        if (_scanner->_strip_outer_array) {
            // Expand outer array automatically according to _is_ndjson.
            if (_is_ndjson) {
                _parser.reset(new ExpandedJsonDocumentStreamParserWithRoot(_scanner->_root_paths));
            } else {
                _parser.reset(new ExpandedJsonArrayParserWithRoot(_scanner->_root_paths));
            }
        } else {
            if (_is_ndjson) {
                _parser.reset(new JsonDocumentStreamParserWithRoot(_scanner->_root_paths));
            } else {
                _parser.reset(new JsonArrayParserWithRoot(_scanner->_root_paths));
            }
        }
    } else {
        // Without json root set, the strip_outer_array determines whether to expand outer array.
        if (_scanner->_strip_outer_array) {
            _parser.reset(new JsonArrayParser);
        } else {
            _parser.reset(new JsonDocumentStreamParser);
        }
    }

    _empty_parser = false;
    return _parser->parse(data, length, length + simdjson::SIMDJSON_PADDING);
}

// _construct_column constructs column based on no value.
Status JsonReader::_construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                                     const std::string& col_name) {
    return add_nullable_column(column, type_desc, col_name, &value, !_strict_mode);
}

} // namespace starrocks::vectorized
