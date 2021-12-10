// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/json_scanner.h"

#include <fmt/compile.h>
#include <fmt/format.h>
#include <ryu/ryu.h>

#include <algorithm>
#include <sstream>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "env/env.h"
#include "exec/broker_reader.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/decimal_cast_expr.h"
#include "exprs/vectorized/json_functions.h"
#include "exprs/vectorized/unary_function.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/string_parser.hpp"
#include "formats/json/column.h"

namespace starrocks::vectorized {

static auto literal_0_slice{Slice{"0"}};
static auto literal_1_slice{Slice{"1"}};

// cast_int64 returns 1 when there's overflow happened in the cast.
template <typename T>
static inline int cast_int64(int64_t from, T* to) {
    if constexpr (std::is_same_v<T, int128_t>) {
        *to = static_cast<int128_t>(from);
        return 0;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        *to = from;
        return 0;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        if (from > INT32_MAX || from < INT32_MIN) return 1;
        *to = static_cast<int32_t>(from);
        return 0;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        if (from > INT16_MAX || from < INT16_MIN) return 1;
        *to = static_cast<int16_t>(from);
        return 0;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        if (from > INT8_MAX || from < INT8_MIN) return 1;
        *to = static_cast<int8_t>(from);
        return 0;
    }
    return 1;
}

// trim_quotes trims the first/last quote from the std::string_view.
static void trim_quotes(std::string_view &sv) {
    // Trim left.
    if (sv.empty()) return;

    if (sv[0] == '\"') sv.remove_prefix(1);

    if (sv.empty()) return;

    if (sv[sv.size() - 1] == '\"') sv.remove_suffix(1);
}

JsonScanner::JsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                         ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _next_range(0),
          _max_chunk_size(config::vector_chunk_size),
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
    Status status = _cur_file_reader->read_chunk(src_chunk.get(), _max_chunk_size, _src_slot_descriptors);
    if (status.is_end_of_file()) {
        _cur_file_eof = true;
    }

    if (src_chunk->num_rows() == 0) {
        return Status::EndOfFile("EOF of reading json file");
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
            if(slot_type->type == TYPE_VARCHAR)  {
                auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
                child_type->children.emplace_back(varchar_type);
            } else {
                child_type->children.emplace_back(slot_type->type);
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

Status JsonScanner::_parse_json_paths(const std::string& jsonpath, std::vector<std::vector<JsonPath>>* path_vecs) {
    simdjson::dom::parser parser;
    simdjson::dom::element elem;

    auto err = parser.parse(jsonpath.c_str(), jsonpath.length()).get(elem);

    if (err) {
        return Status::InvalidArgument(strings::Substitute("Invalid json path: $0, code $1, error: $2", jsonpath, err,
                                                           simdjson::error_message(err)));
    }

    simdjson::dom::array paths;
    err = elem.get_array().get(paths);
    if (err) {
        return Status::InvalidArgument(strings::Substitute("Invalid json path: $0, code $1, error: $2", jsonpath, err,
                                                           simdjson::error_message(err)));
    }

    for (const auto& path : paths) {
        if (!path.is_string()) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
        }

        std::vector<JsonPath> parsed_paths;
        const char* cstr;
        auto err = path.get_c_str().get(cstr);
        if (err) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0, code $1, error: $2", jsonpath,
                                                               err, simdjson::error_message(err)));
        }

        JsonFunctions::parse_json_paths(std::string(cstr), &parsed_paths);
        path_vecs->push_back(parsed_paths);
    }
    return Status::OK();
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
    _cur_file_reader = std::make_unique<JsonReader>(_state, _counter, this, file, _strict_mode);
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
                       JsonScanner* scanner, std::shared_ptr<SequentialFile> file, bool strict_mode)
        : _state(state),
          _counter(counter),
          _scanner(scanner),
          _strict_mode(strict_mode),
          _file(std::move(file)),
          _next_line(0),
          _total_lines(0),
          _closed(false) {
#if BE_TEST
    raw::RawVector<char> buf(_buf_size);
    std::swap(buf, _buf);
#endif
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
 *      colunm1    colunm2
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
 *      colunm1    colunm2
 *      ------------------
 *      value1     10
 *      value2     30
 */
Status JsonReader::read_chunk(Chunk* chunk, int32_t rows_to_read, const std::vector<SlotDescriptor*>& slot_descs) {
    simdjson::ondemand::object row;
    auto st = Status::OK();

    std::vector<SlotDescriptor*> reordered_slot_descs(slot_descs);
    for (int32_t n = 0; n < rows_to_read; n++) {
        st = _next_row();
        if (!st.ok()) {
            if (st.is_end_of_file()) {
                return st;
            }
            chunk->set_num_rows(n);
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", st.to_string());
            return st;
        }

        bool empty = false;
        st = _get_row(&row, &empty);
        if (!st.ok()) {
            chunk->set_num_rows(n);
            _counter->num_rows_filtered++;
            _state->append_error_msg_to_file("", st.to_string());
            return st;
        }

        if (empty) {
            // Empty row.
            continue;
        }

        if (n == 0 && _scanner->_json_paths.empty() && _scanner->_root_paths.empty()) {
            // Try to reorder the column according to the column order of first json row.
            // It is much faster when we access the json field as the json key order.
            _reorder_column(&reordered_slot_descs, row);
            row.reset();
        }

        st = _construct_row(&row, chunk, reordered_slot_descs);
        if (!st.ok()) {
            chunk->set_num_rows(n);
            _counter->num_rows_filtered++;
            row.reset();
            std::string_view sv;
            (void)!row.raw_json().get(sv);
            _state->append_error_msg_to_file(std::string(sv.data(), sv.size()), st.to_string());
            return st;
        }
    }
    return st;
}

Status JsonReader::_next_row() {
    // Try to forward array iterator.
    if (!_array_is_empty) {
        // _array_begin == array_end is always true in empty array.
        if (_array_begin != _array_end && ++_array_begin != _array_end) {
            return Status::OK();
        }
        _array_is_empty = true;
    }

    // Try to forward document stream iterator.
    if (!_stream_is_empty && _doc_stream_itr != _doc_stream.end()) {
        if (++_doc_stream_itr != _doc_stream.end()) {
            return Status::OK();
        }
        _stream_is_empty = true;
    }

    // Try to parse json.
    RETURN_IF_ERROR(_read_and_parse_json());
    return Status::OK();
}

Status JsonReader::_get_row(simdjson::ondemand::object* row, bool* empty) {
    if (!_array_is_empty) {
        // There's left object(s) in the array.
        return _get_row_from_array(row);
    } else if (!_stream_is_empty) {
        // There's left object(s) in the document_stream.
        return _get_row_from_document_stream(row, empty);
    }
    return Status::EndOfFile("EOF of reading file");
}

Status JsonReader::_get_row_from_array(simdjson::ondemand::object* row) {
    auto value = *_array_begin;

    simdjson::ondemand::json_type tp;
    auto err = value.type().get(tp);
    if (err) {
        std::string err_msg = strings::Substitute("Failed to determine json type. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    if (tp != simdjson::ondemand::json_type::object) {
        return Status::DataQualityError("nested array is not supported");
    }

    err = value.get_object().get(*row);
    if (err) {
        std::string err_msg = strings::Substitute("Failed to parse json as object. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }
    return Status::OK();
}

Status JsonReader::_get_row_from_document_stream(simdjson::ondemand::object* row, bool *empty) {
    simdjson::ondemand::document_reference doc;
    auto err = (*_doc_stream_itr).get(doc);
    if (err) {
        std::string err_msg = strings::Substitute("Failed to parse json as document. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    simdjson::ondemand::json_type tp;
    Status st;

    err = doc.type().get(tp);
    if (err) {
        std::string err_msg = strings::Substitute("Failed to determine json type. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    switch (tp) {
    case simdjson::ondemand::json_type::array: {
        if (!_scanner->_strip_outer_array) {
            std::string err_msg("JSON data is an array, strip_outer_array must be set true");
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }

        simdjson::ondemand::array arr;
        err = doc.get_array().get(arr);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to parse json as array. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        err = arr.is_empty().get(*empty);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to parse json as array. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        if (*empty) {
            return Status::OK();
        }

        err = arr.begin().get(_array_begin);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to parse json as array. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        err = arr.end().get(_array_end);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to parse json as array. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        _array_is_empty = false;

        st = _get_row_from_array(row);
        break;
    }

    case simdjson::ondemand::json_type::object: {
        if (_scanner->_strip_outer_array) {
            std::string err_msg("JSON data is an object, strip_outer_array must be set false");
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }

        err = doc.get_object().get(*row);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to decode json as object. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }
        st = Status::OK();
        break;
    }
    default:
        return Status::DataQualityError("Failed to decode json as array/object");
    }

    return st;
}

Status JsonReader::_construct_row(simdjson::ondemand::object* row, Chunk* chunk,
                                  const std::vector<SlotDescriptor*>& slot_descs) {
    RETURN_IF_ERROR(_filter_row_with_jsonroot(row));

    if (_scanner->_json_paths.empty()) {
        // No json path.

        for (SlotDescriptor* slot_desc : slot_descs) {
            if (slot_desc == nullptr) {
                continue;
            }

            // The columns in JsonReader's chunk are all in NullableColumn type;
            auto column = chunk->get_column_by_slot_id(slot_desc->id());
            auto col_name = slot_desc->col_name();

            simdjson::ondemand::value val;
            auto err = row->find_field_unordered(col_name).get(val);
            if (err) {
                if (col_name == "__op") {
                    // special treatment for __op column, fill default value '0' rather than null
                    _construct_string_column(column.get(), literal_0_slice);
                } else {
                    // Column name not found, fill column with null.
                    column->append_nulls(1);
                }
                continue;
            }

            RETURN_IF_ERROR(_construct_column(val, column.get(), slot_desc->type(), slot_desc->col_name()));
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

            // The columns in JsonReader's chunk are all in NullableColumn type;
            auto column = down_cast<NullableColumn*>(chunk->get_column_by_slot_id(slot_descs[i]->id()).get());
            if (i >= jsonpath_size) {
                column->append_nulls(1);
                continue;
            }

            simdjson::ondemand::value val;
            if (!JsonFunctions::extract_from_object(*row, _scanner->_json_paths[i], val)) {
                column->append_nulls(1);
            } else {
                RETURN_IF_ERROR(_construct_column(val, column, slot_descs[i]->type(), slot_descs[i]->col_name()));
            }
        }
        return Status::OK();
    }
}

// Try to reorder the slot_descs as the key order in json document.
// Nothing would be done if got any error.
void JsonReader::_reorder_column(std::vector<SlotDescriptor*>* slot_descs, simdjson::ondemand::object& obj) {
    // Build slot_desc_dict.
    std::unordered_map<std::string, SlotDescriptor*> slot_desc_dict;
    for (const auto& desc : *slot_descs) {
        if (desc == nullptr) {
            continue;
        }
        slot_desc_dict.emplace(desc->col_name(), desc);
    }

    std::vector<SlotDescriptor*> ordered_slot_descs;
    ordered_slot_descs.reserve(slot_descs->size());

    std::ostringstream oss;
    simdjson::ondemand::raw_json_string json_str;

    // Sort the column in the slot_descs as the key order in json document.
    for (auto field : obj) {
        auto err = field.key().get(json_str);
        if (err) {
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

    std::swap(ordered_slot_descs, *slot_descs);

    return;
}

Status JsonReader::_filter_row_with_jsonroot(simdjson::ondemand::object* row) {
    if (!_scanner->_root_paths.empty()) {
        // json root filter.
        simdjson::ondemand::value val;
        if (!JsonFunctions::extract_from_object(*row, _scanner->_root_paths, val)) {
            return Status::DataQualityError("illegal json root");
        }

        auto err = val.get_object().get(*row);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to filter row with jsonroot. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }
    }
    return Status::OK();
}

// read one json string from file read and parse it to json doc.
Status JsonReader::_read_and_parse_json() {
#ifdef BE_TEST
    [[maybe_unused]] size_t message_size = 0;
    Slice result(_buf.data(), _buf_size);
    RETURN_IF_ERROR(_file->read(&result));
    if (result.size == 0) {
        return Status::EndOfFile("EOF of reading file");
    }
    auto err = _parser.iterate_many(result.data, result.size, 32 * 1024 * 1024).get(_doc_stream);

#else
    size_t length = 0;
    StreamPipeSequentialFile* stream_file = reinterpret_cast<StreamPipeSequentialFile*>(_file.get());
    RETURN_IF_ERROR(stream_file->read_one_message(&_json_binary_ptr, &length));
    if (length == 0) {
        return Status::EndOfFile("EOF of reading file");
    }

    //TODO: Pre-allocate 200MB memory. We could process big array in stream.
    auto err = _parser.iterate_many(_json_binary_ptr.get(), length, 200 * 1024 * 1024).get(_doc_stream);
#endif

    if (err) {
        std::string err_msg = strings::Substitute("Failed to parse string to json. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    _doc_stream_itr = _doc_stream.begin();
    _stream_is_empty = false;

    return Status::OK();
}

// _construct_column constructs column based on no value.
Status JsonReader::_construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                                     const std::string& col_name) {
    simdjson::ondemand::json_type tp;
    auto err = value.type().get(tp);
    if (err) {
        std::string err_msg = strings::Substitute("Failed to determine the type of column value. code=$0, error=$1",
                                                  err, simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    if (type_desc.type == TYPE_FLOAT || type_desc.type == TYPE_DOUBLE || type_desc.type == TYPE_BIGINT ||
        type_desc.type == TYPE_INT || type_desc.type == TYPE_SMALLINT || type_desc.type == TYPE_TINYINT) {
        // Treat those types as what they are.

        switch (tp) {
        case simdjson::ondemand::json_type::null: {
            column->append_nulls(1);
            return Status::OK();
        }

        case simdjson::ondemand::json_type::number: {
            return _construct_column_with_numeric_value(value, column, type_desc, col_name);
        }

        case simdjson::ondemand::json_type::string: {
            return _construct_column_with_string_value(value, column, type_desc, col_name);
        }

        default: {
            std::string err_msg = strings::Substitute("Unable to cast the column value. column=$0", col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        }

    } else {
        // Treat other types as VARCHAR.
        switch (tp) {
        case simdjson::ondemand::json_type::null: {
            column->append_nulls(1);
            return Status::OK();
        }

        // Special treatment for bool.
        case simdjson::ondemand::json_type::boolean: {
            return _construct_column_with_boolean_value(value, column, type_desc, col_name);
        }

        // Special treatment for array.
        case simdjson::ondemand::json_type::array: {
            return _construct_column_with_array_value(value, column, type_desc, col_name);
        }

        // Special treatment for array.
        case simdjson::ondemand::json_type::string: {
            std::string_view sv;
            err = value.get_string().get(sv);
            if (err) {
                std::string err_msg = strings::Substitute("Unable to cast the column value. column=$0", col_name);
                return Status::DataQualityError(err_msg.c_str());
            }
            _construct_string_column(column, Slice{sv.data(), sv.size()});
            return Status::OK();
        }

        default: {
            auto sv = value.raw_json_token();
            _construct_string_column(column, Slice{sv.data(), sv.size()});
            return Status::OK();
        }
        }
    }
    return Status::OK();
}

Status JsonReader::_construct_column_with_boolean_value(simdjson::ondemand::value& value, Column* column,
                                                        const TypeDescriptor& type_desc, const std::string& col_name) {
    bool ok;
    auto err = value.get_bool().get(ok);
    if (UNLIKELY(err)) {
        std::string err_msg = strings::Substitute("Failed to parse the column value as bool. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    if (ok) {
        _construct_string_column(column, literal_1_slice);
    } else {
        _construct_string_column(column, literal_0_slice);
    }
    return Status::OK();
}

Status JsonReader::_construct_column_with_array_value(simdjson::ondemand::value& value, Column* column,
                                                      const TypeDescriptor& type_desc, const std::string& col_name) {
    if (type_desc.type == TYPE_ARRAY) {
        simdjson::ondemand::array arr;
        auto err = value.get_array().get(arr);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to parse json as array. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        size_t sz = 0;
        err = arr.count_elements().get(sz);
        if (err) {
            std::string err_msg =
                    strings::Substitute("Failed to parse the column value as array. code=$0, error=$1, column=$2", err,
                                        simdjson::error_message(err), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }

        auto null_column = down_cast<NullableColumn*>(column);
        auto array_column = down_cast<ArrayColumn*>(null_column->mutable_data_column());

        NullData& null_data = null_column->null_column_data();
        null_data.emplace_back(0);
        ColumnPtr& elements_column = array_column->elements_column();
        for(auto a : arr) {
            simdjson::ondemand::value val;
            err = a.get(val);
            if (err) {
                std::string err_msg =
                        strings::Substitute("Failed to parse the column value as array. code=$0, error=$1, column=$2",
                                            err, simdjson::error_message(err),col_name);
                return Status::DataQualityError(err_msg.c_str());
            }
            _construct_column(val, elements_column.get(), type_desc.children[0], col_name);
        }
        auto offsets = array_column->offsets_column();
        uint32_t size = offsets->get_data().back() + sz;
        offsets->append_numbers(&size, 4);

    } else {
        auto sv = value.raw_json_token();
        _construct_string_column(column, Slice{sv.data(), sv.size()});
    }

    return Status::OK();
}

Status JsonReader::_construct_column_with_numeric_value(simdjson::ondemand::value& value, Column* column,
                                                        const TypeDescriptor& type_desc, const std::string& col_name) {
    simdjson::ondemand::number_type tp;

    auto err = value.get_number_type().get(tp);
    if (UNLIKELY(err)) {
        std::string err_msg =
                strings::Substitute("Failed to determine the numeric column value type. code=$0, error=$1", err,
                                    simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    if (tp == simdjson::ondemand::number_type::signed_integer) {
        int64_t i64 = 0;
        err = value.get_int64().get(i64);
        if (UNLIKELY(err)) {
            std::string err_msg =
                    strings::Substitute("Failed to parse the numeric column value as int64. code=$0, error=$1", err,
                                        simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        switch (type_desc.type) {
        case TYPE_BIGINT: {
            _construct_numeric_column(column, i64);
            return Status::OK();
        }

        case TYPE_INT: {
            int32_t i32 = 0;
            if (cast_int64(i64, &i32) != 0) {
                std::string err_msg = strings::Substitute(
                        "Overflow in cast the integer value as INT. value=$0, column=$1", i64, col_name);
                return Status::DataQualityError(err_msg.c_str());
            }
            _construct_numeric_column(column, i32);
            return Status::OK();
        }

        case TYPE_SMALLINT: {
            int16_t i16 = 0;
            if (cast_int64(i64, &i16) != 0) {
                std::string err_msg = strings::Substitute(
                        "Overflow in cast the integer value as SMALLINT. value=$0, column=$1", i64, col_name);
                return Status::DataQualityError(err_msg.c_str());
            }
            _construct_numeric_column(column, i16);
            return Status::OK();
        }

        case TYPE_TINYINT: {
            int8_t i8 = 0;
            if (cast_int64(i64, &i8) != 0) {
                std::string err_msg = strings::Substitute(
                        "Overflow in cast the integer value as TINYINT. value=$0, column=$1", i64, col_name);
                return Status::DataQualityError(err_msg.c_str());
            }
            _construct_numeric_column(column, i8);
            return Status::OK();
        }

        // Float column, integer value.
        case TYPE_FLOAT: {
            auto f = static_cast<float>(i64);
            _construct_numeric_column(column, f);
            return Status::OK();
        }

        // Double column, integer value.
        case TYPE_DOUBLE: {
            auto d = static_cast<double>(i64);
            _construct_numeric_column(column, d);
            return Status::OK();
        }

        default:
            break;
        }

    } else if (tp == simdjson::ondemand::number_type::floating_point_number) {
        double d = 0;
        err = value.get_double().get(d);
        if (UNLIKELY(err)) {
            std::string err_msg =
                    strings::Substitute("Failed to parse the numeric column value as double. code=$0, error=$1", err,
                                        simdjson::error_message(err));
            return Status::DataQualityError(err_msg.c_str());
        }

        switch (type_desc.type) {
        // Double column, float value.
        case TYPE_DOUBLE: {
            _construct_numeric_column(column, d);
            return Status::OK();
        }

        // Float column, float value.
        case TYPE_FLOAT: {
            auto f = static_cast<float>(d);
            _construct_numeric_column(column, f);
            return Status::OK();
        }

        case TYPE_BIGINT: {
            auto i64 = static_cast<int64_t>(d);
            _construct_numeric_column(column, i64);
            return Status::OK();
        }

        case TYPE_INT: {
            auto i32 = static_cast<int32_t>(d);
            _construct_numeric_column(column, i32);
            return Status::OK();
        }

        case TYPE_SMALLINT: {
            auto i16 = static_cast<int16_t>(d);
            _construct_numeric_column(column, i16);
            return Status::OK();
        }

        case TYPE_TINYINT: {
            auto i8 = static_cast<int8_t>(d);
            _construct_numeric_column(column, i8);
            return Status::OK();
        }

        default:
            break;
        }

    }

    if(_strict_mode) {
        auto sv = value.raw_json_token();
        std::string err_msg = strings::Substitute("unsupported value type. value=$0, column=$1",
                                                  std::string(sv.data(), sv.size()), col_name);
        return Status::DataQualityError(err_msg.c_str());
    }

    column->append_nulls(1);
    return Status::OK();
}

Status JsonReader::_construct_column_with_string_value(simdjson::ondemand::value& value, Column* column,
                                                       const TypeDescriptor& type_desc, const std::string& col_name) {
    std::string_view sv;
    auto err = value.get_string().get(sv);
    if (UNLIKELY(err)) {
        std::string err_msg = strings::Substitute("Failed to parse the string column value. code=$0, error=$1", err,
                                                  simdjson::error_message(err));
        return Status::DataQualityError(err_msg.c_str());
    }

    switch (type_desc.type) {
    case TYPE_BIGINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto i64 = StringParser::string_to_int<int64_t>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            _construct_numeric_column(column, i64);
        } else {
            // Attemp to parse the string as float.
            auto d = StringParser::string_to_float<double>(sv.data(), sv.length(), &parse_result);
            if (parse_result == StringParser::PARSE_SUCCESS) {
                i64 = static_cast<int64_t>(d);
                _construct_numeric_column(column, i64);
            }

            std::string err_msg = strings::Substitute("Unable to cast string value to BIGINT. value=$0, column=$1",
                                                      std::string(sv.data(), sv.size()), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        return Status::OK();
    }

    case TYPE_INT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto i32 = StringParser::string_to_int<int32_t>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            _construct_numeric_column(column, i32);
        } else {
            // Attemp to parse the string as float.
            auto d = StringParser::string_to_float<double>(sv.data(), sv.length(), &parse_result);
            if (parse_result == StringParser::PARSE_SUCCESS) {
                i32 = static_cast<int32_t>(d);
                _construct_numeric_column(column, i32);
            }

            std::string err_msg = strings::Substitute("Unable to cast string value to INT. value=$0, column=$1",
                                                      std::string(sv.data(), sv.size()), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        return Status::OK();
    }

    case TYPE_SMALLINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto i16 = StringParser::string_to_int<int16_t>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            _construct_numeric_column(column, i16);
        } else {
            // Attemp to parse the string as float.
            auto d = StringParser::string_to_float<double>(sv.data(), sv.length(), &parse_result);
            if (parse_result == StringParser::PARSE_SUCCESS) {
                i16 = static_cast<int16_t>(d);
                _construct_numeric_column(column, i16);
            }

            std::string err_msg = strings::Substitute("Unable to cast string value to SMALLINT. value=$0, column=$1",
                                                      std::string(sv.data(), sv.size()), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        return Status::OK();
    }

    case TYPE_TINYINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto i8 = StringParser::string_to_int<int8_t>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            _construct_numeric_column(column, i8);
        } else {
            // Attemp to parse the string as float.
            auto d = StringParser::string_to_float<double>(sv.data(), sv.length(), &parse_result);
            if (parse_result == StringParser::PARSE_SUCCESS) {
                i8 = static_cast<int16_t>(d);
                _construct_numeric_column(column, i8);
            }

            std::string err_msg = strings::Substitute("Unable to cast string value to TINYINT. value=$0, column=$1",
                                                      std::string(sv.data(), sv.size()), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        return Status::OK();
    }

    case TYPE_FLOAT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto f = StringParser::string_to_float<float>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            _construct_numeric_column(column, f);
        } else {
            std::string err_msg = strings::Substitute("Unable to cast string value to FLOAT. value=$0, column=$1",
                                                      std::string(sv.data(), sv.size()), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        return Status::OK();
    }

    case TYPE_DOUBLE: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        auto d = StringParser::string_to_float<double>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            _construct_numeric_column(column, d);
        } else {
            std::string err_msg = strings::Substitute("Unable to cast string value to DOUBLE. value=$0, column=$1",
                                                      std::string(sv.data(), sv.size()), col_name);
            return Status::DataQualityError(err_msg.c_str());
        }
        return Status::OK();
    }

    // Treat all other types as VARCHAR.
    default: {
        _construct_string_column(column, Slice{sv.data(), sv.size()});
        return Status::OK();
    }
    }
}

void JsonReader::_construct_string_column(Column* col, const Slice& s) {
    auto column = down_cast<NullableColumn*>(col);
    auto& data_column = column->data_column();
    auto& null_column = column->null_column();

    down_cast<BinaryColumn*>(data_column.get())->append(s);
    null_column->append(0);
}

// Explicit template Instantiation.
template void JsonReader::_construct_numeric_column(Column* col, const int128_t& val);
template void JsonReader::_construct_numeric_column(Column* col, const int64_t& val);
template void JsonReader::_construct_numeric_column(Column* col, const int32_t& val);
template void JsonReader::_construct_numeric_column(Column* col, const int16_t& val);
template void JsonReader::_construct_numeric_column(Column* col, const int8_t& val);
template void JsonReader::_construct_numeric_column(Column* col, const double& val);
template void JsonReader::_construct_numeric_column(Column* col, const float& val);

template <typename T>
void JsonReader::_construct_numeric_column(Column* col, const T& val) {
    auto nullable_column = down_cast<NullableColumn*>(col);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    auto fixed_length_column = down_cast<FixedLengthColumn<T>*>(data_column.get());

    fixed_length_column->append_numbers(&val, sizeof(val));

    null_column->append(0);
}

} // namespace starrocks::vectorized
