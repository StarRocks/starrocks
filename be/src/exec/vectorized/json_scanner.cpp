// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/json_scanner.h"

#include <fmt/compile.h>
#include <fmt/format.h>
#include <ryu/ryu.h>

#include <algorithm>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
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

namespace starrocks::vectorized {

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
    if (range.__isset.jsonpaths) {
        RETURN_IF_ERROR(_parse_json_paths(range.jsonpaths, &_json_paths));
    }
    if (range.__isset.json_root) {
        return Status::InvalidArgument(strings::Substitute("Use json path to instead json root"));
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

        if (slot_desc->type().type == TYPE_ARRAY) {
            TypeDescriptor json_type(TYPE_ARRAY);
            TypeDescriptor* child_type = &json_type;

            const TypeDescriptor* slot_type = &(slot_desc->type().children[0]);
            while (slot_type->type == TYPE_ARRAY) {
                slot_type = &(slot_type->children[0]);

                child_type->children.emplace_back(TYPE_ARRAY);
                child_type = &(child_type->children[0]);
            }
            auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            child_type->children.emplace_back(varchar_type);

            _json_types[column_pos] = std::move(json_type);
        } else {
            auto varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            _json_types[column_pos] = std::move(varchar_type);
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

    if (err || !elem.is_array()) {
        return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
    }

    simdjson::dom::array paths;
    err = elem.get_array().get(paths);
    if (err) {
        return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
    }

    for (auto path : paths) {
        if (!path.is_string()) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
        }
        std::vector<JsonPath> parsed_paths;
        const char* cstr;
        auto err = path.get_c_str().get(cstr);
        if (err) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
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
    _cur_file_reader = std::make_unique<JsonReader>(_state, _counter, this, file);
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
                       JsonScanner* scanner, std::shared_ptr<SequentialFile> file)
        : _state(state),
          _counter(counter),
          _scanner(scanner),
          _file(std::move(file)),
          _next_line(0),
          _total_lines(0),
          _closed(false) {
    _doc_stream_itr = _doc_stream.end();
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
    // Only operator != is impletement for simdjson::ondemand::document_stream::iterator.
    if (!(_doc_stream_itr != _doc_stream.end())) {
        RETURN_IF_ERROR(_read_and_parse_json());
    }

    for (; _doc_stream_itr != _doc_stream.end() && rows_to_read > 0; ++_doc_stream_itr, --rows_to_read) {
        simdjson::ondemand::document_reference doc;
        auto err = (*_doc_stream_itr).get(doc);
        if (err) {
            std::string err_msg = strings::Substitute("Failed to parse string to json. code=$0, error=$1", err,
                                                      simdjson::error_message(err));
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }

        simdjson::ondemand::json_type tp;
        doc.type().get(tp);

        switch (tp) {
        case simdjson::ondemand::json_type::array: {
            // Expand array.
            if (!_scanner->_strip_outer_array) {
                std::string err_msg("JSON data is an array, strip_outer_array must be set true");
                _state->append_error_msg_to_file(JsonFunctions::minify_json_to_string(doc), err_msg);
                _counter->num_rows_filtered++;
                return Status::DataQualityError(err_msg.c_str());
            }

            simdjson::ondemand::array arr;
            err = doc.get_array().get(arr);
            if (err) {
                std::string err_msg = strings::Substitute("Failed to parse string to json as array. code=$0, error=$1", err,
                                                          simdjson::error_message(err));
                _state->append_error_msg_to_file(JsonFunctions::minify_json_to_string(doc), err_msg);
                _counter->num_rows_filtered++;
                return Status::DataQualityError(err_msg.c_str());
            }

            if (_scanner->_json_paths.empty()) {
                RETURN_IF_ERROR(_process_array(chunk, slot_descs, arr));
            } else {
                RETURN_IF_ERROR(_process_array_with_json_path(chunk, slot_descs, arr));
            }
            break;
        }

        case simdjson::ondemand::json_type::object: {
            if (_scanner->_strip_outer_array) {
                std::string err_msg("JSON data is an object, strip_outer_array must be set false");
                _state->append_error_msg_to_file(JsonFunctions::minify_json_to_string(doc), err_msg);
                _counter->num_rows_filtered++;
                return Status::DataQualityError(err_msg.c_str());
            }

            simdjson::ondemand::object obj;

            err = doc.get_object().get(obj);
            if (err) {
                std::string err_msg = strings::Substitute("Failed to parse string to json as object. code=$0, error=$1", err,
                                                          simdjson::error_message(err));
                _state->append_error_msg_to_file(JsonFunctions::minify_json_to_string(doc), err_msg);
                _counter->num_rows_filtered++;
                return Status::DataQualityError(err_msg.c_str());
            }

            if (_scanner->_json_paths.empty()) {
                RETURN_IF_ERROR(_process_object(chunk, slot_descs, obj));
            } else {
                RETURN_IF_ERROR(_process_object_with_json_path(chunk, slot_descs, obj));
            }
            break;
        }

        default: {
            std::string err_msg("JSON data type is not supported");
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }
        }
    }
    return Status::OK();
}

Status JsonReader::_process_array(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                                  simdjson::ondemand::array& arr) {
    for (auto a : arr) {
        if (a.error()) {
            std::string err_msg = strings::Substitute("Failed to parse json in array. code=$0, error=$1", a.error(),
                                                      simdjson::error_message(a.error()));
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }

        simdjson::ondemand::json_type tp;
        a.type().get(tp);

        if (tp == simdjson::ondemand::json_type::object) {
            simdjson::ondemand::object obj;
            auto err = a.get_object().get(obj);
            if (err) {
                std::string err_msg = strings::Substitute("Failed to parse json in array. code=$0, error=$1", a.error(),
                                                          simdjson::error_message(a.error()));
                _state->append_error_msg_to_file("", err_msg);
                _counter->num_rows_filtered++;
                return Status::DataQualityError(err_msg.c_str());
            }
            RETURN_IF_ERROR(_process_object(chunk, slot_descs, obj));
        } else {
            std::string err_msg = strings::Substitute("Failed to parse json in array. code=$0, error=$1", a.error(),
                                                      simdjson::error_message(a.error()));
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }
    }
    return Status::OK();
}

Status JsonReader::_process_array_with_json_path(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                                                 simdjson::ondemand::array& arr) {
    for (auto a : arr) {
        if (a.error()) {
            std::string err_msg = strings::Substitute("Failed to parse json in array with jsonpath. code=$0, error=$1", a.error(),
                                                      simdjson::error_message(a.error()));
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }

        simdjson::ondemand::json_type tp;
        a.type().get(tp);

        if (tp == simdjson::ondemand::json_type::object) {
            simdjson::ondemand::object obj;
            auto err = a.get_object().get(obj);
            if (err) {
                std::string err_msg = strings::Substitute("Failed to parse json in array. code=$0, error=$1", a.error(),
                                                          simdjson::error_message(a.error()));
                _state->append_error_msg_to_file("", err_msg);
                _counter->num_rows_filtered++;
                return Status::DataQualityError(err_msg.c_str());
            }
            RETURN_IF_ERROR(_process_object_with_json_path(chunk, slot_descs, obj));
        } else {
            std::string err_msg = strings::Substitute("Failed to parse json in array with jsonpath. code=$0, error=$1", a.error(),
                                                      simdjson::error_message(a.error()));
            _state->append_error_msg_to_file("", err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }
    }
    return Status::OK();
}

Status JsonReader::_process_object(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                                   simdjson::ondemand::object& obj) {
    for (SlotDescriptor* slot_desc : slot_descs) {
        if (slot_desc == nullptr) {
            continue;
        }

        ColumnPtr& column = chunk->get_column_by_slot_id(slot_desc->id());
        auto col_name = slot_desc->col_name();

        simdjson::ondemand::value val;
        auto err = obj.find_field_unordered(col_name).get(val);
        if (err) {
            column->append_nulls(1);
            continue;
        }


        _construct_column(val, column.get(), slot_desc->type());
    }
    return Status::OK();
}

Status JsonReader::_process_object_with_json_path(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                                                  simdjson::ondemand::object& obj) {
    size_t slot_size = slot_descs.size();
    size_t jsonpath_size = _scanner->_json_paths.size();
    for (size_t i = 0; i < slot_size; i++) {
        if (slot_descs[i] == nullptr) {
            continue;
        }

        ColumnPtr& column = chunk->get_column_by_slot_id(slot_descs[i]->id());
        if (i >= jsonpath_size) {
            column->append_nulls(1);
            continue;
        }

        simdjson::ondemand::value val;
        if (!JsonFunctions::extract_from_object(obj, _scanner->_json_paths[i], val)) {
            column->append_nulls(1);
        } else {
            _construct_column(val, column.get(), slot_descs[i]->type());
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
        _state->append_error_msg_to_file("", err_msg);
        _counter->num_rows_filtered++;
        return Status::DataQualityError(err_msg.c_str());
    }

    _doc_stream_itr = _doc_stream.begin();

    return Status::OK();
}

// _construct_column constructs column based on no value.
void JsonReader::_construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc) {
    simdjson::ondemand::json_type tp;
    value.type().get(tp);

    switch (tp) {
    case simdjson::ondemand::json_type::null: {
        column->append_nulls(1);
        break;
    }

    case simdjson::ondemand::json_type::boolean: {
        bool ok;
        auto err = value.get_bool().get(ok);
        if (UNLIKELY(err)) {
            column->append_nulls(1);
            break;
        }

        if (ok) {
            column->append_strings(std::vector<Slice>{Slice("1")});
        } else {
            column->append_strings(std::vector<Slice>{Slice("0")});
        }
        break;
    }

    case simdjson::ondemand::json_type::number: {
        simdjson::ondemand::number_type tp;

        auto err = value.get_number_type().get(tp);
        if (UNLIKELY(err)) {
            column->append_nulls(1);
            break;
        }

        switch (tp) {
        case simdjson::ondemand::number_type::signed_integer: {
            int64_t i64;
            err = value.get_int64().get(i64);
            if (UNLIKELY(err)) {
                column->append_nulls(1);
                break;
            }

            auto f = fmt::format_int(i64);
            column->append_strings(std::vector<Slice>{Slice(f.data(), f.size())});
            break;
        }

        case simdjson::ondemand::number_type::unsigned_integer: {
            uint64_t u64;
            err = value.get_uint64().get(u64);
            if (UNLIKELY(err)) {
                column->append_nulls(1);
                break;
            }

            auto f = fmt::format_int(u64);
            column->append_strings(std::vector<Slice>{Slice(f.data(), f.size())});
            break;
        }

        case simdjson::ondemand::number_type::floating_point_number: {
            std::string_view sv;
            err = simdjson::to_json_string(value).get(sv);
            if (UNLIKELY(err)) {
                column->append_nulls(1);
                break;
            }

            column->append_strings(std::vector<Slice>{Slice{sv.data(), sv.size()}});
            break;
        }

        default: {
            column->append_nulls(1);
            break;
        }
        }
        break;
    }

    case simdjson::ondemand::json_type::string: {

        std::string_view sv;
        auto err = value.get_string().get(sv);
        if (UNLIKELY(err)) {
            column->append_nulls(1);
            break;
        }

        column->append_strings(std::vector<Slice>{Slice(sv.data(), sv.length())});
        break;
    }

    case simdjson::ondemand::json_type::object:
    case simdjson::ondemand::json_type::array: {
        std::unique_ptr<char[]> buf;
        size_t buflen{};
        JsonFunctions::minify_json_to_string(value, buf, buflen);

        column->append_strings(std::vector<Slice>{Slice{buf.get(), buflen}});
        break;
    }

    default: {
        column->append_nulls(1);
        break;
    }
    }
}

} // namespace starrocks::vectorized
