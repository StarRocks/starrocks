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

static std::vector<Slice> literal_0_slice_vector{Slice("0")};
static std::vector<Slice> literal_1_slice_vector{Slice("1")};

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

    // Read until we get a non-empty chunk.
    do {
        RETURN_IF_ERROR(_create_src_chunk(&src_chunk));

        if (_cur_file_eof) {
            // If all readers have been read, an EOF would be returned.
            RETURN_IF_ERROR(_open_next_reader());
            _cur_file_eof = false;
        }
        Status status = _cur_file_reader->read_chunk(src_chunk.get(), _max_chunk_size, _src_slot_descriptors);
        if (!status.ok()) {
            if (status.is_end_of_file()) {
                // Set _cur_file_eof to open a new reader.
                _cur_file_eof = true;
            } else if (status.is_data_quality_error()) {
                // To read all readers, we just log and ignore the data quality error returned by read_chunk.
                if (++_error_chunk_num <= _kMaxErrorChunkNum) {
                    LOG(WARNING) << "read chunk failed: : " << status;
                }
            } else {
                return status;
            }
        }

    } while (src_chunk->num_rows() == 0);

    // Materialize non-empty chunk.
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
    rapidjson::Document doc;
    doc.Parse(jsonpath.c_str(), jsonpath.length());
    if (doc.HasParseError() || !doc.IsArray()) {
        return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
    }
    for (int i = 0; i < doc.Size(); i++) {
        const rapidjson::Value& path = doc[i];
        if (!path.IsString()) {
            return Status::InvalidArgument(strings::Substitute("Invalid json path: $0", jsonpath));
        }
        std::vector<JsonPath> parsed_paths;
        JsonFunctions::parse_json_paths(path.GetString(), &parsed_paths);
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
          _closed(false),
          _buf(_buf_size) {}

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
    Status st = Status::OK();
    do {
        if (_next_line >= _total_lines) {
            RETURN_IF_ERROR(_read_and_parse_json());
            _next_line = 0;
        }
        _total_lines = (_json_doc->IsArray()) ? _json_doc->Size() : 1;
        while (_next_line < _total_lines && rows_to_read > 0) {
            rapidjson::Value* objectValue = _json_doc;
            if (_json_doc->IsArray()) {
                objectValue = &(*_json_doc)[_next_line];
            }
            if (_scanner->_json_paths.empty()) {
                for (SlotDescriptor* slot_desc : slot_descs) {
                    if (slot_desc == nullptr) {
                        continue;
                    }
                    ColumnPtr& column = chunk->get_column_by_slot_id(slot_desc->id());
                    const char* column_name = slot_desc->col_name().c_str();
                    if (!objectValue->IsObject() || !objectValue->HasMember(column_name)) {
                        if (strcmp(column_name, "__op") == 0) {
                            // special treatment for __op column, fill default value '0' rather than null
                            column->append_strings(literal_0_slice_vector);
                        } else {
                            column->append_nulls(1);
                        }
                    } else {
                        _construct_column((*objectValue)[column_name], column.get(), slot_desc->type());
                    }
                }
            } else {
                size_t slot_size = slot_descs.size();
                size_t jsonpath_size = _scanner->_json_paths.size();
                for (size_t i = 0; i < slot_size; i++) {
                    if (slot_descs[i] == nullptr) {
                        continue;
                    }
                    const char* column_name = slot_descs[i]->col_name().c_str();
                    ColumnPtr& column = chunk->get_column_by_slot_id(slot_descs[i]->id());
                    if (i >= jsonpath_size) {
                        if (strcmp(column_name, "__op") == 0) {
                            // special treatment for __op column, fill default value '0' rather than null
                            column->append_strings(literal_0_slice_vector);
                        } else {
                            column->append_nulls(1);
                        }
                        continue;
                    }
                    rapidjson::Value* json_values = JsonFunctions::get_json_object_from_parsed_json(
                            _scanner->_json_paths[i], objectValue, _origin_json_doc.GetAllocator());
                    if (strcmp(column_name, "__op") == 0) {
                        // special treatment for __op column, fill default value '0' rather than null
                        if (json_values != nullptr && json_values->IsInt() && json_values->GetInt() == 1) {
                            column->append_strings(literal_1_slice_vector);
                        } else {
                            column->append_strings(literal_0_slice_vector);
                        }
                    } else {
                        if (json_values == nullptr) {
                            column->append_nulls(1);
                        } else {
                            _construct_column(*json_values, column.get(), slot_descs[i]->type());
                        }
                    }
                }
            }
            rows_to_read--;
            _next_line++;
        }
    } while (rows_to_read > 0);
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
    _origin_json_doc.Parse(result.data, result.size);
#else
    std::unique_ptr<uint8_t[]> json_binary = nullptr;
    size_t length = 0;
    StreamPipeSequentialFile* stream_file = reinterpret_cast<StreamPipeSequentialFile*>(_file.get());

    // read until a non-empty message is returned.
    do {
        SCOPED_RAW_TIMER(&_counter->file_read_ns);
        // If all messages have been read, an EOF is returned.
        RETURN_IF_ERROR(stream_file->read_one_message(&json_binary, &length));
    } while (length == 0);

    _origin_json_doc.Parse((char*)json_binary.get(), length);
#endif

    if (_origin_json_doc.HasParseError()) {
        std::string err_msg = strings::Substitute("Failed to parse string to json. code=$0, error=$1",
                                                  _origin_json_doc.GetParseError(),
                                                  rapidjson::GetParseError_En(_origin_json_doc.GetParseError()));
        _state->append_error_msg_to_file(JsonFunctions::get_raw_json_string(_origin_json_doc), err_msg);
        _counter->num_rows_filtered++;
        return Status::DataQualityError(err_msg.c_str());
    }

    _json_doc = &_origin_json_doc;
    if (!_scanner->_root_paths.empty()) {
        _json_doc = JsonFunctions::get_json_object_from_parsed_json(_scanner->_root_paths, &_origin_json_doc,
                                                                    _origin_json_doc.GetAllocator());
        if (_json_doc == nullptr) {
            std::string err_msg("Root is not valid");
            _state->append_error_msg_to_file(JsonFunctions::get_raw_json_string(_origin_json_doc), err_msg);
            _counter->num_rows_filtered++;
            return Status::DataQualityError(err_msg.c_str());
        }
    }

    if (_json_doc->IsArray() && !_scanner->_strip_outer_array) {
        std::string err_msg("JSON data is an array, strip_outer_array must be set true");
        _state->append_error_msg_to_file(JsonFunctions::get_raw_json_string(_origin_json_doc), err_msg);
        _counter->num_rows_filtered++;
        return Status::DataQualityError(err_msg.c_str());
    }

    if (!_json_doc->IsArray() && _scanner->_strip_outer_array) {
        std::string err_msg("JSON data is not an arrayobject, strip_outer_array must be set false");
        _state->append_error_msg_to_file(JsonFunctions::get_raw_json_string(_origin_json_doc), err_msg);
        _counter->num_rows_filtered++;
        return Status::DataQualityError(err_msg.c_str());
    }

    return Status::OK();
}

void JsonReader::_construct_column(const rapidjson::Value& objectValue, Column* column,
                                   const TypeDescriptor& type_desc) {
    if (objectValue.GetType() != rapidjson::kArrayType && type_desc.type == TYPE_ARRAY) {
        column->append_nulls(1);
        return;
    }

    char buf[64] = {0};
    switch (objectValue.GetType()) {
    case rapidjson::Type::kNullType: {
        column->append_nulls(1);
        break;
    }
    case rapidjson::Type::kFalseType: {
        column->append_strings(literal_0_slice_vector);
        break;
    }
    case rapidjson::Type::kTrueType: {
        column->append_strings(literal_1_slice_vector);
        break;
    }
    case rapidjson::Type::kNumberType: {
        if (objectValue.IsUint()) {
            auto f = fmt::format_int(objectValue.GetUint());
            column->append_strings(std::vector<Slice>{Slice(f.data(), f.size())});
        } else if (objectValue.IsInt()) {
            auto f = fmt::format_int(objectValue.GetInt());
            column->append_strings(std::vector<Slice>{Slice(f.data(), f.size())});
        } else if (objectValue.IsUint64()) {
            auto f = fmt::format_int(objectValue.GetUint64());
            column->append_strings(std::vector<Slice>{Slice(f.data(), f.size())});
        } else if (objectValue.IsInt64()) {
            auto f = fmt::format_int(objectValue.GetInt64());
            column->append_strings(std::vector<Slice>{Slice(f.data(), f.size())});
        } else {
            int len = d2s_buffered_n(objectValue.GetDouble(), buf);
            column->append_strings(std::vector<Slice>{Slice(buf, len)});
        }
        break;
    }
    case rapidjson::Type::kStringType: {
        const char* str_value = objectValue.GetString();
        column->append_strings(std::vector<Slice>{Slice(str_value, objectValue.GetStringLength())});
        break;
    }
    case rapidjson::Type::kArrayType: {
        if (type_desc.type == TYPE_ARRAY) {
            auto null_column = down_cast<NullableColumn*>(column);
            auto array_column = down_cast<ArrayColumn*>(null_column->mutable_data_column());

            NullData& null_data = null_column->null_column_data();
            null_data.emplace_back(0);
            ColumnPtr& elements_column = array_column->elements_column();
            for (size_t i = 0; i < objectValue.Size(); ++i) {
                _construct_column(objectValue[i], elements_column.get(), type_desc.children[0]);
            }
            auto offsets = array_column->offsets_column();
            uint32_t size = offsets->get_data().back() + objectValue.Size();
            offsets->append_numbers(&size, 4);
        } else {
            std::string json_str = JsonFunctions::get_raw_json_string(objectValue);
            column->append_strings(std::vector<Slice>{Slice(json_str.c_str(), json_str.length())});
        }
        break;
    }
    case rapidjson::Type::kObjectType: {
        std::string json_str = JsonFunctions::get_raw_json_string(objectValue);
        column->append_strings(std::vector<Slice>{Slice(json_str.c_str(), json_str.length())});
        break;
    }
    }
}

} // namespace starrocks::vectorized
