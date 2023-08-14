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

#include "exec/es/es_scroll_parser.h"

#include <fmt/format.h>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "util/timezone_utils.h"

namespace starrocks {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_FIELDS = "fields";
static const char* FIELD_ID = "_id";

const char* json_type_to_raw_str(rapidjson::Type type) {
    switch (type) {
    case rapidjson::kNumberType:
        return "Number";
    case rapidjson::kStringType:
        return "Varchar/Char";
    case rapidjson::kArrayType:
        return "Array";
    case rapidjson::kObjectType:
        return "Object";
    case rapidjson::kNullType:
        return "Null Type";
    case rapidjson::kFalseType:
    case rapidjson::kTrueType:
        return "True/False";
    default:
        return "Unknown Type";
    }
}

std::string json_value_to_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> temp_writer(scratch_buffer);
    value.Accept(temp_writer);
    return scratch_buffer.GetString();
}

#define RETURN_NULL_IF_STR_EMPTY(col, column)                                          \
    do {                                                                               \
        const std::string& null_str = json_value_to_string(col);                       \
        if ((null_str.length() <= 0 || "\"\"" == null_str) && column->is_nullable()) { \
            _append_null(column);                                                      \
            return Status::OK();                                                       \
        }                                                                              \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type)                               \
    do {                                                                      \
        if (col.IsArray()) {                                                  \
            std::stringstream ss;                                             \
            ss << "Expected value of type: " << type_to_string(type)          \
               << "; but found type: " << json_type_to_raw_str(col.GetType()) \
               << "; Document slice is : " << json_value_to_string(col);      \
            return Status::RuntimeError(ss.str());                            \
        }                                                                     \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type)                            \
    do {                                                                        \
        if (!col.IsString()) {                                                  \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_raw_str(col.GetType())   \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col, type)                          \
    do {                                                                      \
        if (!col.IsNumber()) {                                                \
            std::stringstream ss;                                             \
            ss << "Expected value of type: " << type_to_string(type)          \
               << "; but found type: " << json_type_to_raw_str(col.GetType()) \
               << "; Document value is: " << json_value_to_string(col);       \
            return Status::RuntimeError(ss.str());                            \
        }                                                                     \
    } while (false)

#define RETURN_ERROR_IF_PARSING_FAILED(result, col, type)                       \
    do {                                                                        \
        if (result != StringParser::PARSE_SUCCESS) {                            \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_raw_str(col.GetType())   \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type)                      \
    do {                                                                  \
        std::stringstream ss;                                             \
        ss << "Expected value of type: " << type_to_string(type)          \
           << "; but found type: " << json_type_to_raw_str(col.GetType()) \
           << "; Document slice is : " << json_value_to_string(col);      \
        return Status::RuntimeError(ss.str());                            \
    } while (false)

template <typename T>
static Status get_int_value(const rapidjson::Value& col, LogicalType type, void* slot, bool pure_doc_value) {
    return Status::OK();
}

ScrollParser::ScrollParser(bool doc_value_mode)
        : _tuple_desc(nullptr), _doc_value_context(nullptr), _size(0), _cur_line(0), _temp_writer(_scratch_buffer) {}

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    _size = 0;
    _cur_line = 0;
    _document_node.Parse(scroll_result.data(), scroll_result.size());
    if (_document_node.HasParseError()) {
        return Status::InternalError(fmt::format("Parsing json error, json is: {}", scroll_result));
    }

    if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(WARNING) << "Document has not a scroll id field scroll response:" << scroll_result;
        return Status::InternalError("Document has not a scroll id field");
    }

    if (!exactly_once) {
        const rapidjson::Value& scroll_node = _document_node[FIELD_SCROLL_ID];
        _scroll_id = scroll_node.GetString();
    }

    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value& outer_hits_node = _document_node[FIELD_HITS];
    // if has no inner hits, there has no data in this index
    if (!outer_hits_node.HasMember(FIELD_INNER_HITS)) {
        return Status::OK();
    }
    const rapidjson::Value& inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    // this happened just the end of scrolling
    if (!inner_hits_node.IsArray()) {
        return Status::OK();
    }
    _inner_hits_node.CopyFrom(inner_hits_node, _document_node.GetAllocator());
    // how many documents contains in this batch
    _size = _inner_hits_node.Size();

    return Status::OK();
}

Status ScrollParser::fill_chunk(RuntimeState* state, ChunkPtr* chunk, bool* line_eos) {
    if (current_eos()) {
        *line_eos = true;
        return Status::OK();
    }
    *line_eos = false;

    *chunk = std::make_shared<Chunk>();
    std::vector<SlotDescriptor*> slot_descs = _tuple_desc->slots();

    size_t left_sz = _size - _cur_line;
    size_t fill_sz = std::min(left_sz, (size_t)state->chunk_size());

    // init column information
    for (auto& slot_desc : slot_descs) {
        ColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        column->reserve(fill_sz);
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }

    auto slots = _tuple_desc->slots();
    // TODO: we could fill chunk by column rather than row
    for (size_t i = 0; i < fill_sz; ++i) {
        const rapidjson::Value& obj = _inner_hits_node[_cur_line + i];
        bool pure_doc_value = _is_pure_doc_value(obj);
        bool has_source = obj.HasMember(FIELD_SOURCE);
        bool has_fields = obj.HasMember(FIELD_FIELDS);

        if (!has_source && !has_fields) {
            for (size_t col_idx = 0; col_idx < slots.size(); ++col_idx) {
                SlotDescriptor* slot_desc = slot_descs[col_idx];
                ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
                if (slot_desc->is_nullable()) {
                    column->append_default();
                } else {
                    return Status::DataQualityError(
                            fmt::format("col `{}` is not null, but value from ES is null", slot_desc->col_name()));
                }
            }
            continue;
        }
        DCHECK(has_source ^ has_fields);
        const rapidjson::Value& line = has_source ? obj[FIELD_SOURCE] : obj[FIELD_FIELDS];

        for (size_t col_idx = 0; col_idx < slots.size(); ++col_idx) {
            SlotDescriptor* slot_desc = slot_descs[col_idx];
            ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());

            // _id field must exists in every document, this is guaranteed by ES
            // if _id was found in tuple, we would get `_id` value from inner-hit node
            // json-format response would like below:
            //    "hits": {
            //            "hits": [
            //                {
            //                    "_id": "UhHNc3IB8XwmcbhBk1ES",
            //                    "_source": {
            //                          "k": 201,
            //                    }
            //                }
            //            ]
            //        }
            if (slot_desc->col_name() == FIELD_ID) {
                // actually this branch will not be reached, this is guaranteed by Doris FE.
                if (pure_doc_value) {
                    return Status::RuntimeError("obtain `_id` is not supported in doc_values mode");
                }
                LogicalType type = slot_desc->type().type;
                DCHECK(type == TYPE_CHAR || type == TYPE_VARCHAR);

                const auto& _id = obj[FIELD_ID];
                Slice slice(_id.GetString(), _id.GetStringLength());
                _append_data<TYPE_VARCHAR>(column.get(), slice);

                continue;
            }

            // if pure_doc_value enabled, docvalue_context must contains the key
            // todo: need move all `pure_docvalue` for every tuple outside fill_tuple
            //  should check pure_docvalue for one table scan not every tuple
            const char* col_name = pure_doc_value ? _doc_value_context->at(slot_desc->col_name()).c_str()
                                                  : slot_desc->col_name().c_str();

            auto has_col = line.HasMember(col_name);
            if (has_col) {
                const rapidjson::Value& col = line[col_name];
                // doc value
                bool is_null = col.IsNull() || (pure_doc_value && col.IsArray() && (col.Empty() || col[0].IsNull()));
                if (!is_null) {
                    // append value from ES to column
                    RETURN_IF_ERROR(_append_value_from_json_val(column.get(), slot_desc->type(), col, pure_doc_value));
                    continue;
                }
                // handle null col
                if (slot_desc->is_nullable()) {
                    _append_null(column.get());
                } else {
                    return Status::DataQualityError(
                            fmt::format("col `{}` is not null, but value from ES is null", slot_desc->col_name()));
                }
            } else {
                // if don't has col in ES , append a default value
                _append_null(column.get());
            }
        }
    }
    _cur_line += fill_sz;
    return Status::OK();
}

void ScrollParser::set_params(const TupleDescriptor* descs, const std::map<std::string, std::string>* docvalue_context,
                              std::string& timezone) {
    _tuple_desc = descs;
    _doc_value_context = docvalue_context;
    _timezone = timezone;
}

bool ScrollParser::_is_pure_doc_value(const rapidjson::Value& obj) {
    if (obj.HasMember(FIELD_FIELDS)) {
        return true;
    }
    return false;
}

template <LogicalType type, typename CppType>
void ScrollParser::_append_data(Column* column, CppType& value) {
    auto appender = [](auto* column, CppType& value) {
        using ColumnType = typename starrocks::RunTimeColumnType<type>;
        auto* runtime_column = down_cast<ColumnType*>(column);
        runtime_column->append(value);
    };

    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto* data_column = nullable_column->data_column().get();
        NullData& null_data = nullable_column->null_column_data();
        null_data.push_back(0);
        appender(data_column, value);
    } else {
        appender(column, value);
    }
}

void ScrollParser::_append_null(Column* column) {
    column->append_default();
}

Status ScrollParser::_append_value_from_json_val(Column* column, const TypeDescriptor& type_desc,
                                                 const rapidjson::Value& col, bool pure_doc_value) {
    LogicalType type = type_desc.type;
    if (type != TYPE_CHAR && type != TYPE_VARCHAR) {
        RETURN_NULL_IF_STR_EMPTY(col, column);
    }
    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        Slice slice;
        auto get_slice = [this](auto& val) {
            if (!val.IsString()) {
                return _json_val_to_slice(val);
            } else {
                return Slice{val.GetString(), val.GetStringLength()};
            }
        };

        if (pure_doc_value) {
            slice = get_slice(col[0]);
        } else {
            RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
            slice = get_slice(col);
        }
        _append_data<TYPE_VARCHAR>(column, slice);
        break;
    }
    case TYPE_INT: {
        RETURN_IF_ERROR(_append_int_val<TYPE_INT>(col, column, pure_doc_value));
        break;
    }
    case TYPE_TINYINT: {
        RETURN_IF_ERROR(_append_int_val<TYPE_TINYINT>(col, column, pure_doc_value));
        break;
    }
    case TYPE_SMALLINT: {
        RETURN_IF_ERROR(_append_int_val<TYPE_SMALLINT>(col, column, pure_doc_value));
        break;
    }
    case TYPE_BIGINT: {
        RETURN_IF_ERROR(_append_int_val<TYPE_BIGINT>(col, column, pure_doc_value));
        break;
    }
    case TYPE_LARGEINT: {
        RETURN_IF_ERROR(_append_int_val<TYPE_LARGEINT>(col, column, pure_doc_value));
        break;
    }
    case TYPE_DOUBLE: {
        RETURN_IF_ERROR(_append_float_val<TYPE_DOUBLE>(col, column, pure_doc_value));
        break;
    }
    case TYPE_FLOAT: {
        RETURN_IF_ERROR(_append_float_val<TYPE_FLOAT>(col, column, pure_doc_value));
        break;
    }
    case TYPE_BOOLEAN: {
        RETURN_IF_ERROR(_append_bool_val(col, column, pure_doc_value));
        break;
    }
    case TYPE_DATE: {
        RETURN_IF_ERROR(_append_date_val<TYPE_DATE>(col, column, pure_doc_value, _timezone));
        break;
    }
    case TYPE_DATETIME: {
        RETURN_IF_ERROR(_append_date_val<TYPE_DATETIME>(col, column, pure_doc_value, _timezone));
        break;
    }
    case TYPE_ARRAY: {
        RETURN_IF_ERROR(_append_array_val(col, type_desc, column, pure_doc_value));
        break;
    }
    case TYPE_JSON: {
        RETURN_IF_ERROR(_append_json_val(col, type_desc, column, pure_doc_value));
        break;
    }
    default: {
        DCHECK(false) << "unknown type:" << type;
        return Status::InvalidArgument(fmt::format("unknown type {}", type));
        break;
    }
    }
    return Status::OK();
}

Slice ScrollParser::_json_val_to_slice(const rapidjson::Value& val) {
    _scratch_buffer.Clear();
    _temp_writer.Reset(_scratch_buffer);
    val.Accept(_temp_writer);
    return {_scratch_buffer.GetString(), _scratch_buffer.GetSize()};
}

template <LogicalType type, typename T>
Status ScrollParser::_append_int_val(const rapidjson::Value& col, Column* column, bool pure_doc_value) {
    T value;
    if (col.IsNumber()) {
        // TODO: performance ?
        if (col.IsInt()) {
            value = static_cast<T>(col.GetInt());
        } else if (col.IsInt64()) {
            value = static_cast<T>(col.GetInt64());
        } else if (col.IsUint()) {
            value = static_cast<T>(col.GetUint());
        } else if (col.IsUint64()) {
            value = static_cast<T>(col.GetUint64());
        } else if (col.IsDouble()) {
            value = static_cast<T>(col.GetDouble());
        } else if (col.IsFloat()) {
            value = static_cast<T>(col.GetFloat());
        } else {
            value = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        }
        _append_data<type>(column, value);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col[0], type);
        value = (T)(sizeof(T) < 8 ? col[0].GetInt() : col[0].GetInt64());
        _append_data<type>(column, value);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const char* raw_str = col.GetString();
    size_t len = col.GetStringLength();
    value = StringParser::string_to_int<T>(raw_str, len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    _append_data<type>(column, value);
    return Status::OK();
}

template <LogicalType type, typename T>
Status ScrollParser::_append_float_val(const rapidjson::Value& col, Column* column, bool pure_doc_value) {
    static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
    T value;

    auto get_value = [](const auto& col) {
        T v;
        if constexpr (std::is_same_v<T, float>) {
            v = col.GetFloat();
        } else if constexpr (std::is_same_v<T, double>) {
            v = col.GetDouble();
        }
        return v;
    };

    if (col.IsNumber()) {
        value = get_value(col);
        _append_data<type>(column, value);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        value = get_value(col[0]);
        _append_data<type>(column, value);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const char* raw_str = col.GetString();
    size_t len = col.GetStringLength();
    value = StringParser::string_to_float<T>(raw_str, len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
    _append_data<type>(column, value);

    return Status::OK();
}

Status ScrollParser::_append_bool_val(const rapidjson::Value& col, Column* column, bool pure_doc_value) {
    LogicalType type = TYPE_BOOLEAN;
    uint8_t value;
    if (col.IsBool()) {
        value = col.GetBool();
        _append_data<TYPE_BOOLEAN>(column, value);
        return Status::OK();
    }

    if (col.IsNumber()) {
        if (col.IsInt()) {
            value = static_cast<int8_t>(col.GetInt() != 0);
        } else if (col.IsInt64()) {
            value = static_cast<int8_t>(col.GetInt64() != 0);
        } else if (col.IsUint()) {
            value = static_cast<int8_t>(col.GetUint() != 0);
        } else if (col.IsUint64()) {
            value = static_cast<int8_t>(col.GetUint64() != 0);
        } else if (col.IsDouble()) {
            value = static_cast<int8_t>(col.GetDouble() != 0);
        } else {
            value = static_cast<int8_t>(col.GetInt() != 0);
        }
        _append_data<TYPE_BOOLEAN>(column, value);
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        value = col[0].GetBool();
        _append_data<TYPE_BOOLEAN>(column, value);
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    const char* raw_string = col.GetString();
    size_t val_size = col.GetStringLength();
    StringParser::ParseResult result;
    value = StringParser::string_to_bool(raw_string, val_size, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
    _append_data<TYPE_BOOLEAN>(column, value);

    return Status::OK();
}

Status ScrollParser::_append_array_val(const rapidjson::Value& col, const TypeDescriptor& type_desc, Column* column,
                                       bool pure_doc_value) {
    // Array type must have child type.
    const auto& child_type = type_desc.children[0];
    DCHECK(child_type.type != TYPE_UNKNOWN);

    // In Elasticsearch, n-dimensional array will be flattened into one-dimensional array.
    // https://www.elastic.co/guide/en/elasticsearch/reference/8.3/array.html
    // So we do not support user to create nested array column.
    // TODO: We should prevent user to create nested array column in FE, but we don't do any schema validation now.
    if (child_type.type == TYPE_ARRAY) {
        std::string str = fmt::format("Invalid array format; Document slice is: {}.", json_value_to_string(col));
        return Status::RuntimeError(str);
    }

    ArrayColumn* array = nullptr;

    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto* data_column = nullable_column->data_column().get();
        NullData& null_data = nullable_column->null_column_data();
        null_data.push_back(0);
        array = down_cast<ArrayColumn*>(data_column);
    } else {
        array = down_cast<ArrayColumn*>(column);
    }

    auto* offsets = array->offsets_column().get();
    auto* elements = array->elements_column().get();

    if (pure_doc_value) {
        RETURN_IF_ERROR(_append_array_val_from_docvalue(col, child_type, elements));
    } else {
        RETURN_IF_ERROR(_append_array_val_from_source(col, child_type, elements));
    }

    size_t new_size = elements->size();
    offsets->append(new_size);
    return Status::OK();
}

Status ScrollParser::_append_json_val(const rapidjson::Value& col, const TypeDescriptor& type_desc, Column* column,
                                      bool pure_doc_value) {
    std::string s = json_value_to_string(col);
    Slice slice{s};
    if (!col.IsObject()) {
        return Status::InternalError("col: " + slice.to_string() + " is not an object");
    }
    ASSIGN_OR_RETURN(JsonValue json, JsonValue::parse_json_or_string(slice));
    JsonValue* tmp = &json;
    _append_data<TYPE_JSON>(column, tmp);
    return Status::OK();
}

Status ScrollParser::_append_array_val_from_docvalue(const rapidjson::Value& val, const TypeDescriptor& child_type_desc,
                                                     Column* column) {
    for (auto& item : val.GetArray()) {
        RETURN_IF_ERROR(_append_value_from_json_val(column, child_type_desc, item, true));
    }
    return Status::OK();
}

Status ScrollParser::_append_array_val_from_source(const rapidjson::Value& val, const TypeDescriptor& child_type_desc,
                                                   Column* column) {
    if (val.IsNull()) {
        // Ignore null item in _source.
        return Status::OK();
    }

    if (!val.IsArray()) {
        // For one item situation, like "1" should be treated as "[1]".
        RETURN_IF_ERROR(_append_value_from_json_val(column, child_type_desc, val, false));
        return Status::OK();
    }

    for (auto& item : val.GetArray()) {
        RETURN_IF_ERROR(_append_array_val_from_source(item, child_type_desc, column));
    }
    return Status::OK();
}

// TODO: test here
template <LogicalType type, typename T>
Status ScrollParser::_append_date_val(const rapidjson::Value& col, Column* column, bool pure_doc_value,
                                      const std::string& timezone) {
    auto append_timestamp = [](auto& col, Column* column, const std::string& timezone) {
        TimestampValue value;
        value.from_unixtime(col.GetInt64() / 1000, timezone);
        if constexpr (type == TYPE_DATE) {
            auto date_val = DateValue(value);
            _append_data<TYPE_DATE>(column, date_val);
        } else if constexpr (type == TYPE_DATETIME) {
            _append_data<TYPE_DATETIME>(column, value);
        }
    };

    auto append_strval = [](auto& col, Column* column, const std::string& timezone) {
        const char* raw_str = col.GetString();
        size_t val_size = col.GetStringLength();

        if constexpr (type == TYPE_DATE) {
            DateValue value;
            if (!value.from_string(raw_str, val_size)) {
                RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
            }
            _append_data<TYPE_DATE>(column, value);
        } else if constexpr (type == TYPE_DATETIME) {
            TimestampValue value;
            if (!value.from_string(raw_str, val_size)) {
                RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
            }
            // https://en.wikipedia.org/wiki/ISO_8601
            // 2020-06-06T16:00:00.000Z was UTC time.
            if (raw_str[val_size - 1] == 'Z') {
                value.from_unixtime(value.to_unix_second(), timezone);
            }
            _append_data<TYPE_DATETIME>(column, value);
        }

        return Status::OK();
    };

    if (col.IsNumber()) {
        append_timestamp(col, column, timezone);
    } else if (col.IsArray() && pure_doc_value) {
        if (col[0].IsString()) {
            RETURN_IF_ERROR(append_strval(col[0], column, timezone));
        } else {
            append_timestamp(col[0], column, timezone);
        }

    } else {
        RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
        RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);
        RETURN_IF_ERROR(append_strval(col, column, timezone));
    }
    return Status::OK();
}

} // namespace starrocks
