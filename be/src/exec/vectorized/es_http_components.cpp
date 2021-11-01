// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/es_http_components.h"

#include <fmt/format.h>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "runtime/primitive_type.h"
#include "runtime/timestamp_value.h"

namespace starrocks::vectorized {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
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

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type)                               \
    do {                                                                      \
        if (col.IsArray()) {                                                  \
            std::stringstream ss;                                             \
            ss << "Expected value of type: " << type_to_string(type)          \
               << "; but found type: " << json_type_to_raw_str(col.GetType()) \
               << "; Docuemnt slice is : " << json_value_to_string(col);      \
            return Status::RuntimeError(ss.str());                            \
        }                                                                     \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type)                            \
    do {                                                                        \
        if (!col.IsString()) {                                                  \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_raw_str(col.GetType())   \
               << "; Docuemnt source slice is : " << json_value_to_string(col); \
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
               << "; Docuemnt source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type)                      \
    do {                                                                  \
        std::stringstream ss;                                             \
        ss << "Expected value of type: " << type_to_string(type)          \
           << "; but found type: " << json_type_to_raw_str(col.GetType()) \
           << "; Docuemnt slice is : " << json_value_to_string(col);      \
        return Status::RuntimeError(ss.str());                            \
    } while (false)

template <typename T>
static Status get_int_value(const rapidjson::Value& col, PrimitiveType type, void* slot, bool pure_doc_value) {
    return Status::OK();
}

ScrollParser::ScrollParser(bool doc_value_mode)
        : _tuple_desc(nullptr),
          _docvalue_context(nullptr),
          _size(0),
          _cur_line(0),
          _doc_value_mode(doc_value_mode),
          _temp_writer(_scratch_buffer) {}

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    _size = 0;
    _cur_line = 0;
    _document_node.Parse(scroll_result.data(), scroll_result.size());
    if (_document_node.HasParseError()) {
        return Status::InternalError(fmt::format("Parsing json error, json is: {}", scroll_result));
    }

    if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(WARNING) << "Document has not a scroll id field scroll reponse:" << scroll_result;
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

Status ScrollParser::fill_chunk(ChunkPtr* chunk, bool* line_eos) {
    if (current_eos()) {
        *line_eos = true;
        return Status::OK();
    }

    *chunk = std::make_shared<Chunk>();
    std::vector<SlotDescriptor*> slot_descs = _tuple_desc->slots();

    // init column information
    for (auto& slot_desc : slot_descs) {
        ColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }

    size_t left_sz = _size - _cur_line;
    size_t fill_sz = std::min(left_sz, (size_t)config::vector_chunk_size);

    auto slots = _tuple_desc->slots();

    // TODO: we could fill chunk by column rather than row
    for (size_t i = 0; i < fill_sz; ++i) {
        const rapidjson::Value& obj = _inner_hits_node[_cur_line + i];
        bool pure_doc_value = _pure_doc_value(obj);
        const rapidjson::Value& line = obj.HasMember(FIELD_SOURCE) ? obj[FIELD_SOURCE] : obj["fields"];

        for (size_t col_idx = 0; col_idx < slots.size(); ++col_idx) {
            SlotDescriptor* slot_desc = slot_descs[col_idx];
            ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_desc->id());

            // because the fe planner filter the non_materialize column
            if (!slot_desc->is_materialized()) {
                continue;
            }

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
                PrimitiveType type = slot_desc->type().type;
                DCHECK(type == TYPE_CHAR || type == TYPE_VARCHAR);

                const auto& _id = obj[FIELD_ID];
                Slice slice(_id.GetString(), _id.GetStringLength());
                _append_data<TYPE_VARCHAR>(column.get(), slice);

                continue;
            }

            // if pure_doc_value enabled, docvalue_context must contains the key
            // todo: need move all `pure_docvalue` for every tuple outside fill_tuple
            //  should check pure_docvalue for one table scan not every tuple
            const char* col_name = pure_doc_value ? _docvalue_context->at(slot_desc->col_name()).c_str()
                                                  : slot_desc->col_name().c_str();

            auto has_col = line.HasMember(col_name);
            if (has_col) {
                const rapidjson::Value& col = line[col_name];
                // doc value
                bool is_null = (pure_doc_value && col.IsArray() && col[0].IsNull()) || col.IsNull();
                if (!is_null) {
                    // append value from ES to column
                    RETURN_IF_ERROR(
                            _append_value_from_json_val(column.get(), slot_desc->type().type, col, pure_doc_value));
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

void ScrollParser::set_params(const TupleDescriptor* descs,
                              const std::map<std::string, std::string>* docvalue_context) {
    _tuple_desc = descs;
    _docvalue_context = docvalue_context;
}

bool ScrollParser::_pure_doc_value(const rapidjson::Value& obj) {
    if (obj.HasMember("fields")) {
        return true;
    }
    return false;
}

template <PrimitiveType type, typename CppType>
void ScrollParser::_append_data(Column* column, CppType& value) {
    auto appender = [](auto* column, CppType& value) {
        using ColumnType = typename vectorized::RunTimeColumnType<type>;
        ColumnType* runtime_column = down_cast<ColumnType*>(column);
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

Status ScrollParser::_append_value_from_json_val(Column* column, PrimitiveType type, const rapidjson::Value& col,
                                                 bool pure_doc_value) {
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
        RETURN_IF_ERROR(_append_date_val<TYPE_DATE>(col, column, pure_doc_value));
        break;
    }
    case TYPE_DATETIME: {
        RETURN_IF_ERROR(_append_date_val<TYPE_DATETIME>(col, column, pure_doc_value));
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
    _temp_writer.Reset(_scratch_buffer);
    val.Accept(_temp_writer);
    return {_scratch_buffer.GetString(), _scratch_buffer.GetSize()};
}

template <PrimitiveType type, typename T>
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

template <PrimitiveType type, typename T>
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
    PrimitiveType type = TYPE_BOOLEAN;
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
// TODO: test here
template <PrimitiveType type, typename T>
Status ScrollParser::_append_date_val(const rapidjson::Value& col, Column* column, bool pure_doc_value) {
    auto append_timestamp = [](auto& col, Column* column) {
        TimestampValue value;
        value.from_unixtime(col.GetInt64() / 1000, "+08:00");
        if constexpr (type == TYPE_DATE) {
            DateValue date_val = DateValue(value);
            _append_data<TYPE_DATE>(column, date_val);
        } else if constexpr (type == TYPE_DATETIME) {
            _append_data<TYPE_DATETIME>(column, value);
        }
    };

    auto append_strval = [](auto& col, Column* column) {
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
            _append_data<TYPE_DATETIME>(column, value);
        }

        return Status::OK();
    };

    if (col.IsNumber()) {
        append_timestamp(col, column);
    } else if (col.IsArray() && pure_doc_value) {
        if (col[0].IsString()) {
            RETURN_IF_ERROR(append_strval(col[0], column));
        } else {
            append_timestamp(col[0], column);
        }

    } else {
        RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
        RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);
        RETURN_IF_ERROR(append_strval(col, column));
    }
    return Status::OK();
}

} // namespace starrocks::vectorized