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

#include "binary_column.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "column/binary_column.h"
#include "column/json_column.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "util/defer_op.h"
#include "util/json.h"

namespace starrocks {

static Status add_column_with_numeric_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                            const std::string& name, const avro_value_t& value) {
    switch (avro_value_get_type(&value)) {
    case AVRO_INT32: {
        int in;
        if (avro_value_get_int(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get int value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        std::string sv = std::to_string(in);
        if (UNLIKELY(type_desc.len < sv.size())) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice(sv));

        return Status::OK();
    }
    case AVRO_INT64: {
        int64_t in;
        if (avro_value_get_long(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get int64 value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        std::string sv = std::to_string(in);
        if (UNLIKELY(type_desc.len < sv.size())) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice(sv));

        return Status::OK();
    }
    case AVRO_FLOAT: {
        float in;
        if (avro_value_get_float(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get float value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        std::string sv = std::to_string(in);
        if (UNLIKELY(type_desc.len < sv.size())) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice(sv));

        return Status::OK();
    }

    case AVRO_DOUBLE: {
        double in;
        if (avro_value_get_double(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get double value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        std::string sv = std::to_string(in);
        if (UNLIKELY(type_desc.len < sv.size())) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice(sv));

        return Status::OK();
    }

    default: {
        auto err_msg = strings::Substitute("Unsupported value type. column=$0", name);
        return Status::InternalError(err_msg);
    }
    }
    return Status::OK();
}

static Status add_column_with_string_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                           const std::string& name, const avro_value_t& value) {
    switch (avro_value_get_type(&value)) {
    case AVRO_STRING: {
        const char* in;
        size_t size;
        if (UNLIKELY(avro_value_get_string(&value, &in, &size) != 0)) {
            auto err_msg = strings::Substitute("Get string value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        --size;
        if (UNLIKELY(type_desc.len < size)) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice{in, size});
        return Status::OK();
    }

    case AVRO_ENUM: {
        avro_schema_t enum_schema;
        int symbol_value;
        const char* symbol_name;
        if (UNLIKELY(avro_value_get_enum(&value, &symbol_value) != 0)) {
            auto err_msg = strings::Substitute("Get enum value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        enum_schema = avro_value_get_schema(&value);
        symbol_name = avro_schema_enum_get(enum_schema, symbol_value);

        column->append(Slice(symbol_name));
        return Status::OK();
    }

    case AVRO_FIXED: {
        const char* in;
        size_t size;
        if (UNLIKELY(avro_value_get_fixed(&value, (const void**)&in, &size) != 0)) {
            auto err_msg = strings::Substitute("Get fixed value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }

        if (UNLIKELY(type_desc.len < size)) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice{in, size});
        return Status::OK();
    }

    case AVRO_BYTES: {
        const char* in;
        size_t size;
        if (UNLIKELY(avro_value_get_bytes(&value, (const void**)&in, &size) != 0)) {
            auto err_msg = strings::Substitute("Get bytes value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }

        if (UNLIKELY(type_desc.len < size)) {
            auto err_msg = strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name,
                                               type_desc.len);
            return Status::InvalidArgument(err_msg);
        }

        column->append(Slice{in, size});
        return Status::OK();
    }

    default: {
        auto err_msg = strings::Substitute("Unsupported value type. column=$0", name);
        return Status::InternalError(err_msg);
    }
    }
    return Status::OK();
}

static Status add_column_with_boolean_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                            const std::string& name, const avro_value_t& value) {
    int in;
    if (avro_value_get_boolean(&value, &in) != 0) {
        auto err_msg = strings::Substitute("Get boolean value error. column=$0", name);
        return Status::InvalidArgument(err_msg);
    }
    if (in == 1) {
        column->append(Slice{"1"});
    } else {
        column->append(Slice{"0"});
    }
    return Status::OK();
}

static Status avro_value_to_rapidjson(const avro_value_t& value, rapidjson::Document::AllocatorType& allocator,
                                      rapidjson::Value& out) {
    switch (avro_value_get_type(&value)) {
    case AVRO_STRING: {
        const char* in;
        size_t size;
        if (avro_value_get_string(&value, &in, &size) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get string value error $0", avro_strerror()));
        }
        out.SetString(in, allocator);
        return Status::OK();
    }
    case AVRO_BYTES: {
        const char* in;
        size_t size;
        if (avro_value_get_bytes(&value, (const void**)&in, &size) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get bytes value error $0", avro_strerror()));
        }
        out.SetString(in, allocator);
        return Status::OK();
    }
    case AVRO_INT32: {
        int32_t in;
        if (avro_value_get_int(&value, &in) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get int32 value error $0", avro_strerror()));
        }
        out.SetInt(in);
        return Status::OK();
    }
    case AVRO_INT64: {
        int64_t in;
        if (avro_value_get_long(&value, &in) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get int64 value error $0", avro_strerror()));
        }
        out.SetInt64(in);
        return Status::OK();
    }
    case AVRO_FLOAT: {
        float in;
        if (avro_value_get_float(&value, &in) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get float value error $0", avro_strerror()));
        }
        out.SetFloat(in);
        return Status::OK();
    }
    case AVRO_DOUBLE: {
        double in;
        if (avro_value_get_double(&value, &in) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get double value error $0", avro_strerror()));
        }
        out.SetDouble(in);
        return Status::OK();
    }
    case AVRO_BOOLEAN: {
        int in;
        if (avro_value_get_boolean(&value, &in) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get boolean value error $0", avro_strerror()));
        }
        out.SetBool(in);
        return Status::OK();
    }
    case AVRO_NULL: {
        out.SetNull();
        return Status::OK();
    }
    case AVRO_RECORD: {
        size_t field_count = 0;
        if (avro_value_get_size(&value, &field_count) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get record field count error $0", avro_strerror()));
        }

        out.SetObject();
        for (size_t i = 0; i < field_count; ++i) {
            avro_value_t field_value;
            const char* field_name;
            if (avro_value_get_by_index(&value, i, &field_value, &field_name) != 0) {
                return Status::InvalidArgument(strings::Substitute("Get record field error $0", avro_strerror()));
            }

            rapidjson::Value field_name_val;
            field_name_val.SetString(field_name, allocator);
            rapidjson::Value field_value_val;
            RETURN_IF_ERROR(avro_value_to_rapidjson(field_value, allocator, field_value_val));
            out.AddMember(field_name_val, field_value_val, allocator);
        }
        return Status::OK();
    }
    case AVRO_ENUM: {
        avro_schema_t enum_schema;
        int symbol_value;
        if (avro_value_get_enum(&value, &symbol_value) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get enum value error $0", avro_strerror()));
        }

        enum_schema = avro_value_get_schema(&value);
        const char* symbol_name;
        symbol_name = avro_schema_enum_get(enum_schema, symbol_value);
        out.SetString(symbol_name, allocator);
        return Status::OK();
    }
    case AVRO_FIXED: {
        const char* in;
        size_t size;
        if (avro_value_get_fixed(&value, (const void**)&in, &size) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get fixed value error $0", avro_strerror()));
        }
        out.SetString(in, allocator);
        return Status::OK();
    }
    case AVRO_MAP: {
        size_t map_size = 0;
        if (avro_value_get_size(&value, &map_size) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get map size error $0", avro_strerror()));
        }

        out.SetObject();
        for (int i = 0; i < map_size; ++i) {
            const char* key;
            avro_value_t map_value;
            if (avro_value_get_by_index(&value, i, &map_value, &key) != 0) {
                return Status::InvalidArgument(strings::Substitute("Get map key value error $0", avro_strerror()));
            }

            rapidjson::Value key_val;
            key_val.SetString(key, allocator);
            rapidjson::Value value_val;
            RETURN_IF_ERROR(avro_value_to_rapidjson(map_value, allocator, value_val));
            out.AddMember(key_val, value_val, allocator);
        }
        return Status::OK();
    }
    case AVRO_ARRAY: {
        size_t array_size = 0;
        if (avro_value_get_size(&value, &array_size) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get array size error $0", avro_strerror()));
        }

        out.SetArray();
        for (int i = 0; i < array_size; ++i) {
            avro_value_t element;
            if (avro_value_get_by_index(&value, i, &element, nullptr) != 0) {
                return Status::InvalidArgument(strings::Substitute("Get array element error $0", avro_strerror()));
            }

            rapidjson::Value element_value;
            RETURN_IF_ERROR(avro_value_to_rapidjson(element, allocator, element_value));
            out.PushBack(element_value, allocator);
        }
        return Status::OK();
    }
    case AVRO_UNION: {
        avro_value_t union_value;
        if (avro_value_get_current_branch(&value, &union_value) != 0) {
            return Status::InvalidArgument(strings::Substitute("Get union value error $0", avro_strerror()));
        }
        RETURN_IF_ERROR(avro_value_to_rapidjson(union_value, allocator, out));
        return Status::OK();
    }
    default:
        return Status::InvalidArgument("Unsupported avro type");
    }
}

// Convert an avro value to a json object using rapidjson.
// Different from avro `avro_value_to_json`, this function will ignore the union type tags.
//
// schema:
// {
//    "type": "record",
//    "name": "User",
//    "fields": [
//        {"name": "id", "type": "int"},
//        {"name": "name", "type": "string"},
//        {"name": "email", "type": ["null",
//                                   {
//                                       "type": "record",
//                                       "name": "email2",
//                                       "fields": [
//                                           {
//                                               "name": "x",
//                                               "type" : ["null", "int"]
//                                           },
//                                           {
//                                               "name": "y",
//                                               "type": ["null", "string"]
//                                           }
//                                       ]
//                                   }
//                                  ]
//         }
//    ]
// }
//
// avro `avro_value_to_json` result:
// {"id": 1, "name": "Alice", "email": {"email2": {"x": {"int": 1}, "y": {"string": "alice@example.com"}}}}
//
// this function result:
// {"id":1,"name":"Alice","email":{"x":1,"y":"alice@example.com"}}
static Status avro_value_to_json_str(const avro_value_t& value, std::string* json_str) {
    rapidjson::Document doc;
    auto& allocator = doc.GetAllocator();
    rapidjson::Value root;
    RETURN_IF_ERROR(avro_value_to_rapidjson(value, allocator, root));

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    root.Accept(writer);
    json_str->append(buffer.GetString(), buffer.GetSize());
    return Status::OK();
}

static Status add_column_with_array_object_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                                 const std::string& name, const avro_value_t& value) {
    if (config::avro_ignore_union_type_tag) {
        std::string json_str;
        auto st = avro_value_to_json_str(value, &json_str);
        if (!st.ok()) {
            LOG(WARNING) << "avro to json failed. column=" << name << ", err=" << st;
            return Status::InternalError(
                    strings::Substitute("avro to json failed. column=$0, err=$1", name, st.message()));
        }

        column->append(Slice(json_str));
    } else {
        char* as_json;
        if (avro_value_to_json(&value, 1, &as_json)) {
            LOG(WARNING) << "avro to json failed. column=" << name << ", err=" << avro_strerror();
            return Status::InternalError(
                    strings::Substitute("avro to json failed. column=$0, err=$1", name, avro_strerror()));
        }
        DeferOp json_deleter([&] { free(as_json); });
        column->append(Slice(as_json));
    }
    return Status::OK();
}

Status add_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         const avro_value_t& value) {
    auto binary_column = down_cast<BinaryColumn*>(column);

    switch (avro_value_get_type(&value)) {
    case AVRO_INT32:
    case AVRO_INT64:
    case AVRO_FLOAT:
    case AVRO_DOUBLE: {
        return add_column_with_numeric_value(binary_column, type_desc, name, value);
    }

    case AVRO_FIXED:
    case AVRO_STRING:
    case AVRO_ENUM:
    case AVRO_BYTES: {
        return add_column_with_string_value(binary_column, type_desc, name, value);
    }

    case AVRO_BOOLEAN: {
        return add_column_with_boolean_value(binary_column, type_desc, name, value);
    }

    case AVRO_MAP:
    case AVRO_ARRAY:
    case AVRO_UNION:
    case AVRO_RECORD: {
        return add_column_with_array_object_value(binary_column, type_desc, name, value);
    }

    default: {
        auto err_msg = strings::Substitute("Unsupported value type. Binary type is required. column=$0", name);
        return Status::DataQualityError(err_msg);
    }
    }
    return Status::OK();
}

Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              const avro_value_t& value) {
    JsonValue json_value;
    Status st;
    if (config::avro_ignore_union_type_tag) {
        std::string json_str;
        st = avro_value_to_json_str(value, &json_str);
        if (!st.ok()) {
            LOG(WARNING) << "avro to json failed. column=" << name << ", err=" << st;
            return Status::InternalError(
                    strings::Substitute("avro to json failed. column=$0, err=$1", name, st.message()));
        }

        st = JsonValue::parse(Slice(json_str), &json_value);
    } else {
        char* as_json;
        if (avro_value_to_json(&value, 1, &as_json)) {
            LOG(WARNING) << "avro to json failed. column=" << name << ", err=" << avro_strerror();
            return Status::InternalError(
                    strings::Substitute("avro to json failed. column=$0, err=$1", name, avro_strerror()));
        }

        DeferOp json_deleter([&] { free(as_json); });
        st = JsonValue::parse(as_json, &json_value);
    }

    if (!st.ok()) {
        return Status::InternalError(strings::Substitute("parse json failed. column=$0, err=$1", name, st.message()));
    }

    auto json_column = down_cast<JsonColumn*>(column);
    json_column->append(std::move(json_value));
    return Status::OK();
}

} // namespace starrocks
