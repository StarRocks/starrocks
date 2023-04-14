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

static Status add_column_with_array_object_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                                 const std::string& name, const avro_value_t& value) {
    char* as_json;
    if (avro_value_to_json(&value, 1, &as_json)) {
        LOG(ERROR) << "avro to json failed: %s" << avro_strerror();
        return Status::InternalError("avro to json failed");
    }
    column->append(Slice(as_json));
    free(as_json);
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
    auto json_column = down_cast<JsonColumn*>(column);
    char* as_json;
    if (avro_value_to_json(&value, 1, &as_json)) {
        LOG(ERROR) << "avro to json failed: %s" << avro_strerror();
        return Status::InternalError("avro to json failed");
    }
    DeferOp json_deleter([&] { free(as_json); });
    JsonValue json_value;
    Status s = JsonValue::parse(as_json, &json_value);
    if (!s.ok()) {
        return Status::InternalError("parse json failed");
    }
    json_column->append(std::move(json_value));
    return Status::OK();
}

} // namespace starrocks
