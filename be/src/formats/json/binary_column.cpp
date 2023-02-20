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
#include "util/json.h"

namespace starrocks {

// The value must be in type simdjson::ondemand::json_type::number;
static Status add_column_with_numeric_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                            const std::string& name, simdjson::ondemand::value* value) {
    std::string_view sv = value->raw_json_token();

    if (type_desc.len < sv.size()) {
        auto err_msg =
                strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name, type_desc.len);
        return Status::InvalidArgument(err_msg);
    }

    column->append(Slice{sv.data(), sv.size()});
    return Status::OK();
}

// The value must be in type simdjson::ondemand::json_type::string;
static Status add_column_with_string_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                           const std::string& name, simdjson::ondemand::value* value) {
    // simdjson::value::get_string() returns string without quotes.
    std::string_view sv = value->get_string();

    if (type_desc.len < sv.size()) {
        auto err_msg =
                strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name, type_desc.len);
        return Status::InvalidArgument(err_msg);
    }

    column->append(Slice{sv.data(), sv.size()});
    return Status::OK();
}

// The value must be in type simdjson::ondemand::json_type::number;
static Status add_column_with_boolean_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                            const std::string& name, simdjson::ondemand::value* value) {
    bool ok = value->get_bool();
    if (ok) {
        column->append(Slice{"1"});
    } else {
        column->append(Slice{"0"});
    }
    return Status::OK();
}

// The value must be in type simdjson::ondemand::json_type::string;
static Status add_column_with_array_object_value(BinaryColumn* column, const TypeDescriptor& type_desc,
                                                 const std::string& name, simdjson::ondemand::value* value) {
    std::string_view sv = simdjson::to_json_string(*value);
    std::unique_ptr<char[]> buf{new char[sv.size()]};
    size_t new_length{};
    auto err = simdjson::minify(sv.data(), sv.size(), buf.get(), new_length);
    if (err) {
        auto err_msg = strings::Substitute("Failed to minify array/object as string. column=$0, error=$1", name,
                                           simdjson::error_message(err));
        return Status::DataQualityError(err_msg);
    }

    if (type_desc.len < new_length) {
        auto err_msg =
                strings::Substitute("Value length is beyond the capacity. column=$0, capacity=$1", name, type_desc.len);
        return Status::InvalidArgument(err_msg);
    }

    column->append(Slice{buf.get(), new_length});
    return Status::OK();
}

Status add_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         simdjson::ondemand::value* value) {
    auto binary_column = down_cast<BinaryColumn*>(column);

    try {
        simdjson::ondemand::json_type tp = value->type();
        switch (tp) {
        case simdjson::ondemand::json_type::number: {
            return add_column_with_numeric_value(binary_column, type_desc, name, value);
        }

        case simdjson::ondemand::json_type::string: {
            return add_column_with_string_value(binary_column, type_desc, name, value);
        }

        case simdjson::ondemand::json_type::boolean: {
            return add_column_with_boolean_value(binary_column, type_desc, name, value);
        }

        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::object: {
            return add_column_with_array_object_value(binary_column, type_desc, name, value);
        }

        default: {
            auto err_msg = strings::Substitute("Unsupported value type. Binary type is required. column=$0", name);
            return Status::DataQualityError(err_msg);
        }
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as string, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              simdjson::ondemand::value* value) {
    auto json_column = down_cast<JsonColumn*>(column);

    auto json_value = JsonValue::from_simdjson(value);
    RETURN_IF_ERROR(json_value);
    json_column->append(std::move(json_value.value()));
    return Status::OK();
}

Status add_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                              simdjson::ondemand::object* value) {
    auto json_column = down_cast<JsonColumn*>(column);

    auto json_value = JsonValue::from_simdjson(value);
    RETURN_IF_ERROR(json_value);
    json_column->append(std::move(json_value.value()));
    return Status::OK();
}

Status add_binary_column_from_json_object(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::object* obj) {
    auto binary_column = down_cast<BinaryColumn*>(column);
    std::string_view json_str = simdjson::to_json_string(*obj);
    if (json_str.length() > type_desc.len) {
        auto err_msg = strings::Substitute("Value length $0 exceed the column length $1($2)", json_str.length(), name,
                                           type_desc.len);
        return Status::InvalidArgument(err_msg);
    }

    binary_column->append(Slice(json_str.data(), json_str.length()));
    return Status::OK();
}

} // namespace starrocks
