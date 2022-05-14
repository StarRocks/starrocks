// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "binary_column.h"

#include "column/binary_column.h"
#include "column/json_column.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "util/json.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
