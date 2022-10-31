// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "util/json_converter.h"

#include "gutil/strings/substitute.h"
#include "simdjson.h"
#include "velocypack/ValueType.h"
#include "velocypack/vpack.h"

namespace starrocks {

namespace so = simdjson::ondemand;
using SimdJsonArray = so::array;
using SimdJsonNumber = so::number;
using SimdJsonNumberType = so::number_type;

// Convert SIMD-JSON object to a JsonValue
class SimdJsonConverter {
public:
    static StatusOr<JsonValue> create(SimdJsonValue value) {
        try {
            vpack::Builder builder;
            RETURN_IF_ERROR(convert(value, {}, &builder));
            return JsonValue(builder.slice());
        } catch (simdjson::simdjson_error& e) {
            std::string_view view(value.get_raw_json_string().raw());
            auto err_msg = strings::Substitute("Failed to convert simdjson value, json=$0, error=$1", view.data(),
                                               simdjson::error_message(e.error()));
            return Status::DataQualityError(err_msg);
        }
    }

    static StatusOr<JsonValue> create(SimdJsonObject value) {
        try {
            vpack::Builder builder;
            RETURN_IF_ERROR(convert(value, {}, &builder));
            return JsonValue(builder.slice());
        } catch (simdjson::simdjson_error& e) {
            std::string_view view(value.raw_json());
            auto err_msg = strings::Substitute("Failed to convert simdjson value, json=$0, error=$1", view.data(),
                                               simdjson::error_message(e.error()));
            return Status::DataQualityError(err_msg);
        }
    }

private:
    static Status convert(SimdJsonValue value, std::string_view field_name, vpack::Builder* builder) {
        switch (value.type()) {
        case so::json_type::array: {
            convert(value.get_array().value(), field_name, builder);
            break;
        }
        case so::json_type::object: {
            convert(value.get_object().value(), field_name, builder);
            break;
        }
        case so::json_type::number: {
            convert(value.get_number().value(), field_name, builder);
            break;
        }
        case so::json_type::string: {
            convert(value.get_string().value(), field_name, builder);
            break;
        }
        case so::json_type::boolean: {
            convert(value.get_bool().value(), field_name, builder);
            break;
        }
        case so::json_type::null: {
            convert_null(field_name, builder);
            break;
        }
        }
        return Status::OK();
    }

    static Status convert(SimdJsonObject obj, std::string_view field_name, vpack::Builder* builder) {
        if (!field_name.empty()) {
            builder->add(toStringRef(field_name), vpack::Value(vpack::ValueType::Object));
        } else {
            builder->add(vpack::Value(vpack::ValueType::Object));
        }
        for (auto field : obj) {
            std::string_view key = field.unescaped_key();
            auto value = field.value().value();
            RETURN_IF_ERROR(convert(value, key, builder));
        }
        builder->close();
        return Status::OK();
    }

    static Status convert(SimdJsonArray arr, std::string_view field_name, vpack::Builder* builder) {
        if (!field_name.empty()) {
            builder->add(toStringRef(field_name), vpack::Value(vpack::ValueType::Array));
        } else {
            builder->add(vpack::Value(vpack::ValueType::Array));
        }
        for (auto element : arr) {
            convert(element.value(), {}, builder);
        }
        builder->close();
        return Status::OK();
    }

    static inline Status convert(SimdJsonNumber num, std::string_view field_name, vpack::Builder* builder) {
        switch (num.get_number_type()) {
        case SimdJsonNumberType::floating_point_number: {
            if (!field_name.empty()) {
                builder->add(toStringRef(field_name), vpack::Value((num.get_double())));
            } else {
                builder->add(vpack::Value((num.get_double())));
            }
            break;
        }
        case SimdJsonNumberType::signed_integer: {
            if (!field_name.empty()) {
                builder->add(toStringRef(field_name), vpack::Value((num.get_int64())));
            } else {
                builder->add(vpack::Value((num.get_int64())));
            }
            break;
        }
        case SimdJsonNumberType::unsigned_integer: {
            if (!field_name.empty()) {
                builder->add(toStringRef(field_name), vpack::Value((num.get_uint64())));
            } else {
                builder->add(vpack::Value((num.get_uint64())));
            }
            break;
        }
        default:
            __builtin_unreachable();
        }
        return Status::OK();
    }

    static inline Status convert(std::string_view str, std::string_view field_name, vpack::Builder* builder) {
        if (!field_name.empty()) {
            builder->add(toStringRef(field_name), vpack::Value(str));
        } else {
            builder->add(vpack::Value(str));
        }
        return Status::OK();
    }

    static inline Status convert(bool value, std::string_view field_name, vpack::Builder* builder) {
        if (!field_name.empty()) {
            builder->add(toStringRef(field_name), vpack::Value(value));
        } else {
            builder->add(vpack::Value(value));
        }
        return Status::OK();
    }

    static inline Status convert_null(std::string_view field_name, vpack::Builder* builder) {
        if (!field_name.empty()) {
            builder->add(toStringRef(field_name), vpack::Value(vpack::ValueType::Null));
        } else {
            builder->add(vpack::Value(vpack::ValueType::Null));
        }
        return Status::OK();
    }

private:
    static inline vpack::StringRef toStringRef(std::string_view view) {
        return {view.data(), view.length()};
    }
};

// Convert SIMD-JSON object/value to a JsonValue
StatusOr<JsonValue> convert_from_simdjson(SimdJsonValue value) {
    return SimdJsonConverter::create(value);
}

StatusOr<JsonValue> convert_from_simdjson(SimdJsonObject value) {
    return SimdJsonConverter::create(value);
}

} //namespace starrocks
