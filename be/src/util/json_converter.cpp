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

#include "util/json_converter.h"

#include "gutil/strings/substitute.h"
#include "simdjson.h"
#include "util/json.h"
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
            RETURN_IF_ERROR(convert(value, {}, false, &builder));
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
            RETURN_IF_ERROR(convert(value, {}, false, &builder));
            return JsonValue(builder.slice());
        } catch (simdjson::simdjson_error& e) {
            std::string_view view(value.raw_json());
            auto err_msg = strings::Substitute("Failed to convert simdjson value, json=$0, error=$1", view.data(),
                                               simdjson::error_message(e.error()));
            return Status::DataQualityError(err_msg);
        }
    }

private:
    static Status convert(SimdJsonValue value, std::string_view field_name, bool is_object, vpack::Builder* builder) {
        switch (value.type()) {
        case so::json_type::array: {
            convert(value.get_array().value(), field_name, is_object, builder);
            break;
        }
        case so::json_type::object: {
            convert(value.get_object().value(), field_name, is_object, builder);
            break;
        }
        case so::json_type::number: {
            convert(value.get_number().value(), field_name, is_object, builder);
            break;
        }
        case so::json_type::string: {
            convert(value.get_string().value(), field_name, is_object, builder);
            break;
        }
        case so::json_type::boolean: {
            convert(value.get_bool().value(), field_name, is_object, builder);
            break;
        }
        case so::json_type::null: {
            convert_null(field_name, is_object, builder);
            break;
        }
        }
        return Status::OK();
    }

    static Status convert(SimdJsonObject obj, std::string_view field_name, bool is_object, vpack::Builder* builder) {
        if (is_object) {
            builder->add(toStringRef(field_name), vpack::Value(vpack::ValueType::Object));
        } else {
            builder->add(vpack::Value(vpack::ValueType::Object));
        }
        for (auto field : obj) {
            std::string_view key = field.unescaped_key();
            auto value = field.value().value();
            RETURN_IF_ERROR(convert(value, key, true, builder));
        }
        builder->close();
        return Status::OK();
    }

    static Status convert(SimdJsonArray arr, std::string_view field_name, bool is_object, vpack::Builder* builder) {
        if (is_object) {
            builder->add(toStringRef(field_name), vpack::Value(vpack::ValueType::Array));
        } else {
            builder->add(vpack::Value(vpack::ValueType::Array));
        }
        for (auto element : arr) {
            convert(element.value(), {}, false, builder);
        }
        builder->close();
        return Status::OK();
    }

    static inline Status convert(SimdJsonNumber num, std::string_view field_name, bool is_object,
                                 vpack::Builder* builder) {
        switch (num.get_number_type()) {
        case SimdJsonNumberType::floating_point_number: {
            if (is_object) {
                builder->add(toStringRef(field_name), vpack::Value((num.get_double())));
            } else {
                builder->add(vpack::Value((num.get_double())));
            }
            break;
        }
        case SimdJsonNumberType::signed_integer: {
            if (is_object) {
                builder->add(toStringRef(field_name), vpack::Value((num.get_int64())));
            } else {
                builder->add(vpack::Value((num.get_int64())));
            }
            break;
        }
        case SimdJsonNumberType::unsigned_integer: {
            if (is_object) {
                builder->add(toStringRef(field_name), vpack::Value((num.get_uint64())));
            } else {
                builder->add(vpack::Value((num.get_uint64())));
            }
            break;
        }
        default:
            return Status::InternalError(fmt::format("unsupported json number: {}", num.get_number_type()));
        }
        return Status::OK();
    }

    static inline Status convert(std::string_view str, std::string_view field_name, bool is_object,
                                 vpack::Builder* builder) {
        if (is_object) {
            builder->add(toStringRef(field_name), vpack::Value(str));
        } else {
            builder->add(vpack::Value(str));
        }
        return Status::OK();
    }

    static inline Status convert(bool value, std::string_view field_name, bool is_object, vpack::Builder* builder) {
        if (is_object) {
            builder->add(toStringRef(field_name), vpack::Value(value));
        } else {
            builder->add(vpack::Value(value));
        }
        return Status::OK();
    }

    static inline Status convert_null(std::string_view field_name, bool is_object, vpack::Builder* builder) {
        if (is_object) {
            builder->add(toStringRef(field_name), vpack::Value(vpack::ValueType::Null));
        } else {
            builder->add(vpack::Value(vpack::ValueType::Null));
        }
        return Status::OK();
    }

private:
    static inline vpack::StringRef toStringRef(std::string_view view) { return {view.data(), view.length()}; }
};

// Convert SIMD-JSON object/value to a JsonValue
StatusOr<JsonValue> convert_from_simdjson(SimdJsonValue value) {
    try {
        return SimdJsonConverter::create(value);
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
}

StatusOr<JsonValue> convert_from_simdjson(SimdJsonObject value) {
    try {
        return SimdJsonConverter::create(value);
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
}

} //namespace starrocks
