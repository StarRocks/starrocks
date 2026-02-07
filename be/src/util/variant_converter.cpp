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

#include "util/variant_converter.h"

#include "types/variant.h"

namespace starrocks {

Status cast_variant_to_bool(const VariantRowValue& variant, ColumnBuilder<TYPE_BOOLEAN>& result) {
    const VariantValue& value = variant.get_value();
    const VariantType type = value.type();
    if (type == VariantType::NULL_TYPE) {
        result.append_null();
        return Status::OK();
    }

    if (type == VariantType::BOOLEAN_TRUE || type == VariantType::BOOLEAN_FALSE) {
        ASSIGN_OR_RETURN(const auto ret, value.get_bool());
        result.append(ret);
        return Status::OK();
    }

    if (type == VariantType::STRING) {
        ASSIGN_OR_RETURN(const auto str, value.get_string());
        const char* str_value = str.data();
        size_t len = str.size();
        StringParser::ParseResult parsed;
        auto r = StringParser::string_to_int<int32_t>(str_value, len, &parsed);
        if (parsed != StringParser::PARSE_SUCCESS) {
            const bool casted = StringParser::string_to_bool(str_value, len, &parsed);
            if (parsed != StringParser::PARSE_SUCCESS) {
                return Status::VariantError(fmt::format("Failed to cast string '{}' to BOOLEAN", str));
            }
            result.append(casted);
        } else {
            result.append(r != 0);
        }
        return Status::OK();
    }

    return VARIANT_CAST_NOT_SUPPORT(type, TYPE_BOOLEAN);
}

Status cast_variant_to_string(const VariantRowValue& variant, const cctz::time_zone& zone,
                              ColumnBuilder<TYPE_VARCHAR>& result) {
    const VariantValue& value = variant.get_value();
    switch (value.type()) {
    case VariantType::NULL_TYPE: {
        result.append(Slice("null"));
        return Status::OK();
    }
    case VariantType::STRING: {
        ASSIGN_OR_RETURN(const auto str, value.get_string());
        result.append(Slice(str));
        return Status::OK();
    }
    default: {
        // TODO: extra copy. from variant -> ss, and ss -> result.
        std::stringstream ss;
        RETURN_IF_ERROR(VariantUtil::variant_to_json(variant.get_metadata(), value, ss, zone));
        std::string json_str = ss.str();
        result.append(Slice(json_str));
        return Status::OK();
    }
    }
}

} // namespace starrocks