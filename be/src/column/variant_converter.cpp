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

#include "column/variant_converter.h"

#include "types/variant.h"

namespace starrocks {

static inline std::pair<int64_t, int64_t> split_micros_to_seconds(int64_t micros) {
    int64_t seconds = micros / USECS_PER_SEC;
    int64_t microseconds = micros % USECS_PER_SEC;
    if (microseconds < 0) {
        microseconds += USECS_PER_SEC;
        --seconds;
    }
    return {seconds, microseconds};
}

Status VariantConverter::cast_to_bool(const VariantRowRef& variant, ColumnBuilder<TYPE_BOOLEAN>& result) {
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

    if (type == VariantType::INT8 || type == VariantType::INT16 || type == VariantType::INT32 ||
        type == VariantType::INT64 || type == VariantType::FLOAT || type == VariantType::DOUBLE ||
        type == VariantType::DECIMAL4 || type == VariantType::DECIMAL8 || type == VariantType::DECIMAL16) {
        if (type == VariantType::INT8) {
            ASSIGN_OR_RETURN(auto v, value.get_int8());
            result.append(v != 0);
        } else if (type == VariantType::INT16) {
            ASSIGN_OR_RETURN(auto v, value.get_int16());
            result.append(v != 0);
        } else if (type == VariantType::INT32) {
            ASSIGN_OR_RETURN(auto v, value.get_int32());
            result.append(v != 0);
        } else if (type == VariantType::INT64) {
            ASSIGN_OR_RETURN(auto v, value.get_int64());
            result.append(v != 0);
        } else if (type == VariantType::FLOAT) {
            ASSIGN_OR_RETURN(auto v, value.get_float());
            result.append(v != 0);
        } else if (type == VariantType::DOUBLE) {
            ASSIGN_OR_RETURN(auto v, value.get_double());
            result.append(v != 0);
        } else if (type == VariantType::DECIMAL4) {
            ASSIGN_OR_RETURN(auto v, value.get_decimal4());
            result.append(v.value != 0);
        } else if (type == VariantType::DECIMAL8) {
            ASSIGN_OR_RETURN(auto v, value.get_decimal8());
            result.append(v.value != 0);
        } else if (type == VariantType::DECIMAL16) {
            ASSIGN_OR_RETURN(auto v, value.get_decimal16());
            result.append(v.value != 0);
        }
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

Status VariantConverter::cast_to_string(const VariantRowRef& variant, const cctz::time_zone& zone,
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

Status VariantConverter::cast_to_date(const VariantRowRef& row, const cctz::time_zone& zone,
                                      ColumnBuilder<TYPE_DATE>& result) {
    const VariantValue& variant = row.get_value();
    switch (const VariantType type = variant.type()) {
    case VariantType::NULL_TYPE:
        result.append_null();
        return Status::OK();
    case VariantType::DATE: {
        ASSIGN_OR_RETURN(int32_t days, variant.get_date());
        result.append(DateValue::from_days_since_unix_epoch(days));
        return Status::OK();
    }
    case VariantType::TIMESTAMP_TZ: {
        ASSIGN_OR_RETURN(int64_t micros, variant.get_timestamp_micros());
        auto [seconds, microseconds] = split_micros_to_seconds(micros);
        TimestampValue tsv{};
        tsv.from_unixtime(seconds, microseconds, zone);
        int year, month, day;
        timestamp::to_date(tsv.timestamp(), &year, &month, &day);
        result.append(DateValue::create(year, month, day));
        return Status::OK();
    }
    default:
        return VARIANT_CAST_NOT_SUPPORT(type, TYPE_DATE);
    }
}

Status VariantConverter::cast_to_time(const VariantRowRef& row, const cctz::time_zone& zone,
                                      ColumnBuilder<TYPE_TIME>& result) {
    const VariantValue& variant = row.get_value();
    switch (const VariantType type = variant.type()) {
    case VariantType::NULL_TYPE:
        result.append_null();
        return Status::OK();
    case VariantType::TIME_NTZ: {
        ASSIGN_OR_RETURN(int64_t micros, variant.get_time_micros_ntz());
        result.append(static_cast<double>(micros) / USECS_PER_SEC);
        return Status::OK();
    }
    case VariantType::TIMESTAMP_TZ: {
        ASSIGN_OR_RETURN(int64_t micros, variant.get_timestamp_micros());
        auto [seconds, microseconds] = split_micros_to_seconds(micros);
        TimestampValue tsv{};
        tsv.from_unixtime(seconds, microseconds, zone);
        int hour, minute, second, microsecond;
        tsv.to_time(&hour, &minute, &second, &microsecond);
        int64_t total_micros = (static_cast<int64_t>(hour) * 3600 + minute * 60 + second) * USECS_PER_SEC + microsecond;
        result.append(static_cast<double>(total_micros) / USECS_PER_SEC);
        return Status::OK();
    }
    default:
        return VARIANT_CAST_NOT_SUPPORT(type, TYPE_TIME);
    }
}

Status VariantConverter::cast_to_datetime(const VariantRowRef& row, const cctz::time_zone& zone,
                                          ColumnBuilder<TYPE_DATETIME>& result) {
    const VariantValue& variant = row.get_value();
    switch (const VariantType type = variant.type()) {
    case VariantType::NULL_TYPE:
        result.append_null();
        return Status::OK();
    case VariantType::TIMESTAMP_NTZ: {
        ASSIGN_OR_RETURN(int64_t micros, variant.get_timestamp_micros_ntz());
        auto [seconds, microseconds] = split_micros_to_seconds(micros);
        TimestampValue tsv{};
        tsv.from_unix_second(seconds, microseconds);
        result.append(tsv);
        return Status::OK();
    }
    case VariantType::TIMESTAMP_TZ: {
        ASSIGN_OR_RETURN(int64_t micros, variant.get_timestamp_micros());
        auto [seconds, microseconds] = split_micros_to_seconds(micros);
        TimestampValue tsv{};
        tsv.from_unixtime(seconds, microseconds, zone);
        result.append(tsv);
        return Status::OK();
    }
    default:
        return VARIANT_CAST_NOT_SUPPORT(type, TYPE_DATETIME);
    }
}

} // namespace starrocks
