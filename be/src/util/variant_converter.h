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

#pragma once

#include <utility>

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"
#include "types/variant.h"

namespace starrocks {

#define VARIANT_CAST_NOT_SUPPORT(variant_type, logical_type)                            \
    Status::NotSupported(fmt::format("Cannot cast variant({}) to type: {}",             \
                                     VariantUtil::variant_type_to_string(variant_type), \
                                     logical_type_to_string(logical_type)))

inline std::pair<int64_t, int64_t> split_micros_to_seconds(int64_t micros) {
    int64_t seconds = micros / USECS_PER_SEC;
    int64_t microseconds = micros % USECS_PER_SEC;
    if (microseconds < 0) {
        microseconds += USECS_PER_SEC;
        --seconds;
    }
    return {seconds, microseconds};
}

Status cast_variant_to_bool(const VariantRowValue& row, ColumnBuilder<TYPE_BOOLEAN>& result);

Status cast_variant_to_string(const VariantRowValue& row, const cctz::time_zone& zone,
                              ColumnBuilder<TYPE_VARCHAR>& result);

inline Status cast_variant_to_date(const VariantRowValue& row, const cctz::time_zone& zone,
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

inline Status cast_variant_to_time(const VariantRowValue& row, const cctz::time_zone& zone,
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

inline Status cast_variant_to_datetime(const VariantRowValue& row, const cctz::time_zone& zone,
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

#define VARIANT_CAST_CASE(VARIANT_TYPE_ENUM, GETTER_METHOD)                    \
    case VariantType::VARIANT_TYPE_ENUM: {                                     \
        auto value = variant.GETTER_METHOD();                                  \
        if (!value.ok()) {                                                     \
            return value.status();                                             \
        }                                                                      \
        result.append(static_cast<RunTimeCppType<ResultType>>(value.value())); \
        return Status::OK();                                                   \
    }

template <LogicalType ResultType>
Status cast_variant_to_arithmetic(const VariantRowValue& row, ColumnBuilder<ResultType>& result) {
    const VariantValue& variant = row.get_value();
    switch (const VariantType type = variant.type()) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
        VARIANT_CAST_CASE(BOOLEAN_TRUE, get_bool)
        VARIANT_CAST_CASE(BOOLEAN_FALSE, get_bool)
        VARIANT_CAST_CASE(INT8, get_int8)
        VARIANT_CAST_CASE(INT16, get_int16)
        VARIANT_CAST_CASE(INT32, get_int32)
        VARIANT_CAST_CASE(INT64, get_int64)
        VARIANT_CAST_CASE(FLOAT, get_float)
        VARIANT_CAST_CASE(DOUBLE, get_double)
        VARIANT_CAST_CASE(DECIMAL4, get_decimal4)
        VARIANT_CAST_CASE(DECIMAL8, get_decimal8)
        VARIANT_CAST_CASE(DECIMAL16, get_decimal16)
    default:
        return VARIANT_CAST_NOT_SUPPORT(type, ResultType);
    }
}

template <LogicalType ResultType, bool AllowThrowException>
static Status cast_variant_value_to(const VariantRowValue& row, const cctz::time_zone& zone,
                                    ColumnBuilder<ResultType>& result) {
    const VariantType variant_type = row.get_value().type();
    // Supported types: arithmetic, string, variant
    // Some casting require more information like target type within ARRAY/MAP/STRUCT which is not available here:
    // VARIANT -> ARRAY<ANY>: CastVariantToArray
    // VARIANT -> MAP<VARCHAR, ANY>: CastVariantToMap
    // VARIANT -> STRUCT<...>: CastVariantToStruct
    // VARIANT -> Decimal types: DecimalNonDecimalCast
    if constexpr (!lt_is_arithmetic<ResultType> && !lt_is_string<ResultType> && !lt_is_time<ResultType> &&
                  !lt_is_date_or_datetime<ResultType> && ResultType != TYPE_VARIANT) {
        if constexpr (AllowThrowException) {
            return VARIANT_CAST_NOT_SUPPORT(variant_type, ResultType);
        }

        result.append_null();
        return Status::OK();
    }

    if (variant_type == VariantType::NULL_TYPE) {
        result.append_null();
        return Status::OK();
    }

    if constexpr (ResultType == TYPE_VARIANT) {
        result.append(VariantRowValue::from_variant(row.get_metadata(), row.get_value()));
        return Status::OK();
    }

    Status status;
    if constexpr (ResultType == TYPE_BOOLEAN) {
        status = cast_variant_to_bool(row, result);
    } else if constexpr (lt_is_arithmetic<ResultType>) {
        status = cast_variant_to_arithmetic<ResultType>(row, result);
    } else if constexpr (lt_is_string<ResultType>) {
        status = cast_variant_to_string(row, zone, result);
    } else if constexpr (ResultType == TYPE_DATE) {
        status = cast_variant_to_date(row, zone, result);
    } else if constexpr (ResultType == TYPE_DATETIME) {
        status = cast_variant_to_datetime(row, zone, result);
    } else if constexpr (ResultType == TYPE_TIME) {
        status = cast_variant_to_time(row, zone, result);
    }

    if (!status.ok()) {
        if constexpr (AllowThrowException) {
            return Status::InternalError(fmt::format("Fail to cast variant: {}, error: {}",
                                                     logical_type_to_string(ResultType), status.to_string()));
        } else {
            result.append_null();
        }
    }

    return Status::OK();
}

} // namespace starrocks
