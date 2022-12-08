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

#include <iostream>

namespace starrocks {

enum LogicalType {
    TYPE_UNKNOWN = 0, // UNKNOW Type
    TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    TYPE_UNSIGNED_TINYINT = 2,
    TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    TYPE_UNSIGNED_SMALLINT = 4,
    TYPE_INT = 5, // MYSQL_TYPE_LONG
    TYPE_UNSIGNED_INT = 6,
    TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    TYPE_UNSIGNED_BIGINT = 8,
    TYPE_LARGEINT = 9,
    TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    TYPE_DISCRETE_DOUBLE = 12,
    TYPE_CHAR = 13,        // MYSQL_TYPE_STRING
    TYPE_DATE_V1 = 14,     // MySQL_TYPE_NEWDATE
    TYPE_DATETIME_V1 = 15, // MySQL_TYPE_DATETIME
    TYPE_DECIMAL = 16,     // DECIMAL, using different store format against MySQL
    TYPE_VARCHAR = 17,

    TYPE_STRUCT = 18, // Struct
    TYPE_ARRAY = 19,  // ARRAY
    TYPE_MAP = 20,    // Map
    TYPE_NONE = 22,
    TYPE_HLL = 23,
    TYPE_BOOLEAN = 24,
    TYPE_OBJECT = 25,

    // Added by StarRocks

    // Reserved some field for commutiy version

    TYPE_NULL = 42,
    TYPE_FUNCTION = 43,
    TYPE_TIME = 44,
    TYPE_BINARY = 45,
    TYPE_VARBINARY = 46,
    // decimal v3 type
    TYPE_DECIMAL32 = 47,
    TYPE_DECIMAL64 = 48,
    TYPE_DECIMAL128 = 49,
    TYPE_DATE = 50,
    TYPE_DATETIME = 51,
    TYPE_DECIMALV2 = 52,
    TYPE_PERCENTILE = 53,

    TYPE_JSON = 54,

    // max value of LogicalType, newly-added type should not exceed this value.
    // used to create a fixed-size hash map.
    TYPE_MAX_VALUE = 55
};

// TODO(lism): support varbinary for zone map.
inline bool is_zone_map_key_type(LogicalType type) {
    return type != TYPE_CHAR && type != TYPE_VARCHAR && type != TYPE_JSON && type != TYPE_VARBINARY;
}

template <LogicalType TYPE>
inline constexpr LogicalType DelegateType = TYPE;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL32> = TYPE_INT;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL64> = TYPE_BIGINT;
template <>
inline constexpr LogicalType DelegateType<TYPE_DECIMAL128> = TYPE_LARGEINT;

inline LogicalType delegate_type(LogicalType type) {
    switch (type) {
    case TYPE_DECIMAL32:
        return TYPE_INT;
    case TYPE_DECIMAL64:
        return TYPE_BIGINT;
    case TYPE_DECIMAL128:
        return TYPE_LARGEINT;
    default:
        return type;
    }
}

inline bool is_string_type(LogicalType type) {
    return type == LogicalType::TYPE_CHAR || type == LogicalType::TYPE_VARCHAR;
}

inline bool is_decimalv3_field_type(LogicalType type) {
    return type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128;
}

LogicalType string_to_logical_type(const std::string& type_str);
const char* logical_type_to_string(LogicalType type);

} // namespace starrocks

inline std::ostream& operator<<(std::ostream& os, starrocks::LogicalType type) {
    os << starrocks::logical_type_to_string(type);
    return os;
}
