// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <iostream>

namespace starrocks {

enum LogicalType {
    LOGICAL_TYPE_UNKNOWN = 0, // UNKNOW Type
    LOGICAL_TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    LOGICAL_TYPE_UNSIGNED_TINYINT = 2,
    LOGICAL_TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    LOGICAL_TYPE_UNSIGNED_SMALLINT = 4,
    LOGICAL_TYPE_INT = 5, // MYSQL_TYPE_LONG
    LOGICAL_TYPE_UNSIGNED_INT = 6,
    LOGICAL_TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    LOGICAL_TYPE_UNSIGNED_BIGINT = 8,
    LOGICAL_TYPE_LARGEINT = 9,
    LOGICAL_TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    LOGICAL_TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    LOGICAL_TYPE_DISCRETE_DOUBLE = 12,
    LOGICAL_TYPE_CHAR = 13,        // MYSQL_TYPE_STRING
    LOGICAL_TYPE_DATE_V1 = 14,     // MySQL_TYPE_NEWDATE
    LOGICAL_TYPE_DATETIME_V1 = 15, // MySQL_TYPE_DATETIME
    LOGICAL_TYPE_DECIMAL = 16,     // DECIMAL, using different store format against MySQL
    LOGICAL_TYPE_VARCHAR = 17,

    LOGICAL_TYPE_STRUCT = 18, // Struct
    LOGICAL_TYPE_ARRAY = 19,  // ARRAY
    LOGICAL_TYPE_MAP = 20,    // Map
    LOGICAL_TYPE_NONE = 22,
    LOGICAL_TYPE_HLL = 23,
    LOGICAL_TYPE_BOOLEAN = 24,
    LOGICAL_TYPE_OBJECT = 25,

    // Added by StarRocks

    // Reserved some field for commutiy version

    LOGICAL_TYPE_NULL = 42,
    LOGICAL_TYPE_FUNCTION = 43,
    LOGICAL_TYPE_TIME = 44,
    LOGICAL_TYPE_BINARY = 45,
    LOGICAL_TYPE_VARBINARY = 46,
    // decimal v3 type
    LOGICAL_TYPE_DECIMAL32 = 47,
    LOGICAL_TYPE_DECIMAL64 = 48,
    LOGICAL_TYPE_DECIMAL128 = 49,
    LOGICAL_TYPE_DATE = 50,
    LOGICAL_TYPE_DATETIME = 51,
    LOGICAL_TYPE_DECIMALV2 = 52,
    LOGICAL_TYPE_PERCENTILE = 53,

    LOGICAL_TYPE_JSON = 54,

    // max value of LogicalType, newly-added type should not exceed this value.
    // used to create a fixed-size hash map.
    LOGICAL_TYPE_MAX_VALUE = 55
};

// TODO(lism): support varbinary for zone map.
inline bool is_zone_map_key_type(LogicalType type) {
    return type != LOGICAL_TYPE_CHAR && type != LOGICAL_TYPE_VARCHAR && type != LOGICAL_TYPE_JSON &&
           type != LOGICAL_TYPE_VARBINARY;
}

template <LogicalType TYPE>
inline constexpr LogicalType DelegateType = TYPE;
template <>
inline constexpr LogicalType DelegateType<LOGICAL_TYPE_DECIMAL32> = LOGICAL_TYPE_INT;
template <>
inline constexpr LogicalType DelegateType<LOGICAL_TYPE_DECIMAL64> = LOGICAL_TYPE_BIGINT;
template <>
inline constexpr LogicalType DelegateType<LOGICAL_TYPE_DECIMAL128> = LOGICAL_TYPE_LARGEINT;

inline LogicalType delegate_type(LogicalType type) {
    switch (type) {
    case LOGICAL_TYPE_DECIMAL32:
        return LOGICAL_TYPE_INT;
    case LOGICAL_TYPE_DECIMAL64:
        return LOGICAL_TYPE_BIGINT;
    case LOGICAL_TYPE_DECIMAL128:
        return LOGICAL_TYPE_LARGEINT;
    default:
        return type;
    }
}

inline bool is_string_type(LogicalType type) {
    return type == LogicalType::LOGICAL_TYPE_CHAR || type == LogicalType::LOGICAL_TYPE_VARCHAR;
}

inline bool is_decimalv3_field_type(LogicalType type) {
    return type == LOGICAL_TYPE_DECIMAL32 || type == LOGICAL_TYPE_DECIMAL64 || type == LOGICAL_TYPE_DECIMAL128;
}

LogicalType string_to_logical_type(const std::string& type_str);
const char* logical_type_to_string(LogicalType type);

} // namespace starrocks

inline std::ostream& operator<<(std::ostream& os, starrocks::LogicalType type) {
    os << starrocks::logical_type_to_string(type);
    return os;
}
