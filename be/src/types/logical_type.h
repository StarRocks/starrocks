// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <iostream>

namespace starrocks {

enum FieldType {
    OLAP_FIELD_TYPE_UNKNOWN = 0, // UNKNOW Type
    OLAP_FIELD_TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    OLAP_FIELD_TYPE_UNSIGNED_TINYINT = 2,
    OLAP_FIELD_TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    OLAP_FIELD_TYPE_UNSIGNED_SMALLINT = 4,
    OLAP_FIELD_TYPE_INT = 5, // MYSQL_TYPE_LONG
    OLAP_FIELD_TYPE_UNSIGNED_INT = 6,
    OLAP_FIELD_TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    OLAP_FIELD_TYPE_UNSIGNED_BIGINT = 8,
    OLAP_FIELD_TYPE_LARGEINT = 9,
    OLAP_FIELD_TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    OLAP_FIELD_TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    OLAP_FIELD_TYPE_DISCRETE_DOUBLE = 12,
    OLAP_FIELD_TYPE_CHAR = 13,     // MYSQL_TYPE_STRING
    OLAP_FIELD_TYPE_DATE = 14,     // MySQL_TYPE_NEWDATE
    OLAP_FIELD_TYPE_DATETIME = 15, // MySQL_TYPE_DATETIME
    OLAP_FIELD_TYPE_DECIMAL = 16,  // DECIMAL, using different store format against MySQL
    OLAP_FIELD_TYPE_VARCHAR = 17,

    OLAP_FIELD_TYPE_STRUCT = 18, // Struct
    OLAP_FIELD_TYPE_ARRAY = 19,  // ARRAY
    OLAP_FIELD_TYPE_MAP = 20,    // Map
    OLAP_FIELD_TYPE_NONE = 22,
    OLAP_FIELD_TYPE_HLL = 23,
    OLAP_FIELD_TYPE_BOOL = 24,
    OLAP_FIELD_TYPE_OBJECT = 25,

    // Added by StarRocks

    // Reserved some field for commutiy version

    OLAP_FIELD_TYPE_VARBINARY = 46,
    // decimal v3 type
    OLAP_FIELD_TYPE_DECIMAL32 = 47,
    OLAP_FIELD_TYPE_DECIMAL64 = 48,
    OLAP_FIELD_TYPE_DECIMAL128 = 49,
    OLAP_FIELD_TYPE_DATE_V2 = 50,
    OLAP_FIELD_TYPE_TIMESTAMP = 51,
    OLAP_FIELD_TYPE_DECIMAL_V2 = 52,
    OLAP_FIELD_TYPE_PERCENTILE = 53,

    OLAP_FIELD_TYPE_JSON = 54,

    // max value of FieldType, newly-added type should not exceed this value.
    // used to create a fixed-size hash map.
    OLAP_FIELD_TYPE_MAX_VALUE = 55
};

inline const char* field_type_to_string(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return "TINYINT";
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED TINYINT";
    case OLAP_FIELD_TYPE_SMALLINT:
        return "SMALLINT";
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED SMALLINT";
    case OLAP_FIELD_TYPE_INT:
        return "INT";
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return "UNSIGNED INT";
    case OLAP_FIELD_TYPE_BIGINT:
        return "BIGINT";
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED BIGINT";
    case OLAP_FIELD_TYPE_LARGEINT:
        return "LARGEINT";
    case OLAP_FIELD_TYPE_FLOAT:
        return "FLOAT";
    case OLAP_FIELD_TYPE_DOUBLE:
        return "DOUBLE";
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        return "DISCRETE DOUBLE";
    case OLAP_FIELD_TYPE_CHAR:
        return "CHAR";
    case OLAP_FIELD_TYPE_DATE:
        return "DATE";
    case OLAP_FIELD_TYPE_DATETIME:
        return "DATETIME";
    case OLAP_FIELD_TYPE_DECIMAL:
        return "DECIMAL";
    case OLAP_FIELD_TYPE_VARCHAR:
        return "VARCHAR";
    case OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";
    case OLAP_FIELD_TYPE_ARRAY:
        return "ARRAY";
    case OLAP_FIELD_TYPE_MAP:
        return "MAP";
    case OLAP_FIELD_TYPE_UNKNOWN:
        return "UNKNOWN";
    case OLAP_FIELD_TYPE_NONE:
        return "NONE";
    case OLAP_FIELD_TYPE_HLL:
        return "HLL";
    case OLAP_FIELD_TYPE_BOOL:
        return "BOOL";
    case OLAP_FIELD_TYPE_OBJECT:
        return "OBJECT";
    case OLAP_FIELD_TYPE_DECIMAL32:
        return "DECIMAL32";
    case OLAP_FIELD_TYPE_DECIMAL64:
        return "DECIMAL64";
    case OLAP_FIELD_TYPE_DECIMAL128:
        return "DECIMAL128";
    case OLAP_FIELD_TYPE_DATE_V2:
        return "DATE V2";
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return "TIMESTAMP";
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return "DECIMAL V2";
    case OLAP_FIELD_TYPE_PERCENTILE:
        return "PERCENTILE";
    case OLAP_FIELD_TYPE_JSON:
        return "JSON";
    case OLAP_FIELD_TYPE_VARBINARY:
        return "VARBINARY";
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return "MAX VALUE";
    }
    return "";
}

inline std::ostream& operator<<(std::ostream& os, FieldType type) {
    os << field_type_to_string(type);
    return os;
}

// TODO(lism): support varbinary for zone map.
inline bool is_zone_map_key_type(FieldType type) {
    return type != OLAP_FIELD_TYPE_CHAR && type != OLAP_FIELD_TYPE_VARCHAR && type != OLAP_FIELD_TYPE_JSON &&
           type != OLAP_FIELD_TYPE_VARBINARY;
}

template <FieldType TYPE>
inline constexpr FieldType DelegateType = TYPE;
template <>
inline constexpr FieldType DelegateType<OLAP_FIELD_TYPE_DECIMAL32> = OLAP_FIELD_TYPE_INT;
template <>
inline constexpr FieldType DelegateType<OLAP_FIELD_TYPE_DECIMAL64> = OLAP_FIELD_TYPE_BIGINT;
template <>
inline constexpr FieldType DelegateType<OLAP_FIELD_TYPE_DECIMAL128> = OLAP_FIELD_TYPE_LARGEINT;

inline FieldType delegate_type(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_DECIMAL32:
        return OLAP_FIELD_TYPE_INT;
    case OLAP_FIELD_TYPE_DECIMAL64:
        return OLAP_FIELD_TYPE_BIGINT;
    case OLAP_FIELD_TYPE_DECIMAL128:
        return OLAP_FIELD_TYPE_LARGEINT;
    default:
        return type;
    }
}

inline bool is_string_type(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR;
}

inline bool is_decimalv3_field_type(FieldType type) {
    return type == OLAP_FIELD_TYPE_DECIMAL32 || type == OLAP_FIELD_TYPE_DECIMAL64 || type == OLAP_FIELD_TYPE_DECIMAL128;
}

} // namespace starrocks
