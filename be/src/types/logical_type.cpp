// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "types/logical_type.h"

#include <algorithm>

#include "common/logging.h"

namespace starrocks {

LogicalType string_to_logical_type(const std::string& type_str) {
    std::string upper_type_str = type_str;
    std::transform(type_str.begin(), type_str.end(), upper_type_str.begin(), ::toupper);
    if (upper_type_str == "TINYINT") return LOGICAL_TYPE_TINYINT;
    if (upper_type_str == "SMALLINT") return LOGICAL_TYPE_SMALLINT;
    if (upper_type_str == "INT") return LOGICAL_TYPE_INT;
    if (upper_type_str == "BIGINT") return LOGICAL_TYPE_BIGINT;
    if (upper_type_str == "LARGEINT") return LOGICAL_TYPE_LARGEINT;
    if (upper_type_str == "UNSIGNED_TINYINT") return LOGICAL_TYPE_UNSIGNED_TINYINT;
    if (upper_type_str == "UNSIGNED_SMALLINT") return LOGICAL_TYPE_UNSIGNED_SMALLINT;
    if (upper_type_str == "UNSIGNED_INT") return LOGICAL_TYPE_UNSIGNED_INT;
    if (upper_type_str == "UNSIGNED_BIGINT") return LOGICAL_TYPE_UNSIGNED_BIGINT;
    if (upper_type_str == "FLOAT") return LOGICAL_TYPE_FLOAT;
    if (upper_type_str == "DISCRETE_DOUBLE") return LOGICAL_TYPE_DISCRETE_DOUBLE;
    if (upper_type_str == "DOUBLE") return LOGICAL_TYPE_DOUBLE;
    if (upper_type_str == "CHAR") return LOGICAL_TYPE_CHAR;
    if (upper_type_str == "DATE_V2") return LOGICAL_TYPE_DATE;
    if (upper_type_str == "DATE") return LOGICAL_TYPE_DATE_V1;
    if (upper_type_str == "DATETIME") return LOGICAL_TYPE_DATETIME_V1;
    if (upper_type_str == "TIMESTAMP") return LOGICAL_TYPE_DATETIME;
    if (upper_type_str == "DECIMAL_V2") return LOGICAL_TYPE_DECIMALV2;
    if (upper_type_str == "DECIMAL") return LOGICAL_TYPE_DECIMAL;
    if (upper_type_str == "VARCHAR") return LOGICAL_TYPE_VARCHAR;
    if (upper_type_str == "BOOLEAN") return LOGICAL_TYPE_BOOLEAN;
    if (upper_type_str == "HLL") return LOGICAL_TYPE_HLL;
    if (upper_type_str == "STRUCT") return LOGICAL_TYPE_STRUCT;
    if (upper_type_str == "ARRAY") return LOGICAL_TYPE_ARRAY;
    if (upper_type_str == "MAP") return LOGICAL_TYPE_MAP;
    if (upper_type_str == "OBJECT") return LOGICAL_TYPE_OBJECT;
    if (upper_type_str == "PERCENTILE") return LOGICAL_TYPE_PERCENTILE;
    if (upper_type_str == "DECIMAL32") return LOGICAL_TYPE_DECIMAL32;
    if (upper_type_str == "DECIMAL64") return LOGICAL_TYPE_DECIMAL64;
    if (upper_type_str == "DECIMAL128") return LOGICAL_TYPE_DECIMAL128;
    if (upper_type_str == "JSON") return LOGICAL_TYPE_JSON;
    if (upper_type_str == "VARBINARY") return LOGICAL_TYPE_VARBINARY;
    LOG(WARNING) << "invalid type string. [type='" << type_str << "']";
    return LOGICAL_TYPE_UNKNOWN;
}

const char* logical_type_to_string(LogicalType type) {
    switch (type) {
    case LOGICAL_TYPE_TINYINT:
        return "TINYINT";
    case LOGICAL_TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED_TINYINT";
    case LOGICAL_TYPE_SMALLINT:
        return "SMALLINT";
    case LOGICAL_TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED_SMALLINT";
    case LOGICAL_TYPE_INT:
        return "INT";
    case LOGICAL_TYPE_UNSIGNED_INT:
        return "UNSIGNED_INT";
    case LOGICAL_TYPE_BIGINT:
        return "BIGINT";
    case LOGICAL_TYPE_LARGEINT:
        return "LARGEINT";
    case LOGICAL_TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED_BIGINT";
    case LOGICAL_TYPE_FLOAT:
        return "FLOAT";
    case LOGICAL_TYPE_DOUBLE:
        return "DOUBLE";
    case LOGICAL_TYPE_DISCRETE_DOUBLE:
        return "DISCRETE_DOUBLE";
    case LOGICAL_TYPE_CHAR:
        return "CHAR";
    case LOGICAL_TYPE_DATE_V1:
        return "DATE";
    case LOGICAL_TYPE_DATE:
        return "DATE_V2";
    case LOGICAL_TYPE_DATETIME_V1:
        return "DATETIME";
    case LOGICAL_TYPE_DATETIME:
        return "TIMESTAMP";
    case LOGICAL_TYPE_DECIMAL:
        return "DECIMAL";
    case LOGICAL_TYPE_DECIMALV2:
        return "DECIMAL_V2";
    case LOGICAL_TYPE_DECIMAL32:
        return "DECIMAL32";
    case LOGICAL_TYPE_DECIMAL64:
        return "DECIMAL64";
    case LOGICAL_TYPE_DECIMAL128:
        return "DECIMAL128";
    case LOGICAL_TYPE_VARCHAR:
        return "VARCHAR";
    case LOGICAL_TYPE_BOOLEAN:
        return "BOOLEAN";
    case LOGICAL_TYPE_HLL:
        return "HLL";
    case LOGICAL_TYPE_STRUCT:
        return "STRUCT";
    case LOGICAL_TYPE_ARRAY:
        return "ARRAY";
    case LOGICAL_TYPE_MAP:
        return "MAP";
    case LOGICAL_TYPE_OBJECT:
        return "OBJECT";
    case LOGICAL_TYPE_PERCENTILE:
        return "PERCENTILE";
    case LOGICAL_TYPE_JSON:
        return "JSON";
    case LOGICAL_TYPE_UNKNOWN:
        return "UNKNOWN";
    case LOGICAL_TYPE_NONE:
        return "NONE";
    case LOGICAL_TYPE_NULL:
        return "NULL";
    case LOGICAL_TYPE_FUNCTION:
        return "FUNCTION";
    case LOGICAL_TYPE_TIME:
        return "TIME";
    case LOGICAL_TYPE_BINARY:
        return "BINARY";
    case LOGICAL_TYPE_MAX_VALUE:
        return "MAX_VALUE";
    case LOGICAL_TYPE_VARBINARY:
        return "VARBINARY";
    }
    return "";
}

} // namespace starrocks
