// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/olap_common.h"

namespace starrocks {

using DataFormatVersion = int8_t;

const DataFormatVersion kDataFormatUnknown = 0;
const DataFormatVersion kDataFormatV1 = 1;
const DataFormatVersion kDataFormatV2 = 2;

class TypeUtils {
public:
    static inline bool specific_type_of_format_v1(LogicalType type) {
        return type == LOGICAL_TYPE_DATE || type == LOGICAL_TYPE_DATETIME || type == LOGICAL_TYPE_DECIMAL;
    }

    static inline bool specific_type_of_format_v2(LogicalType type) {
        return type == LOGICAL_TYPE_DATE_V2 || type == LOGICAL_TYPE_TIMESTAMP || type == LOGICAL_TYPE_DECIMAL_V2;
    }

    static inline size_t estimate_field_size(LogicalType type, size_t variable_length) {
        switch (type) {
        case LOGICAL_TYPE_UNKNOWN:
        case LOGICAL_TYPE_DISCRETE_DOUBLE:
        case LOGICAL_TYPE_STRUCT:
        case LOGICAL_TYPE_MAP:
        case LOGICAL_TYPE_NONE:
        case LOGICAL_TYPE_MAX_VALUE:
        case LOGICAL_TYPE_BOOL:
        case LOGICAL_TYPE_TINYINT:
        case LOGICAL_TYPE_UNSIGNED_TINYINT:
            return 1;
        case LOGICAL_TYPE_SMALLINT:
        case LOGICAL_TYPE_UNSIGNED_SMALLINT:
            return 2;
        case LOGICAL_TYPE_DATE:
            return 3;
        case LOGICAL_TYPE_INT:
        case LOGICAL_TYPE_UNSIGNED_INT:
        case LOGICAL_TYPE_FLOAT:
        case LOGICAL_TYPE_DATE_V2:
        case LOGICAL_TYPE_DECIMAL32:
            return 4;
        case LOGICAL_TYPE_BIGINT:
        case LOGICAL_TYPE_UNSIGNED_BIGINT:
        case LOGICAL_TYPE_DOUBLE:
        case LOGICAL_TYPE_DATETIME:
        case LOGICAL_TYPE_TIMESTAMP:
        case LOGICAL_TYPE_DECIMAL64:
            return 8;
        case LOGICAL_TYPE_DECIMAL:
            return 12;
        case LOGICAL_TYPE_LARGEINT:
        case LOGICAL_TYPE_DECIMAL_V2:
        case LOGICAL_TYPE_DECIMAL128:
            return 16;
        default:
            // CHAR, VARCHAR, HLL, PERCENTILE, JSON, ARRAY, OBJECT
            return variable_length;
        }
    }

    static inline LogicalType convert_to_format(LogicalType type, DataFormatVersion format_version) {
        if (format_version == kDataFormatV2) {
            switch (type) {
            case LOGICAL_TYPE_DATE:
                return LOGICAL_TYPE_DATE_V2;
            case LOGICAL_TYPE_DATETIME:
                return LOGICAL_TYPE_TIMESTAMP;
            case LOGICAL_TYPE_DECIMAL:
                return LOGICAL_TYPE_DECIMAL_V2;
            case LOGICAL_TYPE_UNKNOWN:
            case LOGICAL_TYPE_TINYINT:
            case LOGICAL_TYPE_UNSIGNED_TINYINT:
            case LOGICAL_TYPE_SMALLINT:
            case LOGICAL_TYPE_UNSIGNED_SMALLINT:
            case LOGICAL_TYPE_INT:
            case LOGICAL_TYPE_UNSIGNED_INT:
            case LOGICAL_TYPE_BIGINT:
            case LOGICAL_TYPE_UNSIGNED_BIGINT:
            case LOGICAL_TYPE_LARGEINT:
            case LOGICAL_TYPE_FLOAT:
            case LOGICAL_TYPE_DOUBLE:
            case LOGICAL_TYPE_DISCRETE_DOUBLE:
            case LOGICAL_TYPE_CHAR:
            case LOGICAL_TYPE_VARCHAR:
            case LOGICAL_TYPE_STRUCT:
            case LOGICAL_TYPE_ARRAY:
            case LOGICAL_TYPE_MAP:
            case LOGICAL_TYPE_NONE:
            case LOGICAL_TYPE_HLL:
            case LOGICAL_TYPE_BOOL:
            case LOGICAL_TYPE_OBJECT:
            case LOGICAL_TYPE_DECIMAL32:
            case LOGICAL_TYPE_DECIMAL64:
            case LOGICAL_TYPE_DECIMAL128:
            case LOGICAL_TYPE_DATE_V2:
            case LOGICAL_TYPE_TIMESTAMP:
            case LOGICAL_TYPE_DECIMAL_V2:
            case LOGICAL_TYPE_PERCENTILE:
            case LOGICAL_TYPE_JSON:
            case LOGICAL_TYPE_NULL:
            case LOGICAL_TYPE_FUNCTION:
            case LOGICAL_TYPE_TIME:
            case LOGICAL_TYPE_BINARY:
            case LOGICAL_TYPE_VARBINARY:
            case LOGICAL_TYPE_MAX_VALUE:
                return type;
                // no default by intention.
            }
        } else if (format_version == kDataFormatV1) {
            switch (type) {
            case LOGICAL_TYPE_DATE_V2:
                return LOGICAL_TYPE_DATE;
            case LOGICAL_TYPE_TIMESTAMP:
                return LOGICAL_TYPE_DATETIME;
            case LOGICAL_TYPE_DECIMAL_V2:
                return LOGICAL_TYPE_DECIMAL;
            case LOGICAL_TYPE_UNKNOWN:
            case LOGICAL_TYPE_NONE:
            case LOGICAL_TYPE_STRUCT:
            case LOGICAL_TYPE_ARRAY:
            case LOGICAL_TYPE_MAP:
            case LOGICAL_TYPE_DECIMAL32:
            case LOGICAL_TYPE_DECIMAL64:
            case LOGICAL_TYPE_DECIMAL128:
            case LOGICAL_TYPE_NULL:
            case LOGICAL_TYPE_FUNCTION:
            case LOGICAL_TYPE_TIME:
            case LOGICAL_TYPE_BINARY:
            case LOGICAL_TYPE_VARBINARY:
                return LOGICAL_TYPE_UNKNOWN;
            case LOGICAL_TYPE_TINYINT:
            case LOGICAL_TYPE_UNSIGNED_TINYINT:
            case LOGICAL_TYPE_SMALLINT:
            case LOGICAL_TYPE_UNSIGNED_SMALLINT:
            case LOGICAL_TYPE_INT:
            case LOGICAL_TYPE_UNSIGNED_INT:
            case LOGICAL_TYPE_BIGINT:
            case LOGICAL_TYPE_UNSIGNED_BIGINT:
            case LOGICAL_TYPE_LARGEINT:
            case LOGICAL_TYPE_FLOAT:
            case LOGICAL_TYPE_DOUBLE:
            case LOGICAL_TYPE_DISCRETE_DOUBLE:
            case LOGICAL_TYPE_CHAR:
            case LOGICAL_TYPE_DATE:
            case LOGICAL_TYPE_DATETIME:
            case LOGICAL_TYPE_DECIMAL:
            case LOGICAL_TYPE_VARCHAR:
            case LOGICAL_TYPE_HLL:
            case LOGICAL_TYPE_BOOL:
            case LOGICAL_TYPE_PERCENTILE:
            case LOGICAL_TYPE_JSON:
            case LOGICAL_TYPE_MAX_VALUE:
            case LOGICAL_TYPE_OBJECT:
                return type;
                // no default by intention.
            }
        }
        DCHECK(false) << "unknown format version " << format_version;
        return LOGICAL_TYPE_UNKNOWN;
    }

    static inline LogicalType to_storage_format_v2(LogicalType type) { return convert_to_format(type, kDataFormatV2); }

    static inline LogicalType to_storage_format_v1(LogicalType type) { return convert_to_format(type, kDataFormatV1); }
};

} // namespace starrocks
