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
    static inline bool specific_type_of_format_v1(FieldType type) {
        return type == OLAP_FIELD_TYPE_DATE || type == OLAP_FIELD_TYPE_DATETIME || type == OLAP_FIELD_TYPE_DECIMAL;
    }

    static inline bool specific_type_of_format_v2(FieldType type) {
        return type == OLAP_FIELD_TYPE_DATE_V2 || type == OLAP_FIELD_TYPE_TIMESTAMP ||
               type == OLAP_FIELD_TYPE_DECIMAL_V2;
    }

    static inline size_t estimate_field_size(FieldType type, size_t variable_length) {
        switch (type) {
        case OLAP_FIELD_TYPE_UNKNOWN:
        case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        case OLAP_FIELD_TYPE_STRUCT:
        case OLAP_FIELD_TYPE_MAP:
        case OLAP_FIELD_TYPE_NONE:
        case OLAP_FIELD_TYPE_MAX_VALUE:
        case OLAP_FIELD_TYPE_BOOL:
        case OLAP_FIELD_TYPE_TINYINT:
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            return 1;
        case OLAP_FIELD_TYPE_SMALLINT:
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
            return 2;
        case OLAP_FIELD_TYPE_DATE:
            return 3;
        case OLAP_FIELD_TYPE_INT:
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
        case OLAP_FIELD_TYPE_FLOAT:
        case OLAP_FIELD_TYPE_DATE_V2:
        case OLAP_FIELD_TYPE_DECIMAL32:
            return 4;
        case OLAP_FIELD_TYPE_BIGINT:
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        case OLAP_FIELD_TYPE_DOUBLE:
        case OLAP_FIELD_TYPE_DATETIME:
        case OLAP_FIELD_TYPE_TIMESTAMP:
        case OLAP_FIELD_TYPE_DECIMAL64:
            return 8;
        case OLAP_FIELD_TYPE_DECIMAL:
            return 12;
        case OLAP_FIELD_TYPE_LARGEINT:
        case OLAP_FIELD_TYPE_DECIMAL_V2:
        case OLAP_FIELD_TYPE_DECIMAL128:
            return 16;
        default:
            // CHAR, VARCHAR, HLL, PERCENTILE, JSON, ARRAY, OBJECT
            return variable_length;
        }
    }

    static inline FieldType convert_to_format(FieldType type, DataFormatVersion format_version) {
        if (format_version == kDataFormatV2) {
            switch (type) {
            case OLAP_FIELD_TYPE_DATE:
                return OLAP_FIELD_TYPE_DATE_V2;
            case OLAP_FIELD_TYPE_DATETIME:
                return OLAP_FIELD_TYPE_TIMESTAMP;
            case OLAP_FIELD_TYPE_DECIMAL:
                return OLAP_FIELD_TYPE_DECIMAL_V2;
            case OLAP_FIELD_TYPE_UNKNOWN:
            case OLAP_FIELD_TYPE_TINYINT:
            case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            case OLAP_FIELD_TYPE_SMALLINT:
            case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
            case OLAP_FIELD_TYPE_INT:
            case OLAP_FIELD_TYPE_UNSIGNED_INT:
            case OLAP_FIELD_TYPE_BIGINT:
            case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            case OLAP_FIELD_TYPE_LARGEINT:
            case OLAP_FIELD_TYPE_FLOAT:
            case OLAP_FIELD_TYPE_DOUBLE:
            case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            case OLAP_FIELD_TYPE_CHAR:
            case OLAP_FIELD_TYPE_VARCHAR:
            case OLAP_FIELD_TYPE_STRUCT:
            case OLAP_FIELD_TYPE_ARRAY:
            case OLAP_FIELD_TYPE_MAP:
            case OLAP_FIELD_TYPE_NONE:
            case OLAP_FIELD_TYPE_HLL:
            case OLAP_FIELD_TYPE_BOOL:
            case OLAP_FIELD_TYPE_OBJECT:
            case OLAP_FIELD_TYPE_DECIMAL32:
            case OLAP_FIELD_TYPE_DECIMAL64:
            case OLAP_FIELD_TYPE_DECIMAL128:
            case OLAP_FIELD_TYPE_DATE_V2:
            case OLAP_FIELD_TYPE_TIMESTAMP:
            case OLAP_FIELD_TYPE_DECIMAL_V2:
            case OLAP_FIELD_TYPE_PERCENTILE:
            case OLAP_FIELD_TYPE_JSON:
            case OLAP_FIELD_TYPE_MAX_VALUE:
                return type;
                // no default by intention.
            }
        } else if (format_version == kDataFormatV1) {
            switch (type) {
            case OLAP_FIELD_TYPE_DATE_V2:
                return OLAP_FIELD_TYPE_DATE;
            case OLAP_FIELD_TYPE_TIMESTAMP:
                return OLAP_FIELD_TYPE_DATETIME;
            case OLAP_FIELD_TYPE_DECIMAL_V2:
                return OLAP_FIELD_TYPE_DECIMAL;
            case OLAP_FIELD_TYPE_UNKNOWN:
            case OLAP_FIELD_TYPE_NONE:
            case OLAP_FIELD_TYPE_STRUCT:
            case OLAP_FIELD_TYPE_ARRAY:
            case OLAP_FIELD_TYPE_MAP:
            case OLAP_FIELD_TYPE_DECIMAL32:
            case OLAP_FIELD_TYPE_DECIMAL64:
            case OLAP_FIELD_TYPE_DECIMAL128:
                return OLAP_FIELD_TYPE_UNKNOWN;
            case OLAP_FIELD_TYPE_TINYINT:
            case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            case OLAP_FIELD_TYPE_SMALLINT:
            case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
            case OLAP_FIELD_TYPE_INT:
            case OLAP_FIELD_TYPE_UNSIGNED_INT:
            case OLAP_FIELD_TYPE_BIGINT:
            case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            case OLAP_FIELD_TYPE_LARGEINT:
            case OLAP_FIELD_TYPE_FLOAT:
            case OLAP_FIELD_TYPE_DOUBLE:
            case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            case OLAP_FIELD_TYPE_CHAR:
            case OLAP_FIELD_TYPE_DATE:
            case OLAP_FIELD_TYPE_DATETIME:
            case OLAP_FIELD_TYPE_DECIMAL:
            case OLAP_FIELD_TYPE_VARCHAR:
            case OLAP_FIELD_TYPE_HLL:
            case OLAP_FIELD_TYPE_BOOL:
            case OLAP_FIELD_TYPE_PERCENTILE:
            case OLAP_FIELD_TYPE_JSON:
            case OLAP_FIELD_TYPE_MAX_VALUE:
            case OLAP_FIELD_TYPE_OBJECT:
                return type;
                // no default by intention.
            }
        }
        DCHECK(false) << "unknown format version " << format_version;
        return OLAP_FIELD_TYPE_UNKNOWN;
    }

    static inline FieldType to_storage_format_v2(FieldType type) { return convert_to_format(type, kDataFormatV2); }

    static inline FieldType to_storage_format_v1(FieldType type) { return convert_to_format(type, kDataFormatV1); }
};

} // namespace starrocks
