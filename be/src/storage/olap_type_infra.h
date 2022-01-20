// This file is made available under Elastic License 2.0.

#pragma once

#include "storage/olap_common.h"

// These types should be synced with FieldType in olap_common.h
#define APPLY_FOR_BASIC_OLAP_FIELD_TYPE(M) \
    M(OLAP_FIELD_TYPE_TINYINT)           \
    M(OLAP_FIELD_TYPE_SMALLINT)          \
    M(OLAP_FIELD_TYPE_INT)               \
    M(OLAP_FIELD_TYPE_BIGINT)            \
    M(OLAP_FIELD_TYPE_LARGEINT)          \
    M(OLAP_FIELD_TYPE_FLOAT)             \
    M(OLAP_FIELD_TYPE_DOUBLE)            \
    M(OLAP_FIELD_TYPE_CHAR)              \
    M(OLAP_FIELD_TYPE_DATE)              \
    M(OLAP_FIELD_TYPE_DATE_V2)           \
    M(OLAP_FIELD_TYPE_DATETIME)          \
    M(OLAP_FIELD_TYPE_VARCHAR)           \
    M(OLAP_FIELD_TYPE_BOOL)              \
    M(OLAP_FIELD_TYPE_DECIMAL)           \
    M(OLAP_FIELD_TYPE_DECIMAL32)         \
    M(OLAP_FIELD_TYPE_DECIMAL64)         \
    M(OLAP_FIELD_TYPE_DECIMAL128)        \
    M(OLAP_FIELD_TYPE_TIMESTAMP)         \
    M(OLAP_FIELD_TYPE_DECIMAL_V2)
