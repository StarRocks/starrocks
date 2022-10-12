// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/olap_common.h"

// Infra to build the type system:
// 1. Macro `APPLY_FOR*` to build generic codes
// 2. field_type_dispatch* function dynamic dispatch types

namespace starrocks {

#define APPLY_FOR_TYPE_INTEGER(M) \
    M(OLAP_FIELD_TYPE_TINYINT)    \
    M(OLAP_FIELD_TYPE_SMALLINT)   \
    M(OLAP_FIELD_TYPE_BIGINT)     \
    M(OLAP_FIELD_TYPE_LARGEINT)   \
    M(OLAP_FIELD_TYPE_INT)

#define APPLY_FOR_TYPE_DECIMAL(M) \
    M(OLAP_FIELD_TYPE_DECIMAL)    \
    M(OLAP_FIELD_TYPE_DECIMAL_V2) \
    M(OLAP_FIELD_TYPE_DECIMAL32)  \
    M(OLAP_FIELD_TYPE_DECIMAL64)  \
    M(OLAP_FIELD_TYPE_DECIMAL128)

#define APPLY_FOR_TYPE_TIME(M)  \
    M(OLAP_FIELD_TYPE_DATE)     \
    M(OLAP_FIELD_TYPE_DATE_V2)  \
    M(OLAP_FIELD_TYPE_DATETIME) \
    M(OLAP_FIELD_TYPE_TIMESTAMP)

// Types that suport bitmap index
#define APPLY_FOR_BITMAP_INDEX_TYPE(M) \
    APPLY_FOR_TYPE_INTEGER(M)          \
    APPLY_FOR_TYPE_TIME(M)             \
    M(OLAP_FIELD_TYPE_UNSIGNED_INT)    \
    M(OLAP_FIELD_TYPE_FLOAT)           \
    M(OLAP_FIELD_TYPE_DOUBLE)          \
    M(OLAP_FIELD_TYPE_CHAR)            \
    M(OLAP_FIELD_TYPE_VARCHAR)         \
    M(OLAP_FIELD_TYPE_BOOL)            \
    M(OLAP_FIELD_TYPE_DECIMAL)         \
    M(OLAP_FIELD_TYPE_DECIMAL_V2)

// Types that support bloomfilter(exclude tinyint/float/double)
#define APPLY_FOR_BLOOMFILTER_TYPE(M) \
    APPLY_FOR_TYPE_INTEGER(M)         \
    APPLY_FOR_TYPE_DECIMAL(M)         \
    APPLY_FOR_TYPE_TIME(M)            \
    M(OLAP_FIELD_TYPE_UNSIGNED_INT)   \
    M(OLAP_FIELD_TYPE_CHAR)           \
    M(OLAP_FIELD_TYPE_VARCHAR)

// These types should be synced with FieldType in olap_common.h
#define APPLY_FOR_BASIC_OLAP_FIELD_TYPE(M) \
    APPLY_FOR_BITMAP_INDEX_TYPE(M)         \
    M(OLAP_FIELD_TYPE_JSON)                \
    M(OLAP_FIELD_TYPE_DECIMAL32)           \
    M(OLAP_FIELD_TYPE_DECIMAL64)           \
    M(OLAP_FIELD_TYPE_DECIMAL128)

#define APPLY_FOR_UNSIGN_OLAP_FIELD_TYPE(M) \
    M(OLAP_FIELD_TYPE_UNSIGNED_INT)         \
    M(OLAP_FIELD_TYPE_UNSIGNED_TINYINT)     \
    M(OLAP_FIELD_TYPE_UNSIGNED_SMALLINT)    \
    M(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)

#define APPLY_FOR_COMPLEX_OLAP_FIELD_TYPE(M) \
    M(OLAP_FIELD_TYPE_HLL)                   \
    M(OLAP_FIELD_TYPE_STRUCT)                \
    M(OLAP_FIELD_TYPE_MAP)                   \
    M(OLAP_FIELD_TYPE_OBJECT)                \
    M(OLAP_FIELD_TYPE_PERCENTILE)

#define APPLY_FOR_METRIC_FIELD_TYPE(M) \
    M(OLAP_FIELD_TYPE_HLL)             \
    M(OLAP_FIELD_TYPE_OBJECT)          \
    M(OLAP_FIELD_TYPE_PERCENTILE)

#define APPLY_FOR_SUPPORTED_FIELD_TYPE(M) \
    APPLY_FOR_BASIC_OLAP_FIELD_TYPE(M)    \
    M(OLAP_FIELD_TYPE_UNSIGNED_TINYINT)   \
    M(OLAP_FIELD_TYPE_UNSIGNED_SMALLINT)  \
    M(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)    \
    M(OLAP_FIELD_TYPE_HLL)                \
    M(OLAP_FIELD_TYPE_OBJECT)             \
    M(OLAP_FIELD_TYPE_PERCENTILE)

#define APPLY_FOR_EXTRA_OLAP_FIELD_TYPE(M) \
    APPLY_FOR_UNSIGN_OLAP_FIELD_TYPE(M)    \
    APPLY_FOR_COMPLEX_OLAP_FIELD_TYPE(M)   \
    M(OLAP_FIELD_TYPE_DISCRETE_DOUBLE)     \
    M(OLAP_FIELD_TYPE_ARRAY)

#define APPLY_FOR_TYPE_CONVERT_FROM_VARCHAR(M) \
    APPLY_FOR_TYPE_INTEGER(M)                  \
    APPLY_FOR_TYPE_TIME(M)                     \
    APPLY_FOR_TYPE_DECIMAL(M)                  \
    M(OLAP_FIELD_TYPE_FLOAT)                   \
    M(OLAP_FIELD_TYPE_DOUBLE)                  \
    M(OLAP_FIELD_TYPE_JSON)                    \
    M(OLAP_FIELD_TYPE_BOOL)

#define APPLY_FOR_TYPE_CONVERT_TO_VARCHAR(M) \
    APPLY_FOR_TYPE_INTEGER(M)                \
    APPLY_FOR_TYPE_TIME(M)                   \
    APPLY_FOR_TYPE_DECIMAL(M)                \
    M(OLAP_FIELD_TYPE_FLOAT)                 \
    M(OLAP_FIELD_TYPE_JSON)                  \
    M(OLAP_FIELD_TYPE_DOUBLE)

#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

template <class Functor, class... Args>
auto field_type_dispatch_basic(FieldType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_OLAP_FIELD_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "unknown type " << ftype;
        __builtin_unreachable();
    }
}

// Types could built into columns
template <class Functor, class... Args>
auto field_type_dispatch_column(FieldType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_OLAP_FIELD_TYPE(_TYPE_DISPATCH_CASE)
        APPLY_FOR_METRIC_FIELD_TYPE(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_ARRAY)
    default:
        CHECK(false) << "unknown type " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_all_extra(FieldType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_OLAP_FIELD_TYPE(_TYPE_DISPATCH_CASE)
        APPLY_FOR_COMPLEX_OLAP_FIELD_TYPE(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DISCRETE_DOUBLE)
        _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_ARRAY)
        _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_UNSIGNED_TINYINT)
        _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_UNSIGNED_SMALLINT)
        _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)
    default:
        CHECK(false) << "Unknown type: " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_bitmap_index(FieldType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BITMAP_INDEX_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unsupported type for bitmap: " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_bloomfilter(FieldType ftype, Functor fun, Args... args) {
    // tinyint is not supported specially
    if (ftype == OLAP_FIELD_TYPE_TINYINT) {
        return Status::NotSupported("unsupported type for bloom filter: " + std::to_string(ftype));
    }
    switch (ftype) {
        APPLY_FOR_BLOOMFILTER_TYPE(_TYPE_DISPATCH_CASE)
    default:
        return Status::NotSupported("unsupported type for bloom filter: " + std::to_string(ftype));
    }
    return Status::OK();
}

template <class Functor, class... Args>
auto field_type_dispatch_zonemap_index(FieldType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_BASIC_OLAP_FIELD_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unsupported type for bitmap: " << ftype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto field_type_dispatch_supported(FieldType ftype, Functor fun, Args... args) {
    switch (ftype) {
        APPLY_FOR_SUPPORTED_FIELD_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unsupported type: " << ftype;
        __builtin_unreachable();
    }
}

#undef _TYPE_DISPATCH_CASE

} // namespace starrocks