// This file is made available under Elastic License 2.0.

#pragma once

#include "storage/olap_common.h"

namespace starrocks {
    
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

// type_dispatch_all:
// Dispatch dynamic ptype to static template instance Functor
template <class Functor, class... Args>
auto field_type_dispatch_all(FieldType ftype, Functor fun, Args... args) {
#define _TYPE_DISPATCH_CASE(type) \
    case type: return fun.template operator()<type>(args...);

    switch (ftype) {
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_TINYINT)           \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_SMALLINT)          \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_INT)               \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_BIGINT)            \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_LARGEINT)          \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_FLOAT)             \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DOUBLE)            \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_CHAR)              \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DATE)              \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DATE_V2)           \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DATETIME)          \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_VARCHAR)           \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_BOOL)              \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DECIMAL)           \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DECIMAL32)         \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DECIMAL64)         \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DECIMAL128)        \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_TIMESTAMP)         \
    _TYPE_DISPATCH_CASE(OLAP_FIELD_TYPE_DECIMAL_V2)

    default:
        CHECK(false) << "Unknown type: " << ftype;
    }

#undef _TYPE_DISPATCH_CASE
}

} // namespace starrocks