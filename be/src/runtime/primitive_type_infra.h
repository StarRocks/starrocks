// This file is made available under Elastic License 2.0.

#pragma once

#include "common/logging.h"
#include "runtime/primitive_type.h"

namespace starrocks {

// type_dispatch_all:
// macro to enumate all types
#define TYPE_DISPATCH_ALL(TEMPLATE_FUNC, typeKind, ...)  \
    [&]() {                                              \
        switch (typeKind) {                              \
        case TYPE_INT:                                   \
            return TEMPLATE_FUNC<TYPE_INT>(__VA_ARGS__); \
        default:                                         \
            CHECK(false) << "TODO";                      \
        }                                                \
    }();

#define TYPE_DISPATCH_ALL_WITH_NULL(TEMPLATE_FUNC, typeKind, ...) \
    [&]() {                                                       \
        switch (typeKind) {                                       \
        case TYPE_INT:                                            \
            return TEMPLATE_FUNC<TYPE_INT>(__VA_ARGS__);          \
        case TYPE_NULL:                                           \
            return TEMPLATE_FUNC<TYPE_NULL>(__VA_ARGS__);         \
        default:                                                  \
            CHECK(false) << "TODO";                               \
        }                                                         \
    }();

// type_dispatch_all:
// Dispatch dynamic ptype to static template instance Functor
template <class Functor>
auto type_dispatch_all(PrimitiveType ptype, Functor fun) {
#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>();

    switch (ptype) {
        _TYPE_DISPATCH_CASE(TYPE_NULL)
        _TYPE_DISPATCH_CASE(TYPE_BOOLEAN)
        _TYPE_DISPATCH_CASE(TYPE_TINYINT)
        _TYPE_DISPATCH_CASE(TYPE_SMALLINT)
        _TYPE_DISPATCH_CASE(TYPE_INT)
        _TYPE_DISPATCH_CASE(TYPE_BIGINT)
        _TYPE_DISPATCH_CASE(TYPE_LARGEINT)
        _TYPE_DISPATCH_CASE(TYPE_FLOAT)
        _TYPE_DISPATCH_CASE(TYPE_DOUBLE)
        _TYPE_DISPATCH_CASE(TYPE_VARCHAR)
        _TYPE_DISPATCH_CASE(TYPE_DATE)
        _TYPE_DISPATCH_CASE(TYPE_DATETIME)
        // _TYPE_DISPATCH_CASE(TYPE_BINARY)
        // _TYPE_DISPATCH_CASE(TYPE_DECIMAL)
        _TYPE_DISPATCH_CASE(TYPE_CHAR)
        // _TYPE_DISPATCH_CASE(TYPE_STRUCT)
        // _TYPE_DISPATCH_CASE(TYPE_ARRAY)
        // _TYPE_DISPATCH_CASE(TYPE_MAP)
        _TYPE_DISPATCH_CASE(TYPE_HLL)
        _TYPE_DISPATCH_CASE(TYPE_TIME)
        _TYPE_DISPATCH_CASE(TYPE_OBJECT)
        _TYPE_DISPATCH_CASE(TYPE_PERCENTILE)
        _TYPE_DISPATCH_CASE(TYPE_DECIMALV2)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL32)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL64)
        _TYPE_DISPATCH_CASE(TYPE_DECIMAL128)
    default:
        LOG(FATAL) << "Unknown type " << ptype;
    }

#undef _TYPE_DISPATCH_CASE
}

#define APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_TINYINT)                     \
    M(TYPE_SMALLINT)                    \
    M(TYPE_INT)                         \
    M(TYPE_BIGINT)                      \
    M(TYPE_LARGEINT)                    \
    M(TYPE_FLOAT)                       \
    M(TYPE_DOUBLE)                      \
    M(TYPE_VARCHAR)                     \
    M(TYPE_CHAR)                        \
    M(TYPE_DATE)                        \
    M(TYPE_DATETIME)                    \
    M(TYPE_DECIMALV2)                   \
    M(TYPE_DECIMAL32)                   \
    M(TYPE_DECIMAL64)                   \
    M(TYPE_DECIMAL128)                  \
    M(TYPE_BOOLEAN)

#define APPLY_FOR_ALL_TYPE(M)       \
    APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_STRUCT)                  \
    M(TYPE_ARRAY)                   \
    M(TYPE_MAP)                     \
    M(TYPE_HLL)                     \
    M(TYPE_OBJECT)                  \
    M(TYPE_PERCENTILE)              \
    M(TYPE_DECIMAL)                 \
    M(TYPE_TIME)                    \
    M(TYPE_BINARY)

#define APPLY_FOR_SORTABLE_TYPE(M) \
    APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_TIME)                    \
    

#define APPLY_FOR_ALL_PRIMITIVE_TYPE_WITH_NULL(M) \
    APPLY_FOR_ALL_PRIMITIVE_TYPE(M)               \
    M(TYPE_NULL)

#define APPLY_FOR_ALL_THRIFT_TYPE(M) \
    M(BOOLEAN)                       \
    M(TINYINT)                       \
    M(SMALLINT)                      \
    M(INT)                           \
    M(BIGINT)                        \
    M(FLOAT)                         \
    M(DOUBLE)                        \
    M(DATE)                          \
    M(DATETIME)                      \
    M(BINARY)                        \
    M(DECIMAL)                       \
    M(CHAR)                          \
    M(LARGEINT)                      \
    M(VARCHAR)                       \
    M(HLL)                           \
    M(DECIMALV2)                     \
    M(TIME)                          \
    M(OBJECT)                        \
    M(PERCENTILE)                    \
    M(DECIMAL32)                     \
    M(DECIMAL64)                     \
    M(DECIMAL128)

} // namespace starrocks
