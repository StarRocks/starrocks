// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/logging.h"
#include "runtime/primitive_type.h"

// Infra to build the type system:
// 1. Macro `APPLY_FOR*` to build generic codes
// 2. Macro `TYPE_DISPATCH*` to dispatch type
// 2. Function `field_type_dispatch*` to dynamic dispatch with better customlization

namespace starrocks {

#define APPLY_FOR_ALL_NUMBER_TYPE(M) \
    M(TYPE_TINYINT)                  \
    M(TYPE_SMALLINT)                 \
    M(TYPE_INT)                      \
    M(TYPE_BIGINT)                   \
    M(TYPE_LARGEINT)                 \
    M(TYPE_FLOAT)                    \
    M(TYPE_DOUBLE)                   \
    M(TYPE_DECIMAL32)                \
    M(TYPE_DECIMAL64)                \
    M(TYPE_DECIMAL128)

#define APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    APPLY_FOR_ALL_NUMBER_TYPE(M)        \
    M(TYPE_DECIMALV2)                   \
    M(TYPE_VARCHAR)                     \
    M(TYPE_CHAR)                        \
    M(TYPE_DATE)                        \
    M(TYPE_DATETIME)                    \
    M(TYPE_BOOLEAN)

#define APPLY_FOR_ALL_CAST_TYPE(M)  \
    APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_TIME)

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

#define APPLY_FOR_SORTABLE_TYPE(M)  \
    APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_TIME)

#define APPLY_FOR_ALL_PRIMITIVE_TYPE_WITH_NULL(M) \
    APPLY_FOR_ALL_PRIMITIVE_TYPE(M)               \
    M(TYPE_NULL)

#define APPLY_FOR_COMPLEX_THRIFT_TYPE(M) \
    M(STRUCT)                            \
    M(MAP)                               \
    M(ARRAY)

#define APPLY_FOR_SCALAR_THRIFT_TYPE(M) \
    M(BOOLEAN)                          \
    M(TINYINT)                          \
    M(SMALLINT)                         \
    M(INT)                              \
    M(BIGINT)                           \
    M(FLOAT)                            \
    M(DOUBLE)                           \
    M(DATE)                             \
    M(DATETIME)                         \
    M(BINARY)                           \
    M(DECIMAL)                          \
    M(CHAR)                             \
    M(LARGEINT)                         \
    M(VARCHAR)                          \
    M(HLL)                              \
    M(DECIMALV2)                        \
    M(TIME)                             \
    M(OBJECT)                           \
    M(PERCENTILE)                       \
    M(DECIMAL32)                        \
    M(DECIMAL64)                        \
    M(DECIMAL128)

#define TYPE_DISPATCH_CAST_TYPE(TEMPLATEF_FUNC, typeFrom, typeTo, ...)        \
    [&]() {                                                                   \
        switch (typeFrom) {                                                   \
            TYPE_DISPATCH_PREDICATE_TYPE(TEMPLATE_FUNC, typeTo, __VA_ARGS__); \
        default:;                                                             \
        }                                                                     \
    }

#define TYPE_DISPATCH_ALL_WITH_NULL(TEMPLATE_FUNC, typeKind, ...)             \
    [&]() {                                                                   \
        switch (typeKind) {                                                   \
            TYPE_DISPTACH_IN_PREDIACTE(TEMPLATE_FUNC, typeKind, __VA_ARGS__); \
        case TYPE_NULL:                                                       \
            return TEMPLATE_FUNC<TYPE_NULL>(__VA_ARGS__);                     \
        default:                                                              \
            CHECK(false) << "Unknown type: " << typeKind;                     \
        }                                                                     \
    }();

#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

// type_dispatch_all:
// Dispatch dynamic ptype to static template instance Functor
template <class Functor, class... Args>
auto type_dispatch_all(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_PRIMITIVE_TYPE_WITH_NULL(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
    }
}

template <class Functor, class... Args>
auto type_dispatch_sortable(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_SORTABLE_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
    }
}

template <class Functor, class... Args>
auto type_dispatch_predicate(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_PRIMITIVE_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
    }
}

template <class Functor, class... Args>
auto type_dispatch_filter(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_PRIMITIVE_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
    }
}

#undef _TYPE_DISPATCH_CASE

} // namespace starrocks
