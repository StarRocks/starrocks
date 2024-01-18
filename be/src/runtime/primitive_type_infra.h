// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/logging.h"
#include "runtime/primitive_type.h"

// Infra to build the type system:
// 1. Macro `APPLY_FOR*` to build generic codes
// 2. Function `field_type_dispatch*` to dynamic dispatch with better customlization

namespace starrocks {

#define APPLY_FOR_ALL_INT_TYPE(M) \
    M(TYPE_TINYINT)               \
    M(TYPE_SMALLINT)              \
    M(TYPE_INT)                   \
    M(TYPE_BIGINT)                \
    M(TYPE_LARGEINT)

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

#define APPLY_FOR_ALL_SCALAR_TYPE(M) \
    APPLY_FOR_ALL_NUMBER_TYPE(M)     \
    M(TYPE_DECIMALV2)                \
    M(TYPE_VARCHAR)                  \
    M(TYPE_CHAR)                     \
    M(TYPE_DATE)                     \
    M(TYPE_DATETIME)                 \
    M(TYPE_TIME)                     \
    M(TYPE_JSON)                     \
    M(TYPE_BOOLEAN)

#define APPLY_FOR_ALL_STRING_TYPE(M) \
    M(TYPE_VARCHAR)                  \
    M(TYPE_CHAR)                     \
    M(TYPE_BINARY)

#define APPLY_FOR_ALL_SCALAR_TYPE_WITH_NULL(M) \
    APPLY_FOR_ALL_SCALAR_TYPE(M)               \
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
    M(DECIMAL128)                       \
    M(FUNCTION)                         \
    M(JSON)

#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

// Aggregate types
template <class Functor, class... Args>
auto type_dispatch_aggregate(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_NUMBER_TYPE(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(TYPE_BOOLEAN)
        _TYPE_DISPATCH_CASE(TYPE_DATE)
        _TYPE_DISPATCH_CASE(TYPE_DATETIME)
        _TYPE_DISPATCH_CASE(TYPE_DECIMALV2)
    default:
        CHECK(false) << "Unknown type: " << ptype;
        __builtin_unreachable();
    }
}

// type_dispatch_*:
template <class Functor, class... Args>
auto type_dispatch_basic(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_SCALAR_TYPE_WITH_NULL(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
        __builtin_unreachable();
    }
}

template <class Functor, class... Args>
auto type_dispatch_all(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_SCALAR_TYPE_WITH_NULL(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(TYPE_ARRAY)
        _TYPE_DISPATCH_CASE(TYPE_HLL)
        _TYPE_DISPATCH_CASE(TYPE_OBJECT)
        _TYPE_DISPATCH_CASE(TYPE_PERCENTILE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
        __builtin_unreachable();
    }
}

// Types could build into columns
template <class Functor, class... Args>
auto type_dispatch_column(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_SCALAR_TYPE_WITH_NULL(_TYPE_DISPATCH_CASE)
        _TYPE_DISPATCH_CASE(TYPE_HLL)
        _TYPE_DISPATCH_CASE(TYPE_OBJECT)
        _TYPE_DISPATCH_CASE(TYPE_PERCENTILE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
        __builtin_unreachable();
    }
}

// Types which are sortable
template <class Functor, class... Args>
auto type_dispatch_sortable(PrimitiveType ptype, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_SCALAR_TYPE(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
        __builtin_unreachable();
    }
}

template <class Ret, class Functor, class... Args>
Ret type_dispatch_predicate(PrimitiveType ptype, bool assert, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_SCALAR_TYPE(_TYPE_DISPATCH_CASE)
    default:
        if (assert) {
            CHECK(false) << "Unknown type: " << ptype;
            __builtin_unreachable();
        } else {
            return Ret{};
        }
    }
}

template <class Functor, class Ret, class... Args>
auto type_dispatch_filter(PrimitiveType ptype, Ret default_value, Functor fun, Args... args) {
    switch (ptype) {
        APPLY_FOR_ALL_SCALAR_TYPE(_TYPE_DISPATCH_CASE)
    default:
        return default_value;
    }
}

#undef _TYPE_DISPATCH_CASE

} // namespace starrocks
