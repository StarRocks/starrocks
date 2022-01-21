// This file is made available under Elastic License 2.0.

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

// TYPE_DISPATCH_PREDICATE_TYPE
// macro to enumate all types
#define TYPE_DISPATCH_PREDICATE_TYPE(TEMPLATE_FUNC, typeKind, ...) \
    [&]() {                                                        \
        switch (typeKind) {                                        \
        case TYPE_INT:                                             \
            return TEMPLATE_FUNC<TYPE_INT>(__VA_ARGS__);           \
        case TYPE_TINYINT:                                         \
            return TEMPLATE_FUNC<TYPE_TINYINT>(__VA_ARGS__);       \
        case TYPE_SMALLINT:                                        \
            return TEMPLATE_FUNC<TYPE_SMALLINT>(__VA_ARGS__);      \
        case TYPE_LARGEINT:                                        \
            return TEMPLATE_FUNC<TYPE_LARGEINT>(__VA_ARGS__);      \
        case TYPE_BIGINT:                                          \
            return TEMPLATE_FUNC<TYPE_BIGINT>(__VA_ARGS__);        \
        case TYPE_BOOLEAN:                                         \
            return TEMPLATE_FUNC<TYPE_BOOLEAN>(__VA_ARGS__);       \
        case TYPE_CHAR:                                            \
            return TEMPLATE_FUNC<TYPE_CHAR>(__VA_ARGS__);          \
        case TYPE_VARCHAR:                                         \
            return TEMPLATE_FUNC<TYPE_VARCHAR>(__VA_ARGS__);       \
        case TYPE_FLOAT:                                           \
            return TEMPLATE_FUNC<TYPE_FLOAT>(__VA_ARGS__);         \
        case TYPE_DOUBLE:                                          \
            return TEMPLATE_FUNC<TYPE_DOUBLE>(__VA_ARGS__);        \
        case TYPE_DECIMALV2:                                       \
            return TEMPLATE_FUNC<TYPE_DECIMALV2>(__VA_ARGS__);     \
        case TYPE_DECIMAL32:                                       \
            return TEMPLATE_FUNC<TYPE_DECIMAL32>(__VA_ARGS__);     \
        case TYPE_DECIMAL64:                                       \
            return TEMPLATE_FUNC<TYPE_DECIMAL64>(__VA_ARGS__);     \
        case TYPE_DECIMAL128:                                      \
            return TEMPLATE_FUNC<TYPE_DECIMAL128>(__VA_ARGS__);    \
        case TYPE_HLL:                                             \
            return TEMPLATE_FUNC<TYPE_HLL>(__VA_ARGS__);           \
        case TYPE_PERCENTILE:                                      \
            return TEMPLATE_FUNC<TYPE_PERCENTILE>(__VA_ARGS__);    \
        case TYPE_DATE:                                            \
            return TEMPLATE_FUNC<TYPE_DATE>(__VA_ARGS__);          \
        case TYPE_TIME:                                            \
            return TEMPLATE_FUNC<TYPE_TIME>(__VA_ARGS__);          \
        case TYPE_DATETIME:                                        \
            return TEMPLATE_FUNC<TYPE_DATETIME>(__VA_ARGS__);      \
        default:                                                   \
            CHECK(false) << "Unknown type: " << typeKind;          \
            __builtin_unreachable();                               \
        }                                                          \
    }();

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

// type_dispatch_all:
// Dispatch dynamic ptype to static template instance Functor
template <class Functor, class... Args>
auto type_dispatch_all(PrimitiveType ptype, Functor fun, Args... args) {
#define _TYPE_DISPATCH_CASE(type) \
    case type:                    \
        return fun.template operator()<type>(args...);

    switch (ptype) {
        APPLY_FOR_ALL_PRIMITIVE_TYPE_WITH_NULL(_TYPE_DISPATCH_CASE)
    default:
        CHECK(false) << "Unknown type: " << ptype;
    }

#undef _TYPE_DISPATCH_CASE
}

} // namespace starrocks
