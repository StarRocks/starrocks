// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "util/guard.h"
#include "util/meta_macro.h"

// DEF_PRED_GUARD is useful to define {2,3,4,5}-arity guard.
// e.g.
//
// step#1: use DEF_PRED_GUARD produces codes as follows:
// ```code
// DEF_PRED_GUARD(DirectlyCopyable, is_directly_copyable, ArrowTypeId, AT, PrimitiveType, PT)
// ```
// produces codes as follows
//
// ```code
// template <ArrowTypeId, AT, PrimitiveType, PT>
// struct is_directly_copyable_struct {
//     static constexpr bool value = false;
// };
//
// template <ArrowTypeId, AT, PrimitiveType, PT>
// constexpr bool is_directly_copyable = is_directly_copyable_struct<AT, PT>::value;
//
// template <ArrowTypeId, AT, PrimitiveType, PT>
// using DirectlyCopyableGuard = std::enable_if_t<is_directly_copyable<AT, PT>, guard::Guard>;
// ```
//
// step#2: then, use DEF_PRED_CASE_CTOR define 2-ary constructor macro:
// ```code
// #define IS_DIRECTLY_COPYABLE_CTOR(a, b) = DEF_PRED_CASE_CTOR(is_directly_copyable, a, b)
// ```
// this definition will produces template specialization for is_directly_copyable_struct
//
// step#3: use DEF_BINARY_RELATION_ENTRY_SEP_NONE_R define a macro IS_DIRECTLY_COPYABLE can
// processing variadic arguments
// ```code
// #define IS_DIRECTLY_COPYABLE_R(AT, ...) \
// DEF_BINARY_RELATION_ENTRY_SEP_NONE_R(IS_DIRECTLY_COPYABLE_CTOR, AT, ##__VA_ARGS__)
// ```
// step#4: use IS_DIRECTLY_COPYABLE_R macro to add more 2-ary template specialization for
// is_directly_copyable_struct.
//
// ```code
// IS_DIRECTLY_COPYABLE(TYPE_TINYINT, ArrowTypeId::INT8, ArrowTypeId::UINT8);
// IS_DIRECTLY_COPYABLE(TYPE_SMALLINT, ArrowTypeId::INT16, ArrowTypeId::UINT16);
// IS_DIRECTLY_COPYABLE(TYPE_INT, ArrowTypeId::INT32, ArrowTypeId::UINT32);
// IS_DIRECTLY_COPYABLE(TYPE_BIGINT, ArrowTypeId::INT64, ArrowTypeId::UINT64);
// IS_DIRECTLY_COPYABLE(TYPE_FLOAT, ArrowTypeId::FLOAT);
// IS_DIRECTLY_COPYABLE(TYPE_DOUBLE, ArrowTypeId::DOUBLE);
// ```
// then we can used if constexpr(is_directly_copyable<AT, PT>) in template to select the
// the optimized branches in compile-time.
// And we can also used to DirectlyCopyableGuard to define template specialization to
// match (AT, PT) which satisfies is_directly_copyable predicate.
//
// More demos will be showed in src/exec/vectorized/arrow_converter.cpp in the PR following.

#define PRED_GUARD(guard_name, predicate, ...)             \
    template <META_MACRO_PAIR_LIST_CONCAT_WS(__VA_ARGS__)> \
    using guard_name = std::enable_if_t<predicate<META_MACRO_PAIR_LIST_SECOND(__VA_ARGS__)>, guard::Guard>;

#define DEF_PRED_GUARD(guard_name, pred_name, ...)                                                  \
    template <META_MACRO_PAIR_LIST_CONCAT_WS(__VA_ARGS__)>                                          \
    struct pred_name##_struct {                                                                     \
        static constexpr bool value = false;                                                        \
    };                                                                                              \
    template <META_MACRO_PAIR_LIST_CONCAT_WS(__VA_ARGS__)>                                          \
    constexpr bool pred_name = pred_name##_struct<META_MACRO_PAIR_LIST_SECOND(__VA_ARGS__)>::value; \
                                                                                                    \
    PRED_GUARD(guard_name, pred_name, ##__VA_ARGS__)

#define DEF_PRED_CASE_CTOR(pred_name, ...)   \
    template <>                              \
    struct pred_name##_struct<__VA_ARGS__> { \
        static constexpr bool value = true;  \
    };
