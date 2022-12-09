// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <type_traits>

namespace guard {

// guard is a static(compile-time) predicate for asserting that types from a category for values
// from a domain share common features and satisfy a conditional expressions. It's widely used
// in haskell for pattern matching, in C++, c++2a standard provides concept mechanism for mimic
// guard, but it is not widely adopted and poorly understandable. Guard here just employ the SFINAE
// idiom. Guard is very powerful metaprogramming utility.

using Guard = int;

template <typename T, typename... Args>
constexpr bool type_in = (std::is_same_v<T, Args> || ...);

template <typename T, T v, T... args>
constexpr bool value_in = ((v == args) || ...);

template <typename T, typename... Args>
using TypeGuard = std::enable_if_t<((std::is_same_v<T, Args>) || ...), guard::Guard>;

// TYPE_GUARD is used to define a type guard.
// for an example:
//
// TYPE_GUARD(DecimalArithmeticOp, is_decimal_arithmetic_op, AddOp, SubOp, MulOp, DivOp);
//
// This macro define a guard and a compile-time conditional expression.
//
// guard: DecimalArithmeticOp<T>,  it is guard::Guard(i.e. int) if T is AddOp|SubOp|MulOp|DivOp,
//  it is nothing(i.e. cpp template specialization matching failure) otherwise.
// conditional expression: is_decimal_arithmetic_op<T> return true if T is AddOp|SubOp|MulOp|DivOp,
//  return false otherwise.

#define TYPE_GUARD(guard_name, pred_name, ...)                          \
    template <typename T>                                               \
    struct pred_name##_struct {                                         \
        static constexpr bool value = guard::type_in<T, ##__VA_ARGS__>; \
    };                                                                  \
    template <typename T>                                               \
    constexpr bool pred_name = pred_name##_struct<T>::value;            \
    template <typename T>                                               \
    using guard_name = guard::TypeGuard<T, ##__VA_ARGS__>;

template <typename T, T v, T... args>
using ValueGuard = std::enable_if_t<((v == args) || ...), guard::Guard>;

// VALUE_GUARD is used to define a value guard on an enumeration type.
// for an example:

// enum TYPE {
//  TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
//  TYPE_FLOAT, TYPE_DOUBLE,
//  TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128};
//
// VALUE_GUARD(TYPE, IntegerTypeGuard, type_is_integer, TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG);
// VALUE_GUARD(TYPE, FloatingPointTypeGuard, type_is_floating_point, TYPE_FLOAT, TYPE_DOUBLE);
// VALUE_GUARD(TYPE, DecimalTypeGuard, type_is_decimal, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128);
//
// These macros define value guards and a compile-time conditional expressions.
// IntegerTypeGuard<TYPE_FLOAT> will trigger mismatch when resolving template specialization,
// type_is_floating_point<VALUE> return true if VALUE is TYPE_FLOAT or TYPE_DOUBLE, otherwise false.

#define VALUE_GUARD(type, guard_name, pred_name, ...)                          \
    template <type v>                                                          \
    struct pred_name##_struct {                                                \
        static constexpr bool value = guard::value_in<type, v, ##__VA_ARGS__>; \
    };                                                                         \
    template <type v>                                                          \
    constexpr bool pred_name = pred_name##_struct<v>::value;                   \
    template <type v>                                                          \
    using guard_name = guard::ValueGuard<type, v, ##__VA_ARGS__>;

template <typename T, template <typename> typename... TypePredicates>
struct OrTypePredicates {
    static constexpr bool value = ((TypePredicates<T>::value) || ...);
};

template <typename T, T v, template <T> typename... ValuePredicates>
struct OrValuePredicates {
    static constexpr bool value = ((ValuePredicates<v>::value) || ...);
};

template <typename T, template <typename> typename... TypePredicates>
constexpr bool type_union = OrTypePredicates<T, TypePredicates...>::value;
template <typename T, template <typename> typename... TypePredicates>
using TypeGuardUnion = std::enable_if_t<type_union<T, TypePredicates...>, Guard>;

template <typename T, T v, template <T> typename... ValuePredicates>
constexpr bool value_union = OrValuePredicates<T, v, ValuePredicates...>::value;
template <typename T, T v, template <T> typename... ValuePredicates>
using ValueGuardUnion = std::enable_if_t<value_union<T, v, ValuePredicates...>, Guard>;

// Combine several type guards into one new type guard.
// for an example:
// TYPE_GUARD(IntegerGuard, is_integer, int32_t, int64_t, int128_t);
// TYPE_GUARD(FloatGuard, is_floating, float, double);
// UNION_TYPE_GUARD(NumberGuard, is_number, is_integer_struct, is_floating_struct);

#define UNION_TYPE_GUARD(guard_name, pred_name, ...)                       \
    template <typename T>                                                  \
    struct pred_name##_struct {                                            \
        static constexpr bool value = guard::type_union<T, ##__VA_ARGS__>; \
    };                                                                     \
    template <typename T>                                                  \
    constexpr bool pred_name = pred_name##_struct<T>::value;               \
    template <typename T>                                                  \
    using guard_name = std::enable_if_t<pred_name<T>, guard::Guard>;

// Combine several value guards into a new value guard.
// for an example:
// UNION_VALUE_GUARD(TYPE, NumberTypeGuard, type_is_number,
//  type_is_integer_struct, type_is_floating_point_struct, type_is_decimal_struct);

#define UNION_VALUE_GUARD(type, guard_name, pred_name, ...)                       \
    template <type v>                                                             \
    struct pred_name##_struct {                                                   \
        static constexpr bool value = guard::value_union<type, v, ##__VA_ARGS__>; \
    };                                                                            \
    template <type v>                                                             \
    constexpr bool pred_name = pred_name##_struct<v>::value;                      \
    template <type v>                                                             \
    using guard_name = guard::ValueGuardUnion<type, v, ##__VA_ARGS__>;

} // namespace guard
