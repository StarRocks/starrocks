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

// The macros in this file are elaborated to constructor 2-ary and 3-ary guard/table entries.
// Actually, at most 5-ary guard/table entries are also supported, as long as
// the length of arguments passed to META_MACRO_VA_ARGS_NUM_MINIS_{n} never exceeds 10.
// less-than-5-ary guard/table entries already suffice in practical perspective. If you want
// process args whose length is more than 10, META_MACRO_SELECT_21ST should be changed to accept
// more args and write down more macro cases whose name ends with _{0,...,1,2,..10}. macros in
// C++ is Turing-complete but is not self-recursive, so we must write down all cases, it's very
// tedious! anyway, enjoy!

// select 21st element from list whose length is not less than 21, this macro is used to
// construct macro series META_MACRO_VA_ARGS_NUM_MINIS_{n}.
// DO NOT CHANGE THIS MACRO!!!
#define META_MACRO_SELECT_21ST(a1, a2, a3, a4, a5, b6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, \
                               a20, a21, ...)                                                                        \
    a21

// META_MACRO_VA_ARGS_NUM_MINIS_{n} is used to compute length(args_list) - n
// i.e.
// META_MACRO_VA_ARGS_NUM_MINUS_0(a, b, c) produces 3
// META_MACRO_VA_ARGS_NUM_MINUS_1(a, b, c, d, e, f) produces 5
// META_MACRO_VA_ARGS_NUM_MINUS_3(a, b, c) produces 0
// META_MACRO_VA_ARGS_NUM_MINUS_4(a, b, c) illegal, preprocessor reporter errors.
#define META_MACRO_VA_ARGS_NUM_MINUS_0(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_1(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, 5, 6, 7, 8, 9, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_2(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, 5, 6, 7, 8, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_3(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, 5, 6, 7, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_4(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, 5, 6, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_5(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, 5, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_6(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, 4, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_7(...) \
    META_MACRO_SELECT_21ST(1, 2, 3, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_8(...) \
    META_MACRO_SELECT_21ST(1, 2, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define META_MACRO_VA_ARGS_NUM_MINUS_9(...) META_MACRO_SELECT_21ST(1, ##__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

// META_MACRO_VA_ARGS_NUM is short for META_MACRO_VA_ARGS_NUM_MINUS_0, it's used more frequently.
#define META_MACRO_VA_ARGS_NUM(...) META_MACRO_VA_ARGS_NUM_MINUS_0(__VA_ARGS__)

// META_MACRO_SELECT macro is used to pick another macro to process args list.
// In META_MACRO_SELECT(name, n, ...), first n args are fixed arguments, rest args are variadic
// args. the length of variadic args decide the macro case will be applied on this arguments.
// e.g.
// in META_MACRO_SELECT(PAIR_MACRO_, 1, a, b0, b1, b2), from n=1, we know that first 1 arg (a)
// is fixed arguments, (b0, b1, b2) are variadic arguments whose length is 3, so PAIR_MACRO_3
// will be applied to (a, b0, b1, b2), if PAIR_MACRO_3 defined as follows:
// #define PAIR_MACRO_4(a, b0, b1, b2) (a, b0), (a, b1), (a, b2).
// then will produce 3-element list of pair.
#define META_MACRO_SELECT_VA(name, num, ...) name##num(__VA_ARGS__)
#define META_MACRO_SELECT_VA_HELPER(name, num, ...) META_MACRO_SELECT_VA(name, num, ##__VA_ARGS__)
#define META_MACRO_SELECT(name, n, ...) \
    META_MACRO_SELECT_VA_HELPER(name, META_MACRO_VA_ARGS_NUM_MINUS_##n(__VA_ARGS__), ##__VA_ARGS__)

// META_MACRO_{n}_PART{1,2,3} is used to part an integer into 3 part.
// when define a macro cases, if we write down all cases, it's very tedious, so we use
// simple cases {macro_name}_{1,2,4} to define other compound cases {macro_name}_{3,5,6,7,8,9,10}
// we don't decompose 3 into (0, 1, 2) but (1, 1, 1) because empty __VA_ARGS__ cannot remove extra
// COMMA following it which causes the macro misbehaves.
// wo also give up iterating style because of its slowness, the iterating style is depicted:
// 1. {macro_name}_0 remove and process first arg, then pass rest args to {macro_name}_1.
// 2. then {macro_name}_1, {macro_name}_2, ..., {macro_name}_n do the same work as rest args is
// empty, and each {macro_name}_n should have to sub-cases, one for empty args list, the other
// for non-empty list.
#define META_MACRO_3_PART0 1
#define META_MACRO_3_PART1 1
#define META_MACRO_3_PART2 1

#define META_MACRO_5_PART0 1
#define META_MACRO_5_PART1 2
#define META_MACRO_5_PART2 2

#define META_MACRO_6_PART0 1
#define META_MACRO_6_PART1 1
#define META_MACRO_6_PART2 4

#define META_MACRO_7_PART0 1
#define META_MACRO_7_PART1 2
#define META_MACRO_7_PART2 4

#define META_MACRO_8_PART0 2
#define META_MACRO_8_PART1 2
#define META_MACRO_8_PART2 4

#define META_MACRO_9_PART0 1
#define META_MACRO_9_PART1 4
#define META_MACRO_9_PART2 4

#define META_MACRO_10_PART0 2
#define META_MACRO_10_PART1 4
#define META_MACRO_10_PART2 4

// META_MACRO_PART(n, m) is used instead of macro cases.
#define META_MACRO_PART(n, m) META_MACRO_##n##_PART##m

// META_MACRO_TAKE and META_MACRO_DROP is used to part an args list into two parts,
// then pick the former and latter part respectively.
#define META_MACRO_TAKE_0(...)
#define META_MACRO_TAKE_1(a1, ...) a1
#define META_MACRO_TAKE_2(a1, a2, ...) a1, a2
#define META_MACRO_TAKE_3(a1, a2, a3, ...) a1, a2, a3
#define META_MACRO_TAKE_4(a1, a2, a3, a4, ...) a1, a2, a3, a4
#define META_MACRO_TAKE_5(a1, a2, a3, a4, a5, ...) a1, a2, a3, a4, a5
#define META_MACRO_TAKE(n, ...) META_MACRO_TAKE_##n(__VA_ARGS__)

#define META_MACRO_DROP_0(...) __VA_ARGS__
#define META_MACRO_DROP_1(a1, ...) __VA_ARGS__
#define META_MACRO_DROP_2(a1, a2, ...) __VA_ARGS__
#define META_MACRO_DROP_3(a1, a2, a3, ...) __VA_ARGS__
#define META_MACRO_DROP_4(a1, a2, a3, a4, ...) __VA_ARGS__
#define META_MACRO_DROP_5(a1, a2, a3, a4, a5, ...) __VA_ARGS__
#define META_MACRO_DROP(n, ...) META_MACRO_DROP_##n(__VA_ARGS__)

// META_MACRO_CASE series used to define compound macro cases {macro_name}_{3,5,6,7,8,9,10}
// from simple cases {macro_name}_{1,2,4}.
// e.g. {macro_name}_10 is constructed from {macro_name}_{2,4,4}, in according to
// META_MACRO_PART(10,0), META_MACRO_PART(10,1) and META_MACRO_PART(10, 2)
#define META_MACRO_CASE_DEF_PART_HELPER2(name, ...) name(__VA_ARGS__)

#define META_MACRO_CASE_DEF_PART0_HELPER1(name, n, m0, m1, m2, ...)           \
    META_MACRO_CASE_DEF_PART_HELPER2(name, META_MACRO_TAKE(n, ##__VA_ARGS__), \
                                     META_MACRO_TAKE(m0, META_MACRO_DROP(n, ##__VA_ARGS__)))

#define META_MACRO_CASE_DEF_PART1_HELPER1(name, n, m0, m1, m2, ...)           \
    META_MACRO_CASE_DEF_PART_HELPER2(name, META_MACRO_TAKE(n, ##__VA_ARGS__), \
                                     META_MACRO_TAKE(m1, META_MACRO_DROP(m0, META_MACRO_DROP(n, ##__VA_ARGS__))))

#define META_MACRO_CASE_DEF_PART2_HELPER1(name, n, m0, m1, m2, ...)           \
    META_MACRO_CASE_DEF_PART_HELPER2(name, META_MACRO_TAKE(n, ##__VA_ARGS__), \
                                     META_MACRO_DROP(m1, META_MACRO_DROP(m0, META_MACRO_DROP(n, ##__VA_ARGS__))))

#define META_MACRO_CASE_DEF_DECOMPOSE_SEP_NONE_MARK(name, n, m, m0, m1, m2, ...) \
    META_MACRO_CASE_DEF_PART0_HELPER1(name##m0, n, m0, m1, m2, ##__VA_ARGS__)    \
    META_MACRO_CASE_DEF_PART1_HELPER1(name##m1, n, m0, m1, m2, ##__VA_ARGS__)    \
    META_MACRO_CASE_DEF_PART2_HELPER1(name##m2, n, m0, m1, m2, ##__VA_ARGS__)

#define META_MACRO_CASE_DEF_DECOMPOSE_SEP_SEMICOLON_MARK(name, n, m, m0, m1, m2, ...) \
    META_MACRO_CASE_DEF_PART0_HELPER1(name##m0, n, m0, m1, m2, ##__VA_ARGS__);        \
    META_MACRO_CASE_DEF_PART1_HELPER1(name##m1, n, m0, m1, m2, ##__VA_ARGS__);        \
    META_MACRO_CASE_DEF_PART2_HELPER1(name##m2, n, m0, m1, m2, ##__VA_ARGS__)

#define META_MACRO_CASE_DEF_DECOMPOSE_SEP_COMMA_MARK(name, n, m, m0, m1, m2, ...)      \
    META_MACRO_CASE_DEF_PART0_HELPER1(name##m0, n, m0, m1, m2, ##__VA_ARGS__),         \
            META_MACRO_CASE_DEF_PART1_HELPER1(name##m1, n, m0, m1, m2, ##__VA_ARGS__), \
            META_MACRO_CASE_DEF_PART2_HELPER1(name##m2, n, m0, m1, m2, ##__VA_ARGS__)

#define META_MACRO_CASE_DEF_DECOMPOSE_SEP_HELPER(name, sep_macro, n, m, m0, m1, m2, ...) \
    sep_macro(name, n, m, m0, m1, m2, ##__VA_ARGS__)

#define META_MACRO_CASE_DEF_DECOMPOSE_HELPER(name, sep, n, m, m0, m1, m2, ...)                                \
    META_MACRO_CASE_DEF_DECOMPOSE_SEP_HELPER(name, META_MACRO_CASE_DEF_DECOMPOSE_SEP_##sep, n, m, m0, m1, m2, \
                                             ##__VA_ARGS__)

#define META_MACRO_CASE_DEF_HELPER(name, sep, n, m, ...)                                                \
    META_MACRO_CASE_DEF_DECOMPOSE_HELPER(name, sep, n, m, META_MACRO_PART(m, 0), META_MACRO_PART(m, 1), \
                                         META_MACRO_PART(m, 2), ##__VA_ARGS__)

// META_MACRO_CASE_DEF is exported for user-defined macro, other macros in this series
// is for internal uses.
// META_MACRO_CASE_DEF(name, sep, n, ...)
// 1. name: name prefix of target macro case
// 2. sep: COMMA_MARK(,)|SEMICOLON_MARK(;)|NONE_MARK(), macro defined by this macro will
// produce list of items separated by sep.
// 3. n: first n args are fixed arguments.
// 4. ...: rest arguments, length of which will be name suffix of target macro case.
// e.g.
// META_MACRO_CASE_DEF(MACRO_TRIPLE_, SEMICOLON_MARK, 2, a, b, c0, c1, c2, c3) will apply
// MACRO_TRIPLE_4 to (a, b, c0, c1, c2, c3) and produce 4-element list separated by
// SEMICOLON_MARK(;), items is generated by a constructor MACRO_TRIPLE_1 which is applied to
// item list (a, b, c0), (a, b, c1), (a, b, c2), (c, b, c3).
// assume MACRO_TRIPLE_1 if defined as follow:
// #define MACRO_TRIPLE_1(a, b, c) template<> \
// foobar_struct<a, b, c> { static constexpr bool value = true;}
// then META_MACRO_CASE_DEF(MACRO_TRIPLE_, SEMICOLON_MARK, 2, int, float, double, int8_t, int16_t, int32_t)
// will produces:
// template <> foobar_struct<int, float, int8_t> { static constexpr bool value = true;};
// template <> foobar_struct<int, float, int16_t> { static constexpr bool value = true;};
// template <> foobar_struct<int, float, int32_t> { static constexpr bool value = true;};
#define META_MACRO_CASE_DEF(name, sep, n, ...) \
    META_MACRO_CASE_DEF_HELPER(name, sep, n, META_MACRO_VA_ARGS_NUM_MINUS_##n(__VA_ARGS__), ##__VA_ARGS__)

// DEF_BINARY_RELATION_ENTRY series is used to define macros that performs a binary constructor
// to a list of args, and the first 1 arg is fixed, and rest n-1 args are variadic. it's very
// useful to define 2-ary guard and 2-ary template and corresponding 2-ary function table entries.
// If you want to extend meta macros to 3-ary, 4-ary, even 4-ary, just reading these macros'
// definitions.

// perform ctor to (a, b), ORDER01 means args are arranged in a normal order.
#define DEF_BINARY_RELATION_ENTRY_1_ORDER01(ctor, a, b) ctor(a, b)

// similar to DEF_BINARY_RELATION_ENTRY_1_ORDER01, but swap (a, b), ORDER10 means
// args are arranged in a reverse
#define DEF_BINARY_RELATION_ENTRY_1_ORDER10(ctor, a, b) DEF_BINARY_RELATION_ENTRY_1_ORDER01(ctor, b, a)

// DEF_BINARY_RELATION_ENTRY_1_ORDER_HELPER is used to chose macro case between
// DEF_BINARY_RELATION_ENTRY_1_{ORDER01, ORDER10}
#define DEF_BINARY_RELATION_ENTRY_1_ORDER_HELPER(macro_name, ctor, a, b0) macro_name(ctor, a, b0)

// case 1 of DEF_BINARY_RELATION_ENTRY, sep is useless, but it must be provided
// for argument matching
#define DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b0) \
    DEF_BINARY_RELATION_ENTRY_1_ORDER_HELPER(DEF_BINARY_RELATION_ENTRY_1_##order, ctor, a, b0)

// case 2 of DEF_BINARY_RELATION_ENTRY is constructed from two case 1(s).
// separated by NONE_MARK()
#define DEF_BINARY_RELATION_ENTRY_2_SEP_NONE_MARK(order, ctor, sep, a, b0, b1) \
    DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b0)                       \
    DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b1)

// case 2 of DEF_BINARY_RELATION_ENTRY is constructed from two case 1(s).
// separated by SEMICOLON_MARK(;)
#define DEF_BINARY_RELATION_ENTRY_2_SEP_SEMICOLON_MARK(order, ctor, sep, a, b0, b1) \
    DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b0);                           \
    DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b1)

// case 2 of DEF_BINARY_RELATION_ENTRY is constructed from two case 1(s).
// separated by SEMICOLON_MARK(,)
#define DEF_BINARY_RELATION_ENTRY_2_SEP_COMMA_MARK(order, ctor, sep, a, b0, b1) \
    DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b0), DEF_BINARY_RELATION_ENTRY_1(order, ctor, sep, a, b1)

#define DEF_BINARY_RELATION_ENTRY_2_SEP_HELPER(order, ctor, sep, sep_macro, ...) \
    sep_macro(order, ctor, sep, ##__VA_ARGS__)

// case 2 of DEF_BINARY_RELATION_ENTRY
#define DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b0, b1) \
    DEF_BINARY_RELATION_ENTRY_2_SEP_HELPER(order, ctor, sep, DEF_BINARY_RELATION_ENTRY_2_SEP_##sep, a, b0, b1)

// case 4 of DEF_BINARY_RELATION_ENTRY is constructed from two case 2(s), it resembles to case 2.
// but we write down again just because macro not support self-recursive invocation.
#define DEF_BINARY_RELATION_ENTRY_4_SEP_NONE_MARK(order, ctor, sep, a, b0, b1, b2, b3) \
    DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b0, b1)                           \
    DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b2, b3)

#define DEF_BINARY_RELATION_ENTRY_4_SEP_SEMICOLON_MARK(order, ctor, sep, a, b0, b1, b2, b3) \
    DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b0, b1);                               \
    DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b2, b3)

#define DEF_BINARY_RELATION_ENTRY_4_SEP_COMMA_MARK(order, ctor, sep, a, b0, b1, b2, b3) \
    DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b0, b1), DEF_BINARY_RELATION_ENTRY_2(order, ctor, sep, a, b2, b3)

#define DEF_BINARY_RELATION_ENTRY_4_SEP_HELPER(order, ctor, sep, sep_macro, ...) \
    sep_macro(order, ctor, sep, ##__VA_ARGS__)

#define DEF_BINARY_RELATION_ENTRY_4(order, ctor, sep, a, b0, b1, b2, b3) \
    DEF_BINARY_RELATION_ENTRY_4_SEP_HELPER(order, ctor, sep, DEF_BINARY_RELATION_ENTRY_4_SEP_##sep, a, b0, b1, b2, b3)

// case 3, 5, 6, 7, 8, 9, 10 of DEF_BINARY_RELATION_ENTRY defined by META_MACRO_CASE_DEF
// NOTICE two sep(s) in args list is required, the first sep is processed by
// META_MACRO_CASE_DEF, the second sep is processed by DEF_BINARY_RELATION_ENTRY_{1, 2, 4}
#define DEF_BINARY_RELATION_ENTRY_3(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_5(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_6(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_7(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_8(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_9(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_10(order, ctor, sep, a, ...) \
    META_MACRO_CASE_DEF(DEF_BINARY_RELATION_ENTRY_, sep, 4, order, ctor, sep, a, ##__VA_ARGS__)

// helper macros for processing separator.
#define DEF_BINARY_RELATION_ENTRY_SEP(ctor, sep, a, ...) \
    META_MACRO_SELECT(DEF_BINARY_RELATION_ENTRY_, 4, ORDER01, ctor, sep, a, ##__VA_ARGS__)
#define DEF_BINARY_RELATION_ENTRY_SEP_R(ctor, sep, a, ...) \
    META_MACRO_SELECT(DEF_BINARY_RELATION_ENTRY_, 4, ORDER10, ctor, sep, a, ##__VA_ARGS__)

// these six macros following is export for external use.

// DEF_BINARY_RELATION_ENTRY_SEP_COMMA(ctor, a, b0, b1, b2,...) produces COMMA(,)-separated
// list whose elements are generated by perform ctor on (a, b0), (a, b1), (a, b2), ...
#define DEF_BINARY_RELATION_ENTRY_SEP_COMMA(ctor, a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP(ctor, COMMA_MARK, a, ##__VA_ARGS__)

// DEF_BINARY_RELATION_ENTRY_SEP_COMMA_R(ctor, a, b0, b1, b2,...) produces COMMA(,)-separated
// list whose elements are generated by perform ctor on (b0, a), (b1, a), (b2, a), ...
#define DEF_BINARY_RELATION_ENTRY_SEP_COMMA_R(ctor, a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_R(ctor, COMMA_MARK, a, ##__VA_ARGS__)

// DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON(ctor, a, b0, b1, b2,...) produces
// SEMICOLON(;)-separated list whose elements are generated by perform ctor
// on (a, b0), (a, b1), (a, b2), ...
#define DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON(ctor, a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP(ctor, SEMICOLON_MARK, a, ##__VA_ARGS__)

// DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(ctor, a, b0, b1, b2,...) produces
// SEMICOLON(;)-separated list whose elements are generated by perform ctor
// on (b0, a), (b1, a), (b2, a), ...
#define DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON_R(ctor, a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_R(ctor, SEMICOLON_MARK, a, ##__VA_ARGS__)

// DEF_BINARY_RELATION_ENTRY_SEP_NONE(ctor, a, b0, b1, b2,...) produces
// NONE()-separated list whose elements are generated by perform ctor
// on (a, b0), (a, b1), (a, b2), ...
#define DEF_BINARY_RELATION_ENTRY_SEP_NONE(ctor, a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP(ctor, NONE_MARK, a, ##__VA_ARGS__)

// DEF_BINARY_RELATION_ENTRY_SEP_NONE_R(ctor, a, b0, b1, b2,...) produces
// NONE()-separated list whose elements are generated by perform ctor
// on (b0, a), (b1, a), (b2, a), ...
#define DEF_BINARY_RELATION_ENTRY_SEP_NONE_R(ctor, a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_R(ctor, NONE_MARK, a, ##__VA_ARGS__)

// pair list processing

// META_MACRO_PAIR_LIST_MAP is used to perform ctor on each two-argument group and produces
// a list whose length is half of original arguments' length.
#define META_MACRO_PAIR_LIST_MAP_0(ctor)
#define META_MACRO_PAIR_LIST_MAP_2(ctor, a0, b0) ctor(a0, b0)
#define META_MACRO_PAIR_LIST_MAP_4(ctor, a0, b0, a1, b1) ctor(a0, b0), ctor(a1, b1)
#define META_MACRO_PAIR_LIST_MAP_6(ctor, a0, b0, a1, b1, a2, b2) ctor(a0, b0), ctor(a1, b1), ctor(a2, b2)
#define META_MACRO_PAIR_LIST_MAP_8(ctor, a0, b0, a1, b1, a2, b2, a3, b3) \
    ctor(a0, b0), ctor(a1, b1), ctor(a2, b2), ctor(a3, b3)
#define META_MACRO_PAIR_LIST_MAP_10(ctor, a0, b0, a1, b1, a2, b2, a3, b3, a4, b4) \
    ctor(a0, b0), ctor(a1, b1), ctor(a2, b2), ctor(a3, b3), ctor(a4, b4)

// META_MACRO_PAIR_LIST_{FIRST, SECOND, CONCAT_WS} is very useful to define macros involving
// templates, e.g. DEF_PRED_GUARD.

// META_MACRO_PAIR_LIST_FIRST is used to take first arg of two-argument group
// e.g.
// META_MACRO_PAIR_LIST_FIRST(a, b, c, d, e, f) produces (a, c, e)
#define META_MACRO_PAIR_FIRST(a, b) a
#define META_MACRO_PAIR_LIST_FIRST(...) \
    META_MACRO_SELECT(META_MACRO_PAIR_LIST_MAP_, 1, META_MACRO_PAIR_FIRST, ##__VA_ARGS__)

// META_MACRO_PAIR_LIST_SECOND is used to take second arg of two-argument group
// e.g.
// META_MACRO_PAIR_LIST_SECOND(a, b, c, d, e, f) produces (b, d, f)
#define META_MACRO_PAIR_SECOND(a, b) b
#define META_MACRO_PAIR_LIST_SECOND(...) \
    META_MACRO_SELECT(META_MACRO_PAIR_LIST_MAP_, 1, META_MACRO_PAIR_SECOND, ##__VA_ARGS__)

// META_MACRO_PAIR_LIST_CONCAT_WS is used to concat two-argument group by whitespace
// e.g.
// META_MACRO_PAIR_LIST_CONCAT_WS(a, b, c, d, e, f) produces (a b, c d, e f)
#define META_MACRO_PAIR_CONCAT_WS(a, b) a b
#define META_MACRO_PAIR_LIST_CONCAT_WS(...) \
    META_MACRO_SELECT(META_MACRO_PAIR_LIST_MAP_, 1, META_MACRO_PAIR_CONCAT_WS, ##__VA_ARGS__)
