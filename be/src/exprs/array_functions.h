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

#include "exprs/array_functions.tpp"

namespace starrocks {

class ArrayFunctions {
public:
    DEFINE_VECTORIZED_FN(array_length);

    DEFINE_VECTORIZED_FN(array_ndims);

    DEFINE_VECTORIZED_FN(array_append);

    DEFINE_VECTORIZED_FN(array_remove);

    DEFINE_VECTORIZED_FN(array_contains);
    DEFINE_VECTORIZED_FN(array_position);

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_distinct(FunctionContext* context, const Columns& columns) {
        return ArrayDistinct<type>::process(context, columns);
    }

#define APPLY_COMMONE_TYPES_FOR_ARRAY(M)      \
    M(boolean, LogicalType::TYPE_BOOLEAN)     \
    M(tinyint, LogicalType::TYPE_TINYINT)     \
    M(smallint, LogicalType::TYPE_SMALLINT)   \
    M(int, LogicalType::TYPE_INT)             \
    M(bigint, LogicalType::TYPE_BIGINT)       \
    M(largeint, LogicalType::TYPE_LARGEINT)   \
    M(float, LogicalType::TYPE_FLOAT)         \
    M(double, LogicalType::TYPE_DOUBLE)       \
    M(varchar, LogicalType::TYPE_VARCHAR)     \
    M(char, LogicalType::TYPE_CHAR)           \
    M(decimalv2, LogicalType::TYPE_DECIMALV2) \
    M(datetime, LogicalType::TYPE_DATETIME)   \
    M(date, LogicalType::TYPE_DATE)

#define DEFINE_ARRAY_DIFFERENCE_FN(NAME, LT)                                                               \
    static StatusOr<ColumnPtr> array_difference_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayDifference<LT>::process(context, columns);                                             \
    }

    DEFINE_ARRAY_DIFFERENCE_FN(boolean, LogicalType::TYPE_BOOLEAN)
    DEFINE_ARRAY_DIFFERENCE_FN(tinyint, LogicalType::TYPE_TINYINT)
    DEFINE_ARRAY_DIFFERENCE_FN(smallint, LogicalType::TYPE_SMALLINT)
    DEFINE_ARRAY_DIFFERENCE_FN(int, LogicalType::TYPE_INT)
    DEFINE_ARRAY_DIFFERENCE_FN(bigint, LogicalType::TYPE_BIGINT)
    DEFINE_ARRAY_DIFFERENCE_FN(largeint, LogicalType::TYPE_LARGEINT)
    DEFINE_ARRAY_DIFFERENCE_FN(float, LogicalType::TYPE_FLOAT)
    DEFINE_ARRAY_DIFFERENCE_FN(double, LogicalType::TYPE_DOUBLE)
    DEFINE_ARRAY_DIFFERENCE_FN(decimalv2, LogicalType::TYPE_DECIMALV2)

#undef DEFINE_ARRAY_DIFFERENCE_FN

    DEFINE_VECTORIZED_FN(array_slice);

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_overlap(FunctionContext* context, const Columns& columns) {
        return ArrayOverlap<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_intersect(FunctionContext* context, const Columns& columns) {
        return ArrayIntersect<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sort(FunctionContext* context, const Columns& columns) {
        return ArraySort<type>::process(context, columns);
    }

#define DEFINE_ARRAY_SORTBY_FN(NAME, LT)                                                               \
    static StatusOr<ColumnPtr> array_sortby_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArraySortBy<LT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_SORTBY_FN)
    DEFINE_ARRAY_SORTBY_FN(json, LogicalType::TYPE_JSON)
#undef DEFINE_ARRAY_SORTBY_FN

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_reverse(FunctionContext* context, const Columns& columns) {
        return ArrayReverse<type>::process(context, columns);
    }

    static StatusOr<ColumnPtr> array_join(FunctionContext* context, const Columns& columns) {
        return ArrayJoin::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sum(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_arithmetic<type, ArithmeticType::SUM>(columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_avg(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_arithmetic<type, ArithmeticType::AVG>(columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_min(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_arithmetic<type, ArithmeticType::MIN>(columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_max(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_arithmetic<type, ArithmeticType::MAX>(columns);
    }

    DEFINE_VECTORIZED_FN(concat);

    DEFINE_VECTORIZED_FN(array_cum_sum_bigint);
    DEFINE_VECTORIZED_FN(array_cum_sum_double);

    DEFINE_VECTORIZED_FN(array_contains_any);
    DEFINE_VECTORIZED_FN(array_contains_all);
    DEFINE_VECTORIZED_FN(array_map);
    DEFINE_VECTORIZED_FN(array_filter);
};

} // namespace starrocks
