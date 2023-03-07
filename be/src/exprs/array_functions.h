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

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_difference(FunctionContext* context, const Columns& columns) {
        return ArrayDifference<type>::process(context, columns);
    }

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

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sortby(FunctionContext* context, const Columns& columns) {
        return ArraySortBy<type>::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_reverse(FunctionContext* context, const Columns& columns) {
        return ArrayReverse<type>::process(context, columns);
    }

    static StatusOr<ColumnPtr> array_join(FunctionContext* context, const Columns& columns) {
        return ArrayJoin::process(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_sum(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_sum<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_avg(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_avg<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_min(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_min<type>(context, columns);
    }

    template <LogicalType type>
    static StatusOr<ColumnPtr> array_max(FunctionContext* context, const Columns& columns) {
        return ArrayArithmetic::template array_max<type>(context, columns);
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
