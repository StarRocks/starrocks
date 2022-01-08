// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/vectorized/function_helper.h"

namespace starrocks::vectorized {

class ArrayFunctions {
public:
    DEFINE_VECTORIZED_FN(array_length);

    DEFINE_VECTORIZED_FN(array_ndims);

    DEFINE_VECTORIZED_FN(array_append);

    DEFINE_VECTORIZED_FN(array_remove);

    DEFINE_VECTORIZED_FN(array_contains);
    DEFINE_VECTORIZED_FN(array_position);

    DEFINE_VECTORIZED_FN(array_distinct_boolean);
    DEFINE_VECTORIZED_FN(array_distinct_tinyint);
    DEFINE_VECTORIZED_FN(array_distinct_smallint);
    DEFINE_VECTORIZED_FN(array_distinct_int);
    DEFINE_VECTORIZED_FN(array_distinct_bigint);
    DEFINE_VECTORIZED_FN(array_distinct_largeint);
    DEFINE_VECTORIZED_FN(array_distinct_float);
    DEFINE_VECTORIZED_FN(array_distinct_double);
    DEFINE_VECTORIZED_FN(array_distinct_varchar);
    DEFINE_VECTORIZED_FN(array_distinct_char);
    DEFINE_VECTORIZED_FN(array_distinct_decimalv2);
    DEFINE_VECTORIZED_FN(array_distinct_datetime);
    DEFINE_VECTORIZED_FN(array_distinct_date);

    DEFINE_VECTORIZED_FN(array_sum_boolean);
    DEFINE_VECTORIZED_FN(array_sum_tinyint);
    DEFINE_VECTORIZED_FN(array_sum_smallint);
    DEFINE_VECTORIZED_FN(array_sum_int);
    DEFINE_VECTORIZED_FN(array_sum_bigint);
    DEFINE_VECTORIZED_FN(array_sum_largeint);
    DEFINE_VECTORIZED_FN(array_sum_float);
    DEFINE_VECTORIZED_FN(array_sum_double);
    DEFINE_VECTORIZED_FN(array_sum_decimalv2);

    DEFINE_VECTORIZED_FN(array_avg_boolean);
    DEFINE_VECTORIZED_FN(array_avg_tinyint);
    DEFINE_VECTORIZED_FN(array_avg_smallint);
    DEFINE_VECTORIZED_FN(array_avg_int);
    DEFINE_VECTORIZED_FN(array_avg_bigint);
    DEFINE_VECTORIZED_FN(array_avg_largeint);
    DEFINE_VECTORIZED_FN(array_avg_float);
    DEFINE_VECTORIZED_FN(array_avg_double);
    DEFINE_VECTORIZED_FN(array_avg_decimalv2);
    DEFINE_VECTORIZED_FN(array_avg_date);
    DEFINE_VECTORIZED_FN(array_avg_datetime);

    DEFINE_VECTORIZED_FN(array_min_boolean);
    DEFINE_VECTORIZED_FN(array_min_tinyint);
    DEFINE_VECTORIZED_FN(array_min_smallint);
    DEFINE_VECTORIZED_FN(array_min_int);
    DEFINE_VECTORIZED_FN(array_min_bigint);
    DEFINE_VECTORIZED_FN(array_min_largeint);
    DEFINE_VECTORIZED_FN(array_min_float);
    DEFINE_VECTORIZED_FN(array_min_double);
    DEFINE_VECTORIZED_FN(array_min_decimalv2);
    DEFINE_VECTORIZED_FN(array_min_date);
    DEFINE_VECTORIZED_FN(array_min_datetime);
    DEFINE_VECTORIZED_FN(array_min_varchar);

    DEFINE_VECTORIZED_FN(array_max_boolean);
    DEFINE_VECTORIZED_FN(array_max_tinyint);
    DEFINE_VECTORIZED_FN(array_max_smallint);
    DEFINE_VECTORIZED_FN(array_max_int);
    DEFINE_VECTORIZED_FN(array_max_bigint);
    DEFINE_VECTORIZED_FN(array_max_largeint);
    DEFINE_VECTORIZED_FN(array_max_float);
    DEFINE_VECTORIZED_FN(array_max_double);
    DEFINE_VECTORIZED_FN(array_max_decimalv2);
    DEFINE_VECTORIZED_FN(array_max_date);
    DEFINE_VECTORIZED_FN(array_max_datetime);
    DEFINE_VECTORIZED_FN(array_max_varchar);

    enum ArithmeticType { SUM, AVG, MIN, MAX };

private:
    template <PrimitiveType column_type, ArithmeticType type>
    static ColumnPtr array_arithmetic(const Columns& columns);

    template <PrimitiveType column_type, ArithmeticType type>
    static ColumnPtr _array_process_not_nullable(const Column* array_column, std::vector<uint8_t>* null_ptr);

    template <PrimitiveType column_type, bool has_null, ArithmeticType type>
    static ColumnPtr _array_process_not_nullable_types(const Column* elements, const UInt32Column& offsets,
                                                       const NullColumn::Container* null_elements,
                                                       std::vector<uint8_t>* null_ptr);

    template <PrimitiveType PT, typename HashSet>
    static void _array_distinct_item(const ArrayColumn& column, size_t index, HashSet* hash_set,
                                     ArrayColumn* dest_column);

    template <PrimitiveType type, typename HashSet>
    static ColumnPtr array_distinct(const Columns& column);

    template <PrimitiveType type>
    static ColumnPtr array_sum(const Columns& columns);

    template <PrimitiveType type>
    static ColumnPtr array_avg(const Columns& columns);

    template <PrimitiveType type>
    static ColumnPtr array_min(const Columns& columns);

    template <PrimitiveType type>
    static ColumnPtr array_max(const Columns& columns);
};

} // namespace starrocks::vectorized
