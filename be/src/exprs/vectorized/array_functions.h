// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exprs/vectorized/array_functions.tpp"

namespace starrocks::vectorized {

class ArrayFunctions {
public:
    DEFINE_VECTORIZED_FN(array_length);

    DEFINE_VECTORIZED_FN(array_ndims);

    DEFINE_VECTORIZED_FN(array_append);

    DEFINE_VECTORIZED_FN(array_remove);

    DEFINE_VECTORIZED_FN(array_contains);
    DEFINE_VECTORIZED_FN(array_position);

#define DEFINE_ARRAY_DISTINCT_FN(NAME, PT)                                                     \
    static ColumnPtr array_distinct_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayDistinct<PT>::process(context, columns);                                   \
    }

    DEFINE_ARRAY_DISTINCT_FN(boolean, PrimitiveType::TYPE_BOOLEAN);
    DEFINE_ARRAY_DISTINCT_FN(tinyint, PrimitiveType::TYPE_TINYINT);
    DEFINE_ARRAY_DISTINCT_FN(smallint, PrimitiveType::TYPE_SMALLINT);
    DEFINE_ARRAY_DISTINCT_FN(int, PrimitiveType::TYPE_INT);
    DEFINE_ARRAY_DISTINCT_FN(bigint, PrimitiveType::TYPE_BIGINT);
    DEFINE_ARRAY_DISTINCT_FN(largeint, PrimitiveType::TYPE_LARGEINT);
    DEFINE_ARRAY_DISTINCT_FN(float, PrimitiveType::TYPE_FLOAT);
    DEFINE_ARRAY_DISTINCT_FN(double, PrimitiveType::TYPE_DOUBLE);
    DEFINE_ARRAY_DISTINCT_FN(varchar, PrimitiveType::TYPE_VARCHAR);
    DEFINE_ARRAY_DISTINCT_FN(char, PrimitiveType::TYPE_CHAR);
    DEFINE_ARRAY_DISTINCT_FN(decimalv2, PrimitiveType::TYPE_DECIMALV2);
    DEFINE_ARRAY_DISTINCT_FN(datetime, PrimitiveType::TYPE_DATETIME);
    DEFINE_ARRAY_DISTINCT_FN(date, PrimitiveType::TYPE_DATE);

#undef DEFINE_ARRAY_DISTINCT_FN

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
