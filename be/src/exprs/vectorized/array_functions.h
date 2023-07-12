// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

#define APPLY_COMMONE_TYPES_FOR_ARRAY(M)        \
    M(boolean, PrimitiveType::TYPE_BOOLEAN)     \
    M(tinyint, PrimitiveType::TYPE_TINYINT)     \
    M(smallint, PrimitiveType::TYPE_SMALLINT)   \
    M(int, PrimitiveType::TYPE_INT)             \
    M(bigint, PrimitiveType::TYPE_BIGINT)       \
    M(largeint, PrimitiveType::TYPE_LARGEINT)   \
    M(float, PrimitiveType::TYPE_FLOAT)         \
    M(double, PrimitiveType::TYPE_DOUBLE)       \
    M(varchar, PrimitiveType::TYPE_VARCHAR)     \
    M(char, PrimitiveType::TYPE_CHAR)           \
    M(decimalv2, PrimitiveType::TYPE_DECIMALV2) \
    M(datetime, PrimitiveType::TYPE_DATETIME)   \
    M(date, PrimitiveType::TYPE_DATE)

#define DEFINE_ARRAY_DISTINCT_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_distinct_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayDistinct<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_DISTINCT_FN)
#undef DEFINE_ARRAY_DISTINCT_FN

#define DEFINE_ARRAY_DIFFERENCE_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_difference_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayDifference<PT>::process(context, columns);                                             \
    }

    DEFINE_ARRAY_DIFFERENCE_FN(boolean, PrimitiveType::TYPE_BOOLEAN)
    DEFINE_ARRAY_DIFFERENCE_FN(tinyint, PrimitiveType::TYPE_TINYINT)
    DEFINE_ARRAY_DIFFERENCE_FN(smallint, PrimitiveType::TYPE_SMALLINT)
    DEFINE_ARRAY_DIFFERENCE_FN(int, PrimitiveType::TYPE_INT)
    DEFINE_ARRAY_DIFFERENCE_FN(bigint, PrimitiveType::TYPE_BIGINT)
    DEFINE_ARRAY_DIFFERENCE_FN(largeint, PrimitiveType::TYPE_LARGEINT)
    DEFINE_ARRAY_DIFFERENCE_FN(float, PrimitiveType::TYPE_FLOAT)
    DEFINE_ARRAY_DIFFERENCE_FN(double, PrimitiveType::TYPE_DOUBLE)
    DEFINE_ARRAY_DIFFERENCE_FN(decimalv2, PrimitiveType::TYPE_DECIMALV2)

#undef DEFINE_ARRAY_DIFFERENCE_FN

#define DEFINE_ARRAY_SLICE_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_slice_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArraySlice<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_SLICE_FN)
    DEFINE_ARRAY_SLICE_FN(json, PrimitiveType::TYPE_JSON)
#undef DEFINE_ARRAY_SLICE_FN

#define DEFINE_ARRAY_CONCAT_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_concat_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayConcat<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_CONCAT_FN)
    DEFINE_ARRAY_CONCAT_FN(json, PrimitiveType::TYPE_JSON)
#undef DEFINE_ARRAY_CONCAT_FN

#define DEFINE_ARRAY_OVERLAP_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_overlap_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayOverlap<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_OVERLAP_FN)
#undef DEFINE_ARRAY_OVERLAP_FN

#define DEFINE_ARRAY_INTERSECT_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_intersect_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayIntersect<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_INTERSECT_FN)
#undef DEFINE_ARRAY_INTERSECT_FN

#define DEFINE_ARRAY_SORT_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_sort_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArraySort<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_SORT_FN)
    DEFINE_ARRAY_SORT_FN(json, PrimitiveType::TYPE_JSON)
#undef DEFINE_ARRAY_SORT_FN

#define DEFINE_ARRAY_SORTBY_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_sortby_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArraySortBy<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_SORTBY_FN)
    DEFINE_ARRAY_SORTBY_FN(json, PrimitiveType::TYPE_JSON)
#undef DEFINE_ARRAY_SORTBY_FN

#define DEFINE_ARRAY_REVERSE_FN(NAME, PT)                                                               \
    static StatusOr<ColumnPtr> array_reverse_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayReverse<PT>::process(context, columns);                                             \
    }
    APPLY_COMMONE_TYPES_FOR_ARRAY(DEFINE_ARRAY_REVERSE_FN)
    DEFINE_ARRAY_REVERSE_FN(json, PrimitiveType::TYPE_JSON)
#undef DEFINE_ARRAY_REVERSE_FN

#define DEFINE_ARRAY_JOIN_FN(NAME)                                                                   \
    static StatusOr<ColumnPtr> array_join_##NAME(FunctionContext* context, const Columns& columns) { \
        return ArrayJoin::process(context, columns);                                                 \
    }

    DEFINE_ARRAY_JOIN_FN(varchar);

#undef DEFINE_ARRAY_JOIN_FN

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

    DEFINE_VECTORIZED_FN(array_cum_sum_bigint);
    DEFINE_VECTORIZED_FN(array_cum_sum_double);

    DEFINE_VECTORIZED_FN(array_contains_any);
    DEFINE_VECTORIZED_FN(array_contains_all);
    DEFINE_VECTORIZED_FN(array_map);
    DEFINE_VECTORIZED_FN(array_filter);

    enum ArithmeticType { SUM, AVG, MIN, MAX };

private:
    template <PrimitiveType column_type, ArithmeticType type>
    static StatusOr<ColumnPtr> array_arithmetic(const Columns& columns);

    template <PrimitiveType column_type, ArithmeticType type>
    static StatusOr<ColumnPtr> _array_process_not_nullable(const Column* array_column, std::vector<uint8_t>* null_ptr);

    template <PrimitiveType column_type, bool has_null, ArithmeticType type>
    static StatusOr<ColumnPtr> _array_process_not_nullable_types(const Column* elements, const UInt32Column& offsets,
                                                                 const NullColumn::Container* null_elements,
                                                                 std::vector<uint8_t>* null_ptr);

    template <PrimitiveType type>
    static StatusOr<ColumnPtr> array_sum(const Columns& columns);

    template <PrimitiveType type>
    static StatusOr<ColumnPtr> array_avg(const Columns& columns);

    template <PrimitiveType type>
    static StatusOr<ColumnPtr> array_min(const Columns& columns);

    template <PrimitiveType type>
    static StatusOr<ColumnPtr> array_max(const Columns& columns);
};

} // namespace starrocks::vectorized
