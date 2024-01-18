// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/type_traits.h"

namespace starrocks_udf {
class FunctionContext;
}
namespace starrocks::vectorized {

class FunctionHelper {
public:
    /**
     * if ptr is NullableColumn, return data column
     * else return ptr
     * @param ptr 
     */
    static inline const ColumnPtr& get_data_column_of_nullable(const ColumnPtr& ptr) {
        if (ptr->is_nullable()) {
            return down_cast<NullableColumn*>(ptr.get())->data_column();
        }
        return ptr;
    }

    /**
     * if ptr is ConstColumn, return data column
     * else return ptr
     * @param ptr 
     */
    static inline const ColumnPtr& get_data_column_of_const(const ColumnPtr& ptr) {
        if (ptr->is_constant()) {
            return down_cast<ConstColumn*>(ptr.get())->data_column();
        }
        return ptr;
    }

    /**
     * if v1 is NullableColumn and v2 is NullableColumn, union
     * if v1 is NullableColumn and v2 is not NullableColumn, return v1.nullColumn
     * if v1 is not NullableColumn and v2 is NullableColumn, return v2.nullColumn
     * if v1 is not NullableColumn and v2 is not NullableColumn, impossible
     * 
     * @param v1 
     * @param v2 
     */
    static NullColumnPtr union_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2);

    static void union_produce_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2,
                                              NullColumnPtr* produce_null_column);

    static NullColumnPtr union_null_column(const NullColumnPtr& v1, const NullColumnPtr& v2);

    // merge a column and null_column and generate a column with null values.
    static ColumnPtr merge_column_and_null_column(ColumnPtr&& column, NullColumnPtr&& null_column);
};

#define DEFINE_VECTORIZED_FN(NAME) \
    static StatusOr<ColumnPtr> NAME(starrocks_udf::FunctionContext* context, const Columns& columns)

#define DEFINE_VECTORIZED_FN_TEMPLATE(NAME) \
    template <PrimitiveType Type>           \
    static StatusOr<ColumnPtr> NAME(starrocks_udf::FunctionContext* context, const Columns& columns)

#define VECTORIZED_FN_ARGS(IDX) columns[IDX]

#define RETURN_IF_COLUMNS_ONLY_NULL(COLUMNS) \
    do {                                     \
        for (auto& col : COLUMNS) {          \
            if (col->only_null()) {          \
                return col;                  \
            }                                \
        }                                    \
    } while (false)

} // namespace starrocks::vectorized
