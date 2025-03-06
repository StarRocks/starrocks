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

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/type_traits.h"
#include "gutil/casts.h"

namespace starrocks {
class FunctionContext;
}
namespace starrocks {

class FunctionHelper {
public:
    /**
     * if ptr is NullableColumn, return data column
     * else return ptr
     * @param ptr 
     */
    static inline const ColumnPtr& get_data_column_of_nullable(const ColumnPtr& ptr) {
        if (ptr->is_nullable()) {
            return down_cast<const NullableColumn*>(ptr.get())->data_column();
        }
        return ptr;
    }

    /**
     * get data of column.
     * @param col, row_num, data 
     */
    template <typename ToColumnType, typename CppType>
    static void get_data_of_column(const Column* col, size_t row_num, CppType& data) {
        if (col->is_constant()) {
            auto const_col = down_cast<const ConstColumn*>(col);
            col = const_col->data_column().get();
            row_num = 0;
        }
        const auto* column = down_cast<const ToColumnType*>(col);
        data = column->get_data()[row_num];
    }

    /**
     * if ptr is ConstColumn, return data column
     * else return ptr
     * @param ptr 
     */
    static inline const ColumnPtr& get_data_column_of_const(const ColumnPtr& ptr) {
        if (ptr->is_constant()) {
            return down_cast<const ConstColumn*>(ptr.get())->data_column();
        }
        return ptr;
    }

    /**
     * if v1 is NullableColumn and v2 is NullableColumn, union
     * if v1 is NullableColumn and v2 is not NullableColumn, return v1.nullColumn
     * if v1 is not NullableColumn and v2 is NullableColumn, return v2.nullColumn
     * if v1 is not NullableColumn and v2 is not NullableColumn, return nullptr
     * 
     * @param v1 
     * @param v2 
     */
    static NullColumn::MutablePtr union_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2);

    static void union_produce_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2,
                                              NullColumnPtr* produce_null_column);

    static void union_produce_nullable_column(const ColumnPtr& v1, NullColumnPtr* produce_null_column);

    static NullColumn::MutablePtr union_null_column(const NullColumnPtr& v1, const NullColumnPtr& v2);

    // merge a column and null_column and generate a column with null values.
    static ColumnPtr merge_column_and_null_column(ColumnPtr&& column, NullColumnPtr&& null_column);
};

#define DEFINE_VECTORIZED_FN(NAME) static StatusOr<ColumnPtr> NAME(FunctionContext* context, const Columns& columns)

#define DEFINE_VECTORIZED_FN_TEMPLATE(NAME) \
    template <LogicalType Type>             \
    static StatusOr<ColumnPtr> NAME(FunctionContext* context, const Columns& columns)

#define VECTORIZED_FN_CTX() context
#define VECTORIZED_FN_ARGS(IDX) columns[IDX]

#define RETURN_IF_COLUMNS_ONLY_NULL(COLUMNS) \
    do {                                     \
        for (auto& col : COLUMNS) {          \
            if (col->only_null()) {          \
                return col;                  \
            }                                \
        }                                    \
    } while (false)

} // namespace starrocks
