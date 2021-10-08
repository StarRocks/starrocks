// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/es_functions.h"

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "udf/udf_internal.h"

namespace starrocks::vectorized {

ColumnPtr ESFunctions::match(FunctionContext* context, const Columns& columns) {
    ColumnBuilder<TYPE_BOOLEAN> result;
    auto size = columns[0]->size();
    for (int row = 0; row < size; ++row) {
        result.append(true);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks::vectorized
