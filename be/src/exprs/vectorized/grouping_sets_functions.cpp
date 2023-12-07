// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/grouping_sets_functions.h"

#include "column/column_helper.h"
#include "column/column_viewer.h"

namespace starrocks::vectorized {

StatusOr<ColumnPtr> GroupingSetsFunctions::grouping_id(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    return columns[0];
}

StatusOr<ColumnPtr> GroupingSetsFunctions::grouping(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    return columns[0];
}

} // namespace starrocks::vectorized
