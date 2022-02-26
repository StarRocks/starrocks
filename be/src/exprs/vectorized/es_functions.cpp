// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/es_functions.h"

#include <ext/alloc_traits.h>
#include <memory>

#include "column/column_builder.h"
#include "udf/udf_internal.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"

namespace starrocks::vectorized {

ColumnPtr ESFunctions::match(FunctionContext* context, const Columns& columns) {
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        result.append(true);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks::vectorized
