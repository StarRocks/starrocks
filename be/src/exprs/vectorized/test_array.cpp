//
// Created by gubichev on 1/25/23.
//
#include "exprs/vectorized/test_array.h"

#include "column/array_column.h"
#include "column/column.h"

namespace starrocks::vectorized {

ColumnPtr TestArray::test_array(starrocks_udf::FunctionContext* context, const Columns& columns) {
    auto result = Int64Column::create();

    auto offsets = UInt32Column::create();
    offsets->append(0);
    result->append(1);
    result->append(2);
    result->append(3);
    offsets->append(3);

    return  ArrayColumn::create(result, offsets);
}
}
