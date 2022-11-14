// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/map_functions.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/type_traits.h"

namespace starrocks::vectorized {

ColumnPtr MapFunctions::map_size(starrocks_udf::FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    Column* arg0 = columns[0].get();
    const size_t num_rows = arg0->size();

    auto* col_map = down_cast<MapColumn*>(ColumnHelper::get_data_column(arg0));
    auto col_result = Int32Column::create();
    raw::make_room(&col_result->get_data(), num_rows);
    DCHECK_EQ(num_rows, col_result->size());

    const uint32_t* offsets = col_map->offsets().get_data().data();

    int32_t* p = col_result->get_data().data();
    for (size_t i = 0; i < num_rows; i++) {
        p[i] = offsets[i + 1] - offsets[i];
    }

    if (arg0->has_null()) {
        return NullableColumn::create(std::move(col_result), down_cast<NullableColumn*>(arg0)->null_column());
    } else {
        return col_result;
    }
}

ColumnPtr MapFunctions::map_keys(starrocks_udf::FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    Column* arg0 = columns[0].get();

    auto* col_map = down_cast<MapColumn*>(ColumnHelper::get_data_column(arg0));
    auto map_keys = col_map->keys_column();
    auto map_keys_array = ArrayColumn::create(std::move(map_keys), UInt32Column::create(col_map->offsets()));

    if (arg0->has_null()) {
        return NullableColumn::create(std::move(map_keys_array), down_cast<NullableColumn*>(arg0)->null_column());
    } else {
        return map_keys_array;
    }
}

ColumnPtr MapFunctions::map_values(starrocks_udf::FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    Column* arg0 = columns[0].get();

    auto* col_map = down_cast<MapColumn*>(ColumnHelper::get_data_column(arg0));
    auto map_values = col_map->values_column();
    auto map_values_array = ArrayColumn::create(std::move(map_values), UInt32Column::create(col_map->offsets()));

    if (arg0->has_null()) {
        return NullableColumn::create(std::move(map_values_array), down_cast<NullableColumn*>(arg0)->null_column());
    } else {
        return map_values_array;
    }
}

} // namespace starrocks::vectorized
