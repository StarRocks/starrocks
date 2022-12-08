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

#include "exprs/vectorized/map_functions.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/type_traits.h"

namespace starrocks::vectorized {

StatusOr<ColumnPtr> MapFunctions::map_size(starrocks_udf::FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> MapFunctions::map_keys(starrocks_udf::FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> MapFunctions::map_values(starrocks_udf::FunctionContext* context, const Columns& columns) {
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
