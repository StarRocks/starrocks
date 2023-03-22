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

#include "exprs/map_functions.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "common/logging.h"

namespace starrocks {

// Used to construct a Map type value from keys and values. Input keys and values are in the
// format of Array type.
// For example,
//  map([1, 2], [3, 4]) will generate a map which value is {1=3,2=4}
StatusOr<ColumnPtr> MapFunctions::map_from_arrays(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(2, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto& keys_column = columns[0];
    NullColumn* keys_null = nullptr;
    ArrayColumn* keys_data = nullptr;
    if (keys_column->is_nullable()) {
        auto keys = down_cast<NullableColumn*>(keys_column.get());
        keys_null = keys->null_column().get();
        keys_data = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(keys));
    } else {
        keys_data = down_cast<ArrayColumn*>(keys_column.get());
    }

    auto& values_column = columns[1];
    NullColumn* values_null = nullptr;
    ArrayColumn* values_data = nullptr;
    if (values_column->is_nullable()) {
        auto values = down_cast<NullableColumn*>(values_column.get());
        values_null = values->null_column().get();
        values_data = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(values));
    } else {
        values_data = down_cast<ArrayColumn*>(values_column.get());
    }

    auto& keys_offsets = keys_data->offsets().get_data();
    auto& values_offsets = values_data->offsets().get_data();
    auto num_rows = keys_data->size();
    if (!keys_column->has_null() && !values_column->has_null()) {
        size_t num_equals = 0;
        // NOTE: only need to check the accumulated offset of each element
        for (int i = 1; i <= num_rows; ++i) {
            num_equals += (keys_offsets[i] == values_offsets[i]);
        }
        if (num_equals != num_rows) {
            return Status::InvalidArgument("Key and value arrays must be the same length");
        }
        auto copied_key_elements = keys_data->elements().clone_shared();
        auto copied_value_elements = values_data->elements().clone_shared();
        auto copied_offsets = keys_data->offsets().clone_shared();
        auto map_column = MapColumn::create(std::move(copied_key_elements), std::move(copied_value_elements),
                                            std::static_pointer_cast<UInt32Column>(copied_offsets));
        map_column->remove_duplicated_keys();
        return map_column;
    } else {
        // build the null column
        NullColumnPtr null_column;
        if (keys_null != nullptr) {
            if (values_null != nullptr) {
                null_column = std::static_pointer_cast<NullColumn>(keys_null->clone_shared());
                ColumnHelper::or_two_filters(num_rows, null_column->get_data().data(), values_null->get_data().data());
            } else {
                null_column = std::static_pointer_cast<NullColumn>(keys_null->clone_shared());
            }
        } else {
            null_column = std::static_pointer_cast<NullColumn>(values_null->clone_shared());
        }
        // check and construct offset column
        auto& null_bits = null_column->get_data();
        uint32_t offset = 0;
        auto map_offsets_column = UInt32Column::create();
        for (int i = 0; i < num_rows; ++i) {
            map_offsets_column->append(offset);
            if (!null_bits[i]) {
                auto num_elements = keys_data->get_element_size(i);
                if (num_elements != values_data->get_element_size(i)) {
                    return Status::InvalidArgument("Key and value arrays must be the same length");
                }
                offset += num_elements;
            }
        }
        map_offsets_column->append(offset);
        // copy key and value elements
        auto map_key_elements = keys_data->elements().clone_empty();
        auto map_value_elements = values_data->elements().clone_empty();
        int row = 0;
        while (row < num_rows) {
            // skip continuous nulls;
            while (row < num_rows && null_bits[row]) {
                row++;
            }
            uint32_t prev_row = row;
            while (row < num_rows && !null_bits[row]) {
                row++;
            }
            if (row > prev_row) {
                map_key_elements->append(keys_data->elements(), keys_offsets[prev_row],
                                         keys_offsets[row] - keys_offsets[prev_row]);
                map_value_elements->append(values_data->elements(), values_offsets[prev_row],
                                           values_offsets[row] - values_offsets[prev_row]);
            }
        }
        auto map_column = MapColumn::create(std::move(map_key_elements), std::move(map_value_elements),
                                            std::static_pointer_cast<UInt32Column>(map_offsets_column));
        map_column->remove_duplicated_keys();
        return NullableColumn::create(std::move(map_column), std::move(null_column));
    }
}

StatusOr<ColumnPtr> MapFunctions::map_size(FunctionContext* context, const Columns& columns) {
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

StatusOr<ColumnPtr> MapFunctions::map_keys(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    Column* arg0 = columns[0].get();

    auto* col_map = down_cast<MapColumn*>(ColumnHelper::get_data_column(arg0));
    auto map_keys = col_map->keys_column();
    auto map_keys_array = ArrayColumn::create(map_keys->clone_shared(), UInt32Column::create(col_map->offsets()));

    if (arg0->has_null()) {
        return NullableColumn::create(
                std::move(map_keys_array),
                std::static_pointer_cast<NullColumn>(down_cast<NullableColumn*>(arg0)->null_column()->clone_shared()));
    } else {
        return map_keys_array;
    }
}

StatusOr<ColumnPtr> MapFunctions::map_values(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(1, columns.size());
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    Column* arg0 = columns[0].get();

    auto* col_map = down_cast<MapColumn*>(ColumnHelper::get_data_column(arg0));
    auto map_values = col_map->values_column();
    auto map_values_array = ArrayColumn::create(map_values->clone_shared(), UInt32Column::create(col_map->offsets()));

    if (arg0->has_null()) {
        return NullableColumn::create(
                std::move(map_values_array),
                std::static_pointer_cast<NullColumn>(down_cast<NullableColumn*>(arg0)->null_column()->clone_shared()));
    } else {
        return map_values_array;
    }
}

} // namespace starrocks
