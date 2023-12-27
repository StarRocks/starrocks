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

#include "exprs/table_function/json_each.h"

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/table_function/table_function.h"
#include "velocypack/vpack.h"

namespace starrocks {

std::pair<Columns, UInt32Column::Ptr> JsonEach::process(RuntimeState* runtime_state, TableFunctionState* state) const {
    size_t num_input_rows = 0;
    JsonColumn* json_column = nullptr;
    if (!state->get_columns().empty()) {
        Column* arg0 = state->get_columns()[0].get();
        num_input_rows = arg0->size();
        json_column = down_cast<JsonColumn*>(ColumnHelper::get_data_column(arg0));
    }
    state->set_processed_rows(num_input_rows);

    Columns result;
    auto key_column_ptr = BinaryColumn::create();
    auto value_column_ptr = JsonColumn::create();
    result.emplace_back(key_column_ptr);
    result.emplace_back(value_column_ptr);
    auto offset_column = UInt32Column::create();
    int offset = 0;
    offset_column->append(offset);

    for (int i = 0; i < num_input_rows; i++) {
        const JsonValue* json = json_column->get_object(i);
        DCHECK(!!json);
        vpack::Slice json_slice = json->to_vslice();
        if (json_slice.isObject()) {
            for (auto [key, value] : vpack::ObjectIterator(json_slice)) {
                std::string_view key_str = key.stringView();
                key_column_ptr->append(Slice(key_str.data(), key_str.size()));
                value_column_ptr->append(JsonValue(value));
                offset++;
            }
        } else if (json_slice.isArray()) {
            int arr_idx = 0;
            for (auto value : vpack::ArrayIterator(json_slice)) {
                std::string key = std::to_string(arr_idx);
                key_column_ptr->append(Slice(key));
                value_column_ptr->append(JsonValue(value));
                offset++;
                arr_idx++;
            }
        }

        offset_column->append(offset);
    }

    return std::make_pair(result, offset_column);
}

} // namespace starrocks
