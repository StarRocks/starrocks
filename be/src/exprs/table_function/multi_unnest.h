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

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_helper.h"
#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"

namespace starrocks {
/**
 * UNNEST can be used to expand an ARRAY into a relation, arrays are expanded into a single column.
 */
class MultiUnnest final : public TableFunction {
public:
    std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* state) const override {
        if (state->get_columns().empty()) {
            return {};
        }

        long row_count = state->get_columns()[0]->size();
        state->set_processed_rows(row_count);

        std::vector<ColumnPtr> compacted_array_list;
        for (auto& col_idx : state->get_columns()) {
            Column* column = col_idx.get();

            auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column));
            ColumnPtr compacted_array_elements = col_array->elements_column()->clone_empty();
            compacted_array_list.emplace_back(compacted_array_elements);
        }

        auto compacted_offset_column = UInt32Column::create();
        long offset = 0;
        compacted_offset_column->append(offset);
        for (int row_idx = 0; row_idx < row_count; ++row_idx) {
            long max_length_array_size = 0;
            for (auto& col_idx : state->get_columns()) {
                Column* column = col_idx.get();
                auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column));
                auto offset_column = col_array->offsets_column();

                long array_element_length =
                        offset_column->get(row_idx + 1).get_int32() - offset_column->get(row_idx).get_int32();
                if (array_element_length > max_length_array_size) {
                    max_length_array_size = array_element_length;
                }
            }
            compacted_offset_column->append(offset + max_length_array_size);
            offset += max_length_array_size;

            for (int col_idx = 0; col_idx < state->get_columns().size(); ++col_idx) {
                Column* column = state->get_columns()[col_idx].get();
                auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column));
                auto offset_column = col_array->offsets_column();

                long array_element_length =
                        offset_column->get(row_idx + 1).get_int32() - offset_column->get(row_idx).get_int32();
                compacted_array_list[col_idx]->append(*(col_array->elements_column()),
                                                      offset_column->get(row_idx).get_int32(), array_element_length);

                if (array_element_length < max_length_array_size) {
                    compacted_array_list[col_idx]->append_nulls(max_length_array_size - array_element_length);
                }
            }
        }

        Columns result;
        for (auto& col_idx : compacted_array_list) {
            result.emplace_back(col_idx);
        }

        return std::make_pair(result, compacted_offset_column);
    }

    class UnnestState : public TableFunctionState {
        /**
         * Unnest does not need to customize the State,
         * UnnestState is just to provide an example for other TableFunction
         */
    };

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new UnnestState();
        return Status::OK();
    }

    Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    Status open(RuntimeState* runtime_state, TableFunctionState* state) const override { return Status::OK(); };

    Status close(RuntimeState* runtime_state, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }
};

} // namespace starrocks
