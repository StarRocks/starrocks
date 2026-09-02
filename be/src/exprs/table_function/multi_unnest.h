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

#include <algorithm>
#include <vector>

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
    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        if (state->get_columns().empty()) {
            return {};
        }

        const size_t column_count = state->get_columns().size();
        const size_t row_count = state->get_columns()[0]->size();
        state->set_processed_rows(row_count);

        // Resolve the per-column views once instead of re-resolving them for every row.
        struct ArrayView {
            const Column* nullable_column;
            const Column* elements;
            const UInt32Column::Container* offsets;
        };
        std::vector<ArrayView> array_views;
        array_views.reserve(column_count);

        Columns unnested_array_list;
        unnested_array_list.reserve(column_count);
        // Everything here is const, all the way down from the input ColumnPtr: nothing on this path
        // may mutate the input columns.
        for (const auto& col : state->get_columns()) {
            const Column* column = col.get();
            const auto* col_array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(column));
            array_views.emplace_back(ArrayView{column, &col_array->elements(), &col_array->offsets().get_data()});
            unnested_array_list.emplace_back(col_array->elements_column()->clone_empty());
        }

        auto copy_count_column = UInt32Column::create();
        uint32_t offset = 0;
        copy_count_column->append(offset);
        for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
            uint32_t max_length_array_size = 0;
            for (const auto& view : array_views) {
                if (view.nullable_column->is_null(row_idx)) {
                    // current row is null, ignore the offset.
                    continue;
                }
                const uint32_t array_element_length = (*view.offsets)[row_idx + 1] - (*view.offsets)[row_idx];
                max_length_array_size = std::max(max_length_array_size, array_element_length);
            }

            if (max_length_array_size == 0 && state->get_is_left_join()) {
                offset += 1;
                copy_count_column->append(offset);
                for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
                    unnested_array_list[col_idx]->append_nulls(1);
                }
                continue;
            }

            offset += max_length_array_size;
            copy_count_column->append(offset);

            for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
                const auto& view = array_views[col_idx];
                if (view.nullable_column->is_null(row_idx)) {
                    // current row is null, ignore element data.
                    unnested_array_list[col_idx]->append_nulls(max_length_array_size);
                    continue;
                }

                const uint32_t array_start = (*view.offsets)[row_idx];
                const uint32_t array_element_length = (*view.offsets)[row_idx + 1] - array_start;
                unnested_array_list[col_idx]->append(*view.elements, array_start, array_element_length);

                if (array_element_length < max_length_array_size) {
                    unnested_array_list[col_idx]->append_nulls(max_length_array_size - array_element_length);
                }
            }
        }

        Columns result;
        for (auto& col_idx : unnested_array_list) {
            result.emplace_back(col_idx);
        }

        return std::make_pair(std::move(result), std::move(copy_count_column));
    }

    bool is_exception_safe() const override { return true; }

    class UnnestState : public TableFunctionState {
        /**
         * Unnest does not need to customize the State,
         * UnnestState is just to provide an example for other TableFunction
         */
    };

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new UnnestState();
        const auto& table_fn = fn.table_fn;
        if (table_fn.__isset.is_left_join) {
            (*state)->set_is_left_join(table_fn.is_left_join);
        }
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
