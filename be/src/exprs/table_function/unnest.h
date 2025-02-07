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
class Unnest final : public TableFunction {
public:
    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        if (state->get_columns().empty()) {
            return {};
        }
        Column* arg0 = state->get_columns()[0].get();
        auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(arg0));
        state->set_processed_rows(arg0->size());
        Columns result;
        if (arg0->has_null() || state->get_is_left_join()) {
            auto offset_column = col_array->offsets_column();
            auto copy_count_column = UInt32Column::create();
            copy_count_column->append(0);
            ColumnPtr unnested_array_elements = col_array->elements_column()->clone_empty();
            uint32_t offset = 0;
            for (int row_idx = 0; row_idx < arg0->size(); ++row_idx) {
                if (arg0->is_null(row_idx)) {
                    if (state->get_is_left_join()) {
                        // to support unnest with null.
                        if (state->is_required()) {
                            unnested_array_elements->append_nulls(1);
                        }
                        offset += 1;
                    }
                    copy_count_column->append(offset);
                } else {
                    if (offset_column->get(row_idx + 1).get_int32() == offset_column->get(row_idx).get_int32() &&
                        state->get_is_left_join()) {
                        // to support unnest with null.
                        if (state->is_required()) {
                            unnested_array_elements->append_nulls(1);
                        }
                        offset += 1;
                    } else {
                        auto length =
                                offset_column->get(row_idx + 1).get_int32() - offset_column->get(row_idx).get_int32();
                        if (state->is_required()) {
                            unnested_array_elements->append(*(col_array->elements_column()),
                                                            offset_column->get(row_idx).get_int32(), length);
                        }
                        offset += length;
                    }
                    copy_count_column->append(offset);
                }
            }

            result.emplace_back(unnested_array_elements);
            return std::make_pair(result, copy_count_column);
        } else {
            result.emplace_back(col_array->elements_column());
            return std::make_pair(result, col_array->offsets_column());
        }
    }

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
