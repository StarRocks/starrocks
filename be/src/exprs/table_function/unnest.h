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
    std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* state) const override {
        if (state->get_columns().empty()) {
            return {};
        }
        Column* arg0 = state->get_columns()[0].get();
        auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(arg0));
        state->set_processed_rows(arg0->size());
        Columns result;
        if (arg0->has_null()) {
            auto* nullable_array_column = down_cast<NullableColumn*>(arg0);

            auto offset_column = col_array->offsets_column();
            auto compacted_offset_column = UInt32Column::create();
            compacted_offset_column->append_datum(Datum(0));

            ColumnPtr compacted_array_elements = col_array->elements_column()->clone_empty();
            int compact_offset = 0;

            for (int row_idx = 0; row_idx < nullable_array_column->size(); ++row_idx) {
                if (nullable_array_column->is_null(row_idx)) {
                    compact_offset +=
                            offset_column->get(row_idx + 1).get_int32() - offset_column->get(row_idx).get_int32();
                    int32 offset = offset_column->get(row_idx + 1).get_int32();
                    compacted_offset_column->append_datum(offset - compact_offset);
                } else {
                    int32 offset = offset_column->get(row_idx + 1).get_int32();
                    compacted_offset_column->append_datum(offset - compact_offset);

                    compacted_array_elements->append(
                            *(col_array->elements_column()), offset_column->get(row_idx).get_int32(),
                            offset_column->get(row_idx + 1).get_int32() - offset_column->get(row_idx).get_int32());
                }
            }

            result.emplace_back(compacted_array_elements);
            return std::make_pair(result, compacted_offset_column);
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

    [[nodiscard]] Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new UnnestState();
        return Status::OK();
    }

    [[nodiscard]] Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    [[nodiscard]] Status open(RuntimeState* runtime_state, TableFunctionState* state) const override {
        return Status::OK();
    };

    [[nodiscard]] Status close(RuntimeState* runtime_state, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }
};

} // namespace starrocks
