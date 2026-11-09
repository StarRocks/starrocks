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
        Column* arg0 = state->get_columns()[0]->as_mutable_raw_ptr();
        // const: every ArrayColumn accessor then resolves to its const overload, which keeps the
        // offsets read on immutable_data(). The non-const FixedLengthColumnBase::get_data()
        // materializes a ContainerResource-backed column into its own buffer and drops the
        // resource, and nothing here may mutate the input column.
        const ArrayColumn* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(arg0));
        state->set_processed_rows(arg0->size());
        Columns result;
        // The offsets column doubles as the per-row output row count, so it can be handed downstream
        // as-is whenever it already carries the right counts - and it does, even for a LEFT JOIN: a row
        // that expands to nothing shows up as a zero-length bracket, which TableFunctionOperator turns
        // into the required single NULL row while assembling the (bounded) output chunk. So the only
        // reason left to rebuild is a row marked NULL whose payload was never cleared, which would
        // otherwise contribute rows that do not logically exist.
        //
        // Note the rebuilding path below never emits a zero-length bracket under LEFT JOIN (it adds
        // the NULL row itself, at offset += 1), so the operator never injects a second one.
        bool rebuild_offsets = false;
        if (arg0->has_null()) {
            // Column::has_null() defaults to false, so having nulls means this is a nullable column.
            // If its null representation is not a plain per-row byte buffer (an AdaptiveNullableColumn
            // that has not been materialized), null_rows_are_empty rejects the size mismatch and we
            // stay on the rebuilding path. Note this stays true no matter what the column really is:
            // immutable_null_column_data() is not virtual, so the call below reads NullableColumn's
            // own buffer and does not trigger AdaptiveNullableColumn's materialization.
            const auto& nulls = down_cast<const NullableColumn*>(arg0)->immutable_null_column_data();
            rebuild_offsets = !col_array->null_rows_are_empty(nulls.data(), nulls.size());
        }
        if (rebuild_offsets) {
            // Read the offsets buffer directly: Datum::get_int32() would reinterpret any offset in
            // [2^31, 2^32) as a negative value, because Datum keeps unsigned values in the matching
            // signed slot.
            const auto offsets = col_array->offsets().immutable_data();
            const Column& elements = col_array->elements();
            const size_t row_count = arg0->size();

            auto copy_count_column = UInt32Column::create();
            copy_count_column->append(0);
            MutableColumnPtr unnested_array_elements = col_array->elements_column()->clone_empty();
            uint32_t offset = 0;
            for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
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
                    if (offsets[row_idx + 1] == offsets[row_idx] && state->get_is_left_join()) {
                        // to support unnest with null.
                        if (state->is_required()) {
                            unnested_array_elements->append_nulls(1);
                        }
                        offset += 1;
                    } else {
                        const uint32_t length = offsets[row_idx + 1] - offsets[row_idx];
                        if (state->is_required()) {
                            unnested_array_elements->append(elements, offsets[row_idx], length);
                        }
                        offset += length;
                    }
                    copy_count_column->append(offset);
                }
            }

            result.emplace_back(unnested_array_elements);
            return std::make_pair(std::move(result), std::move(copy_count_column));
        } else {
            result.emplace_back(col_array->elements_column());
            return std::make_pair(std::move(result), col_array->offsets_column());
        }
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
