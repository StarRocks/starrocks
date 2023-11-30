// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/table_function/table_function.h"
#include "exprs/vectorized/function_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
/**
 * UNNEST can be used to expand an ARRAY into a relation, arrays are expanded into a single column.
 */
class MultiUnnest final : public TableFunction {
public:
    std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* state, bool* eos) const override {
        *eos = true;
        if (state->get_columns().empty()) {
            return {};
        }

        long row_count = state->get_columns()[0]->size();

        std::vector<ColumnPtr> compacted_array_list;
        for (int col_idx = 0; col_idx < state->get_columns().size(); ++col_idx) {
            Column* column = state->get_columns()[col_idx].get();

            auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column));
            ColumnPtr compacted_array_elements = col_array->elements_column()->clone_empty();
            compacted_array_list.emplace_back(compacted_array_elements);
        }

        auto compacted_offset_column = UInt32Column::create();
        long offset = 0;
        compacted_offset_column->append(offset);
        for (int row_idx = 0; row_idx < row_count; ++row_idx) {
            long max_length_array_size = 0;
            for (int col_idx = 0; col_idx < state->get_columns().size(); ++col_idx) {
                Column* column = state->get_columns()[col_idx].get();
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
        for (int col_idx = 0; col_idx < compacted_array_list.size(); ++col_idx) {
            result.emplace_back(compacted_array_list[col_idx]);
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

} // namespace starrocks::vectorized
