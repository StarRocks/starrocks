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
#include "common/logging.h"
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
    // Expands a bounded slice of the input rows per call: at most chunk_size output rows.
    //
    // The zip pads every array to the per-row maximum length, and those padding NULLs exist in no
    // source column, so - unlike single-array Unnest - there is no zero-copy path here: the output
    // has to be built. What can be bounded is how much of it is built at a time. The operator
    // already loops `while (processed_rows() < input_rows())` and already emits bounded output
    // chunks; this implementation used to opt out of that loop by declaring the whole input chunk
    // processed on its first line, so a single call materialized the entire expansion (input rows x
    // max array length) in one allocation, with no intermediate memory-limit check.
    //
    // The cursor is the pair (processed_rows(), get_offset()): the input row being expanded, and how
    // many of its zipped rows have already been emitted. Four invariants tie it to the operator, and
    // all of them produce silently wrong data - not a crash - when broken:
    //
    //   1. when this call stops in the middle of a row, processed_rows() must NOT advance. The
    //      operator reads outer columns at `_input_index_of_first_result + _next_output_row_offset`,
    //      where `_input_index_of_first_result` is the processed_rows() captured *before* the call -
    //      i.e. bracket k of the returned offsets column must belong to input row
    //      processed_rows() + k. Advancing early pairs the wrong outer row with the elements.
    //   2. the returned offsets column is batch-local and starts at 0: the operator resets its
    //      cursors before every call and DCHECKs the first bracket against 0.
    //   3. a row must contribute at most one bracket per call, otherwise two brackets of this batch
    //      map to the same input row and invariant 1 breaks. The loop condition guarantees it: a
    //      partial slice fills the batch exactly, so the loop exits right after appending it.
    //   4. the loop bound must be a logical counter, not `outputs[0]->size()`: with is_required()
    //      false nothing is appended while the cursor still advances.
    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        Columns& args = state->get_columns();
        if (args.empty()) {
            return {};
        }

        const size_t num_args = args.size();
        const size_t input_rows = state->input_rows();
        const bool fn_result_required = state->is_required();
        const bool is_left_join = state->get_is_left_join();
        // At least one row per call, so that the operator cannot spin forever on a result that makes
        // no progress should chunk_size ever be configured as 0.
        const uint32_t max_output_rows = std::max<uint32_t>(1, runtime_state->chunk_size());

        // Per-argument views, resolved once per call rather than once per row and argument. The
        // offsets buffer is also read directly instead of through Datum, which is what the per-row
        // `offsets_column()->get(row).get_int32()` pairs used to cost.
        struct ArgView {
            Column* column = nullptr; // outermost column, possibly nullable, for is_null()
            const Column* elements = nullptr;
            const uint32_t* offsets = nullptr;
        };
        std::vector<ArgView> arg_views(num_args);
        MutableColumns outputs(num_args);
        for (size_t i = 0; i < num_args; ++i) {
            Column* column = args[i]->as_mutable_raw_ptr();
            auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(column));
            arg_views[i] = ArgView{column, col_array->elements_column().get(),
                                   col_array->offsets_column()->immutable_data().data()};
            outputs[i] = col_array->elements_column()->clone_empty();
            if (fn_result_required) {
                // The output of one call is bounded now, so its size is known up front: one reserve
                // instead of the geometric growth the unbounded version paid, where every doubling
                // reallocated and copied everything already written (the single 320 MB / 640 MB
                // allocations observed under query_mem_limit).
                outputs[i]->reserve(max_output_rows);
            }
        }

        auto copy_count_column = UInt32Column::create();
        copy_count_column->reserve(max_output_rows + 1);
        uint32_t emitted = 0;               // invariant 4: logical count, not outputs[0]->size()
        copy_count_column->append(emitted); // invariant 2: batch-local, starts at 0

        const size_t first_row = state->processed_rows();
        size_t cur_row = first_row;
        while (emitted < max_output_rows && cur_row < input_rows) {
            // Recomputed per call rather than cached in the state: it is O(num_args) per row, so a
            // row spanning k calls costs k extra offset lookups, against the O(elements) copy the
            // batching avoids. Caching it would add a second piece of cursor state to invalidate.
            uint32_t max_length_array_size = 0;
            for (const auto& view : arg_views) {
                if (view.column->is_null(cur_row)) {
                    // current row is null, ignore the offset.
                    continue;
                }
                max_length_array_size =
                        std::max(max_length_array_size, view.offsets[cur_row + 1] - view.offsets[cur_row]);
            }
            // A LEFT JOIN keeps a row whose expansion is empty, as a single all-NULL row. Expressed
            // as an output length of 1 over arrays of length 0, the padding below emits exactly that
            // NULL for every argument, so this needs no branch of its own. It also means the offsets
            // column still never carries a zero-length bracket under LEFT JOIN, which is what keeps
            // TableFunctionOperator's own injection path inert for MultiUnnest.
            const uint32_t expand_len = (max_length_array_size == 0 && is_left_join) ? 1 : max_length_array_size;

            const auto cursor = static_cast<uint32_t>(state->get_offset());
            DCHECK_LE(cursor, expand_len);
            const uint32_t slice_len = std::min(expand_len - cursor, max_output_rows - emitted);
            const uint32_t slice_end = cursor + slice_len;

            // The expanded values only need materializing when something upstream actually reads
            // them. When fn_result_required is false the copy-count column is all the operator uses
            // (it drives outer-column replication), so the whole zip - including the NULL padding,
            // which is the bulk of the work for arrays of unequal length - can be skipped.
            if (fn_result_required && slice_len > 0) {
                for (size_t i = 0; i < num_args; ++i) {
                    const ArgView& view = arg_views[i];
                    const uint32_t len =
                            view.column->is_null(cur_row) ? 0 : view.offsets[cur_row + 1] - view.offsets[cur_row];
                    // Argument i covers [cursor, min(slice_end, len)) with its own elements and pads
                    // [max(cursor, len), slice_end) with NULLs. The two ranges are disjoint and
                    // together are exactly slice_len rows, so the zip stays aligned across arguments
                    // no matter where the slice boundary falls relative to this array's length.
                    const uint32_t copy_end = std::min(slice_end, len);
                    if (copy_end > cursor) {
                        outputs[i]->append(*view.elements, view.offsets[cur_row] + cursor, copy_end - cursor);
                    }
                    const uint32_t pad_begin = std::max(cursor, len);
                    if (slice_end > pad_begin) {
                        outputs[i]->append_nulls(slice_end - pad_begin);
                    }
                }
            }

            emitted += slice_len;
            copy_count_column->append(emitted);

            if (slice_end == expand_len) {
                cur_row++;
                state->set_processed_rows(cur_row);
                state->set_offset(0);
            } else {
                // Invariant 1: the row is only partially emitted, so processed_rows() stays put and
                // this same row becomes the first bracket of the next call. Invariant 3: slice_len
                // was capped by the remaining room, so the loop condition is already false here.
                state->set_offset(slice_end);
            }
        }

        // The whole cursor in one line, for when a query returns the wrong rows rather than failing:
        // the batch's input-row range plus the intra-row offset it stopped at is exactly what the
        // operator pairs its outer columns against. Same VLOG level as TableFunctionOperator's own
        // per-bracket trace, so both sides of the contract can be read together.
        VLOG(2) << "MultiUnnest batch: input_rows=" << input_rows << " first_row=" << first_row
                << " processed_rows=" << state->processed_rows() << " offset=" << state->get_offset()
                << " emitted=" << emitted << " max_output_rows=" << max_output_rows
                << " required=" << fn_result_required;

        Columns result;
        result.reserve(num_args);
        for (auto& output : outputs) {
            result.emplace_back(std::move(output));
        }
        return std::make_pair(std::move(result), std::move(copy_count_column));
    }

    bool is_exception_safe() const override { return true; }

    class UnnestState : public TableFunctionState {
        // set_params() resets processed_rows() but leaves _offset to on_new_params(), so the intra-row
        // cursor has to be reset here - otherwise a chunk arriving while a row is still partially
        // emitted (reset_state(), or a pipeline that re-primes the operator) would start expanding its
        // first row from the leftover offset.
        void on_new_params() override { set_offset(0); }
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
