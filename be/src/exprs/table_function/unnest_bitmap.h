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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/runtime_type_traits.h"
#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"
#include "types/integer_overflow_arithmetics.h"
#include "types/logical_type.h"

namespace starrocks {
class UnnestBitmap final : public TableFunction {
    struct UnnestBitmapState final : public TableFunctionState {
        // Where inside the current row's bitmap the previous call stopped. This iterator also holds a
        // pointer to that BitmapValue and, for the BITMAP representation, a Roaring iterator into it -
        // so it is only meaningful for as long as the parameter columns it was reset from are alive.
        BitmapValueIter iter;

        // The intra-row cursor lives in two places that have to agree: TableFunctionState::_offset,
        // which is the cursor the operator and the tests can see, and `iter`, which is the one that
        // actually reads. Every move goes through here so neither can drift from the other.
        void set_offset(int64_t offset) override {
            iter.set_offset(static_cast<uint64_t>(offset));
            TableFunctionState::set_offset(offset);
        }

    private:
        // set_params() resets processed_rows() but leaves _offset to on_new_params(). Without this
        // override the intra-row cursor survives into the next input chunk, and process() reads a
        // non-zero offset as "carry on with the row I was reading" - a row that belongs to the chunk
        // just abandoned, whose BitmapValue (and the Roaring iterator into it) may already be freed.
        //
        // The operator abandons a chunk mid-row through reset_state(), which calls
        // set_params(Columns{}); TableFunctionNode is allowed inside a query-cache fragment
        // (FragmentNormalizer::isAllowedInLeftMostPath), and switching a cache lane to another tablet
        // resets the whole lane. Resetting `iter` as well as the offset also drops the stale pointer
        // rather than leaving it to be found by the next reset(), which only happens at offset 0.
        void on_new_params() override {
            iter = BitmapValueIter();
            set_offset(0);
        }
    };

public:
    ~UnnestBitmap() override = default;

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new UnnestBitmapState();
        return Status::OK();
    }

    Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    Status open(RuntimeState* runtime_state, TableFunctionState* state) const override { return Status::OK(); }

    Status close(RuntimeState* runtime_state, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }

    // Output is already bounded by chunk_size, so the wrapper is inert here; declared anyway so that
    // every implementation states its answer explicitly rather than inheriting the unsafe default.
    bool is_exception_safe() const override { return true; }

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        if (state->get_columns().size() != 1) {
            state->set_status(Status::InternalError("The number of parameters of unnest_bitmap is not equal to 1"));
            return {};
        }

        // At least one row per call, so the operator cannot spin forever on a result that carries no
        // bracket at all should chunk_size ever be configured as 0.
        const uint32_t chunk_size = std::max<uint32_t>(1, runtime_state->chunk_size());
        // Set once by the operator in prepare() and constant for the life of the state, so the two
        // branches below never interleave on the same row.
        const bool required = state->is_required();

        auto res_data_col = RunTimeColumnType<TYPE_BIGINT>::create();
        if (required) {
            res_data_col->resize(chunk_size);
        }
        auto res_offset_col = UInt32Column::create();

        auto* unnest_bitmap_state = down_cast<UnnestBitmapState*>(state);
        auto cur_row = unnest_bitmap_state->processed_rows();

        const ColumnPtr& c0 = state->get_columns()[0];
        size_t rows = c0->size();
        const auto* src_bitmap_col = ColumnHelper::cast_to_raw<TYPE_OBJECT>(ColumnHelper::get_data_column(c0.get()));

        auto move_to_next_row = [&]() {
            cur_row++;
            unnest_bitmap_state->set_processed_rows(cur_row);
            unnest_bitmap_state->set_offset(0);
        };

        uint32_t cur_size = 0;
        while (cur_size < chunk_size && cur_row < rows) {
            // One bracket per input row touched by this batch: bracket k belongs to input row
            // (processed_rows() on entry) + k, which is how the operator picks the outer-column row.
            res_offset_col->append(cur_size);
            if (c0->is_null(cur_row)) {
                move_to_next_row();
                continue;
            }

            const BitmapValue& bitmap = *src_bitmap_col->get_object(cur_row);
            if (unnest_bitmap_state->get_offset() == 0) {
                unnest_bitmap_state->iter.reset(bitmap);
            }
            const uint32_t room = chunk_size - cur_size;
            // When the expanded value is not required the operator reads nothing but the bracket
            // counts (they drive outer-column replication), so the bitmap need not be iterated at all:
            // how many rows it expands to is its cardinality, which reset() already cached.
            const uint64_t produced =
                    required ? unnest_bitmap_state->iter.next_batch(
                                       reinterpret_cast<uint64_t*>(res_data_col->get_data().data() + cur_size), room)
                             : std::min<uint64_t>(unnest_bitmap_state->iter.remain_rows(), room);
            cur_size += static_cast<uint32_t>(produced);
            // Republish the cursor through the state: in the required path this only mirrors what
            // next_batch() already advanced, in the counting path it is what advances the iterator.
            unnest_bitmap_state->set_offset(unnest_bitmap_state->get_offset() + static_cast<int64_t>(produced));

            if (unnest_bitmap_state->iter.remain_rows() == 0) {
                // Closed here rather than by the next call finding the row empty, so a row whose
                // cardinality is an exact multiple of chunk_size does not cost an extra call and an
                // extra zero-length bracket.
                move_to_next_row();
            }
        }

        if (required) {
            res_data_col->resize(cur_size);
        }
        res_offset_col->append(cur_size);
        return std::make_pair(Columns{std::move(res_data_col)}, std::move(res_offset_col));
    }
};
} // namespace starrocks
