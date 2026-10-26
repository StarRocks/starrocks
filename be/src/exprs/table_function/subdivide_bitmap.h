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

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/runtime_type_traits.h"
#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"
#include "types/integer_overflow_arithmetics.h"
#include "types/logical_type.h"

namespace starrocks {
template <LogicalType Type>
class SubdivideBitmap final : public TableFunction {
    struct SubdivideBitmapState final : public TableFunctionState {
        // Where inside the current row's bitmap the previous call stopped, counted in *elements*
        // (always a multiple of that row's split size). The iterator also holds a pointer to that
        // BitmapValue and, for the BITMAP representation, a Roaring iterator into it - so it is only
        // meaningful for as long as the parameter columns it was reset from are alive.
        BitmapValueIter iter;
        // Cardinality of the row `iter` is positioned in, cached at reset() time. Roaring64Map's
        // cardinality() sums over its containers, and a row wide enough to span many calls would
        // otherwise pay that walk once per call.
        uint64_t cardinality = 0;

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
            cardinality = 0;
            set_offset(0);
        }
    };
    using SrcSizeCppType = typename RunTimeTypeTraits<Type>::CppType;

public:
    ~SubdivideBitmap() override = default;

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new SubdivideBitmapState();
        return Status::OK();
    }

    Status prepare(TableFunctionState* state) const override { return Status::OK(); }

    Status open(RuntimeState* runtime_state, TableFunctionState* state) const override { return Status::OK(); }

    Status close(RuntimeState* runtime_state, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }

    // Output is bounded by chunk_size per call, so the wrapper is inert here; declared anyway so that
    // every implementation states its answer explicitly rather than inheriting the unsafe default.
    // Every local is a MutableColumnPtr, a std::vector or a BitmapValue, so a std::bad_alloc can
    // unwind out of process() without leaking - the cursor is simply left where it stopped, on a
    // query that is already failing.
    bool is_exception_safe() const override { return true; }

    // Emits a bounded slice of the expansion per call: at most chunk_size sub-bitmaps, cutting inside
    // a row when one row alone expands to more than that.
    //
    // This used to declare the whole input chunk processed on its first line and hand back everything
    // at once, and it did so through split_bitmap(), which returns the row's sub-bitmaps by value in a
    // std::vector before they are copied into the result column - so the peak held two full copies of
    // an expansion that is itself far larger than its source. A dense Roaring bitset of cardinality C
    // costs a fraction of a byte per value, while `subdivide_bitmap(b, 1)` turns it into C separate
    // BitmapValue objects at ~48 bytes each: the expansion is multiplicative, and multiplied again by
    // the input chunk's row count.
    //
    // The cursor is the pair (processed_rows(), get_offset()); the invariants tying it to the operator
    // are the ones spelled out in multi_unnest.h, and the intra-row slicing is legitimate here because
    // split_bitmap()'s own definition is positional: sub-bitmap k holds the ascending elements
    // [k*n, (k+1)*n) of the source, which is exactly what BitmapValueIter walks.
    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        if (state->get_columns().size() != 2) {
            state->set_status(Status::InternalError("The number of parameters of subdivide_bitmap is not equal to 2"));
            return {};
        }

        // At least one row per call, so the operator cannot spin forever on a result that carries no
        // bracket at all should chunk_size ever be configured as 0.
        const uint32_t chunk_size = std::max<uint32_t>(1, runtime_state->chunk_size());
        // Set once by the operator in prepare() and constant for the life of the state, so the
        // materializing and the counting branch below never interleave on the same row.
        const bool required = state->is_required();
        auto* subdivide_state = down_cast<SubdivideBitmapState*>(state);

        const ColumnPtr& c0 = state->get_columns()[0];
        const ColumnPtr& c1 = state->get_columns()[1];
        const size_t rows = c0->size();

        auto dst_bitmap_col = c0->clone_empty();
        auto dst_offset_col = UInt32Column::create();
        dst_offset_col->reserve(chunk_size + 1);
        if (required) {
            dst_bitmap_col->reserve(chunk_size);
        }

        const auto* src_bitmap_col = ColumnHelper::cast_to_raw<TYPE_OBJECT>(ColumnHelper::get_data_column(c0.get()));
        const auto* src_size_col = ColumnHelper::cast_to_raw<Type>(ColumnHelper::get_data_column(c1.get()));
        const auto src_bitmap_data = src_bitmap_col->immutable_data();
        const auto src_size_data = src_size_col->immutable_data();
        const bool has_null = c0->has_null() || c1->has_null();

        size_t cur_row = subdivide_state->processed_rows();
        auto move_to_next_row = [&]() {
            cur_row++;
            subdivide_state->set_processed_rows(cur_row);
            subdivide_state->set_offset(0);
        };

        // Reused across pieces and across rows: one piece is at most `batch_size` values, and the
        // fast path below means the general path only runs when batch_size < cardinality.
        std::vector<uint64_t> values;
        uint32_t emitted = 0;
        while (emitted < chunk_size && cur_row < rows) {
            // One bracket per input row touched by this batch: bracket k belongs to input row
            // (processed_rows() on entry) + k, which is how the operator picks the outer-column row.
            dst_offset_col->append(emitted);

            const SrcSizeCppType batch_size = src_size_data[cur_row];
            if (batch_size <= 0 || (has_null && (c0->is_null(cur_row) || c1->is_null(cur_row)))) {
                move_to_next_row();
                continue;
            }

            BitmapValue* bitmap = src_bitmap_data[cur_row];
            if (subdivide_state->get_offset() == 0) {
                subdivide_state->iter.reset(*bitmap);
                subdivide_state->cardinality = bitmap->cardinality();
            }
            const uint64_t cardinality = subdivide_state->cardinality;
            const auto consumed = static_cast<uint64_t>(subdivide_state->get_offset());

            // split_bitmap()'s own fast path, kept because it is observable: a bitmap that fits in a
            // single piece is handed back whole, preserving its representation (a BITMAP stays a
            // BITMAP through a shared_ptr copy) instead of being rebuilt value by value. It is also
            // the only path that emits anything for an empty bitmap, whose cardinality is 0 - one row
            // holding an empty bitmap, not zero rows.
            //
            // Compared in int128 rather than in the uint64 the cursor arithmetic uses, because the
            // split size is declared as any integer type - LARGEINT included - and so can be larger
            // than any cardinality a bitmap can have. Narrowing it first would turn 2^64 into 0, a
            // value that has already passed the `<= 0` guard above and would reach the division below.
            if (consumed == 0 && static_cast<int128_t>(cardinality) <= static_cast<int128_t>(batch_size)) {
                if (required) {
                    dst_bitmap_col->append_datum(Datum(bitmap));
                }
                emitted++;
                move_to_next_row();
                continue;
            }

            // Past that branch the split size is strictly smaller than the cardinality - either this
            // call just checked it, or an earlier call did before leaving `consumed` non-zero, which
            // only the general path can do - so it fits in a uint64 and the narrowing is exact.
            // Spelled as DCHECK rather than DCHECK_LT: glog cannot stream an __int128 operand.
            DCHECK(static_cast<int128_t>(batch_size) < static_cast<int128_t>(cardinality));
            const auto split_size = static_cast<uint64_t>(batch_size);
            const uint64_t remain = cardinality - consumed;
            // Written as a division rather than `(remain + split_size - 1) / split_size`, whose
            // numerator can wrap when both operands are near the top of the uint64 range.
            const uint64_t pieces_left = remain / split_size + (remain % split_size != 0);
            const auto pieces = static_cast<uint32_t>(std::min<uint64_t>(pieces_left, chunk_size - emitted));

            if (required) {
                values.resize(std::min<uint64_t>(split_size, remain));
                for (uint32_t k = 0; k < pieces; ++k) {
                    const uint64_t taken = subdivide_state->iter.next_batch(values.data(), values.size());
                    BitmapValue sub_bitmap;
                    for (uint64_t j = 0; j < taken; ++j) {
                        sub_bitmap.add(values[j]);
                    }
                    // append() copies the value into the column's pool, so the temporary above is the
                    // only extra materialization - one piece at a time, against the whole row's worth
                    // of sub-bitmaps that split_bitmap() used to hold alongside the result column.
                    dst_bitmap_col->append_datum(Datum(&sub_bitmap));
                }
            }

            // When the expanded value is not required the operator reads nothing but the bracket
            // counts (they drive outer-column replication), so the bitmap need not be walked at all:
            // how many rows it expands to follows from its cardinality alone. Republishing the cursor
            // through the state mirrors what next_batch() already advanced in the branch above, and is
            // what advances it in this one.
            // Every piece but the last is exactly split_size values, so the product cannot overflow
            // while pieces < pieces_left; when it is the last one, what remains is `remain` by
            // definition and needs no product at all.
            const uint64_t elements = pieces == pieces_left ? remain : static_cast<uint64_t>(pieces) * split_size;
            emitted += pieces;
            subdivide_state->set_offset(static_cast<int64_t>(consumed + elements));
            if (consumed + elements >= cardinality) {
                // Closed here rather than by the next call finding the row exhausted, so a row whose
                // piece count is an exact multiple of chunk_size does not cost an extra call and an
                // extra zero-length bracket.
                move_to_next_row();
            }
        }
        dst_offset_col->append(emitted);

        Status st = dst_bitmap_col->capacity_limit_reached();
        if (!st.ok()) {
            state->set_status(Status::InternalError(
                    fmt::format("Bitmap column generate by subdivide_bitmap reach limit, {}", st.message())));
            return {};
        }
        st = dst_offset_col->capacity_limit_reached();
        if (!st.ok()) {
            state->set_status(Status::InternalError(
                    fmt::format("Offset column generate by subdivide_bitmap reach limit, {}", st.message())));
            return {};
        }
        return std::make_pair(Columns{std::move(dst_bitmap_col)}, std::move(dst_offset_col));
    }
};
} // namespace starrocks
