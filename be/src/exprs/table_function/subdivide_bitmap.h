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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/runtime_type_traits.h"
#include "exprs/table_function/table_function.h"
#include "types/integer_overflow_arithmetics.h"
#include "types/logical_type.h"

namespace starrocks {
template <LogicalType Type>
class SubdivideBitmap final : public TableFunction {
    struct SubdivideBitmapState final : public TableFunctionState {};
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

    template <class ImmObjectContainer>
    void process_row(const ImmObjectContainer& src_bitmap_col, SrcSizeCppType batch_size, size_t row,
                     Column* dst_bitmap_col, UInt32Column* dst_offset_col, uint32_t* compact_offset) const {
        auto* bitmap = src_bitmap_col[row];

        auto result_bitmaps = bitmap->split_bitmap(batch_size);
        size_t bitmap_num = result_bitmaps.size();
        if (bitmap_num != 0) {
            for (size_t i = 0; i < bitmap_num; i++) {
                dst_bitmap_col->append_datum(Datum(&result_bitmaps[i]));
            }
            (*compact_offset) += bitmap_num;
            dst_offset_col->append(*compact_offset);
        }
    }

    // TODO: The TableFunction framework should support streaming processing to avoid generating large Column
    // Expansion is unbounded (bitmap cardinality / batch_size). Locals are MutableColumnPtr plus a
    // by-value vector<BitmapValue> from split_bitmap(), and ObjectColumn::append() copies the value in
    // (object_column.cpp), so nothing survives or leaks when a std::bad_alloc unwinds.
    bool is_exception_safe() const override { return true; }

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        if (state->get_columns().size() != 2) {
            state->set_status(Status::InternalError("The number of parameters of unnest_bitmap is not equal to 2"));
            return {};
        }

        const ColumnPtr& c0 = state->get_columns()[0];
        const ColumnPtr& c1 = state->get_columns()[1];
        size_t rows = c0->size();
        state->set_processed_rows(rows);

        Columns dst_columns;

        auto dst_bitmap_col = c0->clone_empty();
        auto dst_offset_col = UInt32Column::create();
        dst_offset_col->append_datum(Datum(0));
        uint32_t compact_offset = 0;

        const auto* src_bitmap_col = ColumnHelper::cast_to_raw<TYPE_OBJECT>(ColumnHelper::get_data_column(c0.get()));
        const auto* src_size_col = ColumnHelper::cast_to_raw<Type>(ColumnHelper::get_data_column(c1.get()));
        const auto src_bitmap_data = src_bitmap_col->immutable_data();
        const auto src_size_data = src_size_col->immutable_data();
<<<<<<< HEAD
        bool has_null = c0->has_null() || c1->has_null();
=======
        const bool has_null = c0->has_null() || c1->has_null();

        size_t cur_row = subdivide_state->processed_rows();
        auto move_to_next_row = [&]() {
            cur_row++;
            subdivide_state->set_processed_rows(cur_row);
            subdivide_state->set_offset(0);
        };

        // Reused across pieces and across rows. Capped rather than sized by the piece: a piece holds
        // `batch_size` values and `batch_size` is the caller's, so sizing the buffer by it would make
        // subdivide_bitmap(b, 50000000) ask for 400MB of uint64 scratch to build a piece whose Roaring
        // representation is a fraction of that - an allocation the value-at-a-time iterator this
        // replaced never made, and one that counts against the query's memory limit. next_batch() can
        // be called repeatedly against the same cursor, so a larger piece is filled in several passes.
        //
        // Deliberately its own constant rather than `chunk_size`, which happens to have the same
        // default: chunk_size bounds output *rows* per call and is a session variable, so tying the
        // buffer to it would make `SET chunk_size = 2` - which the sibling batching case does set -
        // fill a piece two values at a time.
        constexpr uint64_t kValueBatchSize = 4096;
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
>>>>>>> c37d12543bf... [BugFix] Cap subdivide_bitmap's scratch buffer so it stops scaling with the caller's split size (#62066)

        if (has_null) {
            for (size_t i = 0; i < rows; i++) {
                if (src_size_data[i] <= 0 || c0->is_null(i) || c1->is_null(i)) {
                    dst_offset_col->append(compact_offset);
                    continue;
                }
                process_row(src_bitmap_data, src_size_data[i], i, dst_bitmap_col.get(), dst_offset_col.get(),
                            &compact_offset);
            }
<<<<<<< HEAD
        } else {
            for (size_t i = 0; i < rows; i++) {
                if (src_size_data[i] <= 0) {
                    dst_offset_col->append(compact_offset);
                    continue;
=======

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
                values.resize(std::min({split_size, remain, kValueBatchSize}));
                for (uint32_t k = 0; k < pieces; ++k) {
                    // A piece is a full split_size, except for the row's last one, which is whatever
                    // the iterator still holds - the same truncation next_batch() applied when it was
                    // asked for a whole piece at once.
                    uint64_t left = std::min<uint64_t>(split_size, subdivide_state->iter.remain_rows());
                    BitmapValue sub_bitmap;
                    while (left > 0) {
                        const uint64_t taken = subdivide_state->iter.next_batch(
                                values.data(), std::min<uint64_t>(left, values.size()));
                        if (taken == 0) {
                            break;
                        }
                        for (uint64_t j = 0; j < taken; ++j) {
                            sub_bitmap.add(values[j]);
                        }
                        left -= taken;
                    }
                    // append() copies the value into the column's pool, so the temporary above is the
                    // only extra materialization - one piece at a time, against the whole row's worth
                    // of sub-bitmaps that split_bitmap() used to hold alongside the result column.
                    dst_bitmap_col->append_datum(Datum(&sub_bitmap));
>>>>>>> c37d12543bf... [BugFix] Cap subdivide_bitmap's scratch buffer so it stops scaling with the caller's split size (#62066)
                }
                process_row(src_bitmap_data, src_size_data[i], i, dst_bitmap_col.get(), dst_offset_col.get(),
                            &compact_offset);
            }
        }
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
        dst_columns.emplace_back(std::move(dst_bitmap_col));
        return std::make_pair(std::move(dst_columns), std::move(dst_offset_col));
    }
};
} // namespace starrocks
