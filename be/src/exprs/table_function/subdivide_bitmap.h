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
#include "column/type_traits.h"
#include "common/config.h"
#include "exprs/table_function/table_function.h"
#include "runtime/integer_overflow_arithmetics.h"
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
        SAFE_DELETE(state);
        return Status::OK();
    }

    void process_row(const Buffer<BitmapValue*>& src_bitmap_col, SrcSizeCppType batch_size, size_t row,
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
        const auto& src_bitmap_data = src_bitmap_col->get_data();
        const auto& src_size_data = src_size_col->get_data();
        bool has_null = c0->has_null() || c1->has_null();

        if (has_null) {
            for (size_t i = 0; i < rows; i++) {
                if (src_size_data[i] <= 0 || c0->is_null(i) || c1->is_null(i)) {
                    dst_offset_col->append(compact_offset);
                    continue;
                }
                process_row(src_bitmap_data, src_size_data[i], i, dst_bitmap_col.get(), dst_offset_col.get(),
                            &compact_offset);
            }
        } else {
            for (size_t i = 0; i < rows; i++) {
                if (src_size_data[i] <= 0) {
                    dst_offset_col->append(compact_offset);
                    continue;
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
