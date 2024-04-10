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
class UnnestBitmap final : public TableFunction {
    struct UnnestBitmapState final : public TableFunctionState {
        BitmapValueIter iter;

        void set_offset(int64_t offset) override {
            iter.set_offset(static_cast<uint64_t>(offset));
            TableFunctionState::set_offset(offset);
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
        SAFE_DELETE(state);
        return Status::OK();
    }

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* state) const override {
        if (state->get_columns().size() != 1) {
            state->set_status(Status::InternalError("The number of parameters of unnest_bitmap is not equal to 1"));
            return {};
        }

        int chunk_size = runtime_state->chunk_size();
        auto res_data_col = RunTimeColumnType<TYPE_BIGINT>::create(chunk_size);
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
            res_offset_col->append(cur_size);
            if (c0->is_null(cur_row)) {
                move_to_next_row();
            } else {
                const BitmapValue& bitmap = *src_bitmap_col->get_object(cur_row);
                if (unnest_bitmap_state->iter.offset() == 0) {
                    unnest_bitmap_state->iter.reset(bitmap);
                }
                size_t read_rows = chunk_size - cur_size;
                size_t real_size = unnest_bitmap_state->iter.next_batch(
                        (uint64_t*)(res_data_col->get_data().data() + cur_size), read_rows);
                cur_size += real_size;
                if (real_size < read_rows) {
                    move_to_next_row();
                }
            }
        }

        res_data_col->resize(cur_size);
        res_offset_col->append(cur_size);
        return std::make_pair(Columns{std::move(res_data_col)}, res_offset_col);
    }
};
} // namespace starrocks
