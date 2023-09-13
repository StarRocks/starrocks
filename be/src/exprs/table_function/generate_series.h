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
class GenerateSeries final : public TableFunction {
    struct MyState final : public TableFunctionState {
        ~MyState() override = default;

        void on_new_params() override { set_offset(0); }
    };

public:
    ~GenerateSeries() override = default;

    Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new MyState();
        return Status::OK();
    }

    Status prepare(TableFunctionState* /*state*/) const override { return Status::OK(); }

    Status open(RuntimeState* /*runtime_state*/, TableFunctionState* /*state*/) const override { return Status::OK(); }

    Status close(RuntimeState* /*runtime_state*/, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }

    std::pair<Columns, UInt32Column::Ptr> process(TableFunctionState* base_state) const override {
        using NumericType = RunTimeCppType<Type>;
        auto max_chunk_size = config::vector_chunk_size;
        auto state = down_cast<MyState*>(base_state);
        auto res = RunTimeColumnType<Type>::create();
        auto offsets = UInt32Column::create();
        auto arg_start = ColumnViewer<Type>(state->get_columns()[0]);
        auto arg_stop = ColumnViewer<Type>(state->get_columns()[1]);
        auto curr_row = state->processed_rows();

        std::unique_ptr<ColumnViewer<Type>> arg_step;
        if (state->get_columns().size() > 2) {
            arg_step = std::make_unique<ColumnViewer<Type>>(state->get_columns()[2]);
        }

        auto move_to_next_row = [&]() {
            curr_row++;
            state->set_processed_rows(curr_row);
            state->set_offset(0);
        };

        auto step_is_null = [&](size_t row) { return (arg_step == nullptr) ? false : arg_step->is_null(row); };

        auto get_step = [&](size_t row) -> NumericType { return (arg_step == nullptr) ? 1 : arg_step->value(row); };

        while (res->size() < max_chunk_size && curr_row < arg_start.size()) {
            offsets->append(res->size());
            if (arg_start.is_null(curr_row) || arg_stop.is_null(curr_row) || step_is_null(curr_row)) {
                move_to_next_row();
            } else {
                auto start = arg_start.value(curr_row);
                auto stop = arg_stop.value(curr_row);
                auto step = get_step(curr_row);
                auto offset = (NumericType)state->get_offset();
                auto current = start;
                if (add_overflow(start, offset, &current)) {
                    move_to_next_row();
                    continue;
                }

                if (step == 0) {
                    state->set_status(Status::InternalError("step size cannot equal zero"));
                    break;
                }

                if ((step > 0 && current > stop) || (step < 0 && current < stop)) {
                    move_to_next_row();
                    continue;
                }

                auto count = (stop - current) / step + 1;
                if (count > max_chunk_size - res->size()) {
                    count = max_chunk_size - res->size();
                }

                bool overflow = false;
                auto old_size = res->size();
                resize_column_uninitialized(res.get(), old_size + count);
                auto* data = res->get_data().data();
                for (decltype(count) i = 0; i < count; i++) {
                    data[old_size + i] = current;
                    overflow = add_overflow(current, step, &current);
                    if (overflow) {
                        break;
                    }
                }

                bool done = (step > 0 && current > stop) || (step < 0 && current < stop);
                if (done || overflow) {
                    move_to_next_row();
                } else {
                    state->set_offset(current - start);
                }
            }
        } // while
        offsets->append(res->size());
        return std::make_pair(Columns{res}, offsets);
    }

private:
    static void resize_column_uninitialized(Column* column, size_t new_size) {
        if (column->size() == 0) {
            column->resize_uninitialized(new_size);
        } else {
            column->resize(new_size);
        }
    }
};

} // namespace starrocks
