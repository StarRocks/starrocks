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
#include <type_traits>

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/runtime_type_traits.h"
#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"
#include "types/integer_overflow_arithmetics.h"
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

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* base_state) const override {
        using NumericType = RunTimeCppType<Type>;
        auto max_chunk_size = runtime_state->chunk_size();
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

                // The number of values still to emit for this row is `(stop - current) / step + 1`,
                // but neither the subtraction nor the division is safe in NumericType:
                //   * `stop - current` overflows whenever the two ends are further apart than the
                //     type range (e.g. `current = -1, stop = INT32_MAX`), which is undefined
                //     behaviour and yields a garbage count;
                //   * `<type>::min() / -1` is not representable. For INT and BIGINT the division
                //     traps with SIGFPE on x86 and takes the BE down, e.g. INT columns feeding
                //     `generate_series(0, -2147483648, -1)`; for LARGEINT the __int128 division
                //     helper returns min() instead, and the resulting negative count skips the fill
                //     loop and hands back uninitialized memory. (TINYINT and SMALLINT escape only
                //     because their operands are promoted to `int` before the division.)
                // Both operands come from input columns, so the offending values can only be known
                // at runtime and cannot be rejected by constant-time analysis in the FE.
                //
                // Compute the distance and the step magnitude as unsigned values instead: unsigned
                // arithmetic is defined to wrap, so it represents the whole two's-complement span
                // exactly, and then clamp to the room left in the output chunk.
                using UnsignedType = std::make_unsigned_t<NumericType>;
                const auto u_current = static_cast<UnsignedType>(current);
                const auto u_stop = static_cast<UnsignedType>(stop);
                const UnsignedType span = (step > 0) ? static_cast<UnsignedType>(u_stop - u_current)
                                                     : static_cast<UnsignedType>(u_current - u_stop);
                const UnsignedType abs_step =
                        (step > 0) ? static_cast<UnsignedType>(step)
                                   : static_cast<UnsignedType>(UnsignedType{0} - static_cast<UnsignedType>(step));
                // Values left after `current` itself; `+ 1` is applied below, after clamping, so it
                // can never overflow UnsignedType.
                const UnsignedType steps_left = span / abs_step;
                const size_t room = static_cast<size_t>(max_chunk_size) - res->size();

                size_t count;
                if constexpr (sizeof(UnsignedType) >= sizeof(size_t)) {
                    count = (steps_left >= static_cast<UnsignedType>(room)) ? room
                                                                            : static_cast<size_t>(steps_left) + 1;
                } else {
                    count = std::min<size_t>(static_cast<size_t>(steps_left) + 1, room);
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
        return std::make_pair(Columns{std::move(res)}, std::move(offsets));
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
