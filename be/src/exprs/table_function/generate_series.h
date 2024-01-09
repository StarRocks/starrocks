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

template <LogicalType SeriesType, LogicalType StepType, TimeUnit UNIT = TimeUnit::DAY>
class GenerateSeries final : public TableFunction {
    struct MyState final : public TableFunctionState {
        ~MyState() override = default;

        void on_new_params() override { set_offset(0); }
    };

public:
    ~GenerateSeries() override = default;

    [[nodiscard]] Status init(const TFunction& fn, TableFunctionState** state) const override {
        *state = new MyState();
        return Status::OK();
    }

    [[nodiscard]] Status prepare(TableFunctionState* /*state*/) const override { return Status::OK(); }

    [[nodiscard]] Status open(RuntimeState* /*runtime_state*/, TableFunctionState* /*state*/) const override {
        return Status::OK();
    }

    [[nodiscard]] Status close(RuntimeState* /*runtime_state*/, TableFunctionState* state) const override {
        delete state;
        return Status::OK();
    }

    std::pair<Columns, UInt32Column::Ptr> process(RuntimeState* runtime_state,
                                                  TableFunctionState* base_state) const override {
        using SeriesRunTimeType = RunTimeCppType<SeriesType>;
        using StepRunTimeType = RunTimeCppType<StepType>;
        auto max_chunk_size = runtime_state->chunk_size();
        auto state = down_cast<MyState*>(base_state);
        auto res = RunTimeColumnType<SeriesType>::create();
        auto offsets = UInt32Column::create();
        auto arg_start = ColumnViewer<SeriesType>(state->get_columns()[0]);
        auto arg_stop = ColumnViewer<SeriesType>(state->get_columns()[1]);
        auto curr_row = state->processed_rows();

        std::unique_ptr<ColumnViewer<StepType>> arg_step;
        if (state->get_columns().size() > 2) {
            arg_step = std::make_unique<ColumnViewer<StepType>>(state->get_columns()[2]);
        }

        auto move_to_next_row = [&]() {
            curr_row++;
            state->set_processed_rows(curr_row);
            state->set_offset(0);
        };

        auto step_is_null = [&](size_t row) { return (arg_step == nullptr) ? false : arg_step->is_null(row); };

        auto get_step = [&](size_t row) -> StepRunTimeType { return (arg_step == nullptr) ? 1 : arg_step->value(row); };

        while (res->size() < max_chunk_size && curr_row < arg_start.size()) {
            offsets->append(res->size());
            if (arg_start.is_null(curr_row) || arg_stop.is_null(curr_row) || step_is_null(curr_row)) {
                move_to_next_row();
            } else {
                auto start = (SeriesRunTimeType)arg_start.value(curr_row);
                auto stop = (SeriesRunTimeType)arg_stop.value(curr_row);
                auto step = get_step(curr_row);
                auto offset = state->get_offset();
                auto current = start;
                if (add_overflow_unit(start, offset, &current)) {
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

                auto count = get_unit_count(current, stop) / step + 1;
                if (count > max_chunk_size - res->size()) {
                    count = max_chunk_size - res->size();
                }

                bool overflow = false;
                auto old_size = res->size();
                resize_column_uninitialized(res.get(), old_size + count);
                auto* data = res->get_data().data();
                for (decltype(count) i = 0; i < count; i++) {
                    data[old_size + i] = current;
                    overflow = add_overflow_unit(current, step, &current);
                    if (overflow) {
                        break;
                    }
                }

                bool done = (step > 0 && current > stop) || (step < 0 && current < stop);
                if (done || overflow) {
                    move_to_next_row();
                } else {
                    state->set_offset(get_unit_count(start, current));
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

private:
    template <typename SERIES_T, typename APPEND_T>
    static bool add_overflow_unit(SERIES_T a, APPEND_T b, SERIES_T* c) {
        return add_overflow(a, (SERIES_T)b, c);
    }

    template <typename APPEND_T>
    static bool add_overflow_unit(TimestampValue a, APPEND_T b, TimestampValue* c) {
        *c = a.add<UNIT>(b);
        return *c == TimestampValue();
    }

    template <typename SERIES_T>
    static SERIES_T get_unit_count(SERIES_T start, SERIES_T stop) {
        return (stop - start);
    }

    static int64_t get_unit_count(TimestampValue start, TimestampValue stop) {
        switch (UNIT) {
        case SECOND:
            return stop.diff_microsecond(start) / USECS_PER_SEC;
        case MINUTE:
            return stop.diff_microsecond(start) / USECS_PER_MINUTE;
        case HOUR:
            return stop.diff_microsecond(start) / USECS_PER_HOUR;
        case DAY:
            return stop.diff_microsecond(start) / USECS_PER_DAY;
        case MONTH:
            return month_diff(stop, start);
        case YEAR:
            return year_diff(stop, start);
        default:
            __builtin_unreachable();
        }
    }

    // from time_function.cpp
    inline static int32_t month_diff(TimestampValue l, TimestampValue r) {
        int year1, month1, day1, hour1, mintue1, second1, usec1;
        int year2, month2, day2, hour2, mintue2, second2, usec2;
        l.to_timestamp(&year1, &month1, &day1, &hour1, &mintue1, &second1, &usec1);
        r.to_timestamp(&year2, &month2, &day2, &hour2, &mintue2, &second2, &usec2);

        int month = (year1 - year2) * 12 + (month1 - month2);

        if (month >= 0) {
            month -= (day1 * 1000000L + (hour1 * 10000 + mintue1 * 100 + second1) <
                      day2 * 1000000L + (hour2 * 10000 + mintue2 * 100 + second2));
        } else {
            month += (day1 * 1000000L + (hour1 * 10000 + mintue1 * 100 + second1) >
                      day2 * 1000000L + (hour2 * 10000 + mintue2 * 100 + second2));
        }
        return month;
    }

    // from time_function.cpp
    inline static int32_t year_diff(TimestampValue l, TimestampValue r) {
        int year1, month1, day1, hour1, mintue1, second1, usec1;
        int year2, month2, day2, hour2, mintue2, second2, usec2;
        l.to_timestamp(&year1, &month1, &day1, &hour1, &mintue1, &second1, &usec1);
        r.to_timestamp(&year2, &month2, &day2, &hour2, &mintue2, &second2, &usec2);

        int year = (year1 - year2);

        if (year >= 0) {
            year -= ((month1 * 100 + day1) * 1000000L + (hour1 * 10000 + mintue1 * 100 + second1) <
                     (month2 * 100 + day2) * 1000000L + (hour2 * 10000 + mintue2 * 100 + second2));
        } else {
            year += ((month1 * 100 + day1) * 1000000L + (hour1 * 10000 + mintue1 * 100 + second1) >
                     (month2 * 100 + day2) * 1000000L + (hour2 * 10000 + mintue2 * 100 + second2));
        }

        return year;
    }
};

} // namespace starrocks
