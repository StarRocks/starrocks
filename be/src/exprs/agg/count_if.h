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

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/value_generator.h"

namespace starrocks {

struct CountIfFunctionState {
    int64_t count = 0;
};

// count not null column
template <LogicalType LT>
class CountIfAggregateFunction final
        : public AggregateFunctionBatchHelper<CountIfFunctionState, CountIfAggregateFunction<LT>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    template <bool is_add>
    void do_update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state, size_t row_num) const {
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        if (column.get_data()[row_num]) {
            if constexpr (is_add) {
                ++this->data(state).count;
            } else {
                --this->data(state).count;
            }
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        do_update<true>(ctx, columns, state, row_num);
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).count = 0;
    }

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override { return AggStateTableKind::RESULT; }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        do_update<false>(ctx, columns, state, row_num);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        if (frame_start >= frame_end) {
            return;
        }
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition) const override {
        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
        const int64_t current_frame_last_position = current_row_position + rows_end_offset;
        if (!ignore_subtraction && previous_frame_first_position >= partition_start &&
            previous_frame_first_position < partition_end) {
            do_update<false>(ctx, columns, state, previous_frame_first_position);
        }
        if (!ignore_addition && current_frame_last_position >= partition_start &&
            current_frame_last_position < partition_end) {
            do_update<true>(ctx, columns, state, current_frame_last_position);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_numeric());
        const auto* input_column = down_cast<const Int64Column*>(column);
        this->data(state).count += input_column->get_data()[row_num];
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).count;
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).count);
    }

    void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        auto* column = down_cast<Int64Column*>(to);
        Buffer<int64_t>& result_data = column->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            result_data.emplace_back(this->data(agg_states[i] + state_offset).count);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).count);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<Int64Column*>((*dst).get());
        column->get_data().assign(chunk_size, 1);
    }

    void batch_finalize_with_selection(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                                       size_t state_offset, Column* to,
                                       const std::vector<uint8_t>& selection) const override {
        DCHECK(to->is_numeric());
        int64_t values[chunk_size];
        size_t selected_length = 0;
        for (size_t i = 0; i < chunk_size; i++) {
            values[selected_length] = this->data(agg_states[i] + state_offset).count;
            selected_length += !selection[i];
        }
        if (selected_length) {
            CHECK(to->append_numbers(values, selected_length * sizeof(int64_t)));
        }
    }

    std::string get_name() const override { return "count_if"; }
};

} // namespace starrocks