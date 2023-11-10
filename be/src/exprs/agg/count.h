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
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks {

struct AggregateCountWindowFunctionState {
    // The following field are only used in "update_state_removable_cumulatively"
    bool is_frame_init = false;
};

template <bool IsWindowFunc>
struct AggregateCountFunctionState
        : public std::conditional_t<IsWindowFunc, AggregateCountWindowFunctionState, AggregateFunctionEmptyState> {
    int64_t count = 0;
};

// count not null column
template <bool IsWindowFunc>
class CountAggregateFunction final : public AggregateFunctionBatchHelper<AggregateCountFunctionState<IsWindowFunc>,
                                                                         CountAggregateFunction<IsWindowFunc>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).count = 0;
        if constexpr (IsWindowFunc) {
            this->data(state).is_frame_init = false;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        ++this->data(state).count;
    }

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override { return AggStateTableKind::RESULT; }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        --this->data(state).count;
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        this->data(state).count += chunk_size;
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // For cases like: rows between 2 preceding and 1 preceding
        // If frame_start ge frame_end, means the frame is empty,
        // we could directly return.
        if (frame_start >= frame_end) {
            return;
        }
        this->data(state).count += (frame_end - frame_start);
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition) const override {
        if constexpr (IsWindowFunc) {
            DCHECK(!ignore_subtraction);
            DCHECK(!ignore_addition);
            // We don't actually update in removable cumulative way, since the ordinary way is effecient enough
            const auto frame_start =
                    std::min(std::max(current_row_position + rows_start_offset, partition_start), partition_end);
            const auto frame_end =
                    std::max(std::min(current_row_position + rows_end_offset + 1, partition_end), partition_start);
            this->data(state).count = (frame_end - frame_start);
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

    std::string get_name() const override { return "count"; }
};

// count null_able column
template <bool IsWindowFunc>
class CountNullableAggregateFunction final
        : public AggregateFunctionBatchHelper<AggregateCountFunctionState<IsWindowFunc>,
                                              CountNullableAggregateFunction<IsWindowFunc>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).count = 0;
        if constexpr (IsWindowFunc) {
            this->data(state).is_frame_init = false;
        }
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        this->data(state).count += !columns[0]->is_null(row_num);
    }

    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        if (columns[0]->has_null()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(columns[0]);
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (size_t i = 0; i < chunk_size; ++i) {
                this->data(states[i] + state_offset).count += !null_data[i];
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                this->data(states[i] + state_offset).count++;
            }
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        if (columns[0]->has_null()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(columns[0]);
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (size_t i = 0; i < chunk_size; ++i) {
                if (filter[i] == 0) {
                    this->data(states[i] + state_offset).count += !null_data[i];
                }
            }
        } else {
            for (size_t i = 0; i < chunk_size; ++i) {
                if (filter[i] == 0) {
                    this->data(states[i] + state_offset).count++;
                }
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        if (columns[0]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(columns[0]);
            if (nullable_column->has_null()) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (size_t i = 0; i < chunk_size; ++i) {
                    this->data(state).count += !null_data[i];
                }
            } else {
                this->data(state).count += nullable_column->size();
            }
        } else {
            this->data(state).count += chunk_size;
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        // For cases like: rows between 2 preceding and 1 preceding
        // If frame_start ge frame_end, means the frame is empty,
        // we could directly return.
        if (frame_start >= frame_end) {
            return;
        }
        if (columns[0]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(columns[0]);
            if (nullable_column->has_null()) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (size_t i = frame_start; i < frame_end; ++i) {
                    this->data(state).count += !null_data[i];
                }
            } else {
                this->data(state).count += (frame_end - frame_start);
            }
        } else {
            this->data(state).count += (frame_end - frame_start);
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition) const override {
        if constexpr (IsWindowFunc) {
            DCHECK(!ignore_subtraction);
            DCHECK(!ignore_addition);
            const auto frame_start =
                    std::min(std::max(current_row_position + rows_start_offset, partition_start), partition_end);
            const auto frame_end =
                    std::max(std::min(current_row_position + rows_end_offset + 1, partition_end), partition_start);
            const auto frame_size = frame_end - frame_start;
            // For cases like: rows between 2 preceding and 1 preceding
            // If frame_start ge frame_end, means the frame is empty,
            // we could directly return.
            if (frame_size <= 0) {
                this->data(state).count = 0;
                return;
            }
            if (columns[0]->is_nullable()) {
                const auto* nullable_column = down_cast<const NullableColumn*>(columns[0]);
                if (nullable_column->has_null()) {
                    const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                    if (this->data(state).is_frame_init) {
                        // Since frame has been evaluated, we only need to update the boundary
                        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
                        const int64_t current_frame_last_position = current_row_position + rows_end_offset;
                        if (previous_frame_first_position >= partition_start &&
                            previous_frame_first_position < partition_end &&
                            null_data[previous_frame_first_position] == 0) {
                            this->data(state).count--;
                        }
                        if (current_frame_last_position >= partition_start &&
                            current_frame_last_position < partition_end &&
                            null_data[current_frame_last_position] == 0) {
                            this->data(state).count++;
                        }
                    } else {
                        // Build the frame for the first time
                        for (size_t i = frame_start; i < frame_end; ++i) {
                            this->data(state).count += !null_data[i];
                        }
                        this->data(state).is_frame_init = true;
                    }
                    return;
                }
            }

            // Has no null value
            // We don't actually update in removable cumulative way, since the ordinary way is effecient enough
            this->data(state).count = frame_size;
            this->data(state).is_frame_init = true;
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
        if (src[0]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(src[0].get());
            if (nullable_column->has_null()) {
                Buffer<int64_t>& dst_data = column->get_data();
                dst_data.resize(chunk_size);
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                for (size_t i = 0; i < chunk_size; ++i) {
                    dst_data[i] = !null_data[i];
                }
            } else {
                column->get_data().assign(chunk_size, 1);
            }
        } else {
            column->get_data().assign(chunk_size, 1);
        }
    }

    std::string get_name() const override { return "count_nullable"; }

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override { return AggStateTableKind::RESULT; }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        this->data(state).count -= !columns[0]->is_null(row_num);
    }
};

} // namespace starrocks
