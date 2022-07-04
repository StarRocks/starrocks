// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/nullable_column.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks::vectorized {

struct AggregateFunctionCountData {
    int64_t count = 0;

    // The following field are only used in "update_state_removable_cumulatively"
    bool is_frame_init = false;
};

// count not null column
class CountAggregateFunction final
        : public AggregateFunctionBatchHelper<AggregateFunctionCountData, CountAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).count = 0;
        this->data(state).is_frame_init = false;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        ++this->data(state).count;
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        this->data(state).count += chunk_size;
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
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
                                             int64_t partition_end, int64_t preceding,
                                             int64_t following) const override {
        DCHECK_GE(preceding, 0);
        DCHECK_GE(following, 0);
        // We don't actually update in removable cumulative way, since the ordinary way is effecient enough
        const auto frame_start = std::max(current_row_position - preceding, partition_start);
        const auto frame_end = std::min(current_row_position + following + 1, partition_end);
        this->data(state).count = (frame_end - frame_start);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_numeric());
        const auto* input_column = down_cast<const Int64Column*>(column);
        data(state).count += input_column->get_data()[row_num];
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        Int64Column* column = down_cast<Int64Column*>(dst);
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
        Int64Column* column = down_cast<Int64Column*>(to);
        Buffer<int64_t>& result_data = column->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            result_data.emplace_back(data(agg_states[i] + state_offset).count);
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

    std::string get_name() const override { return "count"; }
};

// count null_able column
class CountNullableAggregateFunction final
        : public AggregateFunctionBatchHelper<AggregateFunctionCountData, CountNullableAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).count = 0;
        this->data(state).is_frame_init = false;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        this->data(state).count += !columns[0]->is_null(row_num);
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

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
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
                                             int64_t partition_end, int64_t preceding,
                                             int64_t following) const override {
        DCHECK_GE(preceding, 0);
        DCHECK_GE(following, 0);
        const auto frame_start = std::max(current_row_position - preceding, partition_start);
        const auto frame_end = std::min(current_row_position + following + 1, partition_end);
        if (columns[0]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(columns[0]);
            if (nullable_column->has_null()) {
                const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
                if (this->data(state).is_frame_init) {
                    // Since frame has been evaluated, we only need to update the boundary
                    if (current_row_position - 1 - preceding >= partition_start &&
                        null_data[current_row_position - 1 - preceding] == 0) {
                        this->data(state).count--;
                    }
                    if (current_row_position + following < partition_end &&
                        null_data[current_row_position + following] == 0) {
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
        this->data(state).count = (frame_end - frame_start);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_numeric());
        const auto* input_column = down_cast<const Int64Column*>(column);
        data(state).count += input_column->get_data()[row_num];
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        Int64Column* column = down_cast<Int64Column*>(dst);
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
        Int64Column* column = down_cast<Int64Column*>(to);
        Buffer<int64_t>& result_data = column->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            result_data.emplace_back(data(agg_states[i] + state_offset).count);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).count);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        Int64Column* column = down_cast<Int64Column*>((*dst).get());
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
};

} // namespace starrocks::vectorized
