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

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_traits.h"
#include "gutil/casts.h"
#include "util/raw_container.h"

namespace starrocks {

template <LogicalType LT, typename = guard::Guard>
struct MaxAggregateData {};

template <LogicalType LT>
struct MaxAggregateData<LT, AggregateComplexLTGuard<LT>> {
    using T = AggDataValueType<LT>;
    T result = RunTimeTypeLimits<LT>::min_value();

    void reset() { result = RunTimeTypeLimits<LT>::min_value(); }
};

// TODO(murphy) refactor the guard with AggDataTypeTraits
template <LogicalType LT>
struct MaxAggregateData<LT, StringLTGuard<LT>> {
    void assign(const Slice& slice) {
        size_t buffer_length = std::max<size_t>(PADDED_SIZE, slice.size);
        _buffer.resize(buffer_length);
        memcpy(_buffer.data(), slice.data, slice.size);
        _size = slice.size;
    }

    bool has_value() const { return _size > -1; }

    Slice slice() const { return {_buffer.data(), (size_t)_size}; }

    void reset() {
        _buffer.clear();
        _size = -1;
    }

private:
    int32_t _size = -1;
    raw::RawVector<uint8_t> _buffer;
};

template <LogicalType LT, typename = guard::Guard>
struct MinAggregateData {};

template <LogicalType LT>
struct MinAggregateData<LT, AggregateComplexLTGuard<LT>> {
    using T = AggDataValueType<LT>;
    T result = RunTimeTypeLimits<LT>::max_value();

    void reset() { result = RunTimeTypeLimits<LT>::max_value(); }
};

template <LogicalType LT>
struct MinAggregateData<LT, StringLTGuard<LT>> {
    void assign(const Slice& slice) {
        size_t buffer_length = std::max<size_t>(PADDED_SIZE, slice.size);
        _buffer.resize(buffer_length);
        memcpy(_buffer.data(), slice.data, slice.size);
        _size = slice.size;
    }

    bool has_value() const { return _size > -1; }

    Slice slice() const { return {_buffer.data(), (size_t)_size}; }

    void reset() {
        _buffer.clear();
        _size = -1;
    }

private:
    int32_t _size = -1;
    raw::RawVector<uint8_t> _buffer;
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MaxElement {
    using T = RunTimeCppType<LT>;
    // `is_sync` indicates whether to sync detail state to genreate the
    // final result. If retract's row is greater or equal to now maxest value,
    // need sync details from detail state table.
    static bool is_sync(State& state, const T& right) { return state.result <= right; }
    void operator()(State& state, const T& right) const { AggDataTypeTraits<LT>::update_max(state.result, right); }
    static bool equals(const State& state, const T& right) {
        return AggDataTypeTraits<LT>::equals(state.result, right);
    }
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MinElement {
    using T = RunTimeCppType<LT>;
    // `is_sync` indicates whether to sync detail state to genreate the
    // final result. If retract's row is smaller or equal to now maxest value,
    // need sync details from detail state table.
    static bool is_sync(State& state, const T& right) { return state.result >= right; }
    void operator()(State& state, const T& right) const { AggDataTypeTraits<LT>::update_min(state.result, right); }
    static bool equals(const State& state, const T& right) {
        return AggDataTypeTraits<LT>::equals(state.result, right);
    }
};

template <LogicalType LT, typename State>
struct MaxElement<LT, State, StringLTGuard<LT>> {
    // `is_sync` indicates whether to sync detail state to genreate the
    // final result. If retract's row is greater or equal to now maxest value,
    // need sync details from detail state table.
    static bool is_sync(State& state, const Slice& right) {
        return !state.has_value() || state.slice().compare(right) <= 0;
    }
    void operator()(State& state, const Slice& right) const {
        if (!state.has_value() || memcompare_padded(state.slice().get_data(), state.slice().get_size(),
                                                    right.get_data(), right.get_size()) < 0) {
            state.assign(right);
        }
    }

    static bool equals(const State& state, const Slice& right) {
        return !state.has_value() || state.slice().compare(right) == 0;
    }
};

template <LogicalType LT, typename State>
struct MinElement<LT, State, StringLTGuard<LT>> {
    // `is_sync` indicates whether to sync detail state to genreate the
    // final result. If retract's row is smaller or equal to now maxest value,
    // need sync details from detail state table.
    static bool is_sync(State& state, const Slice& right) {
        return !state.has_value() || state.slice().compare(right) >= 0;
    }
    void operator()(State& state, const Slice& right) const {
        if (!state.has_value() || memcompare_padded(state.slice().get_data(), state.slice().get_size(),
                                                    right.get_data(), right.get_size()) > 0) {
            state.assign(right);
        }
    }

    static bool equals(const State& state, const Slice& right) {
        return !state.has_value() || state.slice().compare(right) == 0;
    }
};

template <LogicalType LT, typename State, class OP, typename T = RunTimeCppType<LT>, typename = guard::Guard>
class MaxMinAggregateFunction final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunction<LT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[0]->is_nullable() && !columns[0]->is_binary());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        T value = column.get_data()[row_num];
        OP()(this->data(state), value);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition,
                                             [[maybe_unused]] bool has_null) const override {
        [[maybe_unused]] const auto& column = down_cast<const InputColumnType&>(*columns[0]);

        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
        int64_t current_frame_last_position = current_row_position + rows_end_offset;
        if (!ignore_subtraction && previous_frame_first_position >= partition_start &&
            previous_frame_first_position < partition_end) {
            if (OP::equals(this->data(state), column.get_data()[previous_frame_first_position])) {
                current_frame_last_position = std::min(current_frame_last_position, partition_end - 1);
                this->data(state).reset();
                int64_t frame_start = previous_frame_first_position + 1;
                int64_t frame_end = current_frame_last_position + 1;
                if (has_null) {
                    const auto null_column = down_cast<const NullColumn*>(columns[1]);
                    const uint8_t* f_data = null_column->raw_data();
                    for (size_t i = frame_start; i < frame_end; ++i) {
                        if (f_data[i] == 0) {
                            update(ctx, columns, state, i);
                        }
                    }
                } else {
                    update_batch_single_state_with_frame(ctx, state, columns, partition_start, partition_end,
                                                         frame_start, frame_end);
                }
                return;
            }
        }

        if (!ignore_addition && current_frame_last_position >= partition_start &&
            current_frame_last_position < partition_end) {
            update(ctx, columns, state, current_frame_last_position);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(!column->is_nullable() && !column->is_binary());
        const auto* input_column = down_cast<const InputColumnType*>(column);
        T value = input_column->get_data()[row_num];
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        AggDataTypeTraits<LT>::append_value(down_cast<InputColumnType*>(to), this->data(state).result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        AggDataTypeTraits<LT>::append_value(down_cast<InputColumnType*>(to), this->data(state).result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<InputColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            AggDataTypeTraits<LT>::assign_value(column, i, this->data(state).result);
        }
    }

    std::string get_name() const override { return "maxmin"; }
};

template <LogicalType LT, typename State, class OP>
class MaxMinAggregateFunction<LT, State, OP, RunTimeCppType<LT>, StringLTGuard<LT>> final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunction<LT, State, OP, RunTimeCppType<LT>>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK((*columns[0]).is_binary());
        auto* binary_column = down_cast<const BinaryColumn*>(columns[0]);
        auto value = binary_column->get_slice(row_num);
        OP()(this->data(state), value);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition,
                                             [[maybe_unused]] bool has_null) const override {
        [[maybe_unused]] const auto& column = down_cast<const BinaryColumn&>(*columns[0]);

        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
        int64_t current_frame_last_position = current_row_position + rows_end_offset;
        if (!ignore_subtraction && previous_frame_first_position >= partition_start &&
            previous_frame_first_position < partition_end) {
            if (OP::equals(this->data(state), column.get_data()[previous_frame_first_position])) {
                current_frame_last_position = std::min(current_frame_last_position, partition_end - 1);
                this->data(state).reset();
                int64_t frame_start = previous_frame_first_position + 1;
                int64_t frame_end = current_frame_last_position + 1;
                if (has_null) {
                    const auto null_column = down_cast<const NullColumn*>(columns[1]);
                    const uint8_t* f_data = null_column->raw_data();
                    for (size_t i = frame_start; i < frame_end; ++i) {
                        if (f_data[i] == 0) {
                            update(ctx, columns, state, i);
                        }
                    }
                } else {
                    update_batch_single_state_with_frame(ctx, state, columns, partition_start, partition_end,
                                                         frame_start, frame_end);
                }
                return;
            }
        }

        if (!ignore_addition && current_frame_last_position >= partition_start &&
            current_frame_last_position < partition_end) {
            update(ctx, columns, state, current_frame_last_position);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        auto* binary_column = down_cast<const BinaryColumn*>(column);
        auto value = binary_column->get_slice(row_num);
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        column->append(this->data(state).slice());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        column->append(this->data(state).slice());
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<BinaryColumn*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->append(this->data(state).slice());
        }
    }

    std::string get_name() const override { return "maxmin"; }
};

} // namespace starrocks
