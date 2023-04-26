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
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
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
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MaxElement {
    using T = RunTimeCppType<LT>;
    // `is_sync` indicates whether to sync detail state to genreate the
    // final result. If retract's row is greater or equal to now maxest value,
    // need sync details from detail state table.
    static bool is_sync(State& state, const T& right) { return state.result <= right; }
    void operator()(State& state, const T& right) const { AggDataTypeTraits<LT>::update_max(state.result, right); }
};

template <LogicalType LT, typename State, typename = guard::Guard>
struct MinElement {
    using T = RunTimeCppType<LT>;
    // `is_sync` indicates whether to sync detail state to genreate the
    // final result. If retract's row is smaller or equal to now maxest value,
    // need sync details from detail state table.
    static bool is_sync(State& state, const T& right) { return state.result >= right; }
    void operator()(State& state, const T& right) const { AggDataTypeTraits<LT>::update_min(state.result, right); }
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
        if (!state.has_value() || state.slice().compare(right) < 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
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
        if (!state.has_value() || state.slice().compare(right) > 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
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
        InputColumnType* column = down_cast<InputColumnType*>(dst);
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
        Slice value = columns[0]->get(row_num).get_slice();
        OP()(this->data(state), value);
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice value = column->get(row_num).get_slice();
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
