// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
#include "gutil/casts.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

template <LogicalType PT, typename = guard::Guard>
struct AnyValueAggregateData {};

template <LogicalType PT>
struct AnyValueAggregateData<PT, AggregatePTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = RunTimeTypeLimits<PT>::max_value();
    bool has_value = false;

    void reset() {
        result = RunTimeTypeLimits<PT>::max_value();
        has_value = false;
    }
};

template <LogicalType PT>
struct AnyValueAggregateData<PT, StringPTGuard<PT>> {
    int32_t size = -1;
    Buffer<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <LogicalType PT>
struct AnyValueAggregateData<PT, JsonGuard<PT>> {
    bool has_value = false;
    JsonValue value;

    const JsonValue* json() const { return &value; }

    void reset() {
        value = JsonValue();
        has_value = false;
    }
};

template <LogicalType PT, typename State, typename = guard::Guard>
struct AnyValueElement {
    using T = RunTimeCppType<PT>;
    void operator()(State& state, const T& right) const {
        if (UNLIKELY(!state.has_value)) {
            state.result = right;
            state.has_value = true;
        }
    }
};

template <LogicalType PT, typename State>
struct AnyValueElement<PT, State, StringPTGuard<PT>> {
    void operator()(State& state, const Slice& right) const {
        if (UNLIKELY(!state.has_value())) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
    }
};

template <LogicalType PT, typename State>
struct AnyValueElement<PT, State, JsonGuard<PT>> {
    void operator()(State& state, const JsonValue* right) const {
        if (UNLIKELY(!state.has_value)) {
            state.has_value = true;
            state.value = *right;
        }
    }
};

template <LogicalType PT, typename State, class OP, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class AnyValueAggregateFunction final
        : public AggregateFunctionBatchHelper<State, AnyValueAggregateFunction<PT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[0]->is_nullable() && !columns[0]->is_binary());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        const T& value = column.get_data()[row_num];
        OP()(this->data(state), value);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        update(ctx, columns, state, 0);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(!column->is_nullable() && !column->is_binary());
        const auto* input_column = down_cast<const InputColumnType*>(column);
        T value = input_column->get_data()[row_num];
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        down_cast<InputColumnType*>(to)->append(this->data(state).result);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        down_cast<InputColumnType*>(to)->append(this->data(state).result);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        InputColumnType* column = down_cast<InputColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).result;
        }
    }

    std::string get_name() const override { return "any_value"; }
};

template <LogicalType PT, typename State, class OP>
class AnyValueAggregateFunction<PT, State, OP, RunTimeCppType<PT>, StringPTGuard<PT>> final
        : public AggregateFunctionBatchHelper<State, AnyValueAggregateFunction<PT, State, OP, RunTimeCppType<PT>>> {
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

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        update(ctx, columns, state, 0);
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

    std::string get_name() const override { return "any_value"; }
};

// Specialized for JSON type
template <LogicalType PT, typename State, class OP>
class AnyValueAggregateFunction<PT, State, OP, RunTimeCppType<PT>, JsonGuard<PT>> final
        : public AggregateFunctionBatchHelper<State, AnyValueAggregateFunction<PT, State, OP, RunTimeCppType<PT>>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const JsonValue* value = columns[0]->get(row_num).get_json();
        OP()(this->data(state), value);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        update(ctx, columns, state, 0);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const JsonValue* value = column->get(row_num).get_json();
        OP()(this->data(state), value);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<JsonColumn*>(to);
        column->append(this->data(state).json());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* column = down_cast<JsonColumn*>(to);
        column->append(this->data(state).json());
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<JsonColumn*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->append(this->data(state).json());
        }
    }

    std::string get_name() const override { return "any_value"; }
};

} // namespace starrocks::vectorized
