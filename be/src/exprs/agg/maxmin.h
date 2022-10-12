// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

template <PrimitiveType PT, typename = guard::Guard>
struct MaxAggregateData {};

template <PrimitiveType PT>
struct MaxAggregateData<PT, IntegralPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();

    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <PrimitiveType PT>
struct MaxAggregateData<PT, FloatPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();
    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <>
struct MaxAggregateData<TYPE_DECIMALV2, guard::Guard> {
    DecimalV2Value result = DecimalV2Value::get_min_decimal();

    void reset() { result = DecimalV2Value::get_min_decimal(); }
};

template <PrimitiveType PT>
struct MaxAggregateData<PT, DecimalPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = get_min_decimal<T>();
    void reset() { result = get_min_decimal<T>(); }
};

template <>
struct MaxAggregateData<TYPE_DATETIME, guard::Guard> {
    TimestampValue result = TimestampValue::MIN_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MIN_TIMESTAMP_VALUE; }
};

template <>
struct MaxAggregateData<TYPE_DATE, guard::Guard> {
    DateValue result = DateValue::MIN_DATE_VALUE;

    void reset() { result = DateValue::MIN_DATE_VALUE; }
};

template <PrimitiveType PT>
struct MaxAggregateData<PT, BinaryPTGuard<PT>> {
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <PrimitiveType PT, typename = guard::Guard>
struct MinAggregateData {};

template <PrimitiveType PT>
struct MinAggregateData<PT, IntegralPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <PrimitiveType PT>
struct MinAggregateData<PT, FloatPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <>
struct MinAggregateData<TYPE_DECIMALV2, guard::Guard> {
    DecimalV2Value result = DecimalV2Value::get_max_decimal();

    void reset() { result = DecimalV2Value::get_max_decimal(); }
};

template <PrimitiveType PT>
struct MinAggregateData<PT, DecimalPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = get_max_decimal<T>();
    void reset() { result = get_max_decimal<T>(); }
};
template <>
struct MinAggregateData<TYPE_DATETIME, guard::Guard> {
    TimestampValue result = TimestampValue::MAX_TIMESTAMP_VALUE;

    void reset() { result = TimestampValue::MAX_TIMESTAMP_VALUE; }
};

template <>
struct MinAggregateData<TYPE_DATE, guard::Guard> {
    DateValue result = DateValue::MAX_DATE_VALUE;

    void reset() { result = DateValue::MAX_DATE_VALUE; }
};

template <PrimitiveType PT>
struct MinAggregateData<PT, BinaryPTGuard<PT>> {
    int32_t size = -1;
    Buffer<uint8_t> buffer;

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <PrimitiveType PT, typename State, typename = guard::Guard>
struct MaxElement {
    using T = RunTimeCppType<PT>;
    void operator()(State& state, const T& right) const { state.result = std::max<T>(state.result, right); }
};

template <PrimitiveType PT, typename State, typename = guard::Guard>
struct MinElement {
    using T = RunTimeCppType<PT>;
    void operator()(State& state, const T& right) const { state.result = std::min<T>(state.result, right); }
};

template <PrimitiveType PT>
struct MaxElement<PT, MaxAggregateData<PT>, BinaryPTGuard<PT>> {
    void operator()(MaxAggregateData<PT>& state, const Slice& right) const {
        if (!state.has_value() || state.slice().compare(right) < 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
    }
};

template <PrimitiveType PT>
struct MinElement<PT, MinAggregateData<PT>, BinaryPTGuard<PT>> {
    void operator()(MinAggregateData<PT>& state, const Slice& right) const {
        if (!state.has_value() || state.slice().compare(right) > 0) {
            state.buffer.resize(right.size);
            memcpy(state.buffer.data(), right.data, right.size);
            state.size = right.size;
        }
    }
};

template <PrimitiveType PT, typename State, class OP, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class MaxMinAggregateFunction final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunction<PT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

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

    std::string get_name() const override { return "maxmin"; }
};

template <PrimitiveType PT, typename State, class OP>
class MaxMinAggregateFunction<PT, State, OP, RunTimeCppType<PT>, BinaryPTGuard<PT>> final
        : public AggregateFunctionBatchHelper<State, MaxMinAggregateFunction<PT, State, OP, RunTimeCppType<PT>>> {
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
        BinaryColumn* column = down_cast<BinaryColumn*>(to);
        column->append(this->data(state).slice());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        BinaryColumn* column = down_cast<BinaryColumn*>(to);
        column->append(this->data(state).slice());
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        BinaryColumn* column = down_cast<BinaryColumn*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->append(this->data(state).slice());
        }
    }

    std::string get_name() const override { return "maxmin"; }
};

} // namespace starrocks::vectorized
