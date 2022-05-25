// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
struct MaxByAggregateData {};

template <PrimitiveType PT>
struct MaxByAggregateData<PT, IntegralPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();
    Slice value = {};

    void reset() {
        result = std::numeric_limits<T>::lowest();
        value.clear();
    }
};

template <PrimitiveType PT>
struct MaxByAggregateData<PT, FloatPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::lowest();
    Slice value = {};

    void reset() { result = std::numeric_limits<T>::lowest(); }
};

template <>
struct MaxByAggregateData<TYPE_DECIMALV2, guard::Guard> {
    DecimalV2Value result = DecimalV2Value::get_min_decimal();
    Slice value = {};

    void reset() { result = DecimalV2Value::get_min_decimal(); }
};

template <PrimitiveType PT>
struct MaxByAggregateData<PT, DecimalPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = get_min_decimal<T>();
    Slice value = {};

    void reset() { result = get_min_decimal<T>(); }
};

template <>
struct MaxByAggregateData<TYPE_DATETIME, guard::Guard> {
    TimestampValue result = TimestampValue::MIN_TIMESTAMP_VALUE;
    Slice value = {};

    void reset() { result = TimestampValue::MIN_TIMESTAMP_VALUE; }
};

template <>
struct MaxByAggregateData<TYPE_DATE, guard::Guard> {
    DateValue result = DateValue::MIN_DATE_VALUE;
    Slice value = {};

    void reset() { result = DateValue::MIN_DATE_VALUE; }
};

template <PrimitiveType PT>
struct MaxByAggregateData<PT, BinaryPTGuard<PT>> {
    int32_t size = -1;
    raw::RawVector<uint8_t> buffer;
    Slice value = {};

    bool has_value() const { return buffer.size() > 0; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <PrimitiveType PT, typename = guard::Guard>
struct MinByAggregateData {};

template <PrimitiveType PT>
struct MinByAggregateData<PT, IntegralPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();
    Slice value = {};

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <PrimitiveType PT>
struct MinByAggregateData<PT, FloatPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = std::numeric_limits<T>::max();
    Slice value = {};

    void reset() { result = std::numeric_limits<T>::max(); }
};

template <>
struct MinByAggregateData<TYPE_DECIMALV2, guard::Guard> {
    DecimalV2Value result = DecimalV2Value::get_max_decimal();
    Slice value = {};

    void reset() { result = DecimalV2Value::get_max_decimal(); }
};

template <PrimitiveType PT>
struct MinByAggregateData<PT, DecimalPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    T result = get_max_decimal<T>();
    Slice value = {};

    void reset() { result = get_max_decimal<T>(); }
};
template <>
struct MinByAggregateData<TYPE_DATETIME, guard::Guard> {
    TimestampValue result = TimestampValue::MAX_TIMESTAMP_VALUE;
    Slice value = {};

    void reset() { result = TimestampValue::MAX_TIMESTAMP_VALUE; }
};

template <>
struct MinByAggregateData<TYPE_DATE, guard::Guard> {
    DateValue result = DateValue::MAX_DATE_VALUE;
    Slice value = {};

    void reset() { result = DateValue::MAX_DATE_VALUE; }
};

template <PrimitiveType PT>
struct MinByAggregateData<PT, BinaryPTGuard<PT>> {
    int32_t size = -1;
    Buffer<uint8_t> buffer;
    Slice value = {};

    bool has_value() const { return size > -1; }

    Slice slice() const { return {buffer.data(), buffer.size()}; }

    void reset() {
        buffer.clear();
        size = -1;
    }
};

template <PrimitiveType PT, typename State, typename = guard::Guard>
struct MaxByOP {
    using T = RunTimeCppType<PT>;

    void operator()(State& state, const Column* column, const T& right) const {}
};

template <PrimitiveType PT, typename State, typename = guard::Guard>
struct MinByOP {
    using T = RunTimeCppType<PT>;
    void operator()(State& state, const Column* column, const T& right) const {}
};

template <PrimitiveType PT>
struct MaxByOP<PT, MaxByAggregateData<PT>, BinaryPTGuard<PT>> {};

template <PrimitiveType PT>
struct MinByOP<PT, MinByAggregateData<PT>, BinaryPTGuard<PT>> {};

template <PrimitiveType PT, typename State, class OP, typename T = RunTimeCppType<PT>, typename = guard::Guard>
class MaxMinByAggregateFunction final
        : public AggregateFunctionBatchHelper<State, MaxMinByAggregateFunction<PT, State, OP, T>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(!columns[1]->is_nullable() && !columns[1]->is_binary());
        const auto& column = down_cast<const InputColumnType&>(*columns[1]);
        T value = column.get_data()[row_num];

#define HANDLE_ELEMENT_TYPE(ElementType)                                           \
    do {                                                                           \
        if (typeid(*columns[0]) == typeid(ElementType)) {                          \
            const auto& value_column = down_cast<const ElementType&>(*columns[0]); \
            value_column.get_data()[row_num];                                      \
            std::cout << "Process column" << std::endl;                            \
        }                                                                          \
    } while (0)
        HANDLE_ELEMENT_TYPE(BooleanColumn);
        HANDLE_ELEMENT_TYPE(Int8Column);
        HANDLE_ELEMENT_TYPE(Int16Column);
        HANDLE_ELEMENT_TYPE(Int32Column);
        HANDLE_ELEMENT_TYPE(Int64Column);
        HANDLE_ELEMENT_TYPE(Int128Column);
        HANDLE_ELEMENT_TYPE(FloatColumn);
        HANDLE_ELEMENT_TYPE(DoubleColumn);
        HANDLE_ELEMENT_TYPE(DecimalColumn);
        HANDLE_ELEMENT_TYPE(Decimal32Column);
        HANDLE_ELEMENT_TYPE(Decimal64Column);
        HANDLE_ELEMENT_TYPE(Decimal128Column);
        HANDLE_ELEMENT_TYPE(BinaryColumn);
        HANDLE_ELEMENT_TYPE(DateColumn);
        HANDLE_ELEMENT_TYPE(TimestampColumn);

        OP()(this->data(state), columns[0], value);
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
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
        // OP()(this->data(state), value);
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

    std::string get_name() const override { return "max_min_by"; }
};

template <PrimitiveType PT, typename State, class OP>
class MaxMinByAggregateFunction<PT, State, OP, RunTimeCppType<PT>, BinaryPTGuard<PT>> final
        : public AggregateFunctionBatchHelper<State, MaxMinByAggregateFunction<PT, State, OP, RunTimeCppType<PT>>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK((*columns[0]).is_binary());
        Slice value = columns[0]->get(row_num).get_slice();
        //OP()(this->data(state), value);
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                   int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) const override {
        for (size_t i = frame_start; i < frame_end; ++i) {
            update(ctx, columns, state, i);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice value = column->get(row_num).get_slice();
        // OP()(this->data(state), value);
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

    std::string get_name() const override { return "max_min_by"; }
};

} // namespace starrocks::vectorized
