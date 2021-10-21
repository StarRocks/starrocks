// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

template <PrimitiveType PT, typename = guard::Guard>
inline constexpr PrimitiveType SumResultPT = PT;

template <PrimitiveType PT>
inline constexpr PrimitiveType SumResultPT<PT, SumBigIntPTGuard<PT>> = TYPE_BIGINT;

template <PrimitiveType PT>
inline constexpr PrimitiveType SumResultPT<PT, FloatPTGuard<PT>> = TYPE_DOUBLE;

template <PrimitiveType PT>
inline constexpr PrimitiveType SumResultPT<PT, SumDecimal64PTGuard<PT>> = TYPE_DECIMAL64;

// Only for compile, we don't support timestamp and date sum
template <PrimitiveType PT>
inline constexpr PrimitiveType SumResultPT<PT, DateOrDateTimePTGuard<PT>> = TYPE_DATETIME;

template <typename T>
struct SumAggregateState {
    T sum{};
};

template <PrimitiveType PT, typename T = RunTimeCppType<PT>, PrimitiveType ResultPT = SumResultPT<PT>,
          typename ResultType = RunTimeCppType<ResultPT>>
class SumAggregateFunction final
        : public AggregateFunctionBatchHelper<SumAggregateState<ResultType>,
                                              SumAggregateFunction<PT, T, ResultPT, ResultType>> {
public:
    using InputColumnType = RunTimeColumnType<PT>;
    using ResultColumnType = RunTimeColumnType<ResultPT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).sum = {};
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_numeric() || columns[0]->is_decimal());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).sum += column.get_data()[row_num];
    }

    void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->get_data().data();
        for (size_t i = 0; i < batch_size; ++i) {
            this->data(state).sum += data[i];
        }
    }

    void update_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                   int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                   int64_t frame_end) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->get_data().data();
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state).sum += data[i];
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_numeric() || column->is_decimal());
        const auto* input_column = down_cast<const ResultColumnType*>(column);
        this->data(state).sum += input_column->get_data()[row_num];
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        ResultType result = this->data(state).sum;
        ResultColumnType* column = down_cast<ResultColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());
        down_cast<ResultColumnType*>(to)->append(this->data(state).sum);
    }

    void batch_serialize(size_t batch_size, const Buffer<AggDataPtr>& agg_states, size_t state_offset,
                         Column* to) const override {
        ResultColumnType* column = down_cast<ResultColumnType*>(to);
        Buffer<ResultType>& result_data = column->get_data();
        for (size_t i = 0; i < batch_size; i++) {
            result_data.emplace_back(this->data(agg_states[i] + state_offset).sum);
        }
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());
        down_cast<ResultColumnType*>(to)->append(this->data(state).sum);
    }

    void batch_finalize(size_t batch_size, const Buffer<AggDataPtr>& agg_states, size_t state_offset,
                        Column* to) const {
        batch_serialize(batch_size, agg_states, state_offset, to);
    }

    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        Buffer<ResultType>& dst_data = down_cast<ResultColumnType*>((*dst).get())->get_data();
        const auto* src_data = down_cast<const InputColumnType*>(src[0].get())->get_data().data();

        dst_data.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            dst_data[i] = src_data[i];
        }
    }

    std::string get_name() const override { return "sum"; }
};

} // namespace starrocks::vectorized
