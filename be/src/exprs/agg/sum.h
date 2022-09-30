// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

    void restore(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
                 size_t row_num) const override {
        DCHECK(column->is_numeric() || column->is_decimal());
        const auto* agg_column = down_cast<const InputColumnType*>(column);
        VLOG(1) << " sum restore:" << row_num;
        this->data(state).sum = agg_column->get_data()[row_num];
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->get_data().data();
        for (size_t i = 0; i < chunk_size; ++i) {
            this->data(state).sum += data[i];
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->get_data().data();
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state).sum += data[i];
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->get_data().data();

        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
        const int64_t current_frame_last_position = current_row_position + rows_end_offset;
        if (!ignore_subtraction && previous_frame_first_position >= partition_start &&
            previous_frame_first_position < partition_end) {
            this->data(state).sum -= data[previous_frame_first_position];
        }
        if (!ignore_addition && current_frame_last_position >= partition_start &&
            current_frame_last_position < partition_end) {
            this->data(state).sum += data[current_frame_last_position];
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

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());
        down_cast<ResultColumnType*>(to)->append(this->data(state).sum);
    }

    void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        ResultColumnType* column = down_cast<ResultColumnType*>(to);
        Buffer<ResultType>& result_data = column->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            result_data.emplace_back(this->data(agg_states[i] + state_offset).sum);
        }
    }

    void finalize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                            Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());
        down_cast<ResultColumnType*>(to)->append(this->data(state).sum);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        Buffer<ResultType>& dst_data = down_cast<ResultColumnType*>((*dst).get())->get_data();
        const auto* src_data = down_cast<const InputColumnType*>(src[0].get())->get_data().data();

        dst_data.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            dst_data[i] = src_data[i];
        }
    }

    std::string get_name() const override { return "sum"; }
};

template <PrimitiveType PT, typename = DecimalPTGuard<PT>>
using DecimalSumAggregateFunction =
        SumAggregateFunction<PT, RunTimeCppType<PT>, TYPE_DECIMAL128, RunTimeCppType<TYPE_DECIMAL128>>;

} // namespace starrocks::vectorized
