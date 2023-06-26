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

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/logical_type.h"

namespace starrocks {

template <LogicalType LT, typename = guard::Guard>
inline constexpr LogicalType SumResultLT = LT;

template <LogicalType LT>
inline constexpr LogicalType SumResultLT<LT, SumBigIntLTGuard<LT>> = TYPE_BIGINT;

template <LogicalType LT>
inline constexpr LogicalType SumResultLT<LT, FloatLTGuard<LT>> = TYPE_DOUBLE;

template <LogicalType LT>
inline constexpr LogicalType SumResultLT<LT, SumDecimal64LTGuard<LT>> = TYPE_DECIMAL64;

// Only for compile, we don't support timestamp and date sum
template <LogicalType LT>
inline constexpr LogicalType SumResultLT<LT, DateOrDateTimeLTGuard<LT>> = TYPE_DATETIME;

template <typename T>
struct SumAggregateState {
    T sum{};
};

template <LogicalType LT, typename T = RunTimeCppType<LT>, LogicalType ResultLT = SumResultLT<LT>,
          typename ResultType = RunTimeCppType<ResultLT>>
class SumAggregateFunction final
        : public AggregateFunctionBatchHelper<SumAggregateState<ResultType>,
                                              SumAggregateFunction<LT, T, ResultLT, ResultType>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).sum = {};
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_numeric() || columns[0]->is_decimal());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).sum += column.get_data()[row_num];
    }

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override { return AggStateTableKind::RESULT; }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        DCHECK(columns[0]->is_numeric() || columns[0]->is_decimal());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        this->data(state).sum -= column.get_data()[row_num];
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
        auto* column = down_cast<ResultColumnType*>(dst);
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
        auto* column = down_cast<ResultColumnType*>(to);
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

    void batch_finalize_with_selection(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                                       size_t state_offset, Column* to,
                                       const std::vector<uint8_t>& selection) const override {
        DCHECK(to->is_numeric());
        ResultType values[chunk_size];
        size_t selected_lengh = 0;
        for (size_t i = 0; i < chunk_size; i++) {
            values[selected_lengh] = this->data(agg_states[i] + state_offset).sum;
            selected_lengh += !selection[i];
        }
        if (selected_lengh) {
            CHECK(to->append_numbers(values, selected_lengh * sizeof(ResultType)));
        }
    }

    std::string get_name() const override { return "sum"; }
};

template <LogicalType LT, typename = DecimalLTGuard<LT>>
using DecimalSumAggregateFunction =
        SumAggregateFunction<LT, RunTimeCppType<LT>, TYPE_DECIMAL128, RunTimeCppType<TYPE_DECIMAL128>>;

} // namespace starrocks
