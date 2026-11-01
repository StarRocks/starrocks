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

#include "column/runtime_type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "gutil/compiler_util.h"
#include "types/integer_overflow_arithmetics.h"
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

// `CheckOverflow` is only enabled for the decimal specialization below (see
// `DecimalSumAggregateFunction`). Integer SUM must keep MySQL's silent wraparound semantics, so the
// generic instantiations leave it false and compile down to the exact same `sum += value` as before.
template <LogicalType LT, typename T = RunTimeCppType<LT>, LogicalType ResultLT = SumResultLT<LT>,
          typename ResultType = RunTimeCppType<ResultLT>, bool CheckOverflow = false>
class SumAggregateFunction final
        : public AggregateFunctionBatchHelper<SumAggregateState<ResultType>,
                                              SumAggregateFunction<LT, T, ResultLT, ResultType, CheckOverflow>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using ResultColumnType = RunTimeColumnType<ResultLT>;

    // Same wording as DecimalBinaryFunction uses for `a + b` (exprs/decimal_binary_function.h:68),
    // so the aggregate and the binary operator report the identical text.
    static constexpr const char* kOverflowError = "The 'sum' operation involving decimal values overflows";

    // DECIMAL32/64/128 all accumulate into an int128 (DECIMAL256 into an int256), and a bare `+=`
    // wraps silently: SUM() over two rows of 10^38-1 returns
    // -140282366920938463463374607431768211458, which is the true sum minus 2^128.
    // Detect the wrap and, when `sql_mode = 'ERROR_IF_OVERFLOW'` is set, report it through
    // FunctionContext::set_error(), the non-throwing error channel the aggregate framework already
    // polls: Aggregator::check_has_error() (exec/aggregator.cpp) and Analytor::_check_has_error()
    // (exec/analytor.cpp) turn a set error into Status::RuntimeError at the next RETURN_IF_ERROR.
    //
    // Deliberately NOT `throw`: several aggregation entry points run outside any catch handler -
    // Analytor::finish_process(), SortedStreamingAggregator::_update_states() and the two spillable
    // aggregate sources - so an exception escaping from here reaches the pipeline driver, which has
    // no try/catch, and terminates the BE. set_error() cannot escape on the overflow path itself:
    // it takes a mutex and, only when it is the first error on that context, allocates
    // (exprs/function_context.cpp:132-142 - a std::string assign, a std::stringstream, ss.str() and
    // the Status inside set_process_status), and StarRocks turns a MemTracker breach into
    // std::bad_alloc. That branch runs at most once per context and allocates far less than the
    // surrounding per-chunk work already does, so it is marginal - but it is not "by construction".
    //
    // The accumulator keeps the wrapped value in both modes; see the note on `sum = result` below.
    static void accumulate(FunctionContext* ctx, ResultType& sum, const ResultType& value) {
        if constexpr (CheckOverflow) {
            ResultType result;
            bool overflow = add_overflow(sum, value, &result);
            // Store the wrapped result unconditionally, so the state after an error is exactly the
            // state the DEFAULT mode would hold. set_error() does not unwind, so accumulation keeps
            // running over the rest of the chunk before the error is polled; leaving `sum`
            // untouched instead would make the two modes diverge in a value that is about to be
            // discarded anyway, and would risk re-reporting on every subsequent row from a
            // different starting point. The error flag, not the state, is what fails the query.
            sum = result;
            if (UNLIKELY(overflow) && ctx != nullptr && ctx->error_if_overflow()) {
                // is_udf=false: this is a builtin, so no "UDF ERROR: " prefix. set_error() keeps
                // only the first message, so repeated calls over the remainder of the chunk are
                // idempotent.
                ctx->set_error(kOverflowError, false);
            }
        } else {
            sum += value;
        }
    }

    static void deaccumulate(FunctionContext* ctx, ResultType& sum, const ResultType& value) {
        if constexpr (CheckOverflow) {
            ResultType result;
            bool overflow = sub_overflow(sum, value, &result);
            sum = result;
            if (UNLIKELY(overflow) && ctx != nullptr && ctx->error_if_overflow()) {
                ctx->set_error(kOverflowError, false);
            }
        } else {
            sum -= value;
        }
    }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).sum = {};
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(columns[0]->is_numeric() || columns[0]->is_decimal());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        accumulate(ctx, this->data(state).sum, column.immutable_data()[row_num]);
    }

    AggStateTableKind agg_state_table_kind(bool is_append_only) const override { return AggStateTableKind::RESULT; }

    void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                 size_t row_num) const override {
        DCHECK(columns[0]->is_numeric() || columns[0]->is_decimal());
        const auto& column = down_cast<const InputColumnType&>(*columns[0]);
        deaccumulate(ctx, this->data(state).sum, column.immutable_data()[row_num]);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->immutable_data().data();
        for (size_t i = 0; i < chunk_size; ++i) {
            accumulate(ctx, this->data(state).sum, data[i]);
        }
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->immutable_data().data();
        for (size_t i = frame_start; i < frame_end; ++i) {
            accumulate(ctx, this->data(state).sum, data[i]);
        }
    }

    void update_state_removable_cumulatively(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                             int64_t current_row_position, int64_t partition_start,
                                             int64_t partition_end, int64_t rows_start_offset, int64_t rows_end_offset,
                                             bool ignore_subtraction, bool ignore_addition,
                                             [[maybe_unused]] bool has_null) const override {
        const auto* column = down_cast<const InputColumnType*>(columns[0]);
        const auto* data = column->immutable_data().data();

        const int64_t previous_frame_first_position = current_row_position - 1 + rows_start_offset;
        const int64_t current_frame_last_position = current_row_position + rows_end_offset;
        // No overflow check here, on purpose. This is the incremental sliding-window update: it
        // subtracts the row that just left the frame and then adds the row that just entered, so
        // the accumulator transits through `previous_frame_sum - leaving_row`, a value that belongs
        // to no frame. With a = 9e37 and the rows (a, -a, -a, a) under
        // `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`, every frame sum is in range (a, 0, -a, -a)
        // but the transit at row 4 is -1.8e38, outside int128. Checking here would reject a query
        // whose every emitted value is correct. The non-incremental
        // update_batch_single_state_with_frame(), which recomputes each frame from scratch and
        // therefore only ever holds real partial sums, does keep the check - as do update(),
        // merge() and update_batch_single_state().
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
        accumulate(ctx, this->data(state).sum, input_column->immutable_data()[row_num]);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        ResultType result = this->data(state).sum;
        auto* column = down_cast<ResultColumnType*>(dst);
        auto* data = column->get_data().data();
        for (size_t i = start; i < end; ++i) {
            data[i] = result;
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
        auto& result_data = column->get_data();
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
                                     MutableColumnPtr& dst) const override {
        auto& dst_data = down_cast<ResultColumnType*>(dst.get())->get_data();
        const auto* src_data = down_cast<const InputColumnType*>(src[0].get())->immutable_data().data();

        dst_data.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            dst_data[i] = src_data[i];
        }
    }

    void batch_finalize_with_selection(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                                       size_t state_offset, Column* to, const Filter& selection) const override {
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
        SumAggregateFunction<LT, RunTimeCppType<LT>, lt_is_decimal256<LT> ? TYPE_DECIMAL256 : TYPE_DECIMAL128,
                             RunTimeCppType<lt_is_decimal256<LT> ? TYPE_DECIMAL256 : TYPE_DECIMAL128>,
                             /*CheckOverflow=*/true>;
} // namespace starrocks
