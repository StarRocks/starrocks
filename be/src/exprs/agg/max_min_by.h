// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/any_value_helper.h"
#include "gutil/casts.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

enum MaxMinByOpType { MAX = 0, MIN = 1 };

template <PrimitiveType PT1, PrimitiveType PT2>
struct MaxMinByAggregateData {
    using ResultType = RunTimeCppType<PT1>;

    AggregateData<PT1> value;
    AggregateData<PT2> arg;

    using Self = MaxMinByAggregateData<PT1, PT2>;

    template <class DataType>
    DataType get_result() {
        return value.get_value();
    }

    void reset() {
        value.reset();
        arg.reset();
    }

    void change_if_better(const Column** columns, size_t row_num, MaxMinByOpType OP) {
        if (OP == MIN) {
            if (arg.change_if_less(*columns[1], row_num)) {
                value.change(*columns[0], row_num);
            }
        } else {
            if (arg.change_if_greater(*columns[1], row_num)) {
                value.change(*columns[0], row_num);
            }
        }
    }

    void change_if_better(ResultType right, MaxMinByOpType OP) {
        if (OP == MIN) {
            if (arg.change_if_less(right)) {
                value.change(right);
            }
        } else {
            if (arg.change_if_greater(right)) {
                value.change(right);
            }
        }
    }
};

template <PrimitiveType PT1, PrimitiveType PT2, MaxMinByOpType OP, typename = guard::Guard>
class MaxMinByAggregateFunction final
        : public AggregateFunctionBatchHelper<MaxMinByAggregateData<PT1, PT2>, MaxMinByAggregateFunction<PT1, PT2, OP>> {
public:
    using ResultColumnType = RunTimeColumnType<PT1>;
    using T1 = RunTimeCppType<PT1>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        this->data(state).change_if_better(columns, row_num, OP);
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
        const auto* input_column = down_cast<const ResultColumnType*>(column);
        T1 value = input_column->get_data()[row_num];
        this->data(state).change_if_better(value, OP);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        down_cast<ResultColumnType*>(to)->append(this->data(state).value.value);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(!to->is_nullable() && !to->is_binary());
        down_cast<ResultColumnType*>(to)->append(this->data(state).value.value);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        ResultColumnType* column = down_cast<ResultColumnType*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).value.value;
        }
    }

    std::string get_name() const override { return "max_min_by"; }
};

} // namespace starrocks::vectorized
