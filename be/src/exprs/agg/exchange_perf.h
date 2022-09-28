// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <fmt/format.h>

#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/time.h"

namespace starrocks::vectorized {

enum AggExchangePerfType { BYTES = 0, RATIO = 1 };

struct AggregateExchangePerfFunctionState : public AggregateFunctionEmptyState {
    int64_t bytes = 0;
    int64_t start_time = MonotonicNanos();
};

template <AggExchangePerfType PerfType>
class ExchangePerfAggregateFunction final
        : public AggregateFunctionBatchHelper<AggregateExchangePerfFunctionState,
                                              ExchangePerfAggregateFunction<PerfType>> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).bytes = 0;
        this->start_time = MonotonicNanos();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        for (auto i = 0; i < ctx->get_num_args(); ++i) {
            this->data(state).bytes += columns[i]->byte_size(row_num);
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        for (auto i = 0; i < ctx->get_num_args(); ++i) {
            this->data(state).bytes += columns[i]->byte_size();
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_numeric());
        const auto* input_column = down_cast<const Int64Column*>(column);
        this->data(state).bytes += input_column->get_data()[row_num];
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        Int64Column* column = down_cast<Int64Column*>(dst);
        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = this->data(state).bytes;
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());
        down_cast<Int64Column*>(to)->append(this->data(state).bytes);
    }

    void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        Int64Column* column = down_cast<Int64Column*>(to);
        Buffer<int64_t>& result_data = column->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            result_data.emplace_back(this->data(agg_states[i] + state_offset).bytes);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (PerfType == BYTES) {
            DCHECK(to->is_numeric());
            down_cast<Int64Column*>(to)->append(this->data(state).bytes);
        } else if (PerfType == RATIO) {
            DCHECK(to->is_binary());
            int64_t elapsed_time = MonotonicNanos() - this->data(state).start_time;
            double ratio = 0;
            if (elapsed_time > 0) {
                ratio = this->data(state).bytes * 1000000000.0 / elapsed_time;
            }
            string res = fmt::format("{:.2f} KB/s", ratio);
            BinaryColumn* column = down_cast<BinaryColumn*>(to);
            column->append(Slice(res));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<Int64Column*>((*dst).get());
        column->get_data().assign(chunk_size, 1);
    }

    std::string get_name() const override { return PerfType == BYTES ? "exchange_bytes" : "exchange_ratio"; }
};

} // namespace starrocks::vectorized
