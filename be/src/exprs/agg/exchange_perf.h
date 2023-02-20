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

#include <fmt/format.h>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "util/time.h"

namespace starrocks {

enum AggExchangePerfType { BYTES = 0, SPEED = 1 };

/**
 * exchange_bytes(columns...) shows how many bytes each BE node exchange columns.
 * exchange_speed(columns...) shows how faster each BE node exchange columns.
 */

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
        this->data(state).start_time = MonotonicNanos();
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
        auto* column = down_cast<Int64Column*>(dst);
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
        auto* column = down_cast<Int64Column*>(to);
        Buffer<int64_t>& result_data = column->get_data();
        for (size_t i = 0; i < chunk_size; i++) {
            result_data.emplace_back(this->data(agg_states[i] + state_offset).bytes);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (PerfType == BYTES) {
            DCHECK(to->is_numeric());
            down_cast<Int64Column*>(to)->append(this->data(state).bytes);
        } else if (PerfType == SPEED) {
            DCHECK(to->is_binary());
            int64_t elapsed_time = MonotonicNanos() - this->data(state).start_time;
            double speed = 0;
            if (elapsed_time > 0) {
                speed = this->data(state).bytes * 1.0 / 1048576.0 * 1000000 / (elapsed_time / 1000.0);
            }
            std::string unit = "MB/s";
            if (speed >= 1024) {
                speed /= 1024;
                unit = "GB/s";
            }

            std::string res = "exchange " + std::to_string(this->data(state).bytes) + " bytes in " +
                              std::to_string(elapsed_time * 1.0 / NANOS_PER_SEC) +
                              " s, speed = " + fmt::format("{:.4f} ", speed) + unit;
            auto* column = down_cast<BinaryColumn*>(to);
            column->append(Slice(res));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<Int64Column*>((*dst).get());
        column->get_data().assign(chunk_size, 1);
    }

    std::string get_name() const override { return PerfType == BYTES ? "exchange_bytes" : "exchange_speed"; }
};

} // namespace starrocks
