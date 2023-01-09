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

#include "column/binary_column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"

namespace starrocks {

/**
 * RETURN_TYPE: TYPE_BIGINT
 * ARGS_TYPE: TYPE_HLL
 * SERIALIZED_TYPE: TYPE_HLL
 */
class HllUnionCountAggregateFunction final
        : public AggregateFunctionBatchHelper<HyperLogLog, HllUnionCountAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto* column = down_cast<const HyperLogLogColumn*>(columns[0]);
        this->data(state).merge(*(column->get_object(row_num)));
    }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const HyperLogLogColumn*>(columns[0]);
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state).merge(*(column->get_object(i)));
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_object());

        const auto* hll_column = down_cast<const HyperLogLogColumn*>(column);
        this->data(state).merge(*(hll_column->get_object(row_num)));
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        int64_t result = this->data(state).estimate_cardinality();

        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override {
        DCHECK(to->is_object());
        auto* column = down_cast<HyperLogLogColumn*>(to);
        auto& hll_value = const_cast<HyperLogLog&>(this->data(state));
        column->append(std::move(hll_value));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        DCHECK(to->is_numeric());

        auto* column = down_cast<Int64Column*>(to);
        column->append(this->data(state).estimate_cardinality());
    }

    std::string get_name() const override { return "hll_union_agg"; }
};

} // namespace starrocks
