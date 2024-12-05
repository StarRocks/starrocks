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

#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"

namespace starrocks {
struct AggStateUnionState {};

/**
 * @brief  Union combinator for aggregate function to union the agg state to return the immediate result of aggregate function.
 * DESC: immediate_type {agg_func}_union(immediate_type)
 *  input type          : aggregate function's immediate_type
 *  intermediate type   : aggregate function's immediate_type
 *  return type         : aggregate function's immediate_type
 */
class AggStateUnion final : public AggregateFunctionBatchHelper<AggStateUnionState, AggStateUnion> {
public:
    AggStateUnion(AggStateDesc agg_state_desc, const AggregateFunction* function)
            : _agg_state_desc(std::move(agg_state_desc)), _function(function) {
        DCHECK(_function != nullptr);
    }
    const AggStateDesc* get_agg_state_desc() const { return &_agg_state_desc; }

    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override { _function->create(ctx, ptr); }

    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override { _function->destroy(ctx, ptr); }

    size_t size() const override { return _function->size(); }

    size_t alignof_size() const override { return _function->alignof_size(); }

    bool is_pod_state() const override { return _function->is_pod_state(); }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        _function->reset(ctx, args, state);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        _function->merge(ctx, columns[0], state, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        _function->merge(ctx, column, state, row_num);
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _function->serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& srcs, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK_EQ(1, srcs.size());
        *dst = srcs[0];
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        _function->serialize_to_column(ctx, state, to);
    }

    std::string get_name() const override { return "agg_state_union"; }

private:
    const AggStateDesc _agg_state_desc;
    const AggregateFunction* _function;
};

} // namespace starrocks
