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
#include "exprs/agg/combinator/agg_state_combinator.h"

namespace starrocks {
struct AggStateUnionState {};

// An aggregate union combinator that combines intermediate states to compute the intermediate result of aggregate function.
//
// DESC: intermediate_type {agg_func}_union(intermediate_type)
//  input type          : aggregate function's intermediate_type
//  intermediate type   : aggregate function's intermediate_type
//  return type         : aggregate function's intermediate_type
class AggStateUnion final : public AggStateCombinator<AggStateUnionState, AggStateUnion> {
public:
    AggStateUnion(AggStateDesc agg_state_desc, const AggregateFunction* function)
            : AggStateCombinator(agg_state_desc, function) {
        DCHECK(_function != nullptr);
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
};

} // namespace starrocks
