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

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/combinator/agg_state_combinator.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "runtime/agg_state_desc.h"

namespace starrocks {
struct AggStateCombineState {};

// An aggregate combine combinator that combines aggregate inputs to compute intermediate results.
// This combinator is equivalent to calling `{agg_func}_union({agg_func}_state(arg_types))` in SQL,
// but with reduced function call overhead and memory allocation for better performance.
// eg:
// - SQL: sum_union(sum_state(col))
// - This combinator: sum_combine(col)
//
// DESC: intermediate_type {agg_func}_combine(arg types)
//  input type          : aggregate function's arg types
//  intermediate type   : aggregate function's intermediate_type
//  return type         : aggregate function's intermediate_type
class AggStateCombine final : public AggStateCombinator<AggStateCombineState, AggStateCombine> {
public:
    AggStateCombine(AggStateDesc agg_state_desc, const AggregateFunction* function)
            : AggStateCombinator(agg_state_desc, function) {
        DCHECK(_function != nullptr);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        _function->update(ctx, columns, state, row_num);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        _function->merge(ctx, column, state, row_num);
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _serialize_to_column_nullable(ctx, state, to);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& srcs, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK_EQ(1, srcs.size());
        *dst = srcs[0];
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        _serialize_to_column_nullable(ctx, state, to);
    }

    std::string get_name() const override { return "agg_state_combine"; }

private:
    inline void _serialize_to_column_nullable(FunctionContext* ctx, ConstAggDataPtr __restrict state,
                                              Column* to) const {
        // `count` is a special case because `CountNullableAggregateFunction` is used to handle nullable column
        // and its serialize/finalize is meant to not nullable.
        if (_function->get_name() == AggStateUtils::FUNCTION_COUNT ||
            _function->get_name() == AggStateUtils::FUNCTION_COUNT_NULLABLE) {
            if (LIKELY(to->is_nullable())) {
                auto* nullable_column = down_cast<NullableColumn*>(to);
                _function->serialize_to_column(ctx, state, nullable_column->mutable_data_column());
                nullable_column->null_column_data().push_back(0);
            } else {
                _function->serialize_to_column(ctx, state, to);
            }
        } else {
            _function->serialize_to_column(ctx, state, to);
        }
    }
};

} // namespace starrocks
