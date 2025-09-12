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
#include "exprs/agg/combinator/state_combinator.h"

// AggStateCombinator is a base class for all state combinators which is an aggregate function.
// It is used to implement the state combinator for the aggregate function.
namespace starrocks {
template <typename State, typename Derived>
class AggStateCombinator : public AggregateFunctionBatchHelper<State, Derived> {
public:
    AggStateCombinator(AggStateDesc agg_state_desc, const AggregateFunction* function)
            : _agg_state_desc(std::move(agg_state_desc)), _function(function) {
        DCHECK(_function != nullptr);
        VLOG_ROW << "AggStateCombinator constructor:" << _agg_state_desc.debug_string();
    }

    ~AggStateCombinator() = default;

    // get the agg state desc
    const AggStateDesc* get_agg_state_desc() const { return &_agg_state_desc; }

    // create the agg state
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override { _function->create(ctx, ptr); }

    // destroy the agg state
    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override { _function->destroy(ctx, ptr); }

    // get the size of the agg state
    size_t size() const override { return _function->size(); }

    // get the align of the agg state
    size_t alignof_size() const override { return _function->alignof_size(); }

    // check if the agg state is pod
    bool is_pod_state() const override { return _function->is_pod_state(); }

    // reset the agg state
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        _function->reset(ctx, args, state);
    }

protected:
    const AggStateDesc _agg_state_desc;
    const AggregateFunction* _function;
};

} // namespace starrocks
