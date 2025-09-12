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

#include <string>
#include <utility>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/combinator/state_combinator.h"
#include "exprs/function_context.h"
#include "runtime/agg_state_desc.h"

namespace starrocks {

// A state function that computes the intermediate result of aggregate function.
//
// DESC: intermediate_type {agg_func}_state(arg_types)
//  input type  : aggregate function's argument types
//  return type : aggregate function's intermediate type (e.g. sum_state(col) -> int)
class StateFunction final : public StateCombinator {
public:
    StateFunction(AggStateDesc agg_state_desc, TypeDescriptor intermediate_type, std::vector<bool> arg_nullables)
            : StateCombinator(std::move(agg_state_desc), std::move(intermediate_type), std::move(arg_nullables)) {}

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) override {
        DCHECK_GT(columns.size(), 0);
        DCHECK_EQ(columns.size(), _arg_nullables.size());

        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultAggStateMergeFunctionAllocator);

        Columns new_columns;
        new_columns.reserve(columns.size());
        for (auto i = 0; i < columns.size(); i++) {
            ASSIGN_OR_RETURN(ColumnPtr new_column, _convert_to_nullable_column(columns[i], _arg_nullables[i], false));
            new_columns.emplace_back(new_column);
        }

        // TODO: use mutable ptr as result
        ColumnPtr result = ColumnHelper::create_column(_intermediate_type, _agg_state_desc.is_result_nullable());
        auto chunk_size = columns[0]->size();
        _function->convert_to_serialize_format(context, new_columns, chunk_size, &result);

        return result;
    }
};

} // namespace starrocks
