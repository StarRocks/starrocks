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
#include "exprs/function_context.h"
#include "runtime/agg_state_desc.h"

namespace starrocks {

static MemHookAllocator kDefaultAggStateFunctionAllocator = MemHookAllocator{};

/**
 * @brief compute the immediate result of aggregate function
 * DESC: immediate_type {agg_func}_state(arg_types)
 *  input type  : aggregate function's argument types
 *  return type : aggregate function's immediate type
 */
class AggStateFunction {
public:
    AggStateFunction(AggStateDesc agg_state_desc, TypeDescriptor immediate_type, std::vector<bool> arg_nullables)
            : _agg_state_desc(std::move(agg_state_desc)),
              _immediate_type(std::move(immediate_type)),
              _arg_nullables(std::move(arg_nullables)) {
        _function = AggStateDesc::get_agg_state_func(&_agg_state_desc);
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        if (_function == nullptr) {
            return Status::InternalError("AggStateFunction is nullptr  for " + _agg_state_desc.get_func_name());
        }
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) { return Status::OK(); }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) {
        if (columns.size() == 0) {
            return Status::InternalError("AggStateFunction execute columns is empty");
        }
        if (columns.size() != _arg_nullables.size()) {
            return Status::InternalError("AggStateFunction execute columns size " + std::to_string(columns.size()) +
                                         " not match with arg_nullables size " + std::to_string(_arg_nullables.size()));
        }

        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultAggStateFunctionAllocator);
        Columns new_columns;
        new_columns.reserve(columns.size());
        for (auto i = 0; i < columns.size(); i++) {
            bool arg_nullable = _arg_nullables[i];
            auto& column = columns[i];
            if (!arg_nullable && column->is_nullable()) {
                return Status::InternalError(
                        "AggStateFunction input column is nullable but agg function is not nullable");
            }
            if (arg_nullable && !column->is_nullable()) {
                new_columns.push_back(ColumnHelper::cast_to_nullable_column(column));
            } else {
                new_columns.push_back(column);
            }
        }
        // TODO: use mutable ptr as result
        ColumnPtr result = ColumnHelper::create_column(_immediate_type, _agg_state_desc.is_result_nullable());
        auto chunk_size = columns[0]->size();
        _function->convert_to_serialize_format(context, new_columns, chunk_size, &result);
        return result;
    }

private:
    AggStateDesc _agg_state_desc;
    TypeDescriptor _immediate_type;
    std::vector<bool> _arg_nullables;
    const AggregateFunction* _function;
};
using AggStateFunctionPtr = std::shared_ptr<starrocks::AggStateFunction>;

} // namespace starrocks
