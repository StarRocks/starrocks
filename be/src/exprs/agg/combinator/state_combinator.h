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
#include "column/column_viewer.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/function_context.h"
#include "runtime/agg_state_desc.h"

namespace starrocks {

static MemHookAllocator kDefaultAggStateMergeFunctionAllocator = MemHookAllocator{};

// A base class for all state combinators which is a scalar function.
class StateCombinator {
public:
    StateCombinator(AggStateDesc agg_state_desc, TypeDescriptor immediate_type, std::vector<bool> arg_nullables)
            : _agg_state_desc(std::move(agg_state_desc)),
              _immediate_type(std::move(immediate_type)),
              _arg_nullables(std::move(arg_nullables)) {
        _function = AggStateDesc::get_agg_state_func(&_agg_state_desc);
    }

    ~StateCombinator() = default;

    // prepare the state combinator
    virtual Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) { return Status::OK(); }

    // close the state combinator
    virtual Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) { return Status::OK(); }

    // execute the state combinator
    virtual StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) {
        return Status::InternalError("StateCombinator execute is not implemented");
    }

    // align the size to the align
    static size_t align_to(size_t size, size_t align) noexcept { return (size + align - 1) / align * align; }

protected:
    // convert the column to the nullable column if the arg is nullable and the column is not nullable
    StatusOr<ColumnPtr> _convert_to_nullable_column(const ColumnPtr& column, bool arg_nullable,
                                                    bool is_unpack_column) const {
        auto unpack_column =
                is_unpack_column ? ColumnHelper::unpack_and_duplicate_const_column(column->size(), column) : column;
        if (!arg_nullable && unpack_column->is_nullable()) {
            return Status::InternalError(
                    "AggStateBaseFunction input column is nullable but agg function is not nullable");
        }
        if (arg_nullable && !unpack_column->is_nullable()) {
            return ColumnHelper::cast_to_nullable_column(unpack_column);
        }
        return unpack_column;
    }

protected:
    const AggStateDesc _agg_state_desc;
    const TypeDescriptor _immediate_type;
    const std::vector<bool> _arg_nullables;
    const AggregateFunction* _function;
};
using StateCombinatorPtr = std::shared_ptr<starrocks::StateCombinator>;

} // namespace starrocks
