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

#include "base/bit/bit_util.h"
#include "base/utility/defer_op.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "exprs/agg/combinator/state_combinator.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "types/agg_state_desc.h"

namespace starrocks {

// A state merge function that computes the final result from intermediate states.
//
// DESC: return_type {agg_func}_state_merge(intermediate_type)
//  input type  : intermediate type
//  return type : return type
class StateMergeFunction final : public StateCombinator {
public:
    StateMergeFunction(AggStateDesc agg_state_desc, TypeDescriptor intermediate_type, std::vector<bool> arg_nullables)
            : StateCombinator(std::move(agg_state_desc), std::move(intermediate_type), std::move(arg_nullables)) {
        DCHECK(_function != nullptr);
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (_function == nullptr) {
            return Status::InternalError("AggStateBaseFunction is nullptr  for " + _agg_state_desc.get_func_name());
        }
        return Status::OK();
    }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) override {
        RETURN_IF_UNLIKELY(
                columns.size() != _arg_nullables.size(),
                Status::InternalError("StateMergeFunction execute columns size " + std::to_string(columns.size()) +
                                      " not match with arg_nullables size " + std::to_string(_arg_nullables.size())));

        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultAggStateMergeFunctionAllocator);

        // Drive the wrapped aggregate through a nested FunctionContext backed by a MemPool private to
        // this execute() call. The combinator object is shared across pipeline drivers and evaluated
        // concurrently, so the pool must not be shared (MemPool is not thread-safe); allocating it on
        // the stack also bounds its lifetime to this call, so the variable-length agg state copied out
        // per row (e.g. array_agg_distinct's keys, which _function->destroy() does not reclaim from the
        // pool) is released here instead of accumulating for the whole fragment.
        MemPool nested_mem_pool;
        FunctionContext* nested_ctx = FunctionContext::create_context(
                context->state(), &nested_mem_pool, _agg_state_desc.get_return_type(), _agg_state_desc.get_arg_types());
        DeferOp defer_nested_ctx([&]() { delete nested_ctx; });

        bool is_result_nullable = _agg_state_desc.is_result_nullable() || _arg_nullables[0];
        ASSIGN_OR_RETURN(ColumnPtr new_column, _convert_to_nullable_column(columns[0], is_result_nullable, true));

        auto& ret_type = _agg_state_desc.get_return_type();
        MutableColumnPtr result = ColumnHelper::create_column(ret_type, _agg_state_desc.is_result_nullable());
        auto chunk_size = columns[0]->size();

        // finalize agg states into result
        auto align_size = _function->alignof_size();
        auto state_size = BitUtil::round_up(_function->size(), align_size);
        AlignedMemoryGuard guard(align_size, state_size);
        RETURN_IF_ERROR(guard.allocate());
        AggDataPtr agg_state = guard.get();

        // `count` is a special case because `CountNullableAggregateFunction` is used to handle nullable column
        // and its serialize/finalize is meant to not nullable.
        DCHECK_EQ(is_result_nullable, new_column->is_nullable());
        std::string function_name = _function->get_name();
        if (function_name == AggStateUtils::FUNCTION_COUNT || function_name == AggStateUtils::FUNCTION_COUNT_NULLABLE) {
            auto* data_column = ColumnHelper::get_data_column(new_column.get());
            for (size_t i = 0; i < chunk_size; i++) {
                if (new_column->is_null(i)) {
                    result->append_nulls(1);
                    continue;
                }
                _function->create(nested_ctx, agg_state);
                _function->merge(nested_ctx, data_column, agg_state, i);
                _function->finalize_to_column(nested_ctx, agg_state, result.get());
                _function->destroy(nested_ctx, agg_state);
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                if (new_column->is_null(i)) {
                    result->append_nulls(1);
                    continue;
                }
                _function->create(nested_ctx, agg_state);
                _function->merge(nested_ctx, new_column.get(), agg_state, i);
                _function->finalize_to_column(nested_ctx, agg_state, result.get());
                _function->destroy(nested_ctx, agg_state);
            }
        }
        return result;
    }
};

} // namespace starrocks
