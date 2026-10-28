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

// A state union function that combines intermediate states into a single intermediate state.
//
// DESC: intermediate_type {agg_func}_state_union(intermediate_type, intermediate_type)
//  input type  : (intermediate type, intermediate type)
//  return type : intermediate type
class StateUnionFunction final : public StateCombinator {
public:
    StateUnionFunction(AggStateDesc agg_state_desc, TypeDescriptor intermediate_type, std::vector<bool> arg_nullables)
            : StateCombinator(std::move(agg_state_desc), std::move(intermediate_type), std::move(arg_nullables)) {
        DCHECK(_function != nullptr);
    }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) override {
        RETURN_IF_UNLIKELY(
                columns.size() != _arg_nullables.size(),
                Status::InternalError("StateUnionFunction execute columns size " + std::to_string(columns.size()) +
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

        Columns new_columns;
        new_columns.reserve(columns.size());
        for (auto i = 0; i < columns.size(); i++) {
            bool is_result_nullable = _agg_state_desc.is_result_nullable() || _arg_nullables[i];
            ASSIGN_OR_RETURN(ColumnPtr new_column, _convert_to_nullable_column(columns[i], is_result_nullable, true));
            new_columns.emplace_back(new_column);
        }

        auto chunk_size = columns[0]->size();
        auto align_size = _function->alignof_size();
        auto state_size = BitUtil::round_up(_function->size(), align_size);
        auto result = ColumnHelper::create_column(_intermediate_type, _agg_state_desc.is_result_nullable());
        // allocate the agg_state
        AlignedMemoryGuard guard(align_size, state_size);
        RETURN_IF_ERROR(guard.allocate());
        AggDataPtr agg_state = guard.get();

        // `count` is a special case because `CountNullableAggregateFunction` is used to handle nullable column
        // and its serialize/finalize is meant to not nullable.
        if (_function->get_name() == AggStateUtils::FUNCTION_COUNT ||
            _function->get_name() == AggStateUtils::FUNCTION_COUNT_NULLABLE) {
            std::vector<Column*> data_columns;
            data_columns.reserve(new_columns.size());
            for (size_t i = 0; i < new_columns.size(); i++) {
                data_columns.emplace_back(ColumnHelper::get_data_column(new_columns[i]->as_mutable_raw_ptr()));
            }
            for (size_t i = 0; i < chunk_size; i++) {
                _function->create(nested_ctx, agg_state);
                // merge input agg states into result
                for (size_t j = 0; j < new_columns.size(); j++) {
                    if (UNLIKELY(new_columns[j]->is_null(i))) {
                        continue;
                    }
                    _function->merge(nested_ctx, data_columns[j], agg_state, i);
                }
                // serialize the agg_state into result
                _function->serialize_to_column(nested_ctx, agg_state, result.get());
                // destroy the agg_state
                _function->destroy(nested_ctx, agg_state);
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                _function->create(nested_ctx, agg_state);

                // merge input agg states into result
                for (size_t j = 0; j < new_columns.size(); j++) {
                    _function->merge(nested_ctx, new_columns[j].get(), agg_state, i);
                }
                // serialize the agg_state into result
                _function->serialize_to_column(nested_ctx, agg_state, result.get());

                // destroy the agg_state
                _function->destroy(nested_ctx, agg_state);
            }
        }

        return result;
    }
};

} // namespace starrocks
