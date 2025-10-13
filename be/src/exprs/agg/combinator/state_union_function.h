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

#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "exprs/agg/combinator/state_combinator.h"
#include "exprs/function_context.h"
#include "runtime/agg_state_desc.h"
#include "runtime/mem_pool.h"

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

    ~StateUnionFunction() {
        if (_nested_ctx != nullptr) {
            delete _nested_ctx;
        }
    }

    virtual Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        _nested_ctx =
                FunctionContext::create_context(context->state(), context->mem_pool(),
                                                _agg_state_desc.get_return_type(), _agg_state_desc.get_arg_types());
        return Status::OK();
    }

    StatusOr<ColumnPtr> execute(FunctionContext* context, const Columns& columns) override {
        RETURN_IF_UNLIKELY(
                columns.size() != _arg_nullables.size(),
                Status::InternalError("StateUnionFunction execute columns size " + std::to_string(columns.size()) +
                                      " not match with arg_nullables size " + std::to_string(_arg_nullables.size())));

        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultAggStateMergeFunctionAllocator);

        MutableColumns new_columns;
        new_columns.reserve(columns.size());
        for (auto i = 0; i < columns.size(); i++) {
            bool is_result_nullable = _agg_state_desc.is_result_nullable() || _arg_nullables[i];
            ASSIGN_OR_RETURN(ColumnPtr new_column, _convert_to_nullable_column(columns[i], is_result_nullable, true));
            new_columns.emplace_back(new_column->as_mutable_ptr());
        }

        auto chunk_size = columns[0]->size();
        auto align_size = _function->alignof_size();
        auto state_size = align_to(_function->size(), align_size);
        MutableColumnPtr result = ColumnHelper::create_column(_intermediate_type, _agg_state_desc.is_result_nullable());
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
                data_columns.emplace_back(ColumnHelper::get_data_column(new_columns[i].get()));
            }
            for (size_t i = 0; i < chunk_size; i++) {
                _function->create(_nested_ctx, agg_state);
                // merge input agg states into result
                for (size_t j = 0; j < new_columns.size(); j++) {
                    if (UNLIKELY(new_columns[j]->is_null(i))) {
                        continue;
                    }
                    _function->merge(_nested_ctx, data_columns[j], agg_state, i);
                }
                // serialize the agg_state into result
                _function->serialize_to_column(_nested_ctx, agg_state, result.get());
                // destroy the agg_state
                _function->destroy(_nested_ctx, agg_state);
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                _function->create(_nested_ctx, agg_state);

                // merge input agg states into result
                for (size_t j = 0; j < new_columns.size(); j++) {
                    _function->merge(_nested_ctx, new_columns[j].get(), agg_state, i);
                }
                // serialize the agg_state into result
                _function->serialize_to_column(_nested_ctx, agg_state, result.get());

                // destroy the agg_state
                _function->destroy(_nested_ctx, agg_state);
            }
        }

        return result;
    }

private:
    // for nested function, the nested function's context is injected to the parent function's context.
    FunctionContext* _nested_ctx;
};

} // namespace starrocks
