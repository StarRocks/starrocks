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

#include "exprs/agg_state_function_call_expr.h"

#include <utility>

#include "base/failpoint/fail_point.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "exprs/agg/combinator/state_combinator.h"
#include "exprs/expr_context.h"

namespace starrocks {

AggStateFunctionCallExpr::AggStateFunctionCallExpr(const TExprNode& node) : Expr(node) {}

Status AggStateFunctionCallExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));

    if (!_fn.__isset.agg_state_desc) {
        return Status::InternalError("AggStateFunctionCallExpr requires agg_state_desc for " + _fn.name.function_name);
    }

    std::vector<FunctionContext::TypeDesc> arg_types;
    arg_types.reserve(_children.size());
    _arg_nullables.clear();
    _arg_nullables.reserve(_children.size());
    for (Expr* child : _children) {
        arg_types.push_back(child->type());
        _arg_nullables.emplace_back(child->is_nullable());
    }

    _state_combinator =
            AggStateUtils::create_state_combinator(_fn.agg_state_desc, _fn.name.function_name, _type, _arg_nullables);
    if (_state_combinator == nullptr) {
        return Status::InternalError("Vectorized engine doesn't implement agg state function " +
                                     _fn.name.function_name);
    }

    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    FAIL_POINT_TRIGGER_RETURN_ERROR(expr_prepare_failed);

    _fn_context_index = context->register_func(state, _type, arg_types);
    return Status::OK();
}

Status AggStateFunctionCallExpr::open(RuntimeState* state, ExprContext* context,
                                      FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        Columns const_columns;
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context));
            const_columns.emplace_back(std::move(child_col));
        }
        fn_ctx->set_constant_columns(std::move(const_columns));
    }

    FAIL_POINT_TRIGGER_RETURN_ERROR(random_error);
    FAIL_POINT_TRIGGER_RETURN_ERROR(expr_prepare_fragment_local_call_failed);
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(_state_combinator->prepare(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    }

    FAIL_POINT_TRIGGER_RETURN_ERROR(expr_prepare_fragment_thread_local_call_failed);
    RETURN_IF_ERROR(_state_combinator->prepare(fn_ctx, FunctionContext::THREAD_LOCAL));
    return Status::OK();
}

void AggStateFunctionCallExpr::close(RuntimeState* state, ExprContext* context,
                                     FunctionContext::FunctionStateScope scope) {
    if (_state_combinator != nullptr && _fn_context_index >= 0) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        (void)_state_combinator->close(fn_ctx, FunctionContext::THREAD_LOCAL);

        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            (void)_state_combinator->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
        }
    }

    Expr::close(state, context, scope);
}

StatusOr<ColumnPtr> AggStateFunctionCallExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    if (_state_combinator == nullptr) {
        return Status::InternalError("Agg state function is not opened: " + _fn.name.function_name);
    }

    Columns args;
    args.reserve(_children.size());
    for (Expr* child : _children) {
        ASSIGN_OR_RETURN(ColumnPtr column, context->evaluate(child, ptr));
        args.emplace_back(std::move(column));
    }

    ASSIGN_OR_RETURN(auto result, _state_combinator->execute(fn_ctx, args));

    if (result->is_constant() && ptr != nullptr) {
        result->as_mutable_raw_ptr()->resize(ptr->num_rows());
    }
    RETURN_IF_ERROR(result->as_mutable_raw_ptr()->unfold_const_children(_type));
    return result;
}

} // namespace starrocks
