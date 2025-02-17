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

#include "exprs/lambda_function.h"

#include <fmt/format.h>

#include <iostream>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_context.h"

namespace starrocks {

LambdaFunction::LambdaFunction(const TExprNode& node) : Expr(node, false), _common_sub_expr_num(node.output_column) {}

<<<<<<< HEAD
Status LambdaFunction::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (_is_prepared) {
=======
Status LambdaFunction::extract_outer_common_exprs(RuntimeState* state, ExprContext* expr_ctx, Expr* expr,
                                                  ExtractContext* ctx) {
    if (expr->is_lambda_function()) {
        auto lambda_function = static_cast<LambdaFunction*>(expr);
        RETURN_IF_ERROR(lambda_function->collect_lambda_argument_ids());
        for (auto argument_id : lambda_function->get_lambda_arguments_ids()) {
            ctx->lambda_arguments.insert(argument_id);
        }
        RETURN_IF_ERROR(lambda_function->collect_common_sub_exprs());
        for (auto slot_id : lambda_function->get_common_sub_expr_ids()) {
            ctx->common_sub_expr_ids.insert(slot_id);
        }
    }

    DeferOp defer([&]() {
        if (expr->is_lambda_function()) {
            auto lambda_function = static_cast<LambdaFunction*>(expr);
            for (auto argument_id : lambda_function->get_lambda_arguments_ids()) {
                ctx->lambda_arguments.erase(argument_id);
            }
            for (auto slot_id : lambda_function->get_common_sub_expr_ids()) {
                ctx->common_sub_expr_ids.erase(slot_id);
            }
        }
    });

    if (expr->is_dictmapping_expr()) {
        return Status::OK();
    }

    // for the lambda function, we only consider extracting the outer common expression from the lambda expr,
    // not its arguments
    int child_num = expr->is_lambda_function() ? 1 : expr->get_num_children();
    std::vector<SlotId> slot_ids;

    for (int i = 0; i < child_num; i++) {
        auto child = expr->get_child(i);

        RETURN_IF_ERROR(extract_outer_common_exprs(state, expr_ctx, child, ctx));
        // if child is a slotref or a lambda function or a literal, we can't replace it.
        if (child->is_slotref() || child->is_lambda_function() || child->is_literal() || child->is_constant()) {
            continue;
        }

        slot_ids.clear();
        child->get_slot_ids(&slot_ids);
        bool is_independent = std::all_of(slot_ids.begin(), slot_ids.end(), [ctx](const SlotId& id) {
            return ctx->lambda_arguments.find(id) == ctx->lambda_arguments.end() &&
                   ctx->common_sub_expr_ids.find(id) == ctx->common_sub_expr_ids.end();
        });

        if (is_independent) {
            SlotId slot_id = ctx->next_slot_id++;
#ifdef DEBUG
            expr_ctx->root()->for_each_slot_id([new_slot_id = slot_id](SlotId slot_id) {
                DCHECK_NE(new_slot_id, slot_id) << "slot_id " << new_slot_id << " already exists in expr_ctx";
            });
#endif
            ColumnRef* column_ref = state->obj_pool()->add(new ColumnRef(child->type(), slot_id));
            VLOG(2) << "add new common expr, slot_id: " << slot_id << ", new expr: " << column_ref->debug_string()
                    << ", old expr: " << child->debug_string();
            expr->_children[i] = column_ref;
            ctx->outer_common_exprs.insert({slot_id, child});
        }
    }

    return Status::OK();
}

Status LambdaFunction::extract_outer_common_exprs(RuntimeState* state, ExprContext* expr_ctx, ExtractContext* ctx) {
    RETURN_IF_ERROR(extract_outer_common_exprs(state, expr_ctx, this, ctx));
    return Status::OK();
}

Status LambdaFunction::collect_lambda_argument_ids() {
    if (!_arguments_ids.empty()) {
>>>>>>> ecebd2cb06 ([BugFix] fix lambda rewrite low-cardinality expression bug (#55861))
        return Status::OK();
    }
    _is_prepared = true;
    // common sub expressions include 2 parts in a pair: (slot id, expression)
    const int child_num = get_num_children() - 2 * _common_sub_expr_num;
    // collect the slot ids of lambda arguments
    for (int i = 1; i < child_num; ++i) {
        get_child(i)->get_slot_ids(&_arguments_ids);
    }
    if (child_num - 1 != _arguments_ids.size()) {
        return Status::InternalError(fmt::format("Lambda arguments get ids failed, just get {} ids from {} arguments.",
                                                 _arguments_ids.size(), child_num - 1));
    }
    // sorted common sub expressions so that the later expressions can reference the previous ones.
    for (auto i = child_num; i < child_num + _common_sub_expr_num; ++i) {
        get_child(i)->get_slot_ids(&_common_sub_expr_ids);
    }
    if (_common_sub_expr_ids.size() != _common_sub_expr_num) {
        return Status::InternalError(
                fmt::format("Lambda common sub expression id's size {} is not equal to expected {}",
                            _common_sub_expr_ids.size(), _common_sub_expr_num));
    }

    for (auto i = child_num + _common_sub_expr_num; i < child_num + 2 * _common_sub_expr_num; ++i) {
        _common_sub_expr.push_back(get_child(i));
        get_child(i)->get_slot_ids(&_captured_slot_ids);
    }
    if (_common_sub_expr.size() != _common_sub_expr_num) {
        return Status::InternalError(fmt::format("Lambda common sub expressions' size {} is not equal to expected {}",
                                                 _common_sub_expr.size(), _common_sub_expr_num));
    }

    // get slot ids from the lambda expression
    get_child(0)->get_slot_ids(&_captured_slot_ids);

    // remove current argument ids and duplicated ids from captured_slot_ids
    std::map<int, bool> captured_mask;
    int valid_id = 0;
    for (int& _captured_slot_id : _captured_slot_ids) {
        if (!captured_mask[_captured_slot_id]) { // not duplicated
            for (int arg_id = 0; arg_id < _arguments_ids.size(); ++arg_id) {
                if (_captured_slot_id == _arguments_ids[arg_id]) {
                    captured_mask[_captured_slot_id] = true;
                }
            }
            if (!captured_mask[_captured_slot_id] && _common_sub_expr_num > 0) {
                for (int arg_id = 0; arg_id < _common_sub_expr_ids.size(); ++arg_id) {
                    if (_captured_slot_id == _common_sub_expr_ids[arg_id]) {
                        captured_mask[_captured_slot_id] = true;
                    }
                }
            }

            if (!captured_mask[_captured_slot_id]) { // not from arguments
                _captured_slot_ids[valid_id++] = _captured_slot_id;
            }
            captured_mask[_captured_slot_id] = true;
        }
    }
    // remove invalid elements at the tail
    int removed = _captured_slot_ids.size() - valid_id;
    while (removed--) {
        _captured_slot_ids.pop_back();
    }
    return Status::OK();
}

StatusOr<ColumnPtr> LambdaFunction::evaluate_checked(ExprContext* context, Chunk* chunk) {
    for (auto i = 0; i < _common_sub_expr.size(); ++i) {
        auto sub_col = EVALUATE_NULL_IF_ERROR(context, _common_sub_expr[i], chunk);
        chunk->append_column(sub_col, _common_sub_expr_ids[i]);
    }
    return get_child(0)->evaluate_checked(context, chunk);
}

} // namespace starrocks
