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

#include <algorithm>
#include <iostream>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "util/defer_op.h"

namespace starrocks {

LambdaFunction::LambdaFunction(const TExprNode& node) : Expr(node, false), _common_sub_expr_num(node.output_column) {}

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
        return Status::OK();
    }
    const int child_num = get_num_children() - 2 * _common_sub_expr_num;
    for (int i = 1; i < child_num; i++) {
        _children[i]->get_slot_ids(&_arguments_ids);
    }
    if (child_num - 1 != _arguments_ids.size()) {
        return Status::InternalError(fmt::format("Lambda arguments get ids failed, just get {} ids from {} arguments.",
                                                 _arguments_ids.size(), child_num - 1));
    }
    return Status::OK();
}

Status LambdaFunction::collect_common_sub_exprs() {
    if (!_common_sub_expr_ids.empty()) {
        return Status::OK();
    }

    // common sub expressions include 2 parts in a pair: (slot id, expression)
    const int child_num = get_num_children() - 2 * _common_sub_expr_num;

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

    return Status::OK();
}

Status LambdaFunction::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;

    RETURN_IF_ERROR(collect_lambda_argument_ids());
    RETURN_IF_ERROR(collect_common_sub_exprs());
    // get slot ids from the lambda expression
    get_child(0)->get_slot_ids(&_captured_slot_ids);

    _is_lambda_expr_independent = true;

    // if all captured slot ids are not in lambda arguments ids, then lambda expr is independent.
    // for example,
    // in array_map(x->id, arg1), the lambda expr `id` is independent.
    // but in array_map(x->arg1+id, arg1), the lambda expr `arg1+id` is not independent.
    for (size_t i = 0; i < _captured_slot_ids.size() && _is_lambda_expr_independent; ++i) {
        for (const auto& arguments_id : _arguments_ids) {
            if (_captured_slot_ids[i] == arguments_id) {
                _is_lambda_expr_independent = false;
                break;
            }
        }
    }

    // remove current argument ids and duplicated ids from captured_slot_ids
    std::map<int, bool> captured_mask;
    int valid_id = 0;
    for (int& _captured_slot_id : _captured_slot_ids) {
        if (!captured_mask[_captured_slot_id]) { // not duplicated
            for (const auto& arguments_id : _arguments_ids) {
                if (_captured_slot_id == arguments_id) {
                    captured_mask[_captured_slot_id] = true;
                }
            }
            if (!captured_mask[_captured_slot_id] && _common_sub_expr_num > 0) {
                for (const auto& common_sub_expr_id : _common_sub_expr_ids) {
                    if (_captured_slot_id == common_sub_expr_id) {
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
        ASSIGN_OR_RETURN(auto sub_col, context->evaluate(_common_sub_expr[i], chunk));
        chunk->append_column(sub_col, _common_sub_expr_ids[i]);
    }
    return get_child(0)->evaluate_checked(context, chunk);
}

int LambdaFunction::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    // get_slot_ids only return capture slot ids,
    // if expr is already prepared, we can get result from _captured_slot_ids, otherwise, get result from lambda expr
    if (_is_prepared) {
        slot_ids->insert(slot_ids->end(), _captured_slot_ids.begin(), _captured_slot_ids.end());
        return _captured_slot_ids.size();
    } else {
        return get_child(0)->get_slot_ids(slot_ids);
    }
}

std::string LambdaFunction::debug_string() const {
    std::stringstream out;
    auto expr_debug_string = Expr::debug_string();
    out << "LambaFunction (";
    for (int i = 0; i < _children.size(); i++) {
        out << (i == 0 ? "lambda expr: " : " input argument: ") << _children[i]->debug_string();
    }
    out << ")";
    return out.str();
}

} // namespace starrocks
