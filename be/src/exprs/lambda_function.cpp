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

namespace starrocks {

LambdaFunction::LambdaFunction(const TExprNode& node) : Expr(node, false), _common_sub_expr_num(node.output_column) {
}

Status LambdaFunction::extract_outer_common_exprs(
        RuntimeState* state, Expr* expr, ExtractContext* ctx) {
    if (expr->is_slotref()) {
        return Status::OK();
    }
    int child_num = expr->get_num_children();
    std::vector<SlotId> slot_ids;
    for (int i = 0;i < child_num;i++) {
        auto child = expr->get_child(i);

        RETURN_IF_ERROR(extract_outer_common_exprs(state, child, ctx));
        if (child->is_slotref()) {
            continue;
        }
        slot_ids.clear();
        child->get_slot_ids(&slot_ids);
        bool is_independent = std::all_of(slot_ids.begin(), slot_ids.end(), [ctx](const SlotId& id) {
            return ctx->lambda_arguments.find(id) == ctx->lambda_arguments.end();
        });
        if (is_independent) {
            SlotId slot_id = ctx->next_slot_id++;
            ColumnRef* column_ref = state->obj_pool()->add(new ColumnRef(child->type(), slot_id));
            LOG(INFO) << "add new common expr, slot_id: " << slot_id << ", new expr: " << column_ref->debug_string()
                << ", old expr: " << child->debug_string();
            expr->_children[i] = column_ref;
            ctx->outer_common_exprs.insert({column_ref, child});
        }
    }
    return Status::OK();
}

Status LambdaFunction::extract_outer_common_exprs(RuntimeState* state, ExtractContext* ctx) {
    RETURN_IF_ERROR(collect_lambda_argument_ids());
    for (auto argument_id: _arguments_ids) {
        ctx->lambda_arguments.insert(argument_id);
        LOG(INFO) << "lambda arg id: " << argument_id;
    }
    auto lambda_expr = _children[0];
    RETURN_IF_ERROR(extract_outer_common_exprs(state, lambda_expr, ctx));
    return Status::OK();
}

Status LambdaFunction::collect_lambda_argument_ids() {
    if (!_arguments_ids.empty()) {
        return Status::OK();
    }
    const int child_num = get_num_children() - 2 * _common_sub_expr_num;
    for (int i = 1;i < child_num;i++) {
        _children[i]->get_slot_ids(&_arguments_ids);
    }
    if (child_num - 1 != _arguments_ids.size()) {
        return Status::InternalError(fmt::format("Lambda arguments get ids failed, just get {} ids from {} arguments.",
                                                 _arguments_ids.size(), child_num - 1));
    }
    return Status::OK();
}

SlotId LambdaFunction::max_used_slot_id() const {
    std::vector<SlotId> ids;
    for (auto child: _children) {
        child->get_slot_ids(&ids);
    }
    DCHECK(!ids.empty());
    return *std::max_element(ids.begin(), ids.end());
}

Status LambdaFunction::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;

    // common sub expressions include 2 parts in a pair: (slot id, expression)
    const int child_num = get_num_children() - 2 * _common_sub_expr_num;
    LOG(INFO) << "lambda child num: " << child_num << ", common: " << _common_sub_expr_num;
    LOG(INFO) << debug_string();
    for (int i = 0; i< child_num;i++) {
        LOG(INFO) << "child[" << i << "] = " << get_child(i)->debug_string();
    }
    RETURN_IF_ERROR(collect_lambda_argument_ids());
    // collect the slot ids of lambda arguments
    // for (int i = 1; i < child_num; ++i) {
    //     get_child(i)->get_slot_ids(&_arguments_ids);
    // }
    // for (const auto& arg_id: _arguments_ids) {
    //     LOG(INFO) << "lambda arg id: " << arg_id;
    // }
    // if (child_num - 1 != _arguments_ids.size()) {
    //     return Status::InternalError(fmt::format("Lambda arguments get ids failed, just get {} ids from {} arguments.",
    //                                              _arguments_ids.size(), child_num - 1));
    // }
    // sorted common sub expressions so that the later expressions can reference the previous ones.
    for (auto i = child_num; i < child_num + _common_sub_expr_num; ++i) {
        get_child(i)->get_slot_ids(&_common_sub_expr_ids);
    }
    if (_common_sub_expr_ids.size() != _common_sub_expr_num) {
        return Status::InternalError(
                fmt::format("Lambda common sub expression id's size {} is not equal to expected {}",
                            _common_sub_expr_ids.size(), _common_sub_expr_num));
    }
    LOG(INFO) << "lambda common_sub_expr_num: " << _common_sub_expr_num;

    for (auto i = child_num + _common_sub_expr_num; i < child_num + 2 * _common_sub_expr_num; ++i) {
        LOG(INFO) << "commom expr: " << i << ", " << get_child(i)->debug_string();
        _common_sub_expr.push_back(get_child(i));
        get_child(i)->get_slot_ids(&_captured_slot_ids);
        // @TODO why put into captured slot id
    }
    if (_common_sub_expr.size() != _common_sub_expr_num) {
        return Status::InternalError(fmt::format("Lambda common sub expressions' size {} is not equal to expected {}",
                                                 _common_sub_expr.size(), _common_sub_expr_num));
    } 


    // get slot ids from the lambda expression
    get_child(0)->get_slot_ids(&_captured_slot_ids);
    for (auto id: _captured_slot_ids) {
        LOG(INFO) << "lambda capture id: " << id ;
    }

    // @TODO find all independent capture column, evaluate them first...


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
        auto sub_col = EVALUATE_NULL_IF_ERROR(context, _common_sub_expr[i], chunk);
        chunk->append_column(sub_col, _common_sub_expr_ids[i]);
        LOG(INFO) << "eval common expr: " << _common_sub_expr_ids[i];
    }
    return get_child(0)->evaluate_checked(context, chunk);
}

std::string LambdaFunction::debug_string() const {
    std::stringstream out;
    auto expr_debug_string = Expr::debug_string();
    out << "LambaFunction (";
    for (int i = 0;i < _children.size();i++) {
        out << (i == 0 ? "lambda expr, ": "input argument, ") << _children[i]->debug_string();
    }
    out << ")";
    return out.str();
}
} // namespace starrocks
