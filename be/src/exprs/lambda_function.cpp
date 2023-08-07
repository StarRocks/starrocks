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

Status LambdaFunction::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
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
    }
    return get_child(0)->evaluate_checked(context, chunk);
}

} // namespace starrocks
