// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/lambda_function.h"

#include <fmt/format.h>

#include <iostream>

namespace starrocks::vectorized {
LambdaFunction::LambdaFunction(const TExprNode& node) : Expr(node, false) {}

Status LambdaFunction::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    int child_num = get_num_children();
    // collect the slot ids of lambda arguments
    for (int i = 1; i < child_num; ++i) {
        get_child(i)->get_slot_ids(&_arguments_ids);
    }
    if (child_num - 1 != _arguments_ids.size()) {
        return Status::InternalError(fmt::format("Lambda arguments get ids failed, just get {} ids from {} arguments.",
                                                 _arguments_ids.size(), child_num - 1));
    }
    // get slot ids from the lambda expression
    get_child(0)->get_slot_ids(&_captured_slot_ids);

    // remove current argument ids and duplicated ids from captured_slot_ids
    std::map<int, bool> captured_mask;
    int valid_id = 0;
    for (int& _captured_slot_id : _captured_slot_ids) {
        if (!captured_mask[_captured_slot_id]) { // not duplicated
            for (int arg_id = 0; arg_id < child_num - 1; ++arg_id) {
                if (_captured_slot_id == _arguments_ids[arg_id]) {
                    captured_mask[_captured_slot_id] = true;
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

ColumnPtr LambdaFunction::evaluate(ExprContext* context, Chunk* ptr) {
    return get_child(0)->evaluate(context, ptr);
}

void LambdaFunction::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    _arguments_ids.clear();
    _captured_slot_ids.clear();
}

} // namespace starrocks::vectorized
