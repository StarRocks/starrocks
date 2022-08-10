// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/lambda_function.h"

namespace starrocks::vectorized {
LambdaFunction::LambdaFunction(const TExprNode& node) : Expr(node, false) {}

Status LambdaFunction::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    get_child(0)->get_slot_ids(&captured_slot_ids);
    return Status::OK();
}
ColumnPtr LambdaFunction::evaluate(ExprContext* context, Chunk* ptr) {
    return get_child(0)->evaluate(context, ptr);
}

} // namespace starrocks::vectorized