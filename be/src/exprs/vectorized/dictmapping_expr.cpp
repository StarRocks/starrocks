// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/dictmapping_expr.h"

namespace starrocks::vectorized {
DictMappingExpr::DictMappingExpr(const TExprNode& node) : Expr(node, false) {}

StatusOr<ColumnPtr> DictMappingExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    // If dict_func_expr is nullptr, then it means that this DictExpr has not been rewritten.
    // But in some cases we need to evaluate the original expression directly
    // (usually column_expr_predicate).
    if (dict_func_expr == nullptr) {
        return get_child(1)->evaluate_checked(context, ptr);
    } else {
        return dict_func_expr->evaluate_checked(context, ptr);
    }
}

} // namespace starrocks::vectorized
