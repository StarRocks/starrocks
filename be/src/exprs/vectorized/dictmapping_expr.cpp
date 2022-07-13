// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/dictmapping_expr.h"

namespace starrocks::vectorized {
DictMappingExpr::DictMappingExpr(const TExprNode& node) : Expr(node, false) {}

ColumnPtr DictMappingExpr::evaluate(ExprContext* context, Chunk* ptr) {
    DCHECK(dict_func_expr != nullptr);
    return dict_func_expr->evaluate(context, ptr);
}

} // namespace starrocks::vectorized