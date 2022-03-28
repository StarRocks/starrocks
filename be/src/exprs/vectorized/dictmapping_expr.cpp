// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/dictmapping_expr.h"

namespace starrocks::vectorized {
DictMappingExpr::DictMappingExpr(const TExprNode& node) : Expr(node, false) {}
} // namespace starrocks::vectorized