// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/expr.h"

namespace starrocks {
namespace vectorized {

class VectorizedConditionExprFactory {
public:
    static Expr* create_if_expr(const TExprNode& node);

    static Expr* create_if_null_expr(const TExprNode& node);

    static Expr* create_null_if_expr(const TExprNode& node);

    static Expr* create_coalesce_expr(const TExprNode& node);
};

} // namespace vectorized
} // namespace starrocks