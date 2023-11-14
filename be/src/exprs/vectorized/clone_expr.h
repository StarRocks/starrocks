// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "exprs/expr.h"

namespace starrocks::vectorized {

class CloneExpr final : public Expr {
public:
    CloneExpr(const TExprNode& node) : Expr(node){};

    ~CloneExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CloneExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, get_child(0)->evaluate_checked(context, ptr));
        return column->clone_shared();
    }
};
} // namespace starrocks::vectorized
