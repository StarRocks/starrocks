// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exprs/expr.h"

namespace starrocks::vectorized {

class CloneExpr final : public Expr {
public:
    CloneExpr(const TExprNode& node) : Expr(node){};

    ~CloneExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CloneExpr(*this)); }

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override {
        ColumnPtr column = get_child(0)->evaluate(context, ptr);
        return column->clone_shared();
    }
};
} // namespace starrocks::vectorized
