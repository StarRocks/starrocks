// This file is licensed under the Elastic License 2.0. Copyright 2022 StarRocks Limited.

#pragma once

#include "exprs/expr.h"

namespace starrocks::vectorized {

class VectorizedCloneExpr final : public Expr {
public:
    VectorizedCloneExpr(const TExprNode& node) : Expr(node){};

    ~VectorizedCloneExpr() override = default;
    
    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedCloneExpr(*this)); }

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override { 
        ColumnPtr column = get_child(0)->evaluate(context, ptr);
        return column->clone_shared();
    }
};
} // namespace starrocks::vectorized
