// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <mutex>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"

namespace starrocks::vectorized {

class TransformExpr final : public Expr {
public:
    TransformExpr(const TExprNode& node);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new TransformExpr(*this)); }

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override;

};
} // namespace starrocks::vectorized
