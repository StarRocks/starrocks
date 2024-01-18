// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <mutex>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "glog/logging.h"

namespace starrocks::vectorized {

// array_map(lambda function, array0, array1...)

class ArrayMapExpr final : public Expr {
public:
    ArrayMapExpr(const TExprNode& node);

    // for tests
    explicit ArrayMapExpr(TypeDescriptor type);

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArrayMapExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
};
} // namespace starrocks::vectorized
