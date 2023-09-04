// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {
namespace vectorized {

class VectorizedInfoFunc final : public Expr {
public:
    VectorizedInfoFunc(const TExprNode& node);

    ~VectorizedInfoFunc() override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedInfoFunc(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override;

    std::string debug_string() const override;

private:
    // @IMPORTANT: BinaryColumnPtr's build_slice will cause multi-thread(OLAP_SCANNER) crash
    ColumnPtr _value;
};
} // namespace vectorized
} // namespace starrocks
