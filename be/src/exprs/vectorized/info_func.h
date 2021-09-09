// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {
namespace vectorized {

class VectorizedInfoFunc final : public Expr {
public:
    VectorizedInfoFunc(const TExprNode& node);

    virtual ~VectorizedInfoFunc();

    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedInfoFunc(*this)); }

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override;

    std::string debug_string() const override;

private:
    // @IMPORTANT: BinaryColumnPtr's build_slice will cause multi-thread(OLAP_SCANNER) crash
    ColumnPtr _value;
};
} // namespace vectorized
} // namespace starrocks
