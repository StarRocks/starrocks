// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <string>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/strings/numbers.h"
#include "udf/udf_internal.h"

namespace starrocks {
class TExprNode;

namespace vectorized {
class Chunk;

class VectorizedLiteral final : public Expr {
public:
    VectorizedLiteral(const TExprNode& node);

    ~VectorizedLiteral() override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedLiteral(*this)); }

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override;

    std::string debug_string() const override;

private:
    // @IMPORTANT: BinaryColumnPtr's build_slice will cause multi-thread(OLAP_SCANNER) crash
    ColumnPtr _value;
};
} // namespace vectorized
} // namespace starrocks
