// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/vectorized/builtin_functions.h"
#include "common/status.h"
#include "exprs/expr_context.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace starrocks {
class RuntimeState;
class TExprNode;

namespace vectorized {
class Chunk;
struct FunctionDescriptor;

class VectorizedFunctionCallExpr final : public Expr {
public:
    explicit VectorizedFunctionCallExpr(const TExprNode& node);

    ~VectorizedFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedFunctionCallExpr(*this)); }

protected:
    Status prepare(RuntimeState* state, ExprContext* context) override;

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;

    bool is_constant() const override;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override;

private:
    const FunctionDescriptor* _fn_desc;

    bool _is_returning_random_value;
};

} // namespace vectorized
} // namespace starrocks
