// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include <memory>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
struct JavaUDFContext;
struct UDFFunctionCallHelper;

class JavaFunctionCallExpr final : public Expr {
public:
    JavaFunctionCallExpr(const TExprNode& node);
    ~JavaFunctionCallExpr() override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new JavaFunctionCallExpr(*this)); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override;
    Status prepare(RuntimeState* state, ExprContext* context) override;
    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
    bool is_constant() const override;

private:
    void _call_udf_close();
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<JavaUDFContext> _func_desc;
    std::shared_ptr<UDFFunctionCallHelper> _call_helper;
    bool _is_returning_random_value;
};
} // namespace starrocks::vectorized
