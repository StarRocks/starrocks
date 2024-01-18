#pragma once

#include "column/column.h"
#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {
class DebugExpr final : public Expr {
public:
    using DebugFunctionCall = StatusOr<ColumnPtr> (*)(ExprContext* context, Chunk* columns);
    DebugExpr(const TExprNode& node) : Expr(node){};

    ~DebugExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new DebugExpr(*this)); }

    [[nodiscard]] Status prepare(RuntimeState* state, ExprContext* context) override;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

private:
    DebugFunctionCall _func_caller;
};

class DebugFunctions {
public:
    /**
     * return the memory usage for the chunk
     * @param: []
     * @return const BIGINT column
     */
    static StatusOr<ColumnPtr> chunk_memusage(ExprContext* context, Chunk* ptr);
    /**
     * check chunk is valid
     * @param: []
     * @return const BOOLEAN column always true
     */
    static StatusOr<ColumnPtr> chunk_check_valid(ExprContext* context, Chunk* ptr);
};

} // namespace starrocks