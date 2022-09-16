// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/types.h"

namespace starrocks {
namespace vectorized {

class VectorizedCastExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node, bool exception_if_failed = false);

    static Expr* from_type(const TypeDescriptor& from, const TypeDescriptor& to, Expr* child, ObjectPool* pool,
                           bool exception_if_failed = false);
};

// cast Array to Array.
// only support cast the array to another array with the same nested level
// For example.
//   cast Array<int> to Array<String> is OK
//   cast Array<int> to Array<Array<int>> is not OK
class VectorizedCastArrayExpr final : public Expr {
public:
    VectorizedCastArrayExpr(Expr* cast_element_expr, const TExprNode& node)
            : Expr(node), _cast_element_expr(cast_element_expr) {}

    ~VectorizedCastArrayExpr() override = default;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedCastArrayExpr(*this)); }

private:
    Expr* _cast_element_expr;
};

} // namespace vectorized
} // namespace starrocks
