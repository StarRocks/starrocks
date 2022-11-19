// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/types.h"

namespace starrocks {

class VectorizedCastExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node, bool exception_if_failed = false) {
        return from_thrift(nullptr, node, exception_if_failed);
    }

    // The pool is used for intermediate expression, but not the returned expression
    static Expr* from_thrift(ObjectPool* pool, const TExprNode& node, bool exception_if_failed = false);

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

    ColumnPtr evaluate(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedCastArrayExpr(*this)); }

private:
    Expr* _cast_element_expr;
};

// Cast string to array<ANY>
class CastStringToArray final : public Expr {
public:
    CastStringToArray(const TExprNode& node, Expr* cast_element, TypeDescriptor type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(std::move(type_desc)) {}
    ~CastStringToArray() override = default;
    ColumnPtr evaluate(ExprContext* context, Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastStringToArray(*this)); }

private:
    Slice _unquote(Slice slice);

    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
};

// Cast JsonArray to array<ANY>
class CastJsonToArray final : public Expr {
public:
    CastJsonToArray(const TExprNode& node, Expr* cast_element, TypeDescriptor type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(std::move(type_desc)) {}
    ~CastJsonToArray() override = default;

    ColumnPtr evaluate(ExprContext* context, Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastJsonToArray(*this)); }

private:
    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
};

} // namespace starrocks
