// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
    static Expr* from_thrift(const TExprNode& node) { return from_thrift(nullptr, node); }

    // The pool is used for intermediate expression, but not the returned expression
    static Expr* from_thrift(ObjectPool* pool, const TExprNode& node);

    static Expr* from_type(const TypeDescriptor& from, const TypeDescriptor& to, Expr* child, ObjectPool* pool);
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

// Cast string to array<ANY>
class CastStringToArray final : public Expr {
public:
    CastStringToArray(const TExprNode& node, Expr* cast_element, const TypeDescriptor& type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(type_desc) {}
    ~CastStringToArray() override = default;
    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastStringToArray(*this)); }

private:
    Slice _unquote(Slice slice);
    Slice _trim(Slice slice);

    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
};

// Cast JsonArray to array<ANY>
class CastJsonToArray final : public Expr {
public:
    CastJsonToArray(const TExprNode& node, Expr* cast_element, const TypeDescriptor& type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(type_desc) {}
    ~CastJsonToArray() override = default;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastJsonToArray(*this)); }

private:
    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
};

} // namespace vectorized
} // namespace starrocks
