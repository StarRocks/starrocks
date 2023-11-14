// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/large_int_value.h"
#include "runtime/types.h"

namespace starrocks {
namespace vectorized {

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

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedCastArrayExpr(*this)); }

private:
    Expr* _cast_element_expr;
};

// Cast string to array<ANY>
class CastStringToArray final : public Expr {
public:
    CastStringToArray(const TExprNode& node, Expr* cast_element, TypeDescriptor type_desc, bool throw_exception_if_err)
            : Expr(node),
              _cast_elements_expr(cast_element),
              _cast_to_type_desc(std::move(type_desc)),
              _throw_exception_if_err(throw_exception_if_err) {}
    ~CastStringToArray() override = default;
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastStringToArray(*this)); }

private:
    Slice _unquote(Slice slice) const;
    Slice _trim(Slice slice) const;

    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
    bool _throw_exception_if_err;
};

// Cast JsonArray to array<ANY>
class CastJsonToArray final : public Expr {
public:
    CastJsonToArray(const TExprNode& node, Expr* cast_element, const TypeDescriptor& type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(type_desc) {}
    ~CastJsonToArray() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastJsonToArray(*this)); }

private:
    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
};

/**
 * Cast other type to string without float, double, string
 */
struct CastToString {
    template <typename Type, typename ResultType>
    static std::string apply(const Type& v) {
        if constexpr (IsTemporal<Type>() || IsDecimal<Type>) {
            // DateValue, TimestampValue, DecimalV2
            return v.to_string();
        } else if constexpr (IsInt128<Type>) {
            // int128_t
            return LargeIntValue::to_string(v);
        } else {
            // int8_t ~ int64_t, boolean
            return SimpleItoa(v);
        }
    }

    template <typename Type>
    static bool constexpr extend_type() {
        return (IsTemporal<Type>() || IsDecimal<Type> || IsInt128<Type>);
    }
};

StatusOr<ColumnPtr> cast_nested_to_json(const ColumnPtr& column);

} // namespace vectorized
} // namespace starrocks
