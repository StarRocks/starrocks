// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <type_traits>
#include <utility>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "runtime/large_int_value.h"
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

private:
    static StatusOr<std::unique_ptr<Expr>> create_cast_expr(ObjectPool* pool, const TypeDescriptor& from_type,
                                                            const TypeDescriptor& cast_type,
                                                            bool allow_throw_exception);
    static StatusOr<std::unique_ptr<Expr>> create_cast_expr(ObjectPool* pool, const TExprNode& node,
                                                            const TypeDescriptor& from_type,
                                                            const TypeDescriptor& to_type, bool allow_throw_exception);
    static Expr* create_primitive_cast(ObjectPool* pool, const TExprNode& node, LogicalType from_type,
                                       LogicalType to_type, bool allow_throw_exception);
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
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* input_chunk) override;
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
    CastJsonToArray(const TExprNode& node, Expr* cast_element, TypeDescriptor type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(std::move(type_desc)) {}
    ~CastJsonToArray() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastJsonToArray(*this)); }

private:
    Expr* _cast_elements_expr;
    TypeDescriptor _cast_to_type_desc;
};

// cast one ARRAY to another ARRAY.
// For example.
//   cast ARRAY<tinyint> to ARRAY<int>
class CastArrayExpr final : public Expr {
public:
    CastArrayExpr(const TExprNode& node, std::unique_ptr<Expr> element_cast)
            : Expr(node), _element_cast(std::move(element_cast)) {}

    CastArrayExpr(const CastArrayExpr& rhs) : Expr(rhs) {}

    ~CastArrayExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastArrayExpr(*this)); }

private:
    std::unique_ptr<Expr> _element_cast;
};

// cast one MAP to another MAP.
// For example.
//   cast MAP<tinyint, tinyint> to MAP<int, int>
// TODO(alvin): function is enough, but now all cast operations are implemented in Expr.
//  Need to refactor these Expressions as Functions
class CastMapExpr final : public Expr {
public:
    CastMapExpr(const TExprNode& node, std::unique_ptr<Expr> key_cast, std::unique_ptr<Expr> value_cast)
            : Expr(node), _key_cast(std::move(key_cast)), _value_cast(std::move(value_cast)) {}

    CastMapExpr(const CastMapExpr& rhs) : Expr(rhs) {}

    ~CastMapExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastMapExpr(*this)); }

private:
    std::unique_ptr<Expr> _key_cast;
    std::unique_ptr<Expr> _value_cast;
};

// cast one STRUCT to another STRUCT.
// For example.
//   cast STRUCT<tinyint, tinyint> to STRUCT<int, int>
class CastStructExpr final : public Expr {
public:
    CastStructExpr(const TExprNode& node, std::vector<std::unique_ptr<Expr>> field_casts)
            : Expr(node), _field_casts(std::move(field_casts)) {}

    CastStructExpr(const CastStructExpr& rhs) : Expr(rhs) {}

    ~CastStructExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastStructExpr(*this)); }

private:
    std::vector<std::unique_ptr<Expr>> _field_casts;
};

// cast NULL OR Boolean to ComplexType
// For example.
//  cast map{1: NULL} to map<int, ARRAY<int>>
class MustNullExpr final : public Expr {
public:
    MustNullExpr(const TExprNode& node) : Expr(node) {}

    MustNullExpr(const MustNullExpr& rhs) : Expr(rhs) {}

    ~MustNullExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MustNullExpr(*this)); }
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

// cast column[idx] to coresponding json type.
StatusOr<std::string> cast_type_to_json_str(const ColumnPtr& column, int idx);

} // namespace starrocks
