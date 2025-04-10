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
#include "runtime/types.h"
#include "types/large_int_value.h"

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
    static StatusOr<Expr*> create_cast_expr(ObjectPool* pool, const TypeDescriptor& from_type,
                                            const TypeDescriptor& cast_type, bool allow_throw_exception);
    static StatusOr<Expr*> create_cast_expr(ObjectPool* pool, const TExprNode& node, const TypeDescriptor& from_type,
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
<<<<<<< HEAD
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CastStringToArray(*this)); }
    [[nodiscard]] Status open(RuntimeState* state, ExprContext* context,
                              FunctionContext::FunctionStateScope scope) override;
=======

    Expr* clone(ObjectPool* pool) const override {
        auto cloned = std::unique_ptr<CastStringToArray>(new CastStringToArray(*this));
        if (_cast_elements_expr != nullptr) {
            cloned->_cast_elements_expr = Expr::copy(pool, _cast_elements_expr);
        }
        return pool->add(cloned.release());
    }

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
>>>>>>> 5d2d0a2a99 ([BugFix] Fix clone for semi-structure cast expr (#57804))

private:
    // Invoked only by clone.
    CastStringToArray(const CastStringToArray& rhs)
            : Expr(rhs),
              _cast_to_type_desc(rhs._cast_to_type_desc),
              _throw_exception_if_err(rhs._throw_exception_if_err),
              _constant_res(rhs._constant_res != nullptr ? rhs._constant_res->clone() : nullptr) {}

    Slice _unquote(Slice slice) const;
    Slice _trim(Slice slice) const;

    Expr* _cast_elements_expr = nullptr;
    TypeDescriptor _cast_to_type_desc;
    bool _throw_exception_if_err;
    ColumnPtr _constant_res = nullptr;
};

// Cast JsonArray to array<ANY>
class CastJsonToArray final : public Expr {
public:
    CastJsonToArray(const TExprNode& node, Expr* cast_element, TypeDescriptor type_desc)
            : Expr(node), _cast_elements_expr(cast_element), _cast_to_type_desc(std::move(type_desc)) {}
    ~CastJsonToArray() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* input_chunk) override;

    Expr* clone(ObjectPool* pool) const override {
        auto cloned = std::unique_ptr<CastJsonToArray>(new CastJsonToArray(*this));
        if (_cast_elements_expr != nullptr) {
            cloned->_cast_elements_expr = Expr::copy(pool, _cast_elements_expr);
        }
        return pool->add(cloned.release());
    }

private:
    // Invoked only by clone.
    CastJsonToArray(const CastJsonToArray& rhs) : Expr(rhs), _cast_to_type_desc(rhs._cast_to_type_desc) {}

    Expr* _cast_elements_expr = nullptr;
    TypeDescriptor _cast_to_type_desc;
};

<<<<<<< HEAD
=======
// Cast Json to struct<ANY>
class CastJsonToStruct final : public Expr {
public:
    CastJsonToStruct(const TExprNode& node, std::vector<Expr*> field_casts)
            : Expr(node), _field_casts(std::move(field_casts)) {
        _json_paths.reserve(_type.field_names.size());
        for (int j = 0; j < _type.field_names.size(); j++) {
            std::string path_string = "$." + _type.field_names[j];
            auto res = JsonPath::parse(Slice(path_string));
            if (!res.ok()) {
                throw std::runtime_error("Failed to parse JSON path: " + path_string);
            }
            _json_paths.emplace_back(res.value());
        }
    }

    ~CastJsonToStruct() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* input_chunk) override;
    Expr* clone(ObjectPool* pool) const override {
        auto cloned = std::unique_ptr<CastJsonToStruct>(new CastJsonToStruct(*this));
        cloned->_field_casts.reserve(_field_casts.size());
        for (int i = 0; i < _field_casts.size(); ++i) {
            if (_field_casts[i] != nullptr) {
                cloned->_field_casts.emplace_back(Expr::copy(pool, _field_casts[i]));
            }
        }
        return pool->add(cloned.release());
    }

private:
    // Invoked only by clone.
    CastJsonToStruct(const CastJsonToStruct& rhs) : Expr(rhs), _json_paths(rhs._json_paths) {}

    std::vector<Expr*> _field_casts;
    std::vector<JsonPath> _json_paths;
};

>>>>>>> 5d2d0a2a99 ([BugFix] Fix clone for semi-structure cast expr (#57804))
// cast one ARRAY to another ARRAY.
// For example.
//   cast ARRAY<tinyint> to ARRAY<int>
class CastArrayExpr final : public Expr {
public:
    CastArrayExpr(const TExprNode& node, Expr* element_cast) : Expr(node), _element_cast(element_cast) {}

    ~CastArrayExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override {
        auto cloned = std::unique_ptr<CastArrayExpr>(new CastArrayExpr(*this));
        if (_element_cast != nullptr) {
            cloned->_element_cast = Expr::copy(pool, _element_cast);
        }
        return pool->add(cloned.release());
    }

private:
    // Invoked only by clone.
    CastArrayExpr(const CastArrayExpr& rhs) : Expr(rhs) {}

    Expr* _element_cast = nullptr;
};

// cast one MAP to another MAP.
// For example.
//   cast MAP<tinyint, tinyint> to MAP<int, int>
// TODO(alvin): function is enough, but now all cast operations are implemented in Expr.
//  Need to refactor these Expressions as Functions
class CastMapExpr final : public Expr {
public:
    CastMapExpr(const TExprNode& node, Expr* key_cast, Expr* value_cast)
            : Expr(node), _key_cast(key_cast), _value_cast(value_cast) {}

    ~CastMapExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override {
        auto cloned = std::unique_ptr<CastMapExpr>(new CastMapExpr(*this));
        if (_key_cast != nullptr) {
            cloned->_key_cast = Expr::copy(pool, _key_cast);
        }
        if (_value_cast != nullptr) {
            cloned->_value_cast = Expr::copy(pool, _value_cast);
        }
        return pool->add(cloned.release());
    }

private:
    // Invoked only by clone.
    CastMapExpr(const CastMapExpr& rhs) : Expr(rhs) {}

    Expr* _key_cast = nullptr;
    Expr* _value_cast = nullptr;
};

// cast one STRUCT to another STRUCT.
// For example.
//   cast STRUCT<tinyint, tinyint> to STRUCT<int, int>
class CastStructExpr final : public Expr {
public:
    CastStructExpr(const TExprNode& node, std::vector<Expr*> field_casts)
            : Expr(node), _field_casts(std::move(field_casts)) {}

    ~CastStructExpr() override = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    Expr* clone(ObjectPool* pool) const override {
        auto cloned = std::unique_ptr<CastStructExpr>(new CastStructExpr(*this));
        cloned->_field_casts.reserve(_field_casts.size());
        for (int i = 0; i < _field_casts.size(); ++i) {
            if (_field_casts[i] != nullptr) {
                cloned->_field_casts.emplace_back(Expr::copy(pool, _field_casts[i]));
            }
        }
        return pool->add(cloned.release());
    }

private:
    // Invoked only by clone.
    CastStructExpr(const CastStructExpr& rhs) : Expr(rhs) {}

    std::vector<Expr*> _field_casts;
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

StatusOr<ColumnPtr> cast_nested_to_json(const ColumnPtr& column, bool allow_throw_exception);

// cast column[idx] to coresponding json type.
StatusOr<std::string> cast_type_to_json_str(const ColumnPtr& column, int idx);

} // namespace starrocks
