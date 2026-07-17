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

#include "exprs/cast_expr_tpl.hpp"

namespace starrocks {

template <template <bool> class T, typename... Args>
Expr* dispatch_throw_exception(bool throw_exception, Args&&... args) {
    if (throw_exception) {
        return new T<true>(std::forward<Args>(args)...);
    } else {
        return new T<false>(std::forward<Args>(args)...);
    }
}

template <bool throw_exception>
using CastVarcharToHll = VectorizedCastExpr<TYPE_VARCHAR, TYPE_HLL, throw_exception>;

template <bool throw_exception>
using CastVarcharToBitmap = VectorizedCastExpr<TYPE_VARCHAR, TYPE_OBJECT, throw_exception>;

// Create a SlotRef as the child of children cast for the complex type
// For example: to cast STRUCT<int, int> as STRUCT<double, double>
// Inorder to reuse the primary casting expression, we will generate a new CAST(INT as DOUBLE) for
// each field.
// And for the new generated CAST expressions, we will generate a ColumnRef as its child expression
static std::unique_ptr<Expr> create_slot_ref(const TypeDescriptor& type) {
    TExprNode ref_node;
    ref_node.type = type.to_thrift();
    ref_node.__isset.slot_ref = true;
    ref_node.slot_ref.slot_id = 0;
    ref_node.slot_ref.tuple_id = 0;
    return std::make_unique<ColumnRef>(ref_node);
}

StatusOr<ColumnPtr> MustNullExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    // only null
    auto column = ColumnHelper::create_column(_type, true);
    column->append_nulls(1);
    auto only_null = ConstColumn::create(std::move(column), 1);
    if (ptr != nullptr) {
        only_null->resize(ptr->num_rows());
    }
    return only_null;
}

StatusOr<ColumnPtr> CastToVariantExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));
    const size_t num_rows = column->size();

    if (num_rows != 0 && ColumnHelper::count_nulls(column) == num_rows) {
        return ColumnHelper::create_const_null_column(num_rows);
    }

    ColumnBuilder<TYPE_VARIANT> builder(num_rows);
    RETURN_IF_ERROR(VariantEncoder::encode_column(column, _from_type, &builder, _allow_throw_exception));

    return builder.build(column->is_constant());
}

// Check whether JSON can be cast to complex types (ARRAY / MAP / STRUCT)
inline bool json_to_complex_type(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_JSON:
        switch (to_type) {
        case TYPE_ARRAY:
        case TYPE_MAP:
        case TYPE_STRUCT:
            return true;
        default:
            return false;
        }
    default:
        return false;
    }
}

// Check whether VARIANT can be cast to complex types (ARRAY / MAP / STRUCT)
inline bool variant_to_complex_type(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_VARIANT:
        switch (to_type) {
        case TYPE_ARRAY:
        case TYPE_MAP:
        case TYPE_STRUCT:
            return true;
        default:
            return false;
        }
    default:
        return false;
    }
}

Expr* VectorizedCastExprFactory::create_json_to_complex_type_cast(ObjectPool* pool, const TExprNode& node,
                                                                  LogicalType from_type, LogicalType to_type,
                                                                  bool allow_throw_exception) {
    DCHECK(from_type == TYPE_JSON);

    switch (to_type) {
    case TYPE_ARRAY: {
        TypeDescriptor cast_to = TypeDescriptor::from_thrift(node.type);
        TExprNode cast;
        cast.type = cast_to.children[0].to_thrift();
        cast.child_type = to_thrift(from_type);
        cast.slot_ref.slot_id = 0;
        cast.slot_ref.tuple_id = 0;

        Expr* cast_element_expr = VectorizedCastExprFactory::from_thrift(pool, cast, allow_throw_exception);
        if (cast_element_expr == nullptr) {
            return nullptr;
        }
        DCHECK(pool != nullptr);
        pool->add(cast_element_expr);

        auto* child = new ColumnRef(cast);
        pool->add(child);
        cast_element_expr->add_child(child);

        return new CastJsonToArray(node, cast_element_expr, cast_to);
    }
    case TYPE_STRUCT: {
        TypeDescriptor cast_to = TypeDescriptor::from_thrift(node.type);

        // Validate that every struct field name is a parseable JSON path before constructing
        // CastJsonToStruct, whose ctor would otherwise throw an uncaught std::runtime_error.
        // The throw propagates out of create_expr_tree (no try/catch on that path) and aborts
        // the BE at fragment prepare. Returning nullptr surfaces a clean Status::NotSupported.
        for (const auto& field_name : cast_to.field_names) {
            std::string path_string = "$." + field_name;
            if (!JsonPath::parse(Slice(path_string)).ok()) {
                LOG(WARNING) << "Cannot cast json to struct: field name is not a valid JSON path: " << field_name;
                return nullptr;
            }
        }

        std::vector<Expr*> field_casts(cast_to.children.size());
        for (int i = 0; i < cast_to.children.size(); ++i) {
            TypeDescriptor json_type = TypeDescriptor::create_json_type();
            auto ret = create_cast_expr(pool, json_type, cast_to.children[i], allow_throw_exception);
            if (!ret.ok()) {
                LOG(WARNING) << "Not support cast from type: " << json_type << ", to type: " << cast_to.children[i];
                return nullptr;
            }
            pool->add(ret.value());
            field_casts[i] = ret.value();
            auto cast_input = create_slot_ref(json_type);
            field_casts[i]->add_child(cast_input.get());
            pool->add(cast_input.release());
        }
        return new CastJsonToStruct(node, std::move(field_casts));
    }
    case TYPE_MAP: {
        TypeDescriptor cast_to = TypeDescriptor::from_thrift(node.type);

        // CastJsonToMap will first cast json to MAP<VARCHAR,JSON>, then cast to the target MAP<KEY,VALUE>
        // If the target is already MAP<VARCHAR,JSON>, no need to set key/value cast expr
        Expr* key_cast_expr = nullptr;
        auto& key_desc = cast_to.children[0];
        if (key_desc.type != TYPE_VARCHAR) {
            TypeDescriptor varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            auto result = create_cast_expr(pool, varchar_type, key_desc, allow_throw_exception);
            if (!result.ok()) {
                LOG(ERROR) << "Fail to create cast expr from json to map, map key type: " << key_desc
                           << ", status: " << result.status();
                return nullptr;
            }
            key_cast_expr = result.value();
            Expr* cast_input = create_slot_ref(varchar_type).release();
            key_cast_expr->add_child(cast_input);
            pool->add(key_cast_expr);
            pool->add(cast_input);
        }

        Expr* value_cast_expr = nullptr;
        auto& value_desc = cast_to.children[1];
        if (value_desc.type != TYPE_JSON) {
            TypeDescriptor json_type = TypeDescriptor::create_json_type();
            auto result = create_cast_expr(pool, json_type, value_desc, allow_throw_exception);
            if (!result.ok()) {
                LOG(ERROR) << "Fail to create cast expr from json to map, map value type: " << value_desc
                           << ", status: " << result.status();
                return nullptr;
            }
            value_cast_expr = result.value();
            Expr* cast_input = create_slot_ref(json_type).release();
            value_cast_expr->add_child(cast_input);
            pool->add(value_cast_expr);
            pool->add(cast_input);
        }

        return new CastJsonToMap(node, key_cast_expr, value_cast_expr);
    }
    default:
        LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                     << " to type: " << type_to_string(to_type);
        return nullptr;
    }
}

Expr* VectorizedCastExprFactory::create_variant_to_complex_type_cast(ObjectPool* pool, const TExprNode& node,
                                                                     LogicalType from_type, LogicalType to_type,
                                                                     bool allow_throw_exception) {
    DCHECK(from_type == TYPE_VARIANT);

    switch (to_type) {
    case TYPE_ARRAY: {
        const TypeDescriptor expected_type = TypeDescriptor::from_thrift(node.type);
        TExprNode cast;
        cast.type = expected_type.children[0].to_thrift();
        cast.child_type = to_thrift(from_type);
        cast.slot_ref.slot_id = 0;
        cast.slot_ref.tuple_id = 0;

        Expr* cast_element_expr = from_thrift(pool, cast, allow_throw_exception);
        if (cast_element_expr == nullptr) {
            return nullptr;
        }
        DCHECK(pool != nullptr);
        pool->add(cast_element_expr);

        auto* child = new ColumnRef(cast);
        pool->add(child);
        cast_element_expr->add_child(child);

        return new CastVariantToArray(node, cast_element_expr, expected_type);
    }
    case TYPE_MAP: {
        TypeDescriptor expected_type = TypeDescriptor::from_thrift(node.type);

        DCHECK(expected_type.children.size() == 2);
        Expr* key_cast_expr = nullptr;
        auto& key_desc = expected_type.children[0];
        if (key_desc.type != TYPE_VARCHAR) {
            const TypeDescriptor varchar_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
            auto result = create_cast_expr(pool, varchar_type, key_desc, allow_throw_exception);
            if (!result.ok()) {
                LOG(ERROR) << "Fail to create cast expr from variant to map, map key type: " << key_desc
                           << ", status: " << result.status();
                return nullptr;
            }

            key_cast_expr = result.value();
            Expr* cast_input = create_slot_ref(varchar_type).release();
            key_cast_expr->add_child(cast_input);
            pool->add(key_cast_expr);
            pool->add(cast_input);
        }
        Expr* value_cast_expr = nullptr;
        auto& value_desc = expected_type.children[1];
        if (value_desc.type != TYPE_VARIANT) {
            TypeDescriptor variant_type = TypeDescriptor::create_variant_type();
            auto result = create_cast_expr(pool, variant_type, value_desc, allow_throw_exception);
            if (!result.ok()) {
                LOG(ERROR) << "Fail to create cast expr from variant to map, map value type: " << value_desc
                           << ", status: " << result.status();
                return nullptr;
            }
            value_cast_expr = result.value();
            Expr* cast_input = create_slot_ref(variant_type).release();
            value_cast_expr->add_child(cast_input);
            pool->add(value_cast_expr);
            pool->add(cast_input);
        }

        return new CastVariantToMap(node, key_cast_expr, value_cast_expr);
    }
    case TYPE_STRUCT: {
        TypeDescriptor expected_type = TypeDescriptor::from_thrift(node.type);

        // Validate that every struct field name is a parseable variant path before constructing
        // CastVariantToStruct, whose ctor would otherwise throw an uncaught std::runtime_error
        // that propagates out of create_expr_tree and aborts the BE at fragment prepare.
        // Returning nullptr surfaces a clean Status::NotSupported instead.
        for (const auto& field_name : expected_type.field_names) {
            std::string path_string = "$." + field_name;
            if (!VariantPathParser::parse(Slice(path_string)).ok()) {
                LOG(WARNING) << "Cannot cast variant to struct: field name is not a valid variant path: " << field_name;
                return nullptr;
            }
        }

        std::vector<Expr*> field_casts(expected_type.children.size());
        for (int i = 0; i < expected_type.children.size(); ++i) {
            TypeDescriptor variant_type = TypeDescriptor::create_variant_type();
            auto ret = create_cast_expr(pool, variant_type, expected_type.children[i], allow_throw_exception);
            if (!ret.ok()) {
                LOG(WARNING) << "Fail to create cast expr from variant to struct, field type: "
                             << expected_type.children[i] << ", status: " << ret.status();
                return nullptr;
            }
            pool->add(ret.value());
            field_casts[i] = ret.value();
            auto cast_input = create_slot_ref(variant_type);
            field_casts[i]->add_child(cast_input.get());
            pool->add(cast_input.release());
        }

        return new CastVariantToStruct(node, std::move(field_casts));
    }
    default:
        LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                     << " to type: " << type_to_string(to_type);
        return nullptr;
    }
}

// Need add result to pool by caller.
Expr* VectorizedCastExprFactory::create_primitive_cast(ObjectPool* pool, const TExprNode& node, LogicalType from_type,
                                                       LogicalType to_type, bool allow_throw_exception) {
    if (to_type == TYPE_CHAR) {
        to_type = TYPE_VARCHAR;
    }
    if (from_type == TYPE_CHAR) {
        from_type = TYPE_VARCHAR;
    }
    if (from_type == TYPE_NULL) {
        // NULL TO OTHER TYPE, direct return
        from_type = to_type;
    }
    if (from_type == TYPE_VARCHAR && to_type == TYPE_HLL) {
        return dispatch_throw_exception<CastVarcharToHll>(allow_throw_exception, node);
    }
    // Cast string to array<ANY>
    if (from_type == TYPE_VARCHAR && to_type == TYPE_ARRAY) {
        TypeDescriptor cast_to = TypeDescriptor::from_thrift(node.type);
        TExprNode cast;
        cast.type = cast_to.children[0].to_thrift();
        cast.child_type = to_thrift(from_type);
        cast.slot_ref.slot_id = 0;
        cast.slot_ref.tuple_id = 0;

        Expr* cast_element_expr = VectorizedCastExprFactory::from_thrift(pool, cast, allow_throw_exception);
        if (cast_element_expr == nullptr) {
            return nullptr;
        }
        DCHECK(pool != nullptr);
        pool->add(cast_element_expr);

        auto* child = new ColumnRef(cast);
        pool->add(child);
        cast_element_expr->add_child(child);

        return new CastStringToArray(node, cast_element_expr, cast_to, allow_throw_exception);
    }

    if (from_type == TYPE_VARCHAR && to_type == TYPE_OBJECT) {
        return dispatch_throw_exception<CastVarcharToBitmap>(allow_throw_exception, node);
    }

    if (json_to_complex_type(from_type, to_type)) {
        return create_json_to_complex_type_cast(pool, node, from_type, to_type, allow_throw_exception);
    }

    if (variant_to_complex_type(from_type, to_type)) {
        return create_variant_to_complex_type_cast(pool, node, from_type, to_type, allow_throw_exception);
    }

    if (to_type == TYPE_VARCHAR) {
        switch (from_type) {
            CASE_TO_STRING_FROM(TYPE_BOOLEAN, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_TINYINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_SMALLINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_INT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_BIGINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_LARGEINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_FLOAT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DOUBLE, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMALV2, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_TIME, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DATE, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DATETIME, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_VARCHAR, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL32, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL64, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL128, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL256, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_JSON, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_VARBINARY, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_VARIANT, allow_throw_exception);
        default:
            LOG(WARNING) << "Not support cast " << type_to_string(from_type) << " to " << type_to_string(to_type);
            return nullptr;
        }
    } else if (from_type == TYPE_JSON || to_type == TYPE_JSON) {
        // TODO(mofei) simplify type enumeration
        if (from_type == TYPE_JSON) {
            switch (to_type) {
                CASE_FROM_JSON_TO(TYPE_BOOLEAN, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_TINYINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_SMALLINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_INT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_BIGINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_LARGEINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_FLOAT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_DOUBLE, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_JSON, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_VARIANT, allow_throw_exception);
            default:
                LOG(WARNING) << "Not support cast " << type_to_string(from_type) << " to " << type_to_string(to_type);
                return nullptr;
            }
        } else {
            switch (from_type) {
                CASE_TO_JSON(TYPE_BOOLEAN, allow_throw_exception);
                CASE_TO_JSON(TYPE_TINYINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_SMALLINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_INT, allow_throw_exception);
                CASE_TO_JSON(TYPE_BIGINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_LARGEINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_FLOAT, allow_throw_exception);
                CASE_TO_JSON(TYPE_DOUBLE, allow_throw_exception);
                CASE_TO_JSON(TYPE_JSON, allow_throw_exception);
                CASE_TO_JSON(TYPE_VARIANT, allow_throw_exception);
                CASE_TO_JSON(TYPE_CHAR, allow_throw_exception);
                CASE_TO_JSON(TYPE_VARCHAR, allow_throw_exception);
                CASE_TO_JSON(TYPE_DECIMAL32, allow_throw_exception);
                CASE_TO_JSON(TYPE_DECIMAL64, allow_throw_exception);
                CASE_TO_JSON(TYPE_DECIMAL128, allow_throw_exception);
                CASE_TO_JSON(TYPE_DATE, allow_throw_exception);
                CASE_TO_JSON(TYPE_TIME, allow_throw_exception);
                CASE_TO_JSON(TYPE_DATETIME, allow_throw_exception);
            default:
                LOG(WARNING) << "Not support cast " << type_to_string(from_type) << " to " << type_to_string(to_type);
                return nullptr;
            }
        }
    } else if (from_type == TYPE_VARIANT || to_type == TYPE_VARIANT) {
        if (from_type == TYPE_VARIANT) {
            switch (to_type) {
                CASE_FROM_VARIANT_TO(TYPE_BOOLEAN, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_TINYINT, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_SMALLINT, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_INT, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_BIGINT, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_LARGEINT, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_FLOAT, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_DOUBLE, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_DATE, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_DATETIME, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_TIME, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_DECIMAL32, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_DECIMAL64, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_DECIMAL128, allow_throw_exception);
                CASE_FROM_VARIANT_TO(TYPE_VARIANT, allow_throw_exception);
            default:
                LOG(WARNING) << "Not support cast " << type_to_string(from_type) << " to " << type_to_string(to_type);
                return nullptr;
            }
        }
        if (to_type == TYPE_VARIANT) {
            TypeDescriptor from_desc(thrift_to_type(node.child_type));
            if (node.__isset.child_type_desc) {
                from_desc = TypeDescriptor::from_thrift(node.child_type_desc);
            }
            return new CastToVariantExpr(node, std::move(from_desc), allow_throw_exception);
        }
    } else if (is_binary_type(to_type)) {
        if (is_string_type(from_type)) {
            if (allow_throw_exception) {
                return new VectorizedCastExpr<TYPE_VARCHAR, TYPE_VARBINARY, true>(node);
            } else {
                return new VectorizedCastExpr<TYPE_VARCHAR, TYPE_VARBINARY, false>(node);
            }
        } else {
            LOG(WARNING) << "Not support cast " << type_to_string(from_type) << " to " << type_to_string(to_type);
            return nullptr;
        }
    } else {
        switch (to_type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return create_primitive_cast_group1(node, from_type, to_type, allow_throw_exception);
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
        case TYPE_DECIMALV2:
        case TYPE_TIME:
        case TYPE_DATE:
        case TYPE_DATETIME:
            return create_primitive_cast_group2(node, from_type, to_type, allow_throw_exception);
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128:
        case TYPE_DECIMAL256:
            return create_primitive_cast_group3(node, from_type, to_type, allow_throw_exception);
        default:
            LOG(WARNING) << "Not support cast " << type_to_string(from_type) << " to " << type_to_string(to_type);
            return nullptr;
        }
    }

    return nullptr;
}

// NOTE: should return error status to avoid null in ASSIGN_OR_RETURN, otherwise causing crash
StatusOr<Expr*> VectorizedCastExprFactory::create_cast_expr(ObjectPool* pool, const TExprNode& node,
                                                            const TypeDescriptor& from_type,
                                                            const TypeDescriptor& to_type, bool allow_throw_exception) {
    if (from_type.is_array_type() && to_type.is_array_type()) {
        ASSIGN_OR_RETURN(auto* child_cast,
                         create_cast_expr(pool, from_type.children[0], to_type.children[0], allow_throw_exception));
        pool->add(child_cast);
        auto child_input = create_slot_ref(from_type.children[0]);
        child_cast->add_child(child_input.get());
        pool->add(child_input.release());

        return new CastArrayExpr(node, child_cast);
    }
    if (from_type.is_map_type() && to_type.is_map_type()) {
        ASSIGN_OR_RETURN(auto* key_cast,
                         create_cast_expr(pool, from_type.children[0], to_type.children[0], allow_throw_exception));
        pool->add(key_cast);
        auto key_input = create_slot_ref(from_type.children[0]);
        key_cast->add_child(key_input.get());
        pool->add(key_input.release());

        ASSIGN_OR_RETURN(auto* value_cast,
                         create_cast_expr(pool, from_type.children[1], to_type.children[1], allow_throw_exception));
        pool->add(value_cast);
        auto value_input = create_slot_ref(from_type.children[1]);
        value_cast->add_child(value_input.get());
        pool->add(value_input.release());

        return new CastMapExpr(node, key_cast, value_cast);
    }
    if (from_type.is_struct_type() && to_type.is_struct_type()) {
        bool cast_by_name = node.__isset.cast_struct_by_name && node.cast_struct_by_name;
        if (cast_by_name) {
            // Name-based: match each target field by name in the source; counts may differ.
            // Source fields not referenced in the target are ignored.
            std::unordered_map<std::string, int> from_field_name_to_index;
            for (int i = 0; i < (int)from_type.field_names.size(); ++i) {
                std::string lower_name = from_type.field_names[i];
                std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
                from_field_name_to_index[lower_name] = i;
            }

            std::vector<Expr*> field_casts(to_type.children.size());
            std::vector<int> source_field_indices(to_type.children.size());
            for (int i = 0; i < (int)to_type.children.size(); ++i) {
                std::string target_name = to_type.field_names[i];
                std::transform(target_name.begin(), target_name.end(), target_name.begin(), ::tolower);
                auto it = from_field_name_to_index.find(target_name);
                if (it == from_field_name_to_index.end()) {
                    return Status::NotSupported(
                            fmt::format("Struct field '{}' not found in source struct.", to_type.field_names[i]));
                }
                int source_idx = it->second;
                source_field_indices[i] = source_idx;
                ASSIGN_OR_RETURN(field_casts[i],
                                 create_cast_expr(pool, from_type.children[source_idx], to_type.children[i],
                                                  allow_throw_exception, /*cast_by_name=*/true));
                pool->add(field_casts[i]);
                auto cast_input = create_slot_ref(from_type.children[source_idx]);
                field_casts[i]->add_child(cast_input.get());
                pool->add(cast_input.release());
            }
            return new CastStructExpr(node, std::move(field_casts), std::move(source_field_indices));
        } else {
            // Position-based: match fields by index; counts must be equal.
            if (from_type.children.size() != to_type.children.size()) {
                return Status::NotSupported("Not support cast struct with different number of children.");
            }

            std::vector<Expr*> field_casts(to_type.children.size());
            std::vector<int> source_field_indices(to_type.children.size());
            for (int i = 0; i < (int)to_type.children.size(); ++i) {
                source_field_indices[i] = i;
                ASSIGN_OR_RETURN(field_casts[i], create_cast_expr(pool, from_type.children[i], to_type.children[i],
                                                                  allow_throw_exception));
                pool->add(field_casts[i]);
                auto cast_input = create_slot_ref(from_type.children[i]);
                field_casts[i]->add_child(cast_input.get());
                pool->add(cast_input.release());
            }
            return new CastStructExpr(node, std::move(field_casts), std::move(source_field_indices));
        }
    }
    if ((from_type.type == TYPE_NULL || from_type.type == TYPE_BOOLEAN) && to_type.is_complex_type()) {
        return new MustNullExpr(node);
    }

    auto res = create_primitive_cast(pool, node, from_type.type, to_type.type, allow_throw_exception);
    if (res == nullptr) {
        return Status::NotSupported(
                fmt::format("Not support cast {} to {}.", from_type.debug_string(), to_type.debug_string()));
    }
    return res;
}

// Need add result to pool by caller.
Expr* VectorizedCastExprFactory::from_thrift(ObjectPool* pool, const TExprNode& node, bool allow_throw_exception) {
    TypeDescriptor to_type = TypeDescriptor::from_thrift(node.type);
    TypeDescriptor from_type(thrift_to_type(node.child_type));
    if (node.__isset.child_type_desc) {
        from_type = TypeDescriptor::from_thrift(node.child_type_desc);
    }
    auto ret = create_cast_expr(pool, node, from_type, to_type, allow_throw_exception);
    if (!ret.ok()) {
        LOG(WARNING) << "Not support cast " << from_type << " to " << to_type;
        return nullptr;
    }
    return ret.value();
}

// Need add result to pool by caller.
StatusOr<Expr*> VectorizedCastExprFactory::create_cast_expr(ObjectPool* pool, const TypeDescriptor& from_type,
                                                            const TypeDescriptor& to_type, bool allow_throw_exception,
                                                            bool cast_by_name) {
    TExprNode cast_node;
    cast_node.node_type = TExprNodeType::CAST_EXPR;
    cast_node.type = to_type.to_thrift();
    cast_node.__set_child_type(to_thrift(from_type.type));
    cast_node.__set_child_type_desc(from_type.to_thrift());
    if (cast_by_name) {
        cast_node.__set_cast_struct_by_name(true);
    }
    return create_cast_expr(pool, cast_node, from_type, to_type, allow_throw_exception);
}

// Need add result to pool by callee.
Expr* VectorizedCastExprFactory::from_type(const TypeDescriptor& from, const TypeDescriptor& to, Expr* child,
                                           ObjectPool* pool, bool allow_throw_exception) {
    auto ret = create_cast_expr(pool, from, to, allow_throw_exception);
    if (!ret.ok()) {
        LOG(WARNING) << "Not support cast " << from << " to " << to;
        return nullptr;
    }

    auto* cast_expr = ret.value();
    pool->add(cast_expr);
    cast_expr->add_child(child);
    return cast_expr;
}

} // namespace starrocks
