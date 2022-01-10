// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/in_const_predicate.hpp"

#include "gutil/strings/substitute.h"

namespace starrocks {
namespace vectorized {

#define APPLY_FOR_ALL_PRIMITIVE_TYPE(M) \
    M(TYPE_TINYINT)                     \
    M(TYPE_SMALLINT)                    \
    M(TYPE_INT)                         \
    M(TYPE_BIGINT)                      \
    M(TYPE_LARGEINT)                    \
    M(TYPE_FLOAT)                       \
    M(TYPE_DOUBLE)                      \
    M(TYPE_VARCHAR)                     \
    M(TYPE_CHAR)                        \
    M(TYPE_DATE)                        \
    M(TYPE_DATETIME)                    \
    M(TYPE_DECIMALV2)                   \
    M(TYPE_DECIMAL32)                   \
    M(TYPE_DECIMAL64)                   \
    M(TYPE_DECIMAL128)                  \
    M(TYPE_BOOLEAN)

ExprContext* VectorizedInConstPredicateBuilder::_create() {
    Expr* probe_expr = _expr;

    TExprNode node;
    PrimitiveType probe_type = probe_expr->type().type;

    // create TExprNode
    node.__set_use_vectorized(true);
    node.__set_node_type(TExprNodeType::IN_PRED);
    TScalarType tscalar_type;
    tscalar_type.__set_type(TPrimitiveType::BOOLEAN);
    TTypeNode ttype_node;
    ttype_node.__set_type(TTypeNodeType::SCALAR);
    ttype_node.__set_scalar_type(tscalar_type);
    TTypeDesc t_type_desc;
    t_type_desc.types.push_back(ttype_node);
    node.__set_type(t_type_desc);
    node.in_predicate.__set_is_not_in(_is_not_in);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__isset.vector_opcode = true;
    node.__set_vector_opcode(to_in_opcode(probe_type));

    // create template of in-predicate.
    // and fill actual IN values later.
    switch (probe_type) {
#define M(NAME)                                                                                                      \
    case PrimitiveType::NAME: {                                                                                      \
        if (_array_size != 0 && !VectorizedInConstPredicate<PrimitiveType::NAME>::can_use_array()) {                 \
            _st = Status::NotSupported(                                                                              \
                    strings::Substitute("Can not create in-const-predicate with array set on type $0", probe_type)); \
            return nullptr;                                                                                          \
        }                                                                                                            \
        auto* in_pred = _pool->add(new VectorizedInConstPredicate<PrimitiveType::NAME>(node));                       \
        in_pred->set_null_in_set(_null_in_set);                                                                      \
        in_pred->set_array_size(_array_size);                                                                        \
        _st = in_pred->prepare(_state);                                                                              \
        if (!_st.ok()) return nullptr;                                                                               \
        in_pred->add_child(Expr::copy(_pool, probe_expr));                                                           \
        in_pred->set_is_join_runtime_filter(_is_join_runtime_filter);                                                \
        in_pred->set_eq_null(_eq_null);                                                                              \
        auto* ctx = _pool->add(new ExprContext(in_pred));                                                            \
        return ctx;                                                                                                  \
    }
        APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
    default:
        _st = Status::NotSupported(strings::Substitute("Can not create in-const-predicate on type $0", probe_type));
        return nullptr;
    }
}

Status VectorizedInConstPredicateBuilder::create() {
    _st = Status::OK();
    _in_pred_ctx = _create();
    return _st;
}

Status VectorizedInConstPredicateBuilder::add_values(const ColumnPtr& column, size_t column_offset) {
    PrimitiveType type = _expr->type().type;
    Expr* expr = _in_pred_ctx->root();
    DCHECK(column != nullptr);
    if (!column->is_nullable()) {
        switch (type) {
#define M(FIELD_TYPE)                                                                 \
    case PrimitiveType::FIELD_TYPE: {                                                 \
        using ColumnType = typename RunTimeTypeTraits<FIELD_TYPE>::ColumnType;        \
        auto* in_pred = (VectorizedInConstPredicate<FIELD_TYPE>*)(expr);              \
        auto& data_ptr = ColumnHelper::as_raw_column<ColumnType>(column)->get_data(); \
        if (in_pred->is_use_array()) {                                                \
            for (size_t j = column_offset; j < data_ptr.size(); j++) {                \
                in_pred->insert_array(&data_ptr[j]);                                  \
            }                                                                         \
        } else {                                                                      \
            for (size_t j = column_offset; j < data_ptr.size(); j++) {                \
                in_pred->insert(&data_ptr[j]);                                        \
            }                                                                         \
        }                                                                             \
        break;                                                                        \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
        default:;
        }
    } else {
        switch (type) {
#define M(FIELD_TYPE)                                                                                           \
    case PrimitiveType::FIELD_TYPE: {                                                                           \
        using ColumnType = typename RunTimeTypeTraits<FIELD_TYPE>::ColumnType;                                  \
        auto* in_pred = (VectorizedInConstPredicate<FIELD_TYPE>*)(expr);                                        \
        auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);                            \
        auto& data_array = ColumnHelper::as_raw_column<ColumnType>(nullable_column->data_column())->get_data(); \
        if (in_pred->is_use_array()) {                                                                          \
            for (size_t j = column_offset; j < data_array.size(); j++) {                                        \
                if (!nullable_column->is_null(j)) {                                                             \
                    in_pred->insert_array(&data_array[j]);                                                      \
                } else {                                                                                        \
                    if (_eq_null) {                                                                             \
                        in_pred->insert_array(nullptr);                                                         \
                    }                                                                                           \
                }                                                                                               \
            }                                                                                                   \
        } else {                                                                                                \
            for (size_t j = column_offset; j < data_array.size(); j++) {                                        \
                if (!nullable_column->is_null(j)) {                                                             \
                    in_pred->insert(&data_array[j]);                                                            \
                } else {                                                                                        \
                    if (_eq_null) {                                                                             \
                        in_pred->insert(nullptr);                                                               \
                    }                                                                                           \
                }                                                                                               \
            }                                                                                                   \
        }                                                                                                       \
        break;                                                                                                  \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
#undef M
        default:;
        }
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace starrocks
