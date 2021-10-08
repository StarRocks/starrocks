// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/in_predicate.h"

#include "common/object_pool.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/in_iterator_predicate.hpp"

namespace starrocks::vectorized {

#define CASE_TYPE(TYPE, CLASS)        \
    case TYPE: {                      \
        return new CLASS<TYPE>(node); \
    }

#define SWITCH_ALL_TYPE(CLASS)                                                             \
    switch (child_type) {                                                                  \
        CASE_TYPE(TYPE_NULL, CLASS);                                                       \
        CASE_TYPE(TYPE_BOOLEAN, CLASS);                                                    \
        CASE_TYPE(TYPE_TINYINT, CLASS);                                                    \
        CASE_TYPE(TYPE_SMALLINT, CLASS);                                                   \
        CASE_TYPE(TYPE_INT, CLASS);                                                        \
        CASE_TYPE(TYPE_BIGINT, CLASS);                                                     \
        CASE_TYPE(TYPE_FLOAT, CLASS);                                                      \
        CASE_TYPE(TYPE_DOUBLE, CLASS);                                                     \
        CASE_TYPE(TYPE_DATE, CLASS);                                                       \
        CASE_TYPE(TYPE_DATETIME, CLASS);                                                   \
        CASE_TYPE(TYPE_DECIMALV2, CLASS);                                                  \
        CASE_TYPE(TYPE_LARGEINT, CLASS);                                                   \
        CASE_TYPE(TYPE_CHAR, CLASS);                                                       \
        CASE_TYPE(TYPE_VARCHAR, CLASS);                                                    \
        CASE_TYPE(TYPE_DECIMAL32, CLASS);                                                  \
        CASE_TYPE(TYPE_DECIMAL64, CLASS);                                                  \
        CASE_TYPE(TYPE_DECIMAL128, CLASS);                                                 \
    default:                                                                               \
        LOG(WARNING) << "vectorized engine in predicate not support type: " << child_type; \
        return nullptr;                                                                    \
    }

Expr* VectorizedInPredicateFactory::from_thrift(const TExprNode& node) {
    // children type
    PrimitiveType child_type = thrift_to_type(node.child_type);

    if (child_type == TYPE_CHAR) {
        child_type = TYPE_VARCHAR;
    }

    switch (node.opcode) {
    case TExprOpcode::FILTER_IN:
    case TExprOpcode::FILTER_NOT_IN: {
        SWITCH_ALL_TYPE(VectorizedInConstPredicate);
    }
    case TExprOpcode::FILTER_NEW_IN:
    case TExprOpcode::FILTER_NEW_NOT_IN: {
        SWITCH_ALL_TYPE(VectorizedInIteratorPredicate);
    }

    default:
        LOG(WARNING) << "vectorized engine in predicate not support: " << node.opcode;
        return nullptr;
    }

    return nullptr;
}

#undef SWITCH_ALL_TYPE
#undef CASE_TYPE

} // namespace starrocks::vectorized