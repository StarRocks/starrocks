// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/in_predicate.h"

#include "common/object_pool.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"

namespace starrocks::vectorized {

struct InConstPredicateBuilder {
    template <PrimitiveType ptype>
    Expr* operator()(const TExprNode& node) {
        return new VectorizedInConstPredicate<ptype>(node);
    }
};

Expr* VectorizedInPredicateFactory::from_thrift(const TExprNode& node) {
    // children type
    PrimitiveType child_type = thrift_to_type(node.child_type);

    if (child_type == TYPE_CHAR) {
        child_type = TYPE_VARCHAR;
    }

    switch (node.opcode) {
    case TExprOpcode::FILTER_IN:
    case TExprOpcode::FILTER_NOT_IN:
        return type_dispatch_basic(child_type, InConstPredicateBuilder(), node);
    case TExprOpcode::FILTER_NEW_IN:
    case TExprOpcode::FILTER_NEW_NOT_IN:
        // NOTE: These two opcode are deprecated
    default:
        LOG(WARNING) << "vectorized engine in predicate not support: " << node.opcode;
        return nullptr;
    }

    return nullptr;
}

} // namespace starrocks::vectorized