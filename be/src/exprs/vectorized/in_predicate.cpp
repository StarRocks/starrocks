// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/in_predicate.h"

#include "common/object_pool.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/in_iterator_predicate.hpp"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"

namespace starrocks::vectorized {

struct InConstPredicateBuilder {
    const TExprNode& node;
    InConstPredicateBuilder(const TExprNode& node): node(node) {}
    
    template <PrimitiveType ptype>
    Expr* operator()() {
        return new VectorizedInConstPredicate<ptype>(node);
    }
};

struct InIteratorBuilder {
    const TExprNode& node;
    InIteratorBuilder(const TExprNode& node): node(node) {}
    
    template <PrimitiveType ptype>
    Expr* operator()() {
        return new VectorizedInIteratorPredicate<ptype>(node);
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
    case TExprOpcode::FILTER_NOT_IN: {
        return TYPE_DISPATCH_PREDICATE_TYPE((Expr*)new VectorizedInConstPredicate, child_type, node);
    }
    case TExprOpcode::FILTER_NEW_IN:
    case TExprOpcode::FILTER_NEW_NOT_IN: {
        return TYPE_DISPATCH_PREDICATE_TYPE((Expr*)new VectorizedInIteratorPredicate, child_type, node);
    }

    default:
        LOG(WARNING) << "vectorized engine in predicate not support: " << node.opcode;
        return nullptr;
    }

    return nullptr;
}

} // namespace starrocks::vectorized