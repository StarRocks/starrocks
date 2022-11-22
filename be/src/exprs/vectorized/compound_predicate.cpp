// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/compound_predicate.h"

#include "common/object_pool.h"
#include "exprs/predicate.h"
#include "exprs/vectorized/binary_function.h"
#include "exprs/vectorized/unary_function.h"

namespace starrocks::vectorized {

#define DEFINE_COMPOUND_CONSTRUCT(CLASS)              \
    CLASS(const TExprNode& node) : Predicate(node) {} \
    virtual ~CLASS() {}                               \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new CLASS(*this)); }

/**
 * IS NULL AND IS NULL = IS NULL
 * IS NOT NULL AND IS NOT NULL = IS NOT NULL
 * TRUE AND IS NULL = IS NULL
 * FALSE AND IS NULL = IS NOT NULL(FALSE)
 */
DEFINE_LOGIC_NULL_BINARY_FUNCTION_WITH_IMPL(AndNullImpl, l_value, l_null, r_value, r_null) {
    return (l_null & r_null) | (r_null & (l_null ^ l_value)) | (l_null & (r_null ^ r_value));
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(AndImpl, l_value, r_value) {
    return l_value & r_value;
}

class VectorizedAndCompoundPredicate final : public Predicate {
public:
    DEFINE_COMPOUND_CONSTRUCT(VectorizedAndCompoundPredicate);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));
        int l_falses = ColumnHelper::count_false_with_notnull(l);

        // left all false and not null
        if (l_falses == l->size()) {
            return l->clone();
        }

        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        return VectorizedLogicPredicateBinaryFunction<AndNullImpl, AndImpl>::template evaluate<TYPE_BOOLEAN>(l, r);
    }
};

/**
 * IS NULL OR IS NULL = IS NULL
 * IS NOT NULL OR IS NOT NULL = IS NOT NULL
 * TRUE OR NULL = IS NOT NULL(TRUE)
 * FALSE OR IS NULL = IS NULL
 */
DEFINE_LOGIC_NULL_BINARY_FUNCTION_WITH_IMPL(OrNullImpl, l_value, l_null, r_value, r_null) {
    return (l_null & r_null) | (r_null & (r_null ^ l_value)) | (l_null & (l_null ^ r_value));
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(OrImpl, l_value, r_value) {
    return l_value | r_value;
}

class VectorizedOrCompoundPredicate final : public Predicate {
public:
    DEFINE_COMPOUND_CONSTRUCT(VectorizedOrCompoundPredicate);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));

        int l_trues = ColumnHelper::count_true_with_notnull(l);
        // left all true and not null
        if (l_trues == l->size()) {
            return l->clone();
        }

        ASSIGN_OR_RETURN(auto r, _children[1]->evaluate_checked(context, ptr));

        return VectorizedLogicPredicateBinaryFunction<OrNullImpl, OrImpl>::template evaluate<TYPE_BOOLEAN>(l, r);
    }
};

DEFINE_UNARY_FN_WITH_IMPL(CompoundPredNot, l) {
    return !l;
}

class VectorizedNotCompoundPredicate final : public Predicate {
public:
    DEFINE_COMPOUND_CONSTRUCT(VectorizedNotCompoundPredicate);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(auto l, _children[0]->evaluate_checked(context, ptr));

        return VectorizedStrictUnaryFunction<CompoundPredNot>::template evaluate<TYPE_BOOLEAN>(l);
    }
};

#undef DEFINE_COMPOUND_CONSTRUCT

Expr* VectorizedCompoundPredicateFactory::from_thrift(const TExprNode& node) {
    switch (node.opcode) {
    case TExprOpcode::COMPOUND_AND:
        return new VectorizedAndCompoundPredicate(node);
    case TExprOpcode::COMPOUND_OR:
        return new VectorizedOrCompoundPredicate(node);
    case TExprOpcode::COMPOUND_NOT:
        return new VectorizedNotCompoundPredicate(node);
    default:
        DCHECK(false) << "Not support compound predicate: " << node.opcode;
        return nullptr;
    }
}

} // namespace starrocks::vectorized
