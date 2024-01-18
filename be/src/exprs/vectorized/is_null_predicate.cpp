// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/is_null_predicate.h"

#include "column/column_helper.h"
#include "exprs/vectorized/unary_function.h"

namespace starrocks::vectorized {

#define DEFINE_CLASS_CONSTRUCT_FN(NAME)              \
    NAME(const TExprNode& node) : Predicate(node) {} \
                                                     \
    ~NAME() {}                                       \
                                                     \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new NAME(*this)); }

DEFINE_UNARY_FN_WITH_IMPL(isNullImpl, v) {
    return v;
}

class VectorizedIsNullPredicate final : public Predicate {
public:
    DEFINE_CLASS_CONSTRUCT_FN(VectorizedIsNullPredicate);

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));

        if (column->only_null()) {
            return ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, column->size());
        }

        if (!column->is_nullable() || !column->has_null()) {
            return ColumnHelper::create_const_column<TYPE_BOOLEAN>(false, column->size());
        }

        auto col = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
        return VectorizedStrictUnaryFunction<isNullImpl>::evaluate<TYPE_NULL, TYPE_BOOLEAN>(col);
    }
};

DEFINE_UNARY_FN_WITH_IMPL(isNotNullImpl, v) {
    return v == 0;
}

class VectorizedIsNotNullPredicate final : public Predicate {
public:
    DEFINE_CLASS_CONSTRUCT_FN(VectorizedIsNotNullPredicate);

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));

        if (column->only_null()) {
            return ColumnHelper::create_const_column<TYPE_BOOLEAN>(false, column->size());
        }

        if (!column->is_nullable() || !column->has_null()) {
            return ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, column->size());
        }

        auto col = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
        return VectorizedStrictUnaryFunction<isNotNullImpl>::evaluate<TYPE_NULL, TYPE_BOOLEAN>(col);
    }
};

Expr* VectorizedIsNullPredicateFactory::from_thrift(const TExprNode& node) {
    if (node.fn.name.function_name == "is_null_pred") {
        return new VectorizedIsNullPredicate(node);
    } else {
        return new VectorizedIsNotNullPredicate(node);
    }
}

} // namespace starrocks::vectorized
