// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

/**
 * ScalarOperatorVisitor is used to traverse Scalar operator
 * User should implement Visitor extend ScalarOperatorVisitor
 * R represents the return value of function visitXXX, C represents the global context
 */
public abstract class ScalarOperatorVisitor<R, C> {

    public abstract R visit(ScalarOperator scalarOperator, C context);

    public R visitConstant(ConstantOperator literal, C context) {
        return visit(literal, context);
    }

    public R visitVariableReference(ColumnRefOperator variable, C context) {
        return visit(variable, context);
    }

    public R visitArray(ArrayOperator array, C context) {
        return visit(array, context);
    }

    public R visitCollectionElement(CollectionElementOperator collectionElementOp, C context) {
        return visit(collectionElementOp, context);
    }

    public R visitArraySlice(ArraySliceOperator array, C context) {
        return visit(array, context);
    }

    public R visitCall(CallOperator call, C context) {
        return visit(call, context);
    }

    public R visitPredicate(PredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitBetweenPredicate(BetweenPredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitBinaryPredicate(BinaryPredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitCompoundPredicate(CompoundPredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitExistsPredicate(ExistsPredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitInPredicate(InPredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitIsNullPredicate(IsNullPredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitLikePredicateOperator(LikePredicateOperator predicate, C context) {
        return visit(predicate, context);
    }

    public R visitCastOperator(CastOperator operator, C context) {
        return visit(operator, context);
    }

    public R visitCaseWhenOperator(CaseWhenOperator operator, C context) {
        return visit(operator, context);
    }

    public R visitDictMappingOperator(DictMappingOperator operator, C context) {
        return visit(operator, context);
    }

    public R visitLambdaFunctionOperator(LambdaFunctionOperator operator, C context) {
        return visit(operator, context);
    }

    public R visitCloneOperator(CloneOperator operator, C context) {
        return visit(operator, context);
    }

    public R visitSubqueryOperator(SubqueryOperator operator, C context) {
        return visit(operator, context);
    }
}
