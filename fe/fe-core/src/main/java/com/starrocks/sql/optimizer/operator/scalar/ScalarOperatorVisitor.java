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

    public R visitSubfield(SubfieldOperator subfieldOperator, C context) {
        return visit(subfieldOperator, context);
    }

    public R visitArray(ArrayOperator array, C context) {
        return visit(array, context);
    }

    public R visitMap(MapOperator map, C context) {
        return visit(map, context);
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

    public R visitMultiInPredicate(MultiInPredicateOperator predicate, C context) {
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
