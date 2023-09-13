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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * When you want to replace some types of nodes in a scalarOperator tree, you can extend this class and
 * override specific visit methods. It will return a new scalarOperator tree with specific nodes replaced.
 * shuttle means a scalarOperator bus, it takes you traverse the scalarOperator tree.
 */
public class BaseScalarOperatorShuttle extends ScalarOperatorVisitor<ScalarOperator, Void> {
    private static final Map<Class<? extends ScalarOperator>, BiFunction<ScalarOperator, List<ScalarOperator>, ScalarOperator>>
            CLONE_FUNCTIONS;

    static {
        CLONE_FUNCTIONS = ImmutableMap.<Class<? extends ScalarOperator>,
                BiFunction<ScalarOperator, List<ScalarOperator>, ScalarOperator>>builder()
                .put(ConstantOperator.class, (op, childOps) -> op)
                .put(ColumnRefOperator.class, (op, childOps) -> op)
                .put(ArrayOperator.class, (op, childOps) -> new ArrayOperator(op.getType(), op.isNullable(), childOps))
                .put(CollectionElementOperator.class, (op, childOps) -> new CollectionElementOperator(op.getType(),
                        childOps.get(0), childOps.get(1)))
                .put(ArraySliceOperator.class, (op, childOps) -> new ArraySliceOperator(op.getType(), childOps))
                .put(CallOperator.class, (op, childOps) -> {
                    CallOperator call = (CallOperator) op;
                    return new CallOperator(call.getFnName(), call.getType(), childOps, call.getFunction(), call.isDistinct()); })
                .put(PredicateOperator.class, (op, childOps) -> op)
                .put(BetweenPredicateOperator.class, (op, childOps) -> {
                    BetweenPredicateOperator between = (BetweenPredicateOperator) op;
                    return new BetweenPredicateOperator(between.isNotBetween(), childOps); })
                .put(BinaryPredicateOperator.class, (op, childOps) -> {
                    BinaryPredicateOperator binary = (BinaryPredicateOperator) op;
                    return new BinaryPredicateOperator(binary.getBinaryType(), childOps); })
                .put(CompoundPredicateOperator.class, (op, childOps) -> {
                    CompoundPredicateOperator compound = (CompoundPredicateOperator) op;
                    return new CompoundPredicateOperator(compound.getCompoundType(), childOps); })
                .put(ExistsPredicateOperator.class, (op, childOps) -> {
                    ExistsPredicateOperator exist = (ExistsPredicateOperator) op;
                    return new ExistsPredicateOperator(exist.isNotExists(), childOps); })
                .put(InPredicateOperator.class, (op, childOps) -> {
                    InPredicateOperator inPredicate = (InPredicateOperator) op;
                    return new InPredicateOperator(inPredicate.isNotIn(), childOps); })
                .put(IsNullPredicateOperator.class, (op, childOps) -> {
                    IsNullPredicateOperator isNullPredicate = (IsNullPredicateOperator) op;
                    return new IsNullPredicateOperator(isNullPredicate.isNotNull(), childOps.get(0)); })
                .put(LikePredicateOperator.class, (op, childOps) -> {
                    LikePredicateOperator like = (LikePredicateOperator) op;
                    return new LikePredicateOperator(like.getLikeType(), childOps); })
                .put(CastOperator.class, (op, childOps) -> {
                    CastOperator cast = (CastOperator) op;
                    return new CastOperator(cast.getType(), childOps.get(0), cast.isImplicit()); })
                .put(CaseWhenOperator.class, (op, childOps) -> {
                    CaseWhenOperator caseWhen = (CaseWhenOperator) op;
                    ScalarOperator clonedCaseClause = null;
                    ScalarOperator clonedElseClause = null;
                    List<ScalarOperator> clonedWhenThenClauses;
                    if (caseWhen.hasCase()) {
                        clonedCaseClause = childOps.get(0);
                    }
                    if (caseWhen.hasElse()) {
                        clonedElseClause = childOps.get(childOps.size() - 1);
                    }

                    int whenThenEndIdx = caseWhen.hasElse() ? childOps.size() - 1 : childOps.size();
                    clonedWhenThenClauses = childOps.subList(caseWhen.getWhenStart(), whenThenEndIdx);

                    return new CaseWhenOperator(caseWhen.getType(), clonedCaseClause, clonedElseClause, clonedWhenThenClauses);
                })
                .put(SubfieldOperator.class, (op, childOps) -> {
                    SubfieldOperator subfield = (SubfieldOperator) op;
                    return new SubfieldOperator(childOps.get(0), subfield.getType(), subfield.getFieldNames()); })
                .put(MapOperator.class, (op, childOps) -> new MapOperator(op.getType(), childOps))
                .put(MultiInPredicateOperator.class, (op, childOps) -> {
                    MultiInPredicateOperator multiIn = (MultiInPredicateOperator) op;
                    return new MultiInPredicateOperator(multiIn.isNotIn(), childOps, multiIn.getTupleSize()); })
                .put(LambdaFunctionOperator.class, (op, childOps) -> {
                    LambdaFunctionOperator lambda = (LambdaFunctionOperator) op;
                    return new LambdaFunctionOperator(lambda.getRefColumns(), childOps.get(0), lambda.getType()); })
                .put(CloneOperator.class, (op, childOps) -> new CloneOperator(childOps.get(0)))
                .build();
    }

    public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
        return scalarOperator;
    }

    public Optional<ScalarOperator> preprocess(ScalarOperator scalarOperator) {
        return Optional.empty();
    }

    public ScalarOperator shuttleIfUpdate(ScalarOperator operator) {
        Optional<ScalarOperator> preprocessed = preprocess(operator);
        if (preprocessed.isPresent()) {
            return preprocessed.get();
        }
        boolean[] update = {false};
        List<ScalarOperator> clonedChildOperators = visitList(operator.getChildren(), update);
        if (update[0]) {
            BiFunction<ScalarOperator, List<ScalarOperator>, ScalarOperator> cloningFunction =
                    CLONE_FUNCTIONS.get(operator.getClass());
            Preconditions.checkNotNull(cloningFunction);
            return cloningFunction.apply(operator, clonedChildOperators);
        } else {
            return operator;
        }
    }


    @Override
    public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
        return shuttleIfUpdate(literal);
    }

    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
        return shuttleIfUpdate(variable);
    }

    @Override
    public ScalarOperator visitArray(ArrayOperator array, Void context) {
        return shuttleIfUpdate(array);
    }

    @Override
    public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
        return shuttleIfUpdate(collectionElementOp);
    }

    @Override
    public ScalarOperator visitArraySlice(ArraySliceOperator array, Void context) {
        return shuttleIfUpdate(array);
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, Void context) {
        return shuttleIfUpdate(call);
    }

    @Override
    public ScalarOperator visitPredicate(PredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
        return shuttleIfUpdate(predicate);
    }

    @Override
    public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    @Override
    public ScalarOperator visitSubfield(SubfieldOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    @Override
    public ScalarOperator visitMap(MapOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    @Override
    public ScalarOperator visitMultiInPredicate(MultiInPredicateOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    @Override
    public ScalarOperator visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    @Override
    public ScalarOperator visitCloneOperator(CloneOperator operator, Void context) {
        return shuttleIfUpdate(operator);
    }

    protected List<ScalarOperator> visitList(List<ScalarOperator> operators, boolean[] update) {
        if (operators == null) {
            return null;
        }

        List<ScalarOperator> clonedOperators = Lists.newArrayList();
        for (ScalarOperator operator : operators) {
            ScalarOperator clonedOperator = operator == null ? null : operator.accept(this, null);
            if ((clonedOperator != operator) && (update != null)) {
                update[0] = true;
            }
            clonedOperators.add(clonedOperator);
        }
        return clonedOperators;
    }
}
