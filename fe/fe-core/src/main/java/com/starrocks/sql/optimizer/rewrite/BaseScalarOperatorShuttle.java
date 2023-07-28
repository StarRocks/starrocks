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

/**
 * When you want to replace some types of nodes in a scalarOperator tree, you can extend this class and
 * override specific visit methods. It will return a new scalarOperator tree with specific nodes replaced.
 * shuttle means a scalarOperator bus, it takes you traverse the scalarOperator tree.
 */
public class BaseScalarOperatorShuttle extends ScalarOperatorVisitor<ScalarOperator, Void> {

    public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
        return scalarOperator;
    }

    @Override
    public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
        return literal;
    }

    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
        return variable;
    }

    @Override
    public ScalarOperator visitArray(ArrayOperator array, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(array.getChildren(), update);
        if (update[0]) {
            return new ArrayOperator(array.getType(), array.isNullable(), clonedOperators);
        } else {
            return array;
        }
    }

    @Override
    public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(collectionElementOp.getChildren(), update);
        if (update[0]) {
            return new CollectionElementOperator(collectionElementOp.getType(), clonedOperators.get(0),
                    clonedOperators.get(1));
        }
        return collectionElementOp;
    }

    @Override
    public ScalarOperator visitArraySlice(ArraySliceOperator array, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(array.getChildren(), update);
        if (update[0]) {
            return new ArraySliceOperator(array.getType(), clonedOperators);
        } else {
            return array;
        }
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(call.getChildren(), update);
        if (update[0]) {
            return new CallOperator(call.getFnName(), call.getType(), clonedOperators,
                    call.getFunction(), call.isDistinct());
        } else {
            return call;
        }
    }

    @Override
    public ScalarOperator visitPredicate(PredicateOperator predicate, Void context) {
        return predicate;
    }

    @Override
    public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new BetweenPredicateOperator(predicate.isNotBetween(), clonedOperators);
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new BinaryPredicateOperator(predicate.getBinaryType(), clonedOperators);
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new CompoundPredicateOperator(predicate.getCompoundType(), clonedOperators);
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new ExistsPredicateOperator(predicate.isNotExists(), clonedOperators);
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new InPredicateOperator(predicate.isNotIn(), clonedOperators);
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new IsNullPredicateOperator(predicate.isNotNull(), clonedOperators.get(0));
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(predicate.getChildren(), update);
        if (update[0]) {
            return new LikePredicateOperator(predicate.getLikeType(), clonedOperators);
        } else {
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(operator.getChildren(), update);
        if (update[0]) {
            return new CastOperator(operator.getType(), clonedOperators.get(0), operator.isImplicit());
        } else {
            return operator;
        }
    }

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(operator.getChildren(), update);
        if (update[0]) {
            ScalarOperator clonedCaseClause = null;
            ScalarOperator clonedElseClause = null;
            List<ScalarOperator> clonedWhenThenClauses;
            if (operator.hasCase()) {
                clonedCaseClause = clonedOperators.get(0);
            }
            if (operator.hasElse()) {
                clonedElseClause = clonedOperators.get(clonedOperators.size() - 1);
            }

            int whenThenEndIdx = operator.hasElse() ? clonedOperators.size() - 1 : clonedOperators.size();
            clonedWhenThenClauses = clonedOperators.subList(operator.getWhenStart(), whenThenEndIdx);

            return new CaseWhenOperator(operator.getType(), clonedCaseClause, clonedElseClause, clonedWhenThenClauses);
        } else {
            return operator;
        }
    }

    @Override
    public ScalarOperator visitSubfield(SubfieldOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> child = visitList(operator.getChildren(), update);
        if (update[0]) {
            return new SubfieldOperator(child.get(0), operator.getType(), operator.getFieldNames());
        } else {
            return operator;
        }
    }

    @Override
    public ScalarOperator visitMap(MapOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> children = visitList(operator.getChildren(), update);
        if (update[0]) {
            return new MapOperator(operator.getType(), children);
        } else {
            return operator;
        }
    }

    @Override
    public ScalarOperator visitMultiInPredicate(MultiInPredicateOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> children = visitList(operator.getChildren(), update);
        if (update[0]) {
            return new MultiInPredicateOperator(operator.isNotIn(), children, operator.getTupleSize());
        } else {
            return operator;
        }
    }

    @Override
    public ScalarOperator visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> children = visitList(operator.getChildren(), update);
        if (update[0]) {
            return new LambdaFunctionOperator(operator.getRefColumns(), children.get(0), operator.getType());
        } else {
            return operator;
        }
    }

    @Override
    public ScalarOperator visitCloneOperator(CloneOperator operator, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> children = visitList(operator.getChildren(), update);
        if (update[0]) {
            return new CloneOperator(children.get(0));
        } else {
            return operator;
        }
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
