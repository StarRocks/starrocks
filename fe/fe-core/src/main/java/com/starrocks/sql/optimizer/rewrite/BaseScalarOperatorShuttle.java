// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.clearspring.analytics.util.Lists;
import com.starrocks.sql.optimizer.operator.scalar.ArrayElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

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
    public ScalarOperator visitArrayElement(ArrayElementOperator array, Void context) {
        boolean[] update = {false};
        List<ScalarOperator> clonedOperators = visitList(array.getChildren(), update);
        if (update[0]) {
            return new ArrayElementOperator(array.getType(), clonedOperators.get(0), clonedOperators.get(1));
        }
        return array;
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
            if (operator.getCaseClause() != null) {
                clonedCaseClause = clonedOperators.get(0);
            }
            if (operator.getElseClause() != null) {
                clonedElseClause = clonedOperators.get(clonedOperators.size() - 1);
            }

            int whenThenEndIdx = operator.getElseClause() == null ? clonedOperators.size() : clonedOperators.size() - 1;
            clonedWhenThenClauses = clonedOperators.subList(operator.getWhenStart(), whenThenEndIdx);

            return new CaseWhenOperator(operator.getType(), clonedCaseClause, clonedElseClause, clonedWhenThenClauses);
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
