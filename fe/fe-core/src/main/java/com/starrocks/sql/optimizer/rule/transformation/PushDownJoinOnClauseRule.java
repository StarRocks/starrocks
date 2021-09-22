// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.pushDownOnPredicate;

public class PushDownJoinOnClauseRule extends TransformationRule {
    public PushDownJoinOnClauseRule() {
        super(RuleType.TF_PUSH_DOWN_JOIN_CLAUSE, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        if (joinOperator.getJoinType().isCrossJoin() || joinOperator.isHasPushDownJoinOnClause()) {
            return false;
        }
        return joinOperator.getOnPredicate() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();

        ScalarOperator on = join.getOnPredicate();

        on = JoinPredicateUtils.rangePredicateDerive(on);
        on = equivalenceDeriveOnPredicate(on, input, join);

        OptExpression root = pushDownOnPredicate(input, on);
        ((LogicalJoinOperator) root.getOp()).setHasPushDownJoinOnClause(true);
        if (root.getOp().equals(input.getOp()) && on.equals(join.getOnPredicate())) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(root);
    }

    ScalarOperator equivalenceDeriveOnPredicate(ScalarOperator on, OptExpression joinOpt, LogicalJoinOperator join) {
        // For SQl: select * from t1 left join t2 on t1.id = t2.id and t1.id > 1
        // Infer t2.id > 1 and Push down it to right child
        if (!join.getJoinType().isInnerJoin() && !join.getJoinType().isSemiJoin() &&
                !join.getJoinType().isOuterJoin()) {
            return on;
        }

        List<ScalarOperator> pushDown = Lists.newArrayList(on);
        ColumnRefSet leftOutputColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightOutputColumns = joinOpt.getInputs().get(1).getOutputColumns();

        ScalarOperator derivedPredicate = JoinPredicateUtils.equivalenceDerive(on, false);
        List<ScalarOperator> derivedPredicates = Utils.extractConjuncts(derivedPredicate);

        if (join.getJoinType().isInnerJoin()) {
            return Utils.compoundAnd(on, derivedPredicate);
        } else if (join.getJoinType().isLeftOuterJoin() || join.getJoinType().isLeftSemiJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (rightOutputColumns.contains(derivedPredicate.getUsedColumns())) {
                    pushDown.add(p);
                }
            }
        } else if (join.getJoinType().isRightOuterJoin() || join.getJoinType().isRightSemiJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (leftOutputColumns.contains(derivedPredicate.getUsedColumns())) {
                    pushDown.add(p);
                }
            }
        }
        return Utils.compoundAnd(pushDown);
    }
}
