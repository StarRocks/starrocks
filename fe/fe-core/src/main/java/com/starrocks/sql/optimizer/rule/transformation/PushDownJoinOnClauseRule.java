// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
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
        if (join.getJoinType().isInnerJoin()) {
            on = JoinPredicateUtils.equivalenceDerive(on);
        }

        OptExpression root = pushDownOnPredicate(input, on);
        ((LogicalJoinOperator) root.getOp()).setHasPushDownJoinOnClause(true);
        if (root.getOp().equals(input.getOp()) && on.equals(join.getOnPredicate())) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(root);
    }
}
