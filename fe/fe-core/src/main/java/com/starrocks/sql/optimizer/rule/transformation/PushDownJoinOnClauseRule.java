// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.JoinPredicatePushdown;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PushDownJoinOnClauseRule extends TransformationRule {
    public PushDownJoinOnClauseRule() {
        super(RuleType.TF_PUSH_DOWN_JOIN_CLAUSE, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        if (joinOperator.hasPushDownJoinOnClause()) {
            return false;
        }
        return joinOperator.getOnPredicate() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<OptExpression> children = Lists.newArrayList(input.getInputs());
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();
        ScalarOperator on = join.getOnPredicate();
        JoinPredicatePushdown joinPredicatePushdown = new JoinPredicatePushdown(
<<<<<<< HEAD
                input, true, false, context.getColumnRefFactory());
=======
                input, true, false, context.getColumnRefFactory(),
                context.isEnableLeftRightJoinEquivalenceDerive(), context);
>>>>>>> 37b8aa5a55 ([BugFix] fix left outer join to inner join bug and string not equal rewrite bug (#39331))
        OptExpression root = joinPredicatePushdown.pushdown(join.getOnPredicate());
        ((LogicalJoinOperator) root.getOp()).setHasPushDownJoinOnClause(true);
        if (root.getOp().equals(input.getOp()) && on.equals(join.getOnPredicate()) &&
                children.equals(root.getInputs())) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(root);
    }
}
