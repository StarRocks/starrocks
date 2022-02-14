// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownLimitJoinRule extends TransformationRule {
    public PushDownLimitJoinRule() {
        super(RuleType.TF_PUSH_DOWN_LIMIT_JOIN, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        LogicalJoinOperator join = (LogicalJoinOperator) input.getInputs().get(0).getOp();

        // 1. Has offset can't push down
        // 2. Has predicate can't push down
        return !limit.hasOffset() && join.getPredicate() == null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        OptExpression child = input.inputAt(0);
        LogicalJoinOperator join = (LogicalJoinOperator) child.getOp();
        JoinOperator joinType = join.getJoinType();

        // set limit
        join.setLimit(limit.getLimit());

        if (joinType.isSemiAntiJoin()) {
            return Lists.newArrayList(child);
        } else if (joinType.isInnerJoin() && join.getOnPredicate() != null) {
            return Lists.newArrayList(child);
        } else if (joinType.isCrossJoin() && join.getOnPredicate() != null) {
            return Lists.newArrayList(child);
        }

        // Cross-Join || Full-Outer-Join
        int[] pushDownChildIdx = {0, 1};

        // push down all child
        if (joinType.isLeftOuterJoin()) {
            pushDownChildIdx = new int[] {0};
        } else if (joinType.isRightOuterJoin()) {
            pushDownChildIdx = new int[] {1};
        }

        for (int index : pushDownChildIdx) {
            OptExpression nl = new OptExpression(new LogicalLimitOperator(limit.getLimit()));
            nl.getInputs().add(child.inputAt(index));
            child.getInputs().set(index, nl);
        }

        return Lists.newArrayList(child);
    }
}
