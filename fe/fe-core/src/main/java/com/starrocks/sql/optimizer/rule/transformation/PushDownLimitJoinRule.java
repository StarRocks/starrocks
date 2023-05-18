// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
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
        return limit.isLocal();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        Preconditions.checkState(!limit.hasOffset());

        OptExpression child = input.inputAt(0);
        LogicalJoinOperator newJoin = new LogicalJoinOperator.Builder()
                .withOperator((LogicalJoinOperator) child.getOp())
                .setLimit(limit.getLimit()).build();

        OptExpression result = OptExpression.create(newJoin, child.getInputs());
        JoinOperator joinType = newJoin.getJoinType();

        if (newJoin.getPredicate() != null) {
            return Lists.newArrayList(result);
        }

        // TODO: Push down the limit to the full outer join if BE can output
        // the matched rows first.
        if (joinType.isSemiAntiJoin() || joinType.isFullOuterJoin()) {
            return Lists.newArrayList(result);
        } else if (joinType.isInnerJoin() && newJoin.getOnPredicate() != null) {
            return Lists.newArrayList(result);
        } else if (joinType.isCrossJoin() && newJoin.getOnPredicate() != null) {
            return Lists.newArrayList(result);
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
            OptExpression nl = new OptExpression(LogicalLimitOperator.local(limit.getLimit()));
            nl.getInputs().add(result.inputAt(index));
            result.getInputs().set(index, nl);
        }

        return Lists.newArrayList(result);
    }
}
