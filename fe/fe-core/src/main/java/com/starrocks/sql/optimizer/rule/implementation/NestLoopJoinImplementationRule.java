// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class NestLoopJoinImplementationRule extends JoinImplementationRule {
    private NestLoopJoinImplementationRule() {
        super(RuleType.IMP_JOIN_TO_NESTLOOP_JOIN);
    }

    private static final NestLoopJoinImplementationRule instance = new NestLoopJoinImplementationRule();

    public static NestLoopJoinImplementationRule getInstance() {
        return instance;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        JoinOperator joinType = joinOperator.getJoinType();
        ScalarOperator predicate = joinOperator.getOnPredicate();
        // TODO: support other join types
        return !Utils.containsEqualBinaryPredicate(predicate) && (joinType.isCrossJoin() || joinType.isInnerJoin() ||
                joinType.isOuterJoin());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        PhysicalNestLoopJoinOperator physicalNestLoopJoin = new PhysicalNestLoopJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection());
        OptExpression result = OptExpression.create(physicalNestLoopJoin, input.getInputs());
        return Lists.newArrayList(result);
    }
}
