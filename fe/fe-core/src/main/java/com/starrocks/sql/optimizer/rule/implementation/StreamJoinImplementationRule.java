// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class StreamJoinImplementationRule extends JoinImplementationRule {
    private static final StreamJoinImplementationRule INSTANCE = new StreamJoinImplementationRule(RuleType.IMP_STREAM_JOIN);

    public static StreamJoinImplementationRule getInstance() {
        return INSTANCE;
    }

    private StreamJoinImplementationRule(RuleType type) {
        super(type);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        PhysicalStreamJoinOperator physicalStreamJoin = new PhysicalStreamJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection());
        OptExpression result = OptExpression.create(physicalStreamJoin, input.getInputs());
        return Lists.newArrayList(result);
    }
}
