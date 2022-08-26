// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class HashJoinImplementationRule extends JoinImplementationRule {
    private HashJoinImplementationRule() {
        super(RuleType.IMP_EQ_JOIN_TO_HASH_JOIN);
    }

    private static final HashJoinImplementationRule instance = new HashJoinImplementationRule();

    public static HashJoinImplementationRule getInstance() {
        return instance;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        JoinOperator joinType = joinOperator.getJoinType();
        ScalarOperator predicate = joinOperator.getOnPredicate();
        return predicate == null || Utils.containsEqualBinaryPredicate(predicate);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        PhysicalHashJoinOperator physicalHashJoin = new PhysicalHashJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection());
        OptExpression result = OptExpression.create(physicalHashJoin, input.getInputs());
        return Lists.newArrayList(result);
    }
}
