// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MergeJoinImplementationRule extends JoinImplementationRule {
    private MergeJoinImplementationRule() {
        super(RuleType.IMP_EQ_JOIN_TO_MERGE_JOIN);
    }

    private static final MergeJoinImplementationRule instance = new MergeJoinImplementationRule();

    public static MergeJoinImplementationRule getInstance() {
        return instance;
    }

    // TODO: support non-equal predicate
    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        ScalarOperator predicate = joinOperator.getOnPredicate();
        return Utils.isEqualBinaryPredicate(predicate);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        PhysicalMergeJoinOperator physicalMergeJoin = new PhysicalMergeJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection());
        OptExpression result = OptExpression.create(physicalMergeJoin, input.getInputs());
        return Lists.newArrayList(result);
    }
}
