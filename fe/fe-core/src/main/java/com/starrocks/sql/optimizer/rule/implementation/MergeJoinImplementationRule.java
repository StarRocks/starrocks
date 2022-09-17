// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class MergeJoinImplementationRule extends JoinImplementationRule {
    private MergeJoinImplementationRule() {
        super(RuleType.IMP_EQ_JOIN_TO_MERGE_JOIN);
    }

    private static final MergeJoinImplementationRule INSTANCE = new MergeJoinImplementationRule();

    public static MergeJoinImplementationRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        List<BinaryPredicateOperator> eqPredicates = extractEqPredicate(input, context);
        return CollectionUtils.isNotEmpty(eqPredicates);
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
