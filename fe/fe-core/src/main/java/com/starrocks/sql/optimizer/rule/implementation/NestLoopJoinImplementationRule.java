// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class NestLoopJoinImplementationRule extends JoinImplementationRule {
    private NestLoopJoinImplementationRule() {
        super(RuleType.IMP_JOIN_TO_NESTLOOP_JOIN);
    }

    private static final NestLoopJoinImplementationRule instance = new NestLoopJoinImplementationRule();

    public static NestLoopJoinImplementationRule getInstance() {
        return instance;
    }

    // Only choose NestLoopJoin for such scenarios, which HashJoin could not handle
    // 1. No equal-conjuncts in join clause
    // 2. JoinType is INNER/CROSS/OUTER
    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        List<BinaryPredicateOperator> eqPredicates = extractEqPredicate(input, context);
        JoinOperator joinType = getJoinType(input);
        return supportJoinType(joinType) && CollectionUtils.isEmpty(eqPredicates);
    }

    private boolean supportJoinType(JoinOperator joinType) {
        return joinType.isCrossJoin() || joinType.isInnerJoin() || joinType.isOuterJoin();
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
