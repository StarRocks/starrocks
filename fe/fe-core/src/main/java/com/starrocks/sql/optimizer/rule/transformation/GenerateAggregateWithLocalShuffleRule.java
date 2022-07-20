// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * If there is only one BE and one fragment instance, we could generate one-phase local aggregation
 * with the local shuffle operator (ScanNode->LocalShuffleNode->OnePhaseAggNode) regardless of the differences
 * between grouping keys and scan distribution keys.
 */
public class GenerateAggregateWithLocalShuffleRule extends TransformationRule {

    protected GenerateAggregateWithLocalShuffleRule() {
        super(RuleType.TF_GENERATE_AGGREGATE_WITH_LOCAL_SHUFFLE,
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    private static final GenerateAggregateWithLocalShuffleRule instance = new GenerateAggregateWithLocalShuffleRule();

    public static GenerateAggregateWithLocalShuffleRule getInstance() {
        return instance;
    }

    public static boolean enable(SessionVariable sessionVariable) {
        // Only apply this rule, if there is only one BE.
        return sessionVariable.isEnableLocalShuffleAgg() && sessionVariable.isEnablePipelineEngine()
                && GlobalStateMgr.getCurrentSystemInfo().backendSize() == 1;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        // Only apply this rule, if it is one-phase aggregation and contains grouping keys.
        return !agg.isWithLocalShuffleOperator() && agg.isOnePhaseAgg() && !agg.getGroupingKeys().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator oldAgg = (LogicalAggregationOperator) input.getOp();

        LogicalAggregationOperator newAgg = new LogicalAggregationOperator.Builder().withOperator(oldAgg)
                .setWithLocalShuffleOperator(true).build();
        OptExpression expression = OptExpression.create(newAgg, input.getInputs());
        return Lists.newArrayList(expression);
    }
}
