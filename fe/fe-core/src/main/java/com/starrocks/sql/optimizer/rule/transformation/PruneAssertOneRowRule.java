// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PruneAssertOneRowRule extends TransformationRule {
    public PruneAssertOneRowRule() {
        super(RuleType.TF_PRUNE_ASSERT_ONE_ROW,
                Pattern.create(OperatorType.LOGICAL_ASSERT_ONE_ROW).addChildren(Pattern.create(
                        OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // if child is aggregation and none grouping key, remove AssertOneRow node
        if (OperatorType.LOGICAL_AGGR.equals(input.getInputs().get(0).getOp().getOpType())) {
            LogicalAggregationOperator lao = (LogicalAggregationOperator) input.getInputs().get(0).getOp();

            return lao.getGroupingKeys().isEmpty() && (lao.getPredicate() == null);
        }

        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return input.getInputs();
    }
}
