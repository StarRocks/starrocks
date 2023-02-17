// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class WindowImplementationRule extends ImplementationRule {
    public WindowImplementationRule() {
        super(RuleType.IMP_ANALYTIC,
                Pattern.create(OperatorType.LOGICAL_WINDOW, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalWindowOperator logical = (LogicalWindowOperator) input.getOp();

        PhysicalWindowOperator physical = new PhysicalWindowOperator(
                logical.getWindowCall(),
                logical.getPartitionExpressions(),
                logical.getOrderByElements(),
                logical.getAnalyticWindow(),
                logical.getEnforceSortColumns(),
                logical.isUseHashBasedPartition(),
                logical.getLimit(),
                logical.getPredicate(),
                logical.getProjection());

        return Lists.newArrayList(OptExpression.create(physical, input.getInputs()));
    }
}
