// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class FilterImplementationRule extends ImplementationRule {
    public FilterImplementationRule() {
        super(RuleType.IMP_FILTER,
                Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator logical = (LogicalFilterOperator) input.getOp();

        PhysicalFilterOperator filter = new PhysicalFilterOperator(
                logical.getPredicate(),
                logical.getLimit(),
                logical.getProjection());
        return Lists.newArrayList(OptExpression.create(filter, input.getInputs()));
    }
}
