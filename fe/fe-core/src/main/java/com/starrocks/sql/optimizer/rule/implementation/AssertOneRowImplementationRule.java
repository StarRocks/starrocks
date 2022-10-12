// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class AssertOneRowImplementationRule extends ImplementationRule {
    public AssertOneRowImplementationRule() {
        super(RuleType.IMP_ASSERT_ONE_ROW,
                Pattern.create(OperatorType.LOGICAL_ASSERT_ONE_ROW, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAssertOneRowOperator logical = (LogicalAssertOneRowOperator) input.getOp();

        PhysicalAssertOneRowOperator physical = new PhysicalAssertOneRowOperator(
                logical.getAssertion(), logical.getCheckRows(), logical.getTips(),
                logical.getLimit(), logical.getPredicate(), logical.getProjection());

        return Lists.newArrayList(OptExpression.create(physical, input.getInputs()));
    }
}
