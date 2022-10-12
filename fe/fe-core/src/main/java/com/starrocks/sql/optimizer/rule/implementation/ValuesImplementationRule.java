// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class ValuesImplementationRule extends ImplementationRule {
    public ValuesImplementationRule() {
        super(RuleType.IMP_VALUES, Pattern.create(OperatorType.LOGICAL_VALUES));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalValuesOperator valuesOperator = (LogicalValuesOperator) input.getOp();
        PhysicalValuesOperator physicalValues = new PhysicalValuesOperator(
                valuesOperator.getColumnRefSet(),
                valuesOperator.getRows(),
                valuesOperator.getLimit(),
                valuesOperator.getPredicate(),
                valuesOperator.getProjection());
        return Lists.newArrayList(OptExpression.create(physicalValues, Collections.emptyList()));
    }
}
