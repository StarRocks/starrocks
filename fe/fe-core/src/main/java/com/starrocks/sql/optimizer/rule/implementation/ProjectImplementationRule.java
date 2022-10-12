// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class ProjectImplementationRule extends ImplementationRule {
    public ProjectImplementationRule() {
        super(RuleType.IMP_PROJECT,
                Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getOp();
        PhysicalProjectOperator physicalProject = new PhysicalProjectOperator(
                projectOperator.getColumnRefMap(),
                Maps.newHashMap());
        return Lists.newArrayList(OptExpression.create(physicalProject, input.getInputs()));
    }
}
