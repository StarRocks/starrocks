// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class IntersectImplementationRule extends ImplementationRule {
    public IntersectImplementationRule() {
        super(RuleType.IMP_INTERSECT,
                Pattern.create(OperatorType.LOGICAL_INTERSECT)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalSetOperator setOperator = (LogicalSetOperator) input.getOp();
        PhysicalIntersectOperator physicalIntersect = new PhysicalIntersectOperator(setOperator.getOutputColumnRefOp(),
                setOperator.getChildOutputColumns(), setOperator.getLimit(), setOperator.getPredicate(),
                setOperator.getProjection());
        return Lists.newArrayList(OptExpression.create(physicalIntersect, input.getInputs()));
    }
}
