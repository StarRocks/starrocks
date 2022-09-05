// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class RepeatImplementationRule extends ImplementationRule {
    public RepeatImplementationRule() {
        super(RuleType.IMP_REPEAT, Pattern.create(OperatorType.LOGICAL_REPEAT)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalRepeatOperator repeatOperator = (LogicalRepeatOperator) input.getOp();
        PhysicalRepeatOperator physicalRepeat = new PhysicalRepeatOperator(repeatOperator.getOutputGrouping(),
                repeatOperator.getRepeatColumnRef(), repeatOperator.getGroupingIds(), repeatOperator.getLimit(),
                repeatOperator.getPredicate(), repeatOperator.getProjection());
        return Lists.newArrayList(OptExpression.create(physicalRepeat, input.getInputs()));
    }
}
