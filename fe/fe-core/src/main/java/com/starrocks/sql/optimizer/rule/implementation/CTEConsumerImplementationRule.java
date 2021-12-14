// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CTEConsumerImplementationRule extends ImplementationRule {
    public CTEConsumerImplementationRule() {
        super(RuleType.IMP_CTE_CONSUMER, Pattern.create(OperatorType.LOGICAL_CTE_CONSUME));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalCTEConsumeOperator logical = (LogicalCTEConsumeOperator) input.getOp();
        PhysicalCTEConsumeOperator consume =
                new PhysicalCTEConsumeOperator(logical.getCteId(), logical.getCteOutputColumnRefMap(),
                        logical.getLimit(), logical.getPredicate(), logical.getProjection());
        return Lists.newArrayList(OptExpression.create(consume));
    }
}
