// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CTEProduceImplementationRule extends ImplementationRule {
    public CTEProduceImplementationRule() {
        super(RuleType.IMP_CTE_PRODUCE,
                Pattern.create(OperatorType.LOGICAL_CTE_PRODUCE, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        PhysicalCTEProduceOperator produce =
                new PhysicalCTEProduceOperator(((LogicalCTEProduceOperator) input.getOp()).getCteId());
        return Lists.newArrayList(OptExpression.create(produce, input.getInputs()));
    }
}
