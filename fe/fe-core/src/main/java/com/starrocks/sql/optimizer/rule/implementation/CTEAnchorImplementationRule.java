// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CTEAnchorImplementationRule extends ImplementationRule {
    public CTEAnchorImplementationRule() {
        super(RuleType.IMP_CTE_ANCHOR,
                Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        int cteId = ((LogicalCTEAnchorOperator) input.getOp()).getCteId();
        int consumeNum = context.getCteContext().getCTEConsumeNum(cteId);
        PhysicalCTEAnchorOperator anchor =
                new PhysicalCTEAnchorOperator(cteId, consumeNum, input.getOp().getProjection());
        return Lists.newArrayList(OptExpression.create(anchor, input.getInputs()));
    }
}
