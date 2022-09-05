// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CTEAnchorToNoCTEImplementationRule extends ImplementationRule {
    public CTEAnchorToNoCTEImplementationRule() {
        super(RuleType.IMP_CTE_ANCHOR_TO_NO_CTE,
                Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalCTEAnchorOperator anchor = (LogicalCTEAnchorOperator) input.getOp();
        return !context.getCteContext().isForceCTE(anchor.getCteId());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return Lists.newArrayList(OptExpression
                .create(new PhysicalNoCTEOperator(((LogicalCTEAnchorOperator) input.getOp()).getCteId()),
                        input.getInputs().get(1)));
    }
}
