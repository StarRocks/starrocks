// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoOpOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CTEAnchorToNoOpImplementationRule extends ImplementationRule {
    public CTEAnchorToNoOpImplementationRule() {
        super(RuleType.IMP_CTE_ANCHOR_TO_NO_OP,
                Pattern.create(OperatorType.PHYSICAL_CTE_ANCHOR, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return Lists.newArrayList(OptExpression.create(new PhysicalNoOpOperator(), input.getInputs().get(1)));
    }
}
