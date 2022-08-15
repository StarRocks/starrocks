// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class LimitImplementationRule extends ImplementationRule {
    public LimitImplementationRule() {
        super(RuleType.IMP_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        Preconditions.checkState(limit.isGlobal());
        return Lists.newArrayList(OptExpression
                .create(new PhysicalLimitOperator(limit.getOffset(), limit.getLimit(), limit.getProjection()),
                        input.getInputs()));
    }
}
