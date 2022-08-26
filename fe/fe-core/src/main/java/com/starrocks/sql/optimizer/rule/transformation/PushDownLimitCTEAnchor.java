// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownLimitCTEAnchor extends TransformationRule {
    public PushDownLimitCTEAnchor() {
        super(RuleType.TF_PUSH_DOWN_LIMIT_CTE_ANCHOR, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        OptExpression anchor = input.inputAt(0);

        // push down to right child
        OptExpression nl = new OptExpression(new LogicalLimitOperator(limit.getLimit(), limit.getOffset(), limit.getPhase()));
        nl.getInputs().add(anchor.getInputs().get(1));
        anchor.getInputs().set(1, nl);

        return Lists.newArrayList(anchor);
    }
}
