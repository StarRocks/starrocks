// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PruneCTEConsumePlanRule extends TransformationRule {
    public PruneCTEConsumePlanRule() {
        super(RuleType.TF_PRUNE_CTE_CONSUME_PLAN, Pattern.create(OperatorType.LOGICAL_CTE_CONSUME)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) input.getOp();
        return !context.getCteContext().needInline(consume.getCteId());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // delete cte consume child plan
        return Lists.newArrayList(OptExpression.create(input.getOp()));
    }
}
