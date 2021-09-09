// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MergeLimitWithLimitRule extends TransformationRule {
    public MergeLimitWithLimitRule() {
        super(RuleType.TF_MERGE_LIMIT_WITH_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_LIMIT, OperatorType.PATTERN_LEAF)));
    }

    // eg.
    // before:
    // Limit 1, 5 (hit line range: 1 ~ 6, output line range: 6 ~ 11)
    //     |
    // Limit 5, 2 (hit line range: 5 ~ 7, output line range: 5 ~ 7)
    //
    // after:
    // Limit 6, 1 (hit line range: 6 ~ 7, output line range: 6 ~ 7)
    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator l1 = (LogicalLimitOperator) input.getOp();
        LogicalLimitOperator l2 = (LogicalLimitOperator) input.getInputs().get(0).getOp();

        // l2 range
        long l2Min = l2.hasOffset() ? l2.getOffset() : 0;
        long l2Max = l2Min + l2.getLimit();

        // l1 range
        long l1Min = l1.hasOffset() ? l2Min + l1.getOffset() : l2Min;
        long l1Max = l1Min + l1.getLimit();

        long offset = Math.max(l2Min, l1Min);
        long limit = Math.min(l2Max, l1Max) - offset;

        if (limit <= 0) {
            limit = 0;
            offset = -1;
        }

        if (offset <= 0) {
            offset = -1;
        }

        return Lists.newArrayList(OptExpression.create(new LogicalLimitOperator(limit, offset),
                input.getInputs().get(0).getInputs()));
    }
}
