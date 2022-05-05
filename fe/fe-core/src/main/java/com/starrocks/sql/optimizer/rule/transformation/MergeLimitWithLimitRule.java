// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 * Merge multiple limit, can be support:
 *
 *      Limit (less rows limit)
 *        |                       ===>  Limit(less rows limit)
 *      Limit (more rows limit)
 *
 * can't merge like:
 *
 *      Limit (more rows limit)
 *        |
 *      Limit (less rows limit)
 *
 * */
public class MergeLimitWithLimitRule extends TransformationRule {
    public MergeLimitWithLimitRule() {
        super(RuleType.TF_MERGE_LIMIT_WITH_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_LIMIT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // Any limit operator can be merged with global limit, no scene where the child is local limit
        LogicalLimitOperator childLimit = (LogicalLimitOperator) input.getInputs().get(0).getOp();
        return childLimit.isGlobal();
    }

    // eg.1, child limit is smaller than parent, child must gather
    // before:
    //   Limit 1, 5 (hit line range: [1, 6), output line range: [6, 7))
    //      |
    // Global-Limit 5, 2 (hit line range: [5, 7), output line range: [5, 7))
    //
    // after:
    // Init-Limit 6, 2 (hit line range: [6, 7), output line range: [6, 7))
    //
    // eg.2, child limit is larger than parent, child don't gather
    // before:
    //   Limit 1, 2 (hit line range: [1, 3), output line range: [6, 8))
    //      |
    // Global-Limit 5, 5 (hit line range: [5, 10), output line range: [5, 10))
    //
    // after:
    // Local-Limit 6, 2 (hit line range: [6, 8), output line range: [6, 8))
    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator l1 = (LogicalLimitOperator) input.getOp();
        LogicalLimitOperator l2 = (LogicalLimitOperator) input.getInputs().get(0).getOp();

        // l2 range
        long l2Min = l2.hasOffset() ? l2.getOffset() : Operator.DEFAULT_OFFSET;
        long l2Max = l2Min + l2.getLimit();

        // l1 range
        long l1Min = l1.hasOffset() ? l2Min + l1.getOffset() : l2Min;
        long l1Max = l1Min + l1.getLimit();

        long offset = Math.max(l2Min, l1Min);
        long limit = Math.min(l2Max, l1Max) - offset;

        if (limit <= 0) {
            limit = 0;
            offset = Operator.DEFAULT_OFFSET;
        }

        if (offset <= 0) {
            offset = Operator.DEFAULT_OFFSET;
        }

        Operator result;
        if (l1.getLimit() <= l2.getLimit()) {
            result = new LogicalLimitOperator(limit, offset, LogicalLimitOperator.Phase.LOCAL);
        } else {
            result = new LogicalLimitOperator(limit, offset, LogicalLimitOperator.Phase.INIT);
        }

        return Lists.newArrayList(OptExpression.create(result, input.getInputs().get(0).getInputs()));
    }
}
