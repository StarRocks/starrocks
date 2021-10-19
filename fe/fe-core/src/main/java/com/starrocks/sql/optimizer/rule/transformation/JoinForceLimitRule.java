// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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

// For case:
//          Join(limit 1)
//          /           \
//   left(limit 2)   right(limit 3)
//
// In this case, data must be gather to a HashJoinNode, but the way may inefficient for distributed system,
// such as left node is large table and right node contains limit for right outer join, so we choose
// first gather and then re-shuffle, but the ChildPropertyDeriver can't implemented the way, so we
// use PhysicalLimitOperator to deal with it.
public class JoinForceLimitRule extends TransformationRule {
    public JoinForceLimitRule() {
        super(RuleType.TF_JOIN_FORCE_LIMIT, Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator join = input.getOp();
        Operator left = input.getInputs().get(0).getOp();
        Operator right = input.getInputs().get(1).getOp();

        long nodeLimit = join.hasLimit() ? join.getLimit() : Long.MAX_VALUE;
        long leftLimit = left.hasLimit() ? left.getLimit() : Long.MAX_VALUE;
        long rightLimit = right.hasLimit() ? right.getLimit() : Long.MAX_VALUE;

        return (nodeLimit > leftLimit && left.getOpType() != OperatorType.LOGICAL_VALUES) ||
                (nodeLimit > rightLimit && right.getOpType() != OperatorType.LOGICAL_VALUES);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Operator join = input.getOp();
        Operator left = input.getInputs().get(0).getOp();
        Operator right = input.getInputs().get(1).getOp();

        long nodeLimit = join.hasLimit() ? join.getLimit() : Long.MAX_VALUE;
        long leftLimit = left.hasLimit() ? left.getLimit() : Long.MAX_VALUE;
        long rightLimit = right.hasLimit() ? right.getLimit() : Long.MAX_VALUE;

        OptExpression newLeft = input.getInputs().get(0);
        OptExpression newRight = input.getInputs().get(1);

        if (nodeLimit > leftLimit && left.getOpType() != OperatorType.LOGICAL_VALUES) {
            newLeft = OptExpression.create(new LogicalLimitOperator(left.getLimit()), newLeft);
        }

        if (nodeLimit > rightLimit && right.getOpType() != OperatorType.LOGICAL_VALUES) {
            newRight = OptExpression.create(new LogicalLimitOperator(right.getLimit()), newRight);
        }

        return Lists.newArrayList(OptExpression.create(input.getOp(), newLeft, newRight));
    }
}
