// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownLimitDirectRule extends TransformationRule {
    private static final List<OperatorType> ALLOW_PUSH_DOWN_LIMIT_LIST = ImmutableList.<OperatorType>builder()
            .add(OperatorType.LOGICAL_PROJECT)
            .add(OperatorType.LOGICAL_ASSERT_ONE_ROW)
            .add(OperatorType.LOGICAL_CTE_CONSUME)
            .build();

    public PushDownLimitDirectRule() {
        super(RuleType.TF_PUSH_DOWN_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        OperatorType type = input.getInputs().get(0).getOp().getOpType();

        // 1. Has offset can't push down
        return !limit.hasOffset() && ALLOW_PUSH_DOWN_LIMIT_LIST.contains(type);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        OptExpression child = input.inputAt(0);
        LogicalOperator logicOperator = (LogicalOperator) child.getOp();

        // set limit
        logicOperator.setLimit(limit.getLimit());

        // push down
        OptExpression nl = new OptExpression(new LogicalLimitOperator(limit.getLimit()));
        nl.getInputs().addAll(child.getInputs());
        child.getInputs().clear();
        child.getInputs().add(nl);

        return Lists.newArrayList(child);
    }
}
