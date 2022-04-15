// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownAssertOneRowProjectRule extends TransformationRule {
    public PushDownAssertOneRowProjectRule() {
        super(RuleType.TF_PUSH_DOWN_ASSERT_ONE_ROW_PROJECT, Pattern.create(OperatorType.LOGICAL_ASSERT_ONE_ROW)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_AGGR)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression newChild = new OptExpression(input.getInputs().get(0).getOp());
        OptExpression newAssert = new OptExpression(input.getOp());

        newChild.getInputs().add(newAssert);
        newAssert.getInputs().addAll(input.getInputs().get(0).getInputs());
        return Lists.newArrayList(newChild);
    }
}
