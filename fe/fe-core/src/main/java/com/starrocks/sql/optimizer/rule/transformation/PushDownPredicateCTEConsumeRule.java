// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 *          Filter          CTEConsume(Predicate)
 *            |                 |
 *        CTEConsume   =>     Filter
 *            |                 |
 *           Node              Node
 *
 * */
public class PushDownPredicateCTEConsumeRule extends TransformationRule {
    public PushDownPredicateCTEConsumeRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_CTE_CONSUME, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_CTE_CONSUME, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {

        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) input.getInputs().get(0).getOp();

        LogicalCTEConsumeOperator newConsume = new LogicalCTEConsumeOperator.Builder()
                .withOperator(consume).setPredicate(filter.getPredicate()).build();

        OptExpression output = OptExpression.create(newConsume,
                OptExpression.create(filter, input.getInputs().get(0).getInputs()));

        return Lists.newArrayList(output);
    }
}
