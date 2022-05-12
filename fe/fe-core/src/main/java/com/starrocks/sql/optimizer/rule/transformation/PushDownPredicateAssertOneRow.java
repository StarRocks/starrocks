// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 * Push down the filter that can be passed through
 */
public class PushDownPredicateAssertOneRow extends TransformationRule {
    public PushDownPredicateAssertOneRow() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_ASSERT_ONE_ROW, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_ASSERT_ONE_ROW, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAssertOneRowOperator assertNode = (LogicalAssertOneRowOperator) input.getInputs().get(0).getOp();
        return !assertNode.hasPushDownFilter();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();
        OptExpression assertOpt = input.getInputs().get(0);

        // AssertOneRow will fill output to keep always one line, so can't direct push down predicate
        OptExpression firstFilter =
                OptExpression.create(new LogicalFilterOperator(lfo.getPredicate()), assertOpt.getInputs());

        LogicalAssertOneRowOperator newAssert = LogicalAssertOneRowOperator.createLessEqOne("");
        newAssert.setHasPushDownFilter(true);
        OptExpression newAssertOpt = OptExpression.create(newAssert, firstFilter);
        OptExpression secondFilter = OptExpression.create(new LogicalFilterOperator(lfo.getPredicate()), newAssertOpt);

        return Lists.newArrayList(secondFilter);
    }
}
