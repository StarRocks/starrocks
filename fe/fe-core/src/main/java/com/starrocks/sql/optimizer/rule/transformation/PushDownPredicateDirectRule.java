// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * Push down the filter that can be passed through
 */
public class PushDownPredicateDirectRule extends TransformationRule {
    public PushDownPredicateDirectRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_DIRECT, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OperatorType type = input.getInputs().get(0).getOp().getOpType();
        return OperatorType.LOGICAL_TOPN.equals(type)
                || OperatorType.LOGICAL_ASSERT_ONE_ROW.equals(type);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.getInputs().get(0);

        OptExpression newFilter = new OptExpression(new LogicalFilterOperator(lfo.getPredicate()));

        newFilter.getInputs().addAll(child.getInputs());
        child.getInputs().clear();
        child.getInputs().add(newFilter);

        return Lists.newArrayList(child);
    }
}
