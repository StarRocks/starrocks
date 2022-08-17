// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownPredicateCTEAnchor extends TransformationRule {
    public PushDownPredicateCTEAnchor() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_CTE_ANCHOR, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.getInputs().get(0);

        OptExpression newFilter = new OptExpression(new LogicalFilterOperator(lfo.getPredicate()));
        newFilter.getInputs().add(child.getInputs().get(1));
        child.getInputs().set(1, newFilter);

        return Lists.newArrayList(child);
    }
}
