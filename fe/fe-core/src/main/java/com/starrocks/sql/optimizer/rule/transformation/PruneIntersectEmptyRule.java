// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

/*
case:
         UNION
      /    |     \       ->  Empty
   Empty  Child1  Child2
 */
public class PruneIntersectEmptyRule extends TransformationRule {
    public PruneIntersectEmptyRule() {
        super(RuleType.TF_PRUNE_INTERSECT_EMPTY,
                Pattern.create(OperatorType.LOGICAL_INTERSECT, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return input.getInputs().stream().map(OptExpression::getOp).filter(op -> op instanceof LogicalValuesOperator)
                .anyMatch(op -> ((LogicalValuesOperator) op).getRows().isEmpty());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalIntersectOperator intersectOperator = (LogicalIntersectOperator) input.getOp();
        return Lists.newArrayList(OptExpression
                .create(new LogicalValuesOperator(intersectOperator.getOutputColumnRefOp(), Collections.emptyList())));
    }
}
