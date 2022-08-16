// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneProjectEmptyRule extends TransformationRule {

    public PruneProjectEmptyRule() {
        super(RuleType.TF_PRUNE_PROJECT_EMPTY,
                Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_VALUES));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return ((LogicalValuesOperator) input.getInputs().get(0).getOp()).getRows().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<ColumnRefOperator> outputs =
                Lists.newArrayList(((LogicalProjectOperator) input.getOp()).getColumnRefMap().keySet());
        return Lists.newArrayList(OptExpression.create(new LogicalValuesOperator(outputs, Collections.emptyList())));
    }
}
