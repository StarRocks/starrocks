// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EliminateLimitZeroRule extends TransformationRule {
    public EliminateLimitZeroRule() {
        super(RuleType.TF_ELIMINATE_LIMIT_ZERO, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        return limit.getLimit() == 0;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefSet outputColumnIds =
                ((LogicalLimitOperator) input.getOp()).getOutputColumns(new ExpressionContext(input));

        List<ColumnRefOperator> outputColumns = outputColumnIds.getStream().mapToObj(
                id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());
        LogicalValuesOperator emptyOperator = new LogicalValuesOperator(
                outputColumns,
                Collections.emptyList());
        return Lists.newArrayList(OptExpression.create(emptyOperator));
    }
}
