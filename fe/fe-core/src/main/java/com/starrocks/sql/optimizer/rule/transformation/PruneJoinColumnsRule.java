// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PruneJoinColumnsRule extends TransformationRule {
    public PruneJoinColumnsRule() {
        super(RuleType.TF_PRUNE_JOIN_COLUMNS, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        ColumnRefSet requiredInputColumns = joinOperator.getRequiredChildInputColumns();
        ColumnRefSet requiredColumns = context.getTaskContext().get(0).getRequiredColumns();

        List<ColumnRefOperator> newOutputs = joinOperator.getOutputColumns(new ExpressionContext(input)).getStream()
                .filter(requiredColumns::contains)
                .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id))
                .collect(Collectors.toList());

        // Change the requiredColumns in context
        requiredColumns.union(requiredInputColumns);

        if (joinOperator.getPruneOutputColumns() != null) {
            return Collections.emptyList();
        }

        LogicalJoinOperator newJoinOperator = new LogicalJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                newOutputs,
                joinOperator.isHasPushDownJoinOnClause());

        return Lists.newArrayList(OptExpression.create(newJoinOperator, input.getInputs()));
    }
}
