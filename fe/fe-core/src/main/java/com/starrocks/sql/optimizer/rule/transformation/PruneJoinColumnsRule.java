// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Function;
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

        ColumnRefSet requiredColumns = context.getTaskContext().get(0).getRequiredColumns();
        List<ColumnRefOperator> newOutputs = joinOperator.getOutputColumns(new ExpressionContext(input)).getStream()
                .filter(requiredColumns::contains)
                .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id))
                .collect(Collectors.toList());

        if (newOutputs.isEmpty()) {
            newOutputs.add(Utils.findSmallestColumnRef(input.inputAt(0).getOutputColumns().getStream().
                    mapToObj(context.getColumnRefFactory()::getColumnRef).collect(Collectors.toList())));
        }

        LogicalJoinOperator newJoinOperator = new LogicalJoinOperator.Builder().withOperator(joinOperator)
                .setProjection(new Projection(newOutputs.stream()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()))))
                .build();

        // Change the requiredColumns in context
        requiredColumns.union(newJoinOperator.getRequiredChildInputColumns());

        return Lists.newArrayList(OptExpression.create(newJoinOperator, input.getInputs()));
    }
}
