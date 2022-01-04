// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

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
        ColumnRefSet outputColumns = (ColumnRefSet) joinOperator.getOutputColumns(new ExpressionContext(input)).clone();
        outputColumns.intersect(requiredColumns);
        if (outputColumns.isEmpty()) {
            Preconditions.checkState(false);
            /*
            ColumnRefSet columnRefSet = new ColumnRefSet();
            columnRefSet.union(input.inputAt(0).getOutputColumns());
            columnRefSet.union(input.inputAt(1).getOutputColumns());

            ColumnRefOperator columnRefOperator = Utils.findSmallestColumnRef(
                    joinOperator.getRequiredChildInputColumns()
                            .getStream().mapToObj(c ->
                                    context.getColumnRefFactory().getColumnRef(c)).collect(Collectors.toList()));
            outputColumns.union(columnRefOperator);
             */
        }

        if (!outputColumns.equals(joinOperator.getOutputColumns())) {
            LogicalJoinOperator newJoin = new LogicalJoinOperator.Builder()
                    .withOperator(joinOperator)
                    .setOutputColumns(outputColumns)
                    .build();
            requiredColumns.union(newJoin.getRequiredChildInputColumns());
            return Lists.newArrayList(OptExpression.create(newJoin, input.getInputs()));
        } else {
            requiredColumns.union(joinOperator.getRequiredChildInputColumns());
            return Collections.emptyList();
        }
    }
}
