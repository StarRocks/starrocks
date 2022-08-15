// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PruneTableFunctionColumnRule extends TransformationRule {
    public PruneTableFunctionColumnRule() {
        super(RuleType.TF_PRUNE_TABLE_FUNCTION_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_TABLE_FUNCTION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTableFunctionOperator logicalTableFunctionOperator = (LogicalTableFunctionOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        ColumnRefSet newOuterColumnRefSet = new ColumnRefSet();
        for (int columnId : logicalTableFunctionOperator.getOuterColumnRefSet().getColumnIds()) {
            if (requiredOutputColumns.contains(columnId)) {
                newOuterColumnRefSet.union(columnId);
            }
        }

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                logicalTableFunctionOperator.getFnParamColumnProjectMap().entrySet()) {
            requiredOutputColumns.union(entry.getKey());
        }

        LogicalTableFunctionOperator newOperator = new LogicalTableFunctionOperator(
                logicalTableFunctionOperator.getFnResultColumnRefSet(),
                logicalTableFunctionOperator.getFn(),
                logicalTableFunctionOperator.getFnParamColumnProjectMap(),
                newOuterColumnRefSet);
        newOperator.setLimit(logicalTableFunctionOperator.getLimit());

        if (logicalTableFunctionOperator.equals(newOperator)) {
            return Collections.emptyList();
        }

        return Lists.newArrayList(OptExpression.create(newOperator, input.getInputs()));
    }
}