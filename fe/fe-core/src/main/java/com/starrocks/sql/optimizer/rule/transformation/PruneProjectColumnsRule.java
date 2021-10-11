// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class PruneProjectColumnsRule extends TransformationRule {

    public PruneProjectColumnsRule() {
        super(RuleType.TF_PRUNE_PROJECT_COLUMNS, Pattern.create(OperatorType.LOGICAL_PROJECT).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF).
                        addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getOp();

        ColumnRefSet requiredInputColumns = new ColumnRefSet();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().get(0).getRequiredColumns();

        Map<ColumnRefOperator, ScalarOperator> newMap = Maps.newHashMap();
        projectOperator.getColumnRefMap().forEach(((columnRefOperator, operator) -> {
            if (requiredOutputColumns.contains(columnRefOperator)) {
                requiredInputColumns.union(operator.getUsedColumns());
                newMap.put(columnRefOperator, operator);
            }
        }));

        // Change the requiredOutputColumns in context
        requiredOutputColumns.union(requiredInputColumns);

        return Lists.newArrayList(OptExpression.create(new LogicalProjectOperator(newMap), input.getInputs()));
    }
}
