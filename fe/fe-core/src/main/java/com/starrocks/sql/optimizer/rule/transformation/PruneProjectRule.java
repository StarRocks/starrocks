// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PruneProjectRule extends TransformationRule {
    public PruneProjectRule() {
        super(RuleType.TF_PRUNE_PROJECT, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> projections = ((LogicalProjectOperator) input.getOp()).getColumnRefMap();

        // avoid prune expression
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projections.entrySet()) {
            if (!entry.getKey().equals(entry.getValue())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefSet output = ((LogicalProjectOperator) input.getOp()).getOutputColumns(null);
        ColumnRefSet childOutput = input.getInputs().get(0).getOutputColumns();

        if (output.isSame(childOutput)) {
            return input.getInputs();
        }

        if (output.isEmpty()) {
            return input.getInputs();
        }

        return Collections.emptyList();
    }

}
