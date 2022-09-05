// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneTopNColumnsRule extends TransformationRule {
    public PruneTopNColumnsRule() {
        super(RuleType.TF_PRUNE_TOPN_COLUMNS, Pattern.create(OperatorType.LOGICAL_TOPN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();
        ColumnRefSet requiredInputColumns = topNOperator.getRequiredChildInputColumns();

        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        // Change the requiredOutputColumns in context
        requiredOutputColumns.union(requiredInputColumns);

        return Collections.emptyList();
    }
}
