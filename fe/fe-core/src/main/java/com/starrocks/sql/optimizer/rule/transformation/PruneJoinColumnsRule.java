// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
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

        ColumnRefSet requiredColumns = context.getTaskContext().getRequiredColumns();
        requiredColumns.union(joinOperator.getRequiredChildInputColumns());

        // Retain output columns for SEMI/ANTI join
        if (joinOperator.getJoinType().isLeftSemiAntiJoin()) {
            ColumnRefSet leftOutput = input.getChildOutputColumns(0);
            if (!requiredColumns.containsAny(leftOutput)) {
                ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(
                        leftOutput.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                                .collect(Collectors.toList()));
                requiredColumns.union(smallestColumn);
            }
        } else if (joinOperator.getJoinType().isRightSemiAntiJoin()) {
            ColumnRefSet rightOutput = input.getChildOutputColumns(1);
            if (!requiredColumns.containsAny(rightOutput)) {
                ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(
                        rightOutput.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                                .collect(Collectors.toList()));
                requiredColumns.union(smallestColumn);
            }
        }

        return Collections.emptyList();
    }
}
