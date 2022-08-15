// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PruneUnionColumnsRule extends TransformationRule {
    public PruneUnionColumnsRule() {
        super(RuleType.TF_PRUNE_UNION_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        LogicalSetOperator lso = (LogicalSetOperator) input.getOp();
        List<ColumnRefOperator> outputs = lso.getOutputColumnRefOp();

        List<List<ColumnRefOperator>> childOutputsAfterPruned = new ArrayList<>();
        for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
            childOutputsAfterPruned.add(new ArrayList<>());
        }

        boolean needOutput = false;
        for (int idx = 0; idx < outputs.size(); ++idx) {
            if (requiredOutputColumns.contains(outputs.get(idx))) {
                needOutput = true;
                for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
                    ColumnRefOperator columnRefOperator = lso.getChildOutputColumns().get(childIdx).get(idx);

                    requiredOutputColumns.union(columnRefOperator);
                    childOutputsAfterPruned.get(childIdx).add(columnRefOperator);
                }
            }
        }

        // must output least 1
        if (!needOutput) {
            for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
                ColumnRefOperator columnRefOperator = lso.getChildOutputColumns().get(childIdx).get(0);

                requiredOutputColumns.union(columnRefOperator);
                childOutputsAfterPruned.get(childIdx).add(columnRefOperator);
            }

            ColumnRefOperator first = outputs.get(0);
            outputs.clear();
            outputs.add(first);
        } else {
            outputs.removeIf(d -> !requiredOutputColumns.contains(d));
        }

        /*
         * Because the output of the union may be prune. So we prune output columns of the child
         *
         * The column_id cannot be used for deletion, must use offset of the child.
         * Because the output columns of the child may be the same because of expression reuse
         */
        for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
            lso.getChildOutputColumns().set(childIdx, childOutputsAfterPruned.get(childIdx));
        }

        return Collections.emptyList();
    }
}