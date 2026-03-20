// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PruneUnionColumnsRule extends TransformationRule {
    public PruneUnionColumnsRule() {
        super(RuleType.TF_PRUNE_UNION_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        LogicalUnionOperator luo = new LogicalUnionOperator.Builder()
                .withOperator((LogicalUnionOperator) input.getOp())
                .setOutputColumnRefOp(Lists.newArrayList(((LogicalUnionOperator) input.getOp()).getOutputColumnRefOp()))
                .setChildOutputColumns(((LogicalUnionOperator) input.getOp()).getChildOutputColumns().stream()
                        .map(Lists::newArrayList)
                        .collect(Collectors.toList()))
                .build();

        // If the Union has an embedded projection, propagate required columns from
        // projection expressions into the base output required set, and prune the
        // projection entries that are no longer needed.
        pruneProjectionAndPropagateRequired(luo, requiredOutputColumns);

        List<ColumnRefOperator> outputs = luo.getOutputColumnRefOp();

        List<List<ColumnRefOperator>> childOutputsAfterPruned = new ArrayList<>();
        for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
            childOutputsAfterPruned.add(new ArrayList<>());
        }

        boolean needOutput = false;
        for (int idx = 0; idx < outputs.size(); ++idx) {
            if (requiredOutputColumns.contains(outputs.get(idx))) {
                needOutput = true;
                for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
                    ColumnRefOperator columnRefOperator = luo.getChildOutputColumns().get(childIdx).get(idx);

                    requiredOutputColumns.union(columnRefOperator);
                    childOutputsAfterPruned.get(childIdx).add(columnRefOperator);
                }
            }
        }

        // must output least 1
        if (!needOutput) {
            for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
                ColumnRefOperator columnRefOperator = luo.getChildOutputColumns().get(childIdx).get(0);

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
            luo.getChildOutputColumns().set(childIdx, childOutputsAfterPruned.get(childIdx));
        }

        return Collections.singletonList(OptExpression.create(luo, input.getInputs()));
    }

    /**
     * When a Union has an embedded projection (e.g. from MV rewrite + MergeProjectWithChildRule),
     * the projection's expressions reference the Union's base output columns. If we prune those
     * base columns without considering the projection, a later SeparateProjectRule will create a
     * Project that references columns no longer in the Union's output, causing an "Invalid plan" error.
     *
     * This method:
     * 1. Keeps projection entries whose output column is required
     * 2. Adds the base columns used by kept entries to requiredOutputColumns
     * 3. Removes projection entries that are no longer needed
     */
    void pruneProjectionAndPropagateRequired(LogicalUnionOperator luo,
                                                      ColumnRefSet requiredOutputColumns) {
        Projection projection = luo.getProjection();
        if (projection == null) {
            return;
        }

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = new HashMap<>();
        ColumnRefSet baseColumnsNeededByProjection = new ColumnRefSet();

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            if (requiredOutputColumns.contains(entry.getKey())) {
                newColumnRefMap.put(entry.getKey(), entry.getValue());
                baseColumnsNeededByProjection.union(entry.getValue().getUsedColumns());
            }
        }

        requiredOutputColumns.union(baseColumnsNeededByProjection);

        if (newColumnRefMap.isEmpty()) {
            luo.setProjection(null);
        } else if (newColumnRefMap.size() != columnRefMap.size()) {
            luo.setProjection(new Projection(newColumnRefMap));
        }
    }
}
