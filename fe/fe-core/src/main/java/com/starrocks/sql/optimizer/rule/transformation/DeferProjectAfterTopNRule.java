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
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeferProjectAfterTopNRule extends TransformationRule {
    public DeferProjectAfterTopNRule() {
        super(RuleType.TF_DEFER_PROJECT_AFTER_TOPN,
                Pattern.create(OperatorType.LOGICAL_TOPN).addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (context.getSessionVariable().isEnableDeferProjectAfterTopN()) {
            LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();
            if (topNOperator.getPartitionPreAggCall() != null && !topNOperator.getPartitionPreAggCall().isEmpty()) {
                // ignore window function
                return false;
            }
            if (!topNOperator.hasLimit()) {
                return false;
            }
            return true;
        }
        return false;
    }

    private boolean mayBenefitFromPruningSubField(OptimizerContext context,
                                                  Set<String> columnAccessPaths, ScalarOperator scalarOperator) {
        return scalarOperator.getUsedColumns().getColumnRefOperators(context.getColumnRefFactory())
                .stream().anyMatch(columnRefOperator -> columnAccessPaths.contains(columnRefOperator.getName()));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getInputs().get(0).getOp();
        Map<ColumnRefOperator, ScalarOperator> projectMap = projectOperator.getColumnRefMap();

        ColumnRefSet topNRequiredInputColumns = topNOperator.getRequiredChildInputColumns();

        Set<String> columnsWithAccessPath = new HashSet<>();

        if (input.getInputs().get(0).getInputs().get(0).getOp() instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator =
                    (LogicalOlapScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
            List<ColumnAccessPath> columnAccessPaths = olapScanOperator.getColumnAccessPaths();
            if (columnAccessPaths != null) {
                columnAccessPaths.forEach(columnAccessPath -> {
                    columnsWithAccessPath.add(columnAccessPath.getPath());
                });
            }
        }

        boolean canDeferProject = projectMap.entrySet().stream().anyMatch(entry -> {
            if (!topNRequiredInputColumns.contains(entry.getKey())) {
                if (entry.getValue().isColumnRef()) {
                    return false;
                }
                // If some columns of the expression appear in ColumnAccessPath,
                // it may benefit from pruning subfield, in which case we should keep it.
                return !mayBenefitFromPruningSubField(context, columnsWithAccessPath, entry.getValue());
            }
            return false;
        });

        if (!canDeferProject) {
            return Collections.emptyList();
        }

        Map<ColumnRefOperator, ScalarOperator> preProjectionMap = new HashMap<>();

        Map<ColumnRefOperator, ScalarOperator> postProjectionMap = new HashMap<>(projectOperator.getColumnRefMap());

        projectOperator.getColumnRefMap().forEach((columnRefOperator, scalarOperator) -> {
            if (topNRequiredInputColumns.contains(columnRefOperator) ||
                    mayBenefitFromPruningSubField(context, columnsWithAccessPath, scalarOperator)) {
                preProjectionMap.put(columnRefOperator, scalarOperator);
                // In theory, expressions calculated in pre-project do not need to be recalculated in post-project.
                // Here we only keep the column ref operator in postProjectionMap.
                // @TODO:
                // Since our common expression reuse cannot cross operators,
                // if the sort key itself is an expression and other expressions depend on the sort key,
                // we may lose the opportunity to reuse the common expression in post project operator.
                // e.g. `select v1, hex(v2), length(hex(v2)) from t0 order by hex(v2) limit 10`
                // you can find more details about its plan in DeferProjectAfterTopNTest.
                postProjectionMap.put(columnRefOperator, columnRefOperator);
                return;
            }

            scalarOperator.getUsedColumns().getColumnRefOperators(context.getColumnRefFactory()).forEach(k -> {
                preProjectionMap.put(k, k);
            });
        });

        LogicalProjectOperator preProjectOperator = new LogicalProjectOperator(preProjectionMap);
        LogicalProjectOperator postProjectOperaotr = new LogicalProjectOperator(postProjectionMap);

        OptExpression result = OptExpression.create(
                postProjectOperaotr, OptExpression.create(
                        topNOperator, OptExpression.create(preProjectOperator, input.getInputs().get(0).getInputs())));
        return Lists.newArrayList(result);
    }
}