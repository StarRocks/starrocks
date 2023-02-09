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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/*
 * For ranking window functions, such as row_number, rank, dense_rank, if there exists rank related order by clause
 * and limit clause, then we can add a TopN to filter data in order to reduce the amount of data to be exchanged and sorted
 * E.g.
 *      select * from (
 *          select *, rank() over (order by v2) as rk from t0
 *      ) sub_t0
 *      order by rk
 *      limit 5;
 *
 * Before:
 *       TopN
 *         |
 *       Project
 *         |
 *       Window
 *
 * After:
 *       TopN
 *         |
 *       Project
 *         |
 *       Window
 *         |
 *       TopN
 */
public class PushDownLimitRankingWindowRule extends TransformationRule {
    public PushDownLimitRankingWindowRule() {
        super(RuleType.TF_PUSH_DOWN_LIMIT_RANKING_WINDOW,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)
                                .addChildren(Pattern.create(OperatorType.LOGICAL_WINDOW, OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // This rule introduce a new version of TopNOperator, i.e. PartitionTopNOperator
        // which only supported in pipeline engine, so we cannot apply this rule in non-pipeline engine
        if (!context.getSessionVariable().isEnablePipelineEngine()) {
            return false;
        }

        OptExpression grandChildExpr = input.inputAt(0).inputAt(0);
        LogicalWindowOperator windowOperator = grandChildExpr.getOp().cast();

        if (windowOperator.getWindowCall().size() != 1) {
            return false;
        }

        ColumnRefOperator windowCol = Lists.newArrayList(windowOperator.getWindowCall().keySet()).get(0);
        CallOperator callOperator = windowOperator.getWindowCall().get(windowCol);

        // TODO(hcf) we support dense_rank later
        if (!FunctionSet.ROW_NUMBER.equals(callOperator.getFnName()) &&
                !FunctionSet.RANK.equals(callOperator.getFnName())) {
            return false;
        }

        if (!grandChildExpr.getInputs().isEmpty() &&
                grandChildExpr.inputAt(0).getOp() instanceof LogicalWindowOperator) {
            OptExpression grandGrandChildExpr = grandChildExpr.inputAt(0);
            LogicalWindowOperator nextWindowOperator = grandGrandChildExpr.getOp().cast();
            // There might be a negative gain if we add a partitionTopN between two window operators that
            // share the same sort group
            return !Objects.equals(windowOperator.getEnforceSortColumns(),
                    nextWindowOperator.getEnforceSortColumns());
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOperator = input.getOp().cast();
        if (!topNOperator.hasLimit()) {
            return Collections.emptyList();
        }

        OptExpression childExpr = input.inputAt(0);
        LogicalProjectOperator projectOperator = childExpr.getOp().cast();

        OptExpression grandChildExpr = childExpr.inputAt(0);
        LogicalWindowOperator windowOperator = grandChildExpr.getOp().cast();

        ColumnRefOperator windowCol = Lists.newArrayList(windowOperator.getWindowCall().keySet()).get(0);
        CallOperator callOperator = windowOperator.getWindowCall().get(windowCol);

        List<ColumnRefOperator> partitionByColumns = windowOperator.getPartitionExpressions().stream()
                .map(ScalarOperator::<ColumnRefOperator>cast)
                .collect(Collectors.toList());

        Ordering firstOrdering = topNOperator.getOrderByElements().get(0);
        if (!firstOrdering.isAscending()) {
            return Collections.emptyList();
        }
        // The output column of the window function must be the first order by columns
        if (!firstOrdering.getColumnRef().equals(windowCol)) {
            return Collections.emptyList();
        }

        long limitValue = topNOperator.getOffset() + topNOperator.getLimit();
        TopNType topNType = TopNType.parse(callOperator.getFnName());

        // If partition by columns is not empty, then we cannot derive sort property from the SortNode
        // OutputPropertyDeriver will generate PhysicalPropertySet.EMPTY if sortPhase is SortPhase.PARTIAL
        final SortPhase sortPhase = partitionByColumns.isEmpty() ? SortPhase.FINAL : SortPhase.PARTIAL;
        final long limit = partitionByColumns.isEmpty() ? limitValue : Operator.DEFAULT_LIMIT;
        final long partitionLimit = partitionByColumns.isEmpty() ? Operator.DEFAULT_LIMIT : limitValue;
        OptExpression newTopNOptExp = OptExpression.create(new LogicalTopNOperator.Builder()
                .setPartitionByColumns(partitionByColumns)
                .setPartitionLimit(partitionLimit)
                .setOrderByElements(windowOperator.getEnforceSortColumns())
                .setLimit(limit)
                .setTopNType(topNType)
                .setSortPhase(sortPhase)
                .build(), grandChildExpr.getInputs());

        OptExpression newWindowOptExp = OptExpression.create(windowOperator, newTopNOptExp);
        OptExpression newProjectOptExp = OptExpression.create(projectOperator, newWindowOptExp);
        return Collections.singletonList(OptExpression.create(topNOperator, newProjectOptExp));
    }
}
