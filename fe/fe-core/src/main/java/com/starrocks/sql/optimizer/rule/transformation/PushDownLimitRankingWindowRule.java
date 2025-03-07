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
import com.starrocks.common.Pair;
import com.starrocks.sql.RankingWindowUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
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
import java.util.Map;
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
 *
 * if window operator below rank window have same partition with rank window and
 * without order by and window clause, we can  use pre-Agg optimization
 * to reduce the amount of data to be exchanged and sorted.
 * if cardinality is not super high,this can gain significant performance
 * E.g.
 *     select * from (
 *        select *, rank() over (partition by v1 order by v2) as rk, sum(v1) over (partition by v1),
 *        count(v1) over (partition by v1)from t0
 *    ) sub_t0
 *  order by rk  limit 5;
 *
 * Before:
 *       TopN
 *         |
 *       Project
 *         |
 *       Window(rank)
 *         |
 *      window(sum, count)
 *
 * After:
 *       TopN
 *         |
 *       Project
 *         |
 *       Window(rank)
 *         |
 *      window(sum,count)
 *         |
 *       TopN(preAgg:sum,count)
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
        LogicalWindowOperator firstWindowOperator = grandChildExpr.getOp().cast();

        // in window transform phase, if two window op in same sort group and have same partition exps, then rank-related window will be
        // put on the top of the another window op
        if (!RankingWindowUtils.isSingleRankRelatedAnalyticOperator(firstWindowOperator)) {
            return false;
        }

        // case 1:only one window op which is SingleRankRelatedAnalyticOperator, support rank<=N push down without preAgg
        if (!(grandChildExpr.inputAt(0).getOp() instanceof LogicalWindowOperator)) {
            return true;
        }

        OptExpression grandGrandChildExpr = grandChildExpr.inputAt(0);
        LogicalWindowOperator secondWindowOperator = grandGrandChildExpr.getOp().cast();

        if (!(RankingWindowUtils.satisfyRankingPreAggOptimization(secondWindowOperator, firstWindowOperator))) {
            // case 2:two window ops, second one doesn't satisfy preAgg optimization, check whether we can support rank<=N push down without preAgg
            // There might be a negative gain if we add a partitionTopN between two window operators that
            // share the same sort group, because extra enforcer will add
            return !Objects.equals(firstWindowOperator.getEnforceSortColumns(),
                    secondWindowOperator.getEnforceSortColumns());
        }

        if (!context.getSessionVariable().getEnablePushDownPreAggWithRank()) {
            return false;
        }

        // case 3: two window ops, second one satisfy preAgg optimization, check whether we can support rank<=N push down with preAgg
        if (grandGrandChildExpr.inputAt(0).inputAt(0) != null &&
                grandGrandChildExpr.inputAt(0).inputAt(0).getOp() instanceof LogicalWindowOperator) {
            LogicalWindowOperator thirdWindowOperator = grandGrandChildExpr.inputAt(0).inputAt(0).getOp().cast();
            // There might be a negative gain if we add a partitionTopN between two window operators that
            // share the same sort group, because extra enforcer will add
            return !Objects.equals(secondWindowOperator.getEnforceSortColumns(),
                    thirdWindowOperator.getEnforceSortColumns());
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
        LogicalWindowOperator rankRelatedWindowOperator = grandChildExpr.getOp().cast();

        ColumnRefOperator windowCol = Lists.newArrayList(rankRelatedWindowOperator.getWindowCall().keySet()).get(0);
        CallOperator callOperator = rankRelatedWindowOperator.getWindowCall().get(windowCol);

        List<ColumnRefOperator> partitionByColumns = rankRelatedWindowOperator.getPartitionExpressions().stream()
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
        LogicalTopNOperator.Builder topNBuilder = new LogicalTopNOperator.Builder()
                .setPartitionByColumns(partitionByColumns)
                .setPartitionLimit(partitionLimit)
                .setOrderByElements(rankRelatedWindowOperator.getEnforceSortColumns())
                .setLimit(limit)
                .setTopNType(topNType)
                .setSortPhase(sortPhase);

        OptExpression grandgrandChildOptExpr = grandChildExpr.inputAt(0);
        if (grandgrandChildOptExpr.getOp() instanceof LogicalWindowOperator) {
            ColumnRefFactory columnFactory = context.getColumnRefFactory();
            LogicalWindowOperator secondWindowOperator = grandgrandChildOptExpr.getOp().cast();
            // push Down local topN with pre-agg optimization
            if (RankingWindowUtils.satisfyRankingPreAggOptimization(secondWindowOperator, rankRelatedWindowOperator)) {
                Pair<Map<ColumnRefOperator, CallOperator>, Map<ColumnRefOperator, CallOperator>> twoMaps =
                        RankingWindowUtils.splitWindowCall(
                                secondWindowOperator.getWindowCall(), columnFactory);
                Map<ColumnRefOperator, CallOperator> globalWindowCall = twoMaps.first;
                Map<ColumnRefOperator, CallOperator> localWindowCall = twoMaps.second;

                // change rankRelated window call's input column and mark this window op's input is binary
                secondWindowOperator = new LogicalWindowOperator.Builder().withOperator(secondWindowOperator)
                        .setWindowCall(globalWindowCall)
                        .setInputIsBinary(true)
                        .build();

                topNBuilder.setPartitionPreAggCall(localWindowCall);

                OptExpression newTopNOptExp =
                        OptExpression.create(topNBuilder.build(), grandgrandChildOptExpr.getInputs());
                OptExpression secondWindowOptExp = OptExpression.create(secondWindowOperator, newTopNOptExp);
                OptExpression rankRelatedOptExp = OptExpression.create(rankRelatedWindowOperator, secondWindowOptExp);

                OptExpression newProjectOptExp = OptExpression.create(projectOperator, rankRelatedOptExp);

                return Collections.singletonList(OptExpression.create(topNOperator, newProjectOptExp));
            }
        }

        // push Down local topN without pre-agg optimization
        OptExpression newTopNOptExp = OptExpression.create(topNBuilder.build(), grandChildExpr.getInputs());

        OptExpression newWindowOptExp = OptExpression.create(rankRelatedWindowOperator, newTopNOptExp);
        OptExpression newProjectOptExp = OptExpression.create(projectOperator, newWindowOptExp);
        return Collections.singletonList(OptExpression.create(topNOperator, newProjectOptExp));
    }
}
