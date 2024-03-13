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


package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DeriveStatsTask derives any stats needed for costing a GroupExpression.
 */
public class DeriveStatsTask extends OptimizerTask {
    private final GroupExpression groupExpression;

    public DeriveStatsTask(TaskContext context, GroupExpression expression) {
        super(context);
        this.groupExpression = expression;
    }

    @Override
    public String toString() {
        return "DeriveStatsTask for groupExpression " + groupExpression;
    }

    @Override
    public void execute() {
        if (groupExpression.isStatsDerived() || groupExpression.isUnused()) {
            return;
        }

        for (int i = groupExpression.arity() - 1; i >= 0; --i) {
            GroupExpression childExpression = groupExpression.getInputs().get(i).getFirstLogicalExpression();
            Preconditions.checkState(childExpression.isStatsDerived());
            Preconditions.checkNotNull(groupExpression.getInputs().get(i).getStatistics());
        }

        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                context.getOptimizerContext().getColumnRefFactory(), context.getOptimizerContext());
        statisticsCalculator.estimatorStats();

        Statistics currentStatistics = groupExpression.getGroup().getStatistics();
        // @Todo: update choose algorithm, like choose the least predicate statistics
        // choose best statistics
        Statistics groupExpressionStatistics = expressionContext.getStatistics();
        if (needUpdateGroupStatistics(currentStatistics, groupExpressionStatistics)) {
            if (currentStatistics != null
                    && isMaterializedView(groupExpression)
                    && !groupExpressionStatistics.isTableRowCountMayInaccurate()) {
                // use statistics of materialized view because it is more accurate
                Statistics.Builder newBuilder = Statistics.buildFrom(currentStatistics);
                if (!groupExpressionStatistics.isTableRowCountMayInaccurate()) {
                    newBuilder.setOutputRowCount(groupExpressionStatistics.getOutputRowCount());
                    newBuilder.setTableRowCountMayInaccurate(groupExpressionStatistics.isTableRowCountMayInaccurate());
                }
                Map<ColumnRefOperator, ColumnStatistic> newColumnStatisticMap = groupExpressionStatistics.getColumnStatistics();
                // update ColumnStatistics
                for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : currentStatistics.getColumnStatistics().entrySet()) {
                    ColumnStatistic columnStatistic = newColumnStatisticMap.get(entry.getKey());
                    if (columnStatistic != null && !columnStatistic.isUnknown()) {
                        newBuilder.addColumnStatistic(entry.getKey(), columnStatistic);
                    }
                }
                groupExpression.getGroup().setStatistics(newBuilder.build());
                groupExpression.getGroup().setIsStatisticsAdjustedByMv(true);
            } else {
                groupExpression.getGroup().setStatistics(groupExpressionStatistics);
            }
        }

        // do set group statistics when the groupExpression is a materialized view scan
        if (currentStatistics != null && !currentStatistics.equals(groupExpressionStatistics)
                && isMaterializedView(groupExpression)) {
            LogicalOlapScanOperator scan = groupExpression.getOp().cast();
            MaterializedView mv = (MaterializedView) scan.getTable();
            groupExpression.getGroup().setMvStatistics(mv.getId(), groupExpressionStatistics);
            if (mv.getRelatedMaterializedViews() != null) {
                List<Long> relatedMvIds =
                        mv.getRelatedMaterializedViews().stream().map(mvid -> mvid.getId()).collect(Collectors.toList());
                groupExpression.getGroup().setRelatedMvs(mv.getId(), relatedMvIds);
            }
        }

        groupExpression.setStatsDerived();
    }

    private boolean isMaterializedView(GroupExpression groupExpression) {
        return groupExpression.getOp() instanceof LogicalOlapScanOperator
                && ((LogicalOlapScanOperator) groupExpression.getOp()).getTable().isMaterializedView();
    }


    private boolean needUpdateGroupStatistics(Statistics currentStatistics, Statistics newStatistics) {
        if (currentStatistics == null) {
            return true;
        }
        // if the group expression is mv, use it to update group statistics because it is more accurate
        if (isMaterializedView(groupExpression)) {
            return true;
        }

        if (groupExpression.getGroup().isStatisticsAdjustedByMv()) {
            return false;
        }
        return newStatistics.getComputeSize() < currentStatistics.getComputeSize();
    }
}