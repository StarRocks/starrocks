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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

public class BestMvSelector {
    private final List<OptExpression> expressions;
    private final OptimizerContext context;
    private final OptExpression queryPlan;
    private final boolean isAggQuery;
    private final Rule rule;

    public BestMvSelector(List<OptExpression> expressions, OptimizerContext context, OptExpression queryPlan, Rule rule) {
        this.expressions = expressions;
        this.context = context;
        this.queryPlan = queryPlan;
        this.isAggQuery = queryPlan.getOp() instanceof LogicalAggregationOperator;
        this.rule = rule;
    }

    private List<OptExpression> calculateStatistics(List<OptExpression> expressions, OptimizerContext context) {
        for (OptExpression expression : expressions) {
            try {
                calculateStatistics(expression, context);
            } catch (Exception e) {
                logMVRewrite(context, rule, "calculate statistics failed: {}", DebugUtil.getStackTrace(e));
            }
        }
        return expressions;
    }

    public OptExpression selectBest() {
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        // compute the statistics of OptExpression
        List<OptExpression> validExpressions = calculateStatistics(expressions, context);

        // collect original table scans
        List<Table> originalTables = MvUtils.getAllTables(queryPlan);
        Set<ScalarOperator> predicates = MvUtils.getAllValidPredicatesFromScans(queryPlan);
        Set<String> equivalenceColumns = Sets.newHashSet();
        Set<String> nonEquivalenceColumns = Sets.newHashSet();
        MvUtils.splitPredicate(predicates, equivalenceColumns, nonEquivalenceColumns);
        List<CandidateContext> contexts = Lists.newArrayList();
        for (int i = 0; i < validExpressions.size(); i++) {
            List<Table> expressionTables = MvUtils.getAllTables(validExpressions.get(i));
            originalTables.stream().forEach(originalTable -> expressionTables.remove(originalTable));
            CandidateContext mvContext = getMVContext(
                    validExpressions.get(i), isAggQuery, context, expressionTables, equivalenceColumns, nonEquivalenceColumns);
            Preconditions.checkState(mvContext != null);
            mvContext.setIndex(i);
            contexts.add(mvContext);
        }
        // sort expressions based on statistics output row count and compute size
        contexts.sort(new CandidateContextComparator());
        return validExpressions.get(contexts.get(0).getIndex());
    }

    @VisibleForTesting
    public static class CandidateContext {
        private Statistics mvStatistics;
        private int schemaColumnNum;

        // if mv is aggregated, set it to number of group by key,
        // else set it to Integer.MAX_VALUE
        private int groupbyColumnNum;
        private int index;
        private final int sortScore;

        public CandidateContext(Statistics mvStatistics, int schemaColumnNum, int sortScore) {
            this(mvStatistics, schemaColumnNum, sortScore, 0);
        }

        public CandidateContext(Statistics mvStatistics, int schemaColumnNum, int sortScore, int index) {
            this.mvStatistics = mvStatistics;
            this.schemaColumnNum = schemaColumnNum;
            this.index = index;
            this.sortScore = sortScore;
            this.groupbyColumnNum = Integer.MAX_VALUE;
        }

        public int getSchemaColumnNum() {
            return schemaColumnNum;
        }

        public int getGroupbyColumnNum() {
            return groupbyColumnNum;
        }

        public void setGroupbyColumnNum(int groupbyColumnNum) {
            this.groupbyColumnNum = groupbyColumnNum;
        }

        public Statistics getMvStatistics() {
            return mvStatistics;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }

    @VisibleForTesting
    public static class CandidateContextComparator implements Comparator<CandidateContext> {
        @Override
        public int compare(CandidateContext context1, CandidateContext context2) {
            // larger is better
            int ret = Integer.compare(context2.sortScore, context1.sortScore);
            if (ret != 0) {
                return ret;
            }

            // compare by row number
            ret = Double.compare(context1.getMvStatistics().getOutputRowCount(),
                    context2.getMvStatistics().getOutputRowCount());
            if (ret != 0) {
                return ret;
            }

            // compare group by key num
            ret = Integer.compare(context1.getGroupbyColumnNum(), context2.getGroupbyColumnNum());
            if (ret != 0) {
                return ret;
            }

            // compare by schema column num
            ret = Integer.compare(context1.getSchemaColumnNum(), context2.getSchemaColumnNum());
            if (ret != 0) {
                return ret;
            }

            ret = Double.compare(context1.getMvStatistics().getComputeSize(), context2.getMvStatistics().getComputeSize());
            return ret != 0 ? ret : Integer.compare(context1.getIndex(), context2.getIndex());
        }
    }

    private void calculateStatistics(OptExpression expr, OptimizerContext context) {
        // Avoid repeated calculate
        if (expr.getStatistics() != null) {
            return;
        }

        for (OptExpression child : expr.getInputs()) {
            calculateStatistics(child, context);
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        statisticsCalculator.estimatorStats();
        expr.setStatistics(expressionContext.getStatistics());
    }

    private int calcSortScore(MaterializedView mv, Set<String> equivalenceColumns, Set<String> nonEquivalenceColumns) {
        List<Column> keyColumns = mv.getKeyColumnsByIndexId(mv.getBaseIndexId());
        int score = 0;
        for (Column col : keyColumns) {
            String columName = col.getName().toLowerCase();
            if (equivalenceColumns.contains(columName)) {
                score++;
            } else if (nonEquivalenceColumns.contains(columName)) {
                // UnEquivalence predicate's columns can only match first columns in rollup.
                score++;
                break;
            } else {
                break;
            }
        }
        return score;
    }

    private CandidateContext getMVContext(
            OptExpression expression, boolean isAggregate, OptimizerContext optimizerContext,
            List<Table> diffTables, Set<String> equivalenceColumns, Set<String> nonEquivalenceColumns) {
        if (expression.getOp() instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = expression.getOp().cast();
            if (scanOperator.getTable().isMaterializedView()) {
                MaterializedView mv = (MaterializedView) scanOperator.getTable();
                if (!diffTables.contains(mv)) {
                    return null;
                }
                int sortScore = calcSortScore(mv, equivalenceColumns, nonEquivalenceColumns);
                CandidateContext candidateContext = new CandidateContext(
                        expression.getStatistics(), scanOperator.getTable().getBaseSchema().size(), sortScore);
                if (isAggregate) {
                    List<MvPlanContext> planContexts = CachingMvPlanContextBuilder.getInstance().getPlanContext(
                            optimizerContext.getSessionVariable(), mv);
                    for (MvPlanContext planContext : planContexts) {
                        if (planContext.getLogicalPlan().getOp() instanceof LogicalAggregationOperator) {
                            LogicalAggregationOperator aggregationOperator = planContext.getLogicalPlan().getOp().cast();
                            candidateContext.setGroupbyColumnNum(aggregationOperator.getGroupingKeys().size());
                        }
                    }
                }
                return candidateContext;
            }
        }
        for (OptExpression child : expression.getInputs()) {
            CandidateContext context = getMVContext(
                    child, isAggregate, optimizerContext, diffTables, equivalenceColumns, nonEquivalenceColumns);
            if (context != null) {
                return context;
            }
        }
        return null;
    }
}
