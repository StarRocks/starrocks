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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Comparator;
import java.util.List;

public class BestMvSelector {
    private final List<OptExpression> expressions;
    private final OptimizerContext context;
    private final boolean isAggQuery;

    public BestMvSelector(List<OptExpression> expressions, OptimizerContext context, boolean isAggQuery) {
        this.expressions = expressions;
        this.context = context;
        this.isAggQuery = isAggQuery;
    }

    public OptExpression selectBest() {
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        // compute the statistics of OptExpression
        for (OptExpression expression : expressions) {
            calculateStatistics(expression, context);
        }
        List<CandidateContext> contexts = Lists.newArrayList();
        for (int i = 0; i < expressions.size(); i++) {
            CandidateContext mvContext = getMVContext(expressions.get(i),
                    expressions.get(i).getOp() instanceof LogicalUnionOperator, isAggQuery, context);
            Preconditions.checkState(mvContext != null);
            mvContext.setIndex(i);
            contexts.add(mvContext);
        }
        // sort expressions based on statistics output row count and compute size
        contexts.sort(new CandidateContextComparator());
        return expressions.get(contexts.get(0).getIndex());
    }

    @VisibleForTesting
    public static class CandidateContext {
        private Statistics mvStatistics;
        private int schemaColumnNum;

        // if mv is aggregated, set it to number of group by key,
        // else set it to Integer.MAX_VALUE
        private int groupbyColumnNum;
        private int index;
        private boolean isUnion;

        public CandidateContext(Statistics mvStatistics, int schemaColumnNum, boolean isUnion) {
            this(mvStatistics, schemaColumnNum, isUnion, 0);
        }

        public CandidateContext(Statistics mvStatistics, int schemaColumnNum, boolean isUnion, int index) {
            this.mvStatistics = mvStatistics;
            this.schemaColumnNum = schemaColumnNum;
            this.isUnion = isUnion;
            this.index = index;
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
            int ret = Boolean.compare(context1.isUnion, context2.isUnion);
            if (ret != 0) {
                return ret;
            }
            // compare group by key num
            ret = Integer.compare(context1.getGroupbyColumnNum(), context2.getGroupbyColumnNum());
            if (ret != 0) {
                return ret;
            }
            // compare by row number
            ret = Double.compare(context1.getMvStatistics().getOutputRowCount(),
                    context2.getMvStatistics().getOutputRowCount());
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

    private CandidateContext getMVContext(
            OptExpression expression, boolean isUnion, boolean isAggregate, OptimizerContext optimizerContext) {
        if (expression.getOp() instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = expression.getOp().cast();
            if (scanOperator.getTable().isMaterializedView()) {
                CandidateContext candidateContext = new CandidateContext(
                        expression.getStatistics(), scanOperator.getTable().getBaseSchema().size(), isUnion);
                if (isAggregate) {
                    MaterializedView mv = (MaterializedView) scanOperator.getTable();
                    MvPlanContext planContext = CachingMvPlanContextBuilder.getInstance().getPlanContext(
                            mv, optimizerContext.getSessionVariable().isEnableMaterializedViewPlanCache());
                    if (planContext != null && planContext.getLogicalPlan() != null
                            && planContext.getLogicalPlan().getOp() instanceof LogicalAggregationOperator) {
                        LogicalAggregationOperator aggregationOperator = planContext.getLogicalPlan().getOp().cast();
                        candidateContext.setGroupbyColumnNum(aggregationOperator.getGroupingKeys().size());
                    }
                }
                return candidateContext;
            }
        }
        for (OptExpression child : expression.getInputs()) {
            CandidateContext context = getMVContext(child, isUnion, isAggregate, optimizerContext);
            if (context != null) {
                return context;
            }
        }
        return null;
    }
}
