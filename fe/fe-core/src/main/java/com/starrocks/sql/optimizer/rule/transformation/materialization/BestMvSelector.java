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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

public class BestMvSelector {
    private final List<OptExpression> mvValidExpressions;
    private final OptimizerContext optimizerContext;
    private final OptExpression queryPlan;
    private final boolean isAggQuery;
    private final Rule rule;

    public BestMvSelector(List<OptExpression> mvValidExpressions,
                          OptimizerContext optimizerContext,
                          OptExpression queryPlan, Rule rule) {
        this.mvValidExpressions = mvValidExpressions;
        this.optimizerContext = optimizerContext;
        this.queryPlan = queryPlan;
        this.isAggQuery = queryPlan.getOp() instanceof LogicalAggregationOperator;
        this.rule = rule;
    }

    public enum ChooseMode {
        ENABLE, // choose best mv when an input query pattern is only table scan
        DISABLE, // choose best mv without considering data layout
        FORCE; // choose best mv in eager mode, considering all input query patterns

        public static ChooseMode of(String value) {
            if (value == null) {
                return ENABLE;
            }
            try {
                return ChooseMode.valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return ENABLE;
            }
        }
    }

    private void calculateStatistics(List<OptExpression> expressions, OptimizerContext context) {
        for (OptExpression expression : expressions) {
            try {
                calculateStatistics(expression, context);
            } catch (Exception e) {
                logMVRewrite(context, rule, "calculate statistics failed: {}", DebugUtil.getStackTrace(e));
            }
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

    public List<OptExpression> selectBest(boolean isConsiderQueryDataLayout) {
        if (mvValidExpressions.isEmpty()) {
            return mvValidExpressions;
        }
        List<Table> queryTables = MvUtils.getAllTables(queryPlan);
        // add query's context
        if (queryTables.isEmpty()) {
            return Lists.newArrayList();
        }
        return selectBest(queryTables, isConsiderQueryDataLayout);
    }

    private List<OptExpression> selectBest(List<Table> queryTables,
                                           boolean isConsiderQueryDataLayout) {
        if (mvValidExpressions.size() == 1 && !isConsiderQueryDataLayout) {
            logMVRewrite(optimizerContext, rule, "[ChooseBestMV] Only one candidate mv, skip best mv select.");
            return mvValidExpressions;
        }
        // compute the statistics of OptExpression
        calculateStatistics(mvValidExpressions, optimizerContext);

        // split predicates' columns to equivalence and non-equivalence sets which its names are all lower case.
        Set<ScalarOperator> queryPredicates = MvUtils.getAllValidPredicatesFromScans(queryPlan);
        Set<String> equivalenceColumns = Sets.newHashSet();
        Set<String> nonEquivalenceColumns = Sets.newHashSet();
        MvUtils.splitPredicate(queryPredicates, equivalenceColumns, nonEquivalenceColumns);

        // current query expression can only see to-rewrite plan, and cannot see the whole query plan.
        // so here deliver query context to collect all join-on predicates first.
        QueryMaterializationContext queryMaterializationContext = optimizerContext.getQueryMaterializationContext();
        Set<String> distEqCols = queryMaterializationContext.getQueryDistEqCols();

        List<CandidateContext> contexts = Lists.newArrayList();
        int globalIdx = 0;
        // add all mvs' context first
        for (int i = 0; i < mvValidExpressions.size(); i++) {
            OptExpression mvOptExpression = mvValidExpressions.get(i);
            List<Table> mvTables = MvUtils.getAllTables(mvOptExpression);
            queryTables.stream().forEach(originalTable -> mvTables.remove(originalTable));
            CandidateContext candidateContext = buildCandidateContext(
                    mvOptExpression, distEqCols, equivalenceColumns, nonEquivalenceColumns, globalIdx, false);
            if (candidateContext == null) {
                logMVRewrite(optimizerContext, rule,
                        "[ChooseBestMV] Skip mv {} for building candidate context failed.",
                        mvTables.stream().map(Table::getName).collect(Collectors.joining(",")));
                continue;
            }
            globalIdx += 1;
            contexts.add(candidateContext);
        }
        // then add query context if needed
        if (isConsiderQueryDataLayout) {
            CandidateContext candidateContext = buildCandidateContext(
                    queryPlan, distEqCols, equivalenceColumns, nonEquivalenceColumns, globalIdx, true);
            if (candidateContext != null) {
                contexts.add(candidateContext);
            }
        }
        if (contexts.isEmpty()) {
            logMVRewrite(optimizerContext, rule, "[ChooseBestMV] No candidate mv.");
            return Lists.newArrayList();
        }

        // sort expressions based on statistics output row count and compute size
        Optional<CandidateContext> bestContextOpt = contexts.stream()
                .min(Comparator.comparing(CandidateContext::getScore));
        if (!bestContextOpt.isPresent()) {
            logMVRewrite(optimizerContext, rule, "[ChooseBestMV] No candidate mv after comparing.");
            return Lists.newArrayList();
        }
        CandidateContext bestContext = bestContextOpt.get();
        List<String> debugScores = contexts.stream()
                .map(context -> context.score.toString())
                .collect(Collectors.toUnmodifiableList());
        if (bestContext.isQueryOpt) {
            logMVRewrite(optimizerContext, rule, "[ChooseBestMV] Only left input query after cost comparing," +
                    " scores:[{}]", debugScores);
            return Lists.newArrayList();
        }
        logMVRewrite(optimizerContext, rule, "[ChooseBestMV] Choose mv with table {}. Scores:[{}]",
                bestContext.score.baseTable.getName(), debugScores);
        return Lists.newArrayList(bestContext.getResult());
    }

    /**
     * CandidateContext is used to store the candidate mv's context information which is
     * used to compare with other candidate mvs.
     */
    @VisibleForTesting
    public static class CandidateContext {
        private final OptExpression result;
        private final boolean isQueryOpt;
        private CandidateScore score;

        public CandidateContext(OptExpression result,
                                boolean isQueryOpt,
                                CandidateScore score) {
            this.result = result;
            this.isQueryOpt = isQueryOpt;
            this.score = score;
        }

        public CandidateScore getScore() {
            return score;
        }

        public OptExpression getResult() {
            return result;
        }
    }

    /**
     * CandidateScore is used to store information that is used to compare two candidate scores.
     */
    public static class CandidateScore implements Comparable<CandidateScore> {
        public Table baseTable;
        public int schemaColumnNum;
        public int distScore;
        public int sortScore;
        public int groupBySize;
        public int index;
        public Statistics stats;

        // basic count
        public int scanCount;
        public int aggCount;
        public int joinCount;

        /**
         * Compare two CandidateScore, the comparison logic is as follows:
         * - Compare by agg/join/scan count, smaller is better.
         * - Compare by distribution score, larger is better.
         * - Compare by group by key num, smaller is better.
         * - Compare by sort score, larger is better.
         * - Compare by schema column num, smaller is better.
         * - Compare by statistics size, smaller is better.
         * - Compare by index, smaller is better.
         */
        @Override
        public int compareTo(CandidateScore other) {
            if (other == null) {
                return -1;
            }
            if (this == other) {
                return 0;
            }
            int ret = 0;
            // smaller is better
            ret = Integer.compare(this.aggCount, other.aggCount);
            if (ret != 0) {
                return ret;
            }
            ret = Integer.compare(this.joinCount, other.joinCount);
            if (ret != 0) {
                return ret;
            }
            ret = Integer.compare(this.scanCount, other.scanCount);
            if (ret != 0) {
                return ret;
            }
            // larger is better
            ret = Integer.compare(other.distScore, this.distScore);
            if (ret != 0) {
                return ret;
            }
            // compare group by key num
            ret = Integer.compare(this.groupBySize, other.groupBySize);
            if (ret != 0) {
                return ret;
            }
            // larger is better
            ret = Integer.compare(other.sortScore, this.sortScore);
            if (ret != 0) {
                return ret;
            }
            // compare by schema column num
            ret = Integer.compare(this.schemaColumnNum, other.schemaColumnNum);
            if (ret != 0) {
                return ret;
            }
            // compare by row number
            Statistics stats1 = this.stats;
            Statistics stats2 = other.stats;
            if (stats1 != null && stats2 != null && stats1.getOutputRowCount() > 100 &&
                    stats2.getOutputRowCount() > 100) {
                ret = Double.compare(stats1.getOutputRowCount(), stats2.getOutputRowCount());
                if (ret != 0) {
                    return ret;
                }
            }
            return Integer.compare(this.index, other.index);
        }

        private String getStatsInfo() {
            if (stats == null) {
                return "null";
            }
            return String.format("rowCount=%.2f", stats.getOutputRowCount());
        }

        @Override
        public String toString() {
            return "{" +
                    "baseTable=" + (baseTable == null ? "null" : baseTable.getName()) +
                    ", schemaColumnNum=" + schemaColumnNum +
                    ", distScore=" + distScore +
                    ", sortScore=" + sortScore +
                    ", groupBySize=" + groupBySize +
                    ", scanCount=" + scanCount +
                    ", aggCount=" + aggCount +
                    ", joinCount=" + joinCount +
                    ", index=" + index +
                    ", stats=" + getStatsInfo() +
                    '}';
        }
    }

    /**
     *  CandidateContextVisitor is a visitor to visit an OptExpression and collect its candidate context info.
     */
    private class CandidateContextVisitor extends OptExpressionVisitor<Void, Void> {
        private final Set<String> distEqCols;
        private final Set<String> equivalenceColumns;
        private final Set<String> nonEquivalenceColumns;
        private final boolean isQueryOpt;
        private final CandidateScore score = new CandidateScore();
        // if union all is met in the plan, do not calculate cost for mv since mv can be rewritten by union rewrite.
        private boolean isCrossUnionAll = false;

        public CandidateContextVisitor(Set<String> distEqCols,
                                       Set<String> equivalenceColumns,
                                       Set<String> nonEquivalenceColumns,
                                       boolean isQueryOpt) {
            this.distEqCols = distEqCols;
            this.equivalenceColumns = equivalenceColumns;
            this.nonEquivalenceColumns = nonEquivalenceColumns;
            this.isQueryOpt = isQueryOpt;
        }

        public CandidateScore getScore() {
            return score;
        }

        private boolean isCalCost(Table table) {
            // to avoid mv union rewrite's affect, do not calculate cost for mv if union all is met in the plan.
            if (table == null || isCrossUnionAll) {
                return false;
            }
            return isQueryOpt || (table instanceof MaterializedView);
        }

        @Override
        public Void visitLogicalTableScan(OptExpression optExpression, Void context) {
            Operator operator = optExpression.getOp();
            if (operator instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) operator;
                Table table = olapScanOperator.getTable();
                // since mv can be rewritten by union, only check mv if it contains original table for mv union rewrite.
                if (!isCalCost(table)) {
                    return null;
                }
                score.baseTable = olapScanOperator.getTable();
                score.sortScore += calcSortScore(olapScanOperator.getTable(), equivalenceColumns, nonEquivalenceColumns);
                score.distScore += calcDistScore(score.baseTable, distEqCols);
                score.scanCount += 1;
            }
            return null;
        }

        @Override
        public Void visitLogicalAggregate(OptExpression optExpression, Void context) {
            // visit children first
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }
            if (score.baseTable == null || !isCalCost(score.baseTable)) {
                return null;
            }
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            List<ColumnRefOperator> groupByCols = aggregationOperator.getGroupingKeys();
            score.groupBySize += groupByCols.size();
            score.aggCount += 1;
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, Void context) {
            // visit children first
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }
            if (score.baseTable == null || !isCalCost(score.baseTable)) {
                return null;
            }
            score.joinCount += 1;
            return null;
        }

        @Override
        public Void visitLogicalUnion(OptExpression optExpression, Void context) {
            // visit children first
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }
            this.isCrossUnionAll = true;
            return null;
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }
            return null;
        }
    }

    /**
     * DistKeysCollector is a visitor to collect all distinct-equivalence columns from the input query plan.
     * These columns are used to calculate distribution score for candidate mvs.
     */
    public static class DistKeysCollector extends OptExpressionVisitor<Void, Void> {
        private final Set<String> distEqCols = Sets.newHashSet();
        public DistKeysCollector() {
        }

        @Override
        public Void visitLogicalAggregate(OptExpression optExpression, Void context) {
            // visit children first
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }

            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            // add all grouping keys and distinct keys
            aggregationOperator.getGroupingKeys().stream()
                    .map(ColumnRefOperator::getName)
                    .map(String::toLowerCase)
                    .forEach(colName -> distEqCols.add(colName));
            // add distinct columns
            aggregationOperator.getAggregations().values().stream()
                    .filter(agg -> agg.isDistinct())
                    .forEach(agg -> Utils.extractColumnRef(agg).forEach(col -> distEqCols.add(col.getName().toLowerCase())));
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression optExpression, Void context) {
            // visit children first
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            ScalarOperator joinOnPredicate = joinOperator.getOnPredicate();
            // add all join-on equivalence predicate columns
            List<ScalarOperator> joinOnPredicates = Utils.extractConjuncts(joinOnPredicate);
            distEqCols.addAll(getJoinOnPredicateColumns(joinOnPredicates));
            return null;
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }
            return null;
        }

        public Set<String> getDistEqCols() {
            return distEqCols;
        }
    }

    /**
     * Collect all distinct-equivalence columns from the input query plan.
     * @param queryPlan: the input query plan
     * @return the distinct-equivalence columns' names in lower case
     */
    public static Set<String> collectDistKeys(OptExpression queryPlan) {
        DistKeysCollector collector = new DistKeysCollector();
        queryPlan.getOp().accept(collector, queryPlan, null);
        return collector.getDistEqCols();
    }

    private static Set<String> getJoinOnPredicateColumns(List<ScalarOperator> joinOnPredicates) {
        Set<String> joinOnColumns = Sets.newHashSet();
        for (ScalarOperator joinOnPredOp : joinOnPredicates) {
            List<ScalarOperator> joinOnPreds = Utils.extractConjuncts(joinOnPredOp);
            for (ScalarOperator joinOnPred : joinOnPreds) {
                if (joinOnPred instanceof BinaryPredicateOperator) {
                    BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) joinOnPred;
                    if (binaryPredicate.getBinaryType().isEquivalence()) {
                        List<ColumnRefOperator> columns = Utils.extractColumnRef(binaryPredicate);
                        for (ColumnRefOperator column : columns) {
                            joinOnColumns.add(column.getName().toLowerCase());
                        }
                    }
                }
            }
        }
        return joinOnColumns;
    }

    /**
     * Get table's distribution columns in lower case.
     */
    private Set<String> getTableDistColumns(OlapTable olapTable) {
        return olapTable.getDistributionColumnNames();
    }

    /**
     * Get table's ordered key columns, if mv has defined sort keys, use them first.
     */
    private List<Column> getTableOrderedColumns(OlapTable olapTable) {
        if (olapTable instanceof MaterializedView) {
            MaterializedView mv = (MaterializedView) olapTable;
            // how to deduce sort keys from
            List<String> sortKeys = mv.getTableProperty().getMvSortKeys();
            if (CollectionUtils.isNotEmpty(sortKeys)) {
                return sortKeys.stream()
                        .map(String::toLowerCase)
                        .map(colName -> olapTable.getColumn(colName))
                        .collect(Collectors.toList());
            }
        } else {
            long baseIndexId = olapTable.getBaseIndexId();
            MaterializedIndexMeta baseIndexMeta = olapTable.getIndexMetaByIndexId(baseIndexId);
            List<Column> baseSchema = olapTable.getBaseSchema();
            List<Integer> sortKeyIdxes = baseIndexMeta.getSortKeyIdxes();
            if (CollectionUtils.isNotEmpty(sortKeyIdxes)) {
                return sortKeyIdxes.stream()
                        .map(baseSchema::get)
                        .collect(Collectors.toList());
            }
        }
        return olapTable.getKeyColumnsInOrder();
    }

    /**
     * Calculate a sort score for materialized view:
     * - For each equivalence predicate column that matches the prefix of the mv's sort columns, +1
     * - For the first non-equivalence predicate column that matches the prefix of the mv's sort columns, +1 and stop.
     */
    private int calcSortScore(Table table,
                              Set<String> equivalenceColumns,
                              Set<String> nonEquivalenceColumns) {
        if (table == null || !(table instanceof OlapTable)) {
            return 0;
        }
        OlapTable olapTable = (OlapTable) table;
        List<Column> sortKeyColumns = getTableOrderedColumns(olapTable);
        Set<String> distCols = getTableDistColumns(olapTable);
        boolean isColocateTable = isColocateTable(table);

        int score = 0;
        for (int i = 0; i < sortKeyColumns.size(); i++) {
            Column col = sortKeyColumns.get(i);
            String columName = col.getName().toLowerCase();
            if (equivalenceColumns.contains(columName)) {
                score += 1;
                if (isColocateTable && distCols.contains(columName)) {
                    // if distribution column is matched, add one more score.
                    score += 1;
                }
            } else if (i == 0 && nonEquivalenceColumns.contains(columName)) {
                // UnEquivalence predicate's columns can only match first columns in rollup.
                score += 1;
                break;
            } else {
                break;
            }
        }
        return score;
    }

    private boolean isColocateTable(Table table) {
        if (table == null || !(table instanceof OlapTable)) {
            return false;
        }
        OlapTable olapTable = (OlapTable) table;
        if (!olapTable.isPartitionedTable()) {
            // only hash distribution support colocate
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            return distributionInfo != null && distributionInfo.supportColocate();
        } else {
            boolean isColocateGroupStable = false;
            try {
                isColocateGroupStable = !isColocateGroupUnstable(table);
            } catch (Exception e) {
                // do nothing.
            }
            // if table is not co-locate stable, do not consider distribution columns.
            return isColocateGroupStable;
        }
    }

    private int calcDistScore(Table table,
                              Set<String> equivalenceColumns) {
        if (table == null || !(table instanceof OlapTable)) {
            return 0;
        }
        if (!isColocateTable(table)) {
            return 0;
        }
        OlapTable olapTable = (OlapTable) table;
        Set<String> distCols = getTableDistColumns(olapTable);
        int score = 0;
        for (String distCol : distCols) {
            String columName = distCol.toLowerCase();
            if (equivalenceColumns.contains(columName)) {
                score += 2;
            }
        }
        return score;
    }

    private CandidateContext buildCandidateContext(OptExpression expression,
                                                   Set<String> distEqCols,
                                                   Set<String> equivalenceColumns,
                                                   Set<String> nonEquivalenceColumns,
                                                   int globalIdx,
                                                   boolean isQueryOpt) {
        CandidateContextVisitor visitor = new CandidateContextVisitor(distEqCols,
                equivalenceColumns, nonEquivalenceColumns, isQueryOpt);
        expression.getOp().accept(visitor, expression, null);

        CandidateScore score = visitor.getScore();
        Table baseTable = score.baseTable;
        if (baseTable == null) {
            return null;
        }
        score.stats = expression.getStatistics();
        score.index = globalIdx;
        CandidateContext candidateContext = new CandidateContext(expression, isQueryOpt, score);
        if (!isAggQuery) {
            score.groupBySize = 0;
        }
        return candidateContext;
    }

    private boolean isColocateGroupUnstable(Table table) {
        if (table == null || !(table instanceof OlapTable)) {
            return false;
        }
        OlapTable olapTable = (OlapTable) table;
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long tableId = olapTable.getId();
        if (!colocateIndex.isColocateTable(tableId)) {
            return false;
        }
        return colocateIndex.isGroupUnstable(colocateIndex.getGroup(tableId));
    }
}
