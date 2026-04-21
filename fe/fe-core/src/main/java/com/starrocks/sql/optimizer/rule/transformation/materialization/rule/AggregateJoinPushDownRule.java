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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.api.client.util.Lists;
import com.google.common.base.Predicate;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewPushDownRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.IMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_AGG_PUSH_DOWN_REWRITE;

/**
 * Support to push down aggregate functions below join operator and rewrite the query by mv transparently.
 * eg:
 *
 * MV:
 *  create materialized view mv0
 *  distributed by random as
 *  select a.id, a.dt, a.col, array_agg_distinct(a.user_id) as count_distinct_im_uv
 *  from a group by a.id, a.dt, a.cal;
 *
 * Query:
 *  select a.dt, a.col, array_agg_distinct(a.user_id) as count_distinct_im_uv
 *  from a join b on a.id = b.id group by a.dt, a.cal;
 *
 * Rewrite it by:
 * select a.dt, a.col, cardinality(array_agg_unique(a.count_distinct_im_uv)) as count_distinct_im_uv
 * from
 *  ( select id, dt, col, array_agg_unique(count_distinct_im_uv) as count_distinct_im_uv from mv0 group by id, dt, col
 *  ) as a join b on a.id = b.id
 * group by a.dt, a.cal;
 *
 * Rewrite result:
 * select a.dt, a.col, cardinality(array_agg_unique(a.count_distinct_im_uv)) as count_distinct_im_uv
 * from mv0 as a join b on a.id = b.id group by a.dt, a.cal;
 */
public class AggregateJoinPushDownRule extends BaseMaterializedViewRewriteRule {
    private static AggregateJoinPushDownRule INSTANCE = new AggregateJoinPushDownRule();

    public AggregateJoinPushDownRule() {
        super(RuleType.TF_MV_AGGREGATE_JOIN_PUSH_DOWN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTIJOIN)));
    }

    public static AggregateJoinPushDownRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableMaterializedViewPushDownRewrite()) {
            return false;
        }
        if (Utils.isOptHasAppliedRule(input, OP_MV_AGG_PUSH_DOWN_REWRITE)) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public boolean isChooseBestMVBasedDataLayout(OptExpression expression) {
        return false;
    }

    /**
     * MV can only contain LogicalScanOperator, LogicalProjectOperator, LogicalAggregationOperator for mv push down rewrite.
     */
    public static boolean isLogicalSPG(OptExpression root) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (!(operator instanceof LogicalScanOperator)
                && !(operator instanceof LogicalProjectOperator)
                && !(operator instanceof LogicalAggregationOperator)) {
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPG(child)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Why do we override this method? since queryExpression's tables are greater than mv's, so it's QUERY_DELTA mv match mode
     * which is not supported in the base class.
     *
     * Now AggregatedMaterializedViewPushDownRewriter only supports rewrite aggregation plans which are pushed down in the table
     * scan operator, so filter mv candidates which only contains ScanOperator, ProjectOperator, AggregationOperator.
     *
     * @param queryExpression: query opt expression.
     * @param context: optimizer context.
     * @param mvCandidateContexts: materialized view candidates prepared in the mv preprocessor.
     * @return: pruned materialized view candidates.
     */
    @Override
    public List<MaterializationContext> doPrune(OptExpression queryExpression,
                                                OptimizerContext context,
                                                List<MaterializationContext> mvCandidateContexts) {
        List<LogicalScanOperator> scanOperators = getScanOperator(queryExpression);
        Map<Table, Set<String>> queryGroupByColumnsByTable = new HashMap<>();
        collectGroupByColumnsByTable(queryExpression, queryGroupByColumnsByTable);
        Map<Table, Set<String>> queryPredicateColumnsByTable = new HashMap<>();
        collectPredicateColumnsByTable(queryExpression, queryPredicateColumnsByTable);
        Map<Table, Set<String>> queryGroupByAndPredicateColumnsByTable = new HashMap<>(queryGroupByColumnsByTable);
        queryPredicateColumnsByTable.forEach(
                (tableId, columns) -> queryGroupByAndPredicateColumnsByTable.computeIfAbsent(tableId,
                        k -> new HashSet<>()).addAll(columns));
        List<MaterializationContext> validCandidateContexts = Lists.newArrayList();
        for (MaterializationContext mvContext : mvCandidateContexts) {
            if (!isLogicalSPG(mvContext.getMvExpression())) {
                logMVRewrite(mvContext, this, "mv pruned: not logical SPG");
            } else if (validMv(mvContext, scanOperators, queryGroupByAndPredicateColumnsByTable, queryPredicateColumnsByTable)) {
                validCandidateContexts.add(mvContext);
            }
        }
        return validCandidateContexts;
    }

    private boolean validMv(MaterializationContext mvContext, List<LogicalScanOperator> scanOperators,
                            Map<Table, Set<String>> queryGroupByAndPredicateColumnsByTable,
                            Map<Table, Set<String>> queryPredicateColumnsByTable) {
        // mv is SPG, so there is only one baseTable
        // Use Table.equals rather than getId() so external tables (e.g. IcebergTable) that
        // override equals() by catalog/db/tableIdentifier match correctly across plan rebuilds,
        // where CONNECTOR_ID_GENERATOR may assign different numeric ids to the same logical table.
        Table mvBaseTable = mvContext.getBaseTables().get(0);
        Set<ColumnRefOperator> mvUsedColRefs = MvUtils.collectScanColumn(mvContext.getMvExpression());
        Set<String> mvUsedColNames = mvUsedColRefs.stream()
                .map(ColumnRefOperator::getName)
                .collect(Collectors.toSet());
        boolean baseTableFoundInMv = false;
        boolean checkStage1 = false;
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (!mvBaseTable.equals(scanOperator.getTable())) {
                continue;
            }
            baseTableFoundInMv = true;
            // mv should contain all columns that used in at least one query
            if (mvContainsAllColumnsUsedInScan(mvUsedColNames, scanOperator)) {
                checkStage1 = true;
                break;
            }
        }
        if (!checkStage1) {
            if (baseTableFoundInMv) {
                logMVRewrite(mvContext, this, "mv pruned: not contain all columns used in scan");
            }
            return false;
        }
        // no group by or predicate columns in query
        if (!queryGroupByAndPredicateColumnsByTable.containsKey(mvBaseTable)) {
            return true;
        }
        if (validMvGroupByAndPredicateColumns(mvContext, queryGroupByAndPredicateColumnsByTable.get(mvBaseTable),
                queryPredicateColumnsByTable.get(mvBaseTable))) {
            return true;
        }
        logMVRewrite(mvContext, "mv pruned: mv group by does not cover query group by + predicate columns");
        return false;
    }

    private List<LogicalScanOperator> getScanOperator(OptExpression root) {
        List<LogicalScanOperator> scanOperators = Lists.newArrayList();
        getScanOperator(root, scanOperators);
        return scanOperators;
    }

    private void getScanOperator(OptExpression root, List<LogicalScanOperator> scanOperators) {
        if (root.getOp() instanceof LogicalScanOperator) {
            scanOperators.add((LogicalScanOperator) root.getOp());
        } else {
            for (OptExpression child : root.getInputs()) {
                if (child.getOp() instanceof LogicalAggregationOperator) {
                    return;
                }
                getScanOperator(child, scanOperators);
            }
        }
    }

    private boolean mvContainsAllColumnsUsedInScan(Set<String> mvUsedColNames, LogicalScanOperator scanOperator) {
        return scanOperator.getColRefToColumnMetaMap().values().stream().allMatch(
                (Predicate<Column>) c -> mvUsedColNames.contains(c.getName()));
    }

    private boolean validMvGroupByAndPredicateColumns(MaterializationContext mvContext,
                                                      Set<String> queryGroupByAndPredicateColumns,
                                                      Set<String> queryPredicateColumns) {
        Set<String> mvPredicateColumns = mvContext.getPredicateColumns();
        if (mvPredicateColumns == null) {
            Map<Table, Set<String>> mvPredicateColumnsByTable = new HashMap<>();
            collectPredicateColumnsByTable(mvContext.getMvExpression(), mvPredicateColumnsByTable);
            // MV is SPG, so there is only one baseTable
            mvPredicateColumns = mvPredicateColumnsByTable.values().stream().findFirst().orElse(new HashSet<>());
            mvContext.setPredicateColumns(mvPredicateColumns);
        }
        if (queryPredicateColumns == null) {
            // mv has predicate, query doesn't, then mv should not be used
            if (!mvPredicateColumns.isEmpty()) {
                return false;
            }
        } else if (!queryPredicateColumns.containsAll(mvPredicateColumns)) {
            return false;
        }
        // MV has no Agg anywhere in the tree (detail table) → skip group-by coverage check,
        // because a detail table preserves all rows and can serve any group-by + predicate.
        if (!containsAggregation(mvContext.getMvExpression())) {
            return true;
        }
        Set<String> mvGroupingColumns = mvContext.getGroupingColumns();
        if (mvGroupingColumns == null) {
            Map<Table, Set<String>> mvGroupByColumnsByTable = new HashMap<>();
            collectGroupByColumnsByTable(mvContext.getMvExpression(), mvGroupByColumnsByTable);
            // MV is SPG, so there is only one baseTable
            mvGroupingColumns = mvGroupByColumnsByTable.values().stream().findFirst().orElse(new HashSet<>());
            mvContext.setGroupingColumns(mvGroupingColumns);
        }
        // MV group-by + predicate columns must cover query's required columns (group-by + predicate).
        Set<String> mvGroupByAndPredicateColumns = new HashSet<>(mvGroupingColumns);
        mvGroupByAndPredicateColumns.addAll(mvPredicateColumns);
        return mvGroupByAndPredicateColumns.containsAll(queryGroupByAndPredicateColumns);
    }

    /**
     * Starting from queryExpression (TopAgg -> Join -> ...), collects the physical columns
     * referenced by the top-level Agg's group-by keys, grouped by table id.
     */
    static void collectGroupByColumnsByTable(OptExpression queryExpression, Map<Table, Set<String>> columnsByTable) {
        if (queryExpression.getOp() instanceof LogicalAggregationOperator agg) {
            // colRef -> [<tableId, physicalColName>]
            Map<Integer, List<Pair<Table, String>>> colRefToTableColumns = new HashMap<>();
            buildSPJFullColumnsToTableColumns(queryExpression.inputAt(0), colRefToTableColumns);
            for (ColumnRefOperator key : agg.getGroupingKeys()) {
                List<Pair<Table, String>> resolved = colRefToTableColumns.getOrDefault(key.getId(), Collections.emptyList());
                for (Pair<Table, String> tableColumn : resolved) {
                    columnsByTable.computeIfAbsent(tableColumn.first, t -> new HashSet<>()).add(tableColumn.second);
                }
            }
        }
    }

    /**
     * Post-order DFS over the subtree rooted at {@code node}, building a colId -> List&lt;(tableId, physicalColName)&gt; map.
     *
     * - Scan node: seeds the map from colRefToColumnMetaMap.
     * - All nodes (post-order): if the node carries an inlined Projection (Cascades merges LogicalProject
     *   into its parent before rule matching), expands its columnRefMap into the map.
     * - Nested Agg children are skipped to exclude Agg->Agg->...->Scan paths.
     */
    private static void buildSPJFullColumnsToTableColumns(OptExpression node,
                                                          Map<Integer, List<Pair<Table, String>>> colToTableColumns) {
        if (node.getOp() instanceof LogicalScanOperator scan) {
            Table table = scan.getTable();
            for (Map.Entry<ColumnRefOperator, Column> e : scan.getColRefToColumnMetaMap().entrySet()) {
                colToTableColumns.put(e.getKey().getId(),
                        Collections.singletonList(Pair.create(table, e.getValue().getName())));
            }
        }

        // Recurse into children first (post-order); skip nested Agg subtrees (excludes Agg->Agg->...->Scan paths)
        for (OptExpression child : node.getInputs()) {
            if (child.getOp() instanceof LogicalAggregationOperator) {
                continue;
            }
            buildSPJFullColumnsToTableColumns(child, colToTableColumns);
        }

        Projection inlineProj = node.getOp().getProjection();
        if (inlineProj != null) {
            resolveProjectionIntoMap(inlineProj.getColumnRefMap(), colToTableColumns);
        }

        if (node.getOp() instanceof LogicalProjectOperator) {
            resolveProjectionIntoMap(((LogicalProjectOperator) node.getOp()).getColumnRefMap(), colToTableColumns);
        }
    }

    /**
     * Expands each output colRef in {@code columnRefMap} to the union of physical (tableId, colName) pairs
     */
    private static void resolveProjectionIntoMap(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                 Map<Integer, List<Pair<Table, String>>> columnToTableColumns) {
        for (Map.Entry<ColumnRefOperator, ScalarOperator> e : columnRefMap.entrySet()) {
            columnToTableColumns.computeIfAbsent(e.getKey().getId(), input -> {
                List<Pair<Table, String>> resolved = new ArrayList<>();
                e.getValue().getUsedColumns().getStream()
                        .forEach(id -> resolved.addAll(columnToTableColumns.getOrDefault(id, Collections.emptyList())));
                return resolved.isEmpty() ? null : resolved;
            });
        }
    }

    /**
     * DFS over the expression tree collecting physical columns referenced in each Scan's predicate,
     * grouped by table id into {@code predicateColumnsByTable}.
     * Nested Agg subtrees are skipped to exclude Agg->Agg->...->Scan paths.
     */
    private static void collectPredicateColumnsByTable(OptExpression node, Map<Table, Set<String>> predicateColumnsByTable) {
        if (node.getOp() instanceof LogicalScanOperator) {
            LogicalScanOperator scan = (LogicalScanOperator) node.getOp();
            if (scan.getPredicate() != null) {
                Table table = scan.getTable();
                Map<ColumnRefOperator, Column> refToCol = scan.getColRefToColumnMetaMap();
                scan.getPredicate().getUsedColumns().getStream().forEach(colId ->
                        refToCol.forEach((ref, col) -> {
                            if (ref.getId() == colId) {
                                predicateColumnsByTable.computeIfAbsent(table, t -> new HashSet<>()).add(col.getName());
                            }
                        }));
            }
            return;
        }
        for (OptExpression child : node.getInputs()) {
            if (child.getOp() instanceof LogicalAggregationOperator) {
                continue;
            }
            collectPredicateColumnsByTable(child, predicateColumnsByTable);
        }
    }

    /**
     * Returns true if the expression tree contains at least one LogicalAggregationOperator.
     */
    private static boolean containsAggregation(OptExpression node) {
        if (node.getOp() instanceof LogicalAggregationOperator) {
            return true;
        }
        for (OptExpression child : node.getInputs()) {
            if (containsAggregation(child)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public IMaterializedViewRewriter createRewriter(OptimizerContext optimizerContext,
                                                    MvRewriteContext mvContext) {
        return new AggregatedMaterializedViewPushDownRewriter(mvContext, this);
    }
}