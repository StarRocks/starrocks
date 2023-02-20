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


package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Select best materialized view for olap scan node
 */
public class MaterializedViewRule extends Rule {
    // For Materialized View key columns, which could hit the following functions
    private static final ImmutableList<String> KEY_COLUMN_FUNCTION_NAMES = ImmutableList.of(
            FunctionSet.MAX,
            FunctionSet.MIN,
            FunctionSet.APPROX_COUNT_DISTINCT,
            FunctionSet.MULTI_DISTINCT_COUNT
    );

    public MaterializedViewRule() {
        super(RuleType.TF_MATERIALIZED_VIEW, Pattern.create(OperatorType.PATTERN));
    }

    private final Map<Integer, Set<Integer>> columnIdsInPredicates = Maps.newHashMap();
    private final Map<Integer, Set<Integer>> columnIdsInGrouping = Maps.newHashMap();
    private final Map<Integer, Set<CallOperator>> aggFunctions = Maps.newHashMap();
    private final ColumnRefSet columnIdsInAggregate = new ColumnRefSet();
    private final Map<Integer, Set<Integer>> columnIdsInQueryOutput = Maps.newHashMap();
    private final Map<Integer, Map<String, Integer>> columnNameToIds = Maps.newHashMap();
    private final List<LogicalOlapScanOperator> scanOperators = Lists.newArrayList();

    private final Map<Long, List<RewriteContext>> rewriteContexts = Maps.newHashMap();

    private ColumnRefFactory factory;
    // record the relation id -> disableSPJGMV flag
    // this can be set to true when query has count(*) or count(1)
    private final Map<Integer, Boolean> disableSPJGMVs = Maps.newHashMap();
    // record the relation id -> isSPJQuery flag
    private final Map<Integer, Boolean> isSPJQueries = Maps.newHashMap();
    // record the scan node relation id which has been accessed.
    private final Set<Integer> traceRelationIds = Sets.newHashSet();

    private void init(OptExpression root) {
        collectAllPredicates(root);
        collectGroupByAndAggFunctions(root);
        collectScanOutputColumns(root);
        for (Integer scanId : traceRelationIds) {
            if (!isSPJQueries.containsKey(scanId) && !aggFunctions.containsKey(scanId) &&
                    !columnIdsInGrouping.containsKey(scanId)) {
                isSPJQueries.put(scanId, true);
            }
            if (!disableSPJGMVs.containsKey(scanId)) {
                disableSPJGMVs.put(scanId, false);
            }
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        this.factory = context.getColumnRefFactory();
        OptExpression optExpression = input;
        if (!isExistMVs(optExpression)) {
            return Lists.newArrayList(optExpression);
        }

        init(optExpression);

        for (LogicalOlapScanOperator scan : scanOperators) {
            int relationId = factory.getRelationId(scan.getOutputColumns().get(0).getId());
            // clear rewrite context since we are going to handle another scan operator.
            rewriteContexts.clear();
            Map<Long, List<Column>> candidateIndexIdToSchema = selectValidIndexes(scan, relationId);
            if (candidateIndexIdToSchema.isEmpty()) {
                continue;
            }

            long bestIndex = selectBestIndexes(scan, candidateIndexIdToSchema, relationId);

            if (bestIndex == scan.getSelectedIndexId()) {
                continue;
            }

            BestIndexRewriter bestIndexRewriter = new BestIndexRewriter(scan);
            optExpression = bestIndexRewriter.rewrite(optExpression, bestIndex);

            if (rewriteContexts.containsKey(bestIndex)) {
                List<RewriteContext> rewriteContext = rewriteContexts.get(bestIndex);
                List<RewriteContext> percentileContexts = rewriteContext.stream().
                        filter(e -> e.aggCall.getFnName().equals(FunctionSet.PERCENTILE_APPROX))
                        .collect(Collectors.toList());
                if (!percentileContexts.isEmpty()) {
                    MVProjectAggProjectScanRewrite.getInstance().rewriteOptExpressionTree(
                            factory, relationId, optExpression, percentileContexts);
                }
                rewriteContext.removeAll(percentileContexts);

                MaterializedViewRewriter rewriter = new MaterializedViewRewriter();
                for (MaterializedViewRule.RewriteContext rc : rewriteContext) {
                    optExpression = rewriter.rewrite(optExpression, rc);
                }
            }
        }
        return Lists.newArrayList(optExpression);
    }

    public static boolean isExistMVs(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            if (isExistMVs(child)) {
                return true;
            }
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            OlapTable olapTable = (OlapTable) scanOperator.getTable();
            return olapTable.hasMaterializedView();
        }
        return false;
    }

    private Map<Long, List<Column>> selectValidIndexes(LogicalOlapScanOperator scanOperator, int relationId) {
        OlapTable table = (OlapTable) scanOperator.getTable();
        Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta = table.getVisibleIndexIdToMeta();
        // Step2: check all columns in compensating predicates are available in the view output
        checkCompensatingPredicates(columnNameToIds.get(relationId), columnIdsInPredicates.get(relationId),
                candidateIndexIdToMeta);
        // Step3: group by list in query is the subset of group by list in view or view contains no aggregation
        checkGrouping(columnNameToIds.get(relationId), columnIdsInGrouping.get(relationId), candidateIndexIdToMeta,
                disableSPJGMVs.get(relationId), isSPJQueries.get(relationId));
        // Step4: aggregation functions are available in the view output
        checkAggregationFunction(columnNameToIds.get(relationId), aggFunctions.get(relationId), candidateIndexIdToMeta,
                disableSPJGMVs.get(relationId), isSPJQueries.get(relationId));
        // Step5: columns required to compute output expr are available in the view output
        checkOutputColumns(columnNameToIds.get(relationId), columnIdsInQueryOutput.get(relationId),
                candidateIndexIdToMeta);
        // Step6: if table type is aggregate and the candidateIndexIdToSchema is empty,
        if ((table.getKeysType() == KeysType.AGG_KEYS || table.getKeysType() == KeysType.UNIQUE_KEYS)
                && candidateIndexIdToMeta.size() == 0) {
            // the base index will be added in the candidateIndexIdToSchema.
            /*
             * In StarRocks, it is allowed that the aggregate table should be scanned directly
             * while there is no aggregation info in query.
             * For example:
             * Aggregate tableA: k1, k2, sum(v1)
             * Query: select k1, k2, v1 from tableA
             * Allowed
             * Result: same as select k1, k2, sum(v1) from tableA group by t1, t2
             *
             * However, the query should not be selector normally.
             * The reason is that the level of group by in tableA is upper then the level of group by in query.
             * So, we need to compensate those kinds of index in following step.
             *
             */
            compensateCandidateIndex(candidateIndexIdToMeta, table.getVisibleIndexIdToMeta(),
                    table);
            checkOutputColumns(columnNameToIds.get(relationId), columnIdsInQueryOutput.get(relationId),
                    candidateIndexIdToMeta);
        }
        Map<Long, List<Column>> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : candidateIndexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchema());
        }
        return result;
    }

    private Long selectBestIndexes(LogicalOlapScanOperator scanOperator,
                                   Map<Long, List<Column>> candidateIndexIdToSchema,
                                   int tableId) {
        // Step1: the candidate indexes that satisfies the most prefix index
        final Set<Integer> equivalenceColumns = Sets.newHashSet();
        final Set<Integer> unequivalenceColumns = Sets.newHashSet();
        collectColumns(scanOperator, equivalenceColumns, unequivalenceColumns);
        Set<Long> indexesMatchingBestPrefixIndex =
                matchBestPrefixIndex(
                        columnNameToIds.get(tableId),
                        candidateIndexIdToSchema, equivalenceColumns, unequivalenceColumns);

        // Step2: the best index that satisfies the least number of rows
        return selectBestRowCountIndex(indexesMatchingBestPrefixIndex, (OlapTable) scanOperator.getTable());
    }

    private void collectAllPredicates(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectAllPredicates(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalAggregationOperator) {
            return;
        }
        LogicalOperator logicalOperator = (LogicalOperator) operator;
        if (logicalOperator.getPredicate() != null) {
            ScalarOperator scalarOperator = logicalOperator.getPredicate();
            if (!scalarOperator.isConstantRef()) {
                updateTableToColumns(scalarOperator, columnIdsInPredicates);
            }
        }
        if (logicalOperator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.getOnPredicate() != null) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(joinOperator.getOnPredicate());
                for (ScalarOperator conjunct : conjuncts) {
                    updateTableToColumns(conjunct, columnIdsInPredicates);
                }
            }
        }
    }

    private void updateTableToColumns(ScalarOperator scalarOperator,
                                      Map<Integer, Set<Integer>> tableToColumns) {
        ColumnRefSet columns = scalarOperator.getUsedColumns();
        for (int columnId : columns.getColumnIds()) {
            int table = factory.getRelationId(columnId);
            if (table != -1) {
                if (tableToColumns.containsKey(table)) {
                    tableToColumns.get(table).add(columnId);
                } else {
                    tableToColumns.put(table, Sets.newHashSet(columnId));
                }
            }
        }
    }

    private void collectGroupByAndAggFunctions(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectGroupByAndAggFunctions(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            traceRelationIds.add(factory.getRelationId(scanOperator.getOutputColumns().get(0).getId()));
        }

        if (operator instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) operator;
            Operator childOperator = root.getInputs().get(0).getOp();
            if (!(childOperator instanceof LogicalProjectOperator) &&
                    !(childOperator instanceof LogicalOlapScanOperator) &&
                    !(childOperator instanceof LogicalRepeatOperator)) {
                return;
            }

            if (childOperator instanceof LogicalProjectOperator) {
                LogicalProjectOperator projectOperator = (LogicalProjectOperator) childOperator;
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectOperator.getColumnRefMap());
                List<ScalarOperator> newGroupBys = Lists.newArrayList();
                for (ColumnRefOperator groupBy : aggOperator.getGroupingKeys()) {
                    newGroupBys.add(rewriter.rewrite(groupBy));
                }

                List<CallOperator> newAggs = Lists.newArrayList();
                for (CallOperator agg : aggOperator.getAggregations().values()) {
                    // Must forbidden count(*) or count(1) for materialized view.
                    if (agg.getChildren().size() < 1 ||
                            (agg.getChildren().size() == 1 && agg.getChild(0).isConstantRef())) {
                        disableSPJGMaterializedView();
                        break;
                    }
                    newAggs.add((CallOperator) rewriter.rewrite(agg));
                }
                collectGroupByAndAggFunction(newGroupBys, newAggs);
            } else {
                collectGroupByAndAggFunction(Lists.newArrayList(aggOperator.getGroupingKeys()),
                        Lists.newArrayList(aggOperator.getAggregations().values()));
            }
        }
    }

    // Disable SPJG materialized view for traced scan node which not has aggregation
    private void disableSPJGMaterializedView() {
        for (Integer scanId : traceRelationIds) {
            if (!columnIdsInGrouping.containsKey(scanId) && !aggFunctions.containsKey(scanId)) {
                disableSPJGMVs.put(scanId, true);
            }
        }
    }

    // set table is not SPJ query
    private void disableSPJQueries(int table) {
        if (table != -1) {
            isSPJQueries.put(table, false);
        } else {
            for (Integer scanId : traceRelationIds) {
                if (!isSPJQueries.containsKey(scanId)) {
                    isSPJQueries.put(scanId, false);
                }
            }
        }
    }

    private void collectGroupByAndAggFunction(List<ScalarOperator> groupBys,
                                              List<CallOperator> aggs) {
        if (groupBys.stream().map(ScalarOperator::getUsedColumns).anyMatch(columnIdsInAggregate::isIntersect) ||
                aggs.stream().map(ScalarOperator::getUsedColumns).anyMatch(columnIdsInAggregate::isIntersect)) {
            // Has been collect from other aggregate, only check aggregate node which is closest to scan node
            return;
        }

        for (ScalarOperator groupBy : groupBys) {
            ColumnRefSet columns = groupBy.getUsedColumns();
            for (int columnId : columns.getColumnIds()) {
                int table = factory.getRelationId(columnId);
                if (table != -1) {
                    if (columnIdsInGrouping.containsKey(table)) {
                        columnIdsInGrouping.get(table).add(columnId);
                    } else {
                        columnIdsInGrouping.put(table, Sets.newHashSet(columnId));
                    }
                }
                // This table has group by, disable isSPJQuery
                disableSPJQueries(table);
            }
        }

        for (CallOperator agg : aggs) {
            ColumnRefSet columns = agg.getUsedColumns();
            for (int columnId : columns.getColumnIds()) {
                int table = factory.getRelationId(columnId);
                if (table != -1) {
                    if (aggFunctions.containsKey(table)) {
                        aggFunctions.get(table).add(agg);
                    } else {
                        aggFunctions.put(table, Sets.newHashSet(agg));
                    }
                }
                // This table has aggregation, disable isSPJQuery
                disableSPJQueries(table);
            }
        }

        groupBys.stream().map(ScalarOperator::getUsedColumns).forEach(columnIdsInAggregate::union);
        aggs.stream().map(ScalarOperator::getUsedColumns).forEach(columnIdsInAggregate::union);
    }

    public void collectColumns(LogicalOlapScanOperator scanOperator,
                               Set<Integer> equivalenceColumns,
                               Set<Integer> unEquivalenceColumns) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(scanOperator.getPredicate());
        // 1. Get columns which has predicate on it.
        for (ScalarOperator operator : conjuncts) {
            if (!MVUtils.isPredicateUsedForPrefixIndex(operator)) {
                continue;
            }
            if (MVUtils.isEquivalencePredicate(operator)) {
                equivalenceColumns.add(operator.getUsedColumns().getFirstId());
            } else {
                unEquivalenceColumns.add(operator.getUsedColumns().getFirstId());
            }
        }

        // TODO(kks): 2. Equal join predicates when pushing inner child.
    }

    private void collectScanOutputColumns(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectScanOutputColumns(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            scanOperators.add(scanOperator);

            int tableId = factory.getRelationId(scanOperator.getOutputColumns().get(0).getId());
            Map<String, Integer> nameToIDs = new HashMap<>();
            if (!columnNameToIds.containsKey(tableId)) {
                columnNameToIds.put(tableId, nameToIDs);
            }

            for (Map.Entry<Column, ColumnRefOperator> entry : scanOperator.getColumnMetaToColRefMap().entrySet()) {
                nameToIDs.put(entry.getKey().getName(), entry.getValue().getId());
            }

            for (ColumnRefOperator column : scanOperator.getColRefToColumnMetaMap().keySet()) {
                updateTableToColumns(column, columnIdsInQueryOutput);
            }
        }
    }

    private long selectBestRowCountIndex(Set<Long> indexesMatchingBestPrefixIndex, OlapTable olapTable) {
        long minRowCount = Long.MAX_VALUE;
        long selectedIndexId = 0;
        for (Long indexId : indexesMatchingBestPrefixIndex) {
            long rowCount = 0;
            for (Partition partition : olapTable.getPartitions()) {
                rowCount += partition.getIndex(indexId).getRowCount();
            }
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selectedIndexId = indexId;
            } else if (rowCount == minRowCount) {
                // check column number, select one minimum column number
                int selectedColumnSize = olapTable.getSchemaByIndexId(selectedIndexId).size();
                int currColumnSize = olapTable.getSchemaByIndexId(indexId).size();
                if (currColumnSize < selectedColumnSize) {
                    selectedIndexId = indexId;
                }
            }
        }
        return selectedIndexId;
    }

    private Set<Long> matchBestPrefixIndex(
            Map<String, Integer> columnToIds,
            Map<Long, List<Column>> candidateIndexIdToSchema,
            Set<Integer> equivalenceColumns,
            Set<Integer> unequivalenceColumns) {
        if (equivalenceColumns.size() == 0 && unequivalenceColumns.size() == 0) {
            return candidateIndexIdToSchema.keySet();
        }
        Set<Long> indexesMatchingBestPrefixIndex = Sets.newHashSet();
        int maxPrefixMatchCount = 0;
        for (Map.Entry<Long, List<Column>> entry : candidateIndexIdToSchema.entrySet()) {
            int prefixMatchCount = 0;
            long indexId = entry.getKey();
            List<Column> indexSchema = entry.getValue();
            for (Column col : indexSchema) {
                Integer columnId = columnToIds.get(col.getName());
                if (equivalenceColumns.contains(columnId)) {
                    prefixMatchCount++;
                } else if (unequivalenceColumns.contains(columnId)) {
                    // Unequivalence predicate's columns can match only first column in rollup.
                    prefixMatchCount++;
                    break;
                } else {
                    break;
                }
            }

            if (prefixMatchCount == maxPrefixMatchCount) {
                indexesMatchingBestPrefixIndex.add(indexId);
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                maxPrefixMatchCount = prefixMatchCount;
                indexesMatchingBestPrefixIndex.clear();
                indexesMatchingBestPrefixIndex.add(indexId);
            }
        }
        return indexesMatchingBestPrefixIndex;
    }

    private void checkCompensatingPredicates(
            Map<String, Integer> columnToIds,
            Set<Integer> columnsInPredicates, Map<Long, MaterializedIndexMeta>
                    candidateIndexIdToMeta) {
        // When the query statement does not contain any columns in predicates, all candidate index can pass this check
        if (columnsInPredicates == null) {
            return;
        }
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<Integer> indexNonAggregatedColumns = Sets.newHashSet();
            entry.getValue().getSchema().stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexNonAggregatedColumns.add(columnToIds.get(column.getName())));
            if (!indexNonAggregatedColumns.containsAll(columnsInPredicates)) {
                iterator.remove();
            }
        }
    }

    /**
     * View      Query        result
     * SPJ       SPJG OR SPJ  pass
     * SPJG      SPJ          fail
     * SPJG      SPJG         pass
     * 1. grouping columns in query is subset of grouping columns in view
     * 2. the empty grouping columns in query is subset of all of views
     */
    private void checkGrouping(
            Map<String, Integer> columnToIds,
            Set<Integer> columnsInGrouping, Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta,
            boolean disableSPJGMV, boolean isSPJQuery) {
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            Set<Integer> indexNonAggregatedColumns = Sets.newHashSet();
            MaterializedIndexMeta candidateIndexMeta = entry.getValue();
            List<Column> candidateIndexSchema = candidateIndexMeta.getSchema();
            candidateIndexSchema.stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexNonAggregatedColumns.add(columnToIds.get(column.getName())));
            /*
            If there is no aggregated column in duplicate index, the index will be SPJ.
            For example:
                duplicate table (k1, k2, v1)
                duplicate mv index (k1, v1)
            When the candidate index is SPJ type, it passes the verification directly

            If there is no aggregated column in aggregate index, the index will be deduplicate index.
            For example:
                duplicate table (k1, k2, v1 sum)
                aggregate mv index (k1, k2)
            This kind of index is SPJG which same as select k1, k2 from aggregate_table group by k1, k2.
            It also need to check the grouping column using following steps.

            ISSUE-3016, MaterializedViewFunctionTest: testDeduplicateQueryInAgg
             */
            if (indexNonAggregatedColumns.size() == candidateIndexSchema.size()
                    && candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
            if (disableSPJGMV || isSPJQuery) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            // The grouping columns in query is empty. For example: select sum(A) from T
            if (columnsInGrouping == null) {
                continue;
            }
            // The grouping columns in query must be subset of the grouping columns in view
            if (!indexNonAggregatedColumns.containsAll(columnsInGrouping)) {
                iterator.remove();
            }
        }
    }

    private void checkAggregationFunction(Map<String, Integer> columnToIds,
                                          Set<CallOperator> aggregatedColumnsInQueryOutput,
                                          Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta,
                                          boolean disableSPJGMV, boolean isSPJQuery) {
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            MaterializedIndexMeta candidateIndexMeta = entry.getValue();
            List<CallOperator> indexAggColumnExprList = mvAggColumnsToExprList(columnToIds, candidateIndexMeta);
            // When the candidate index is SPJ type, it passes the verification directly
            if (indexAggColumnExprList.size() == 0 && candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
            if (disableSPJGMV || isSPJQuery) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            /* Situation1: The query is deduplicate SPJG when aggregatedColumnsInQueryOutput is null.
             * For example: select a , b from table group by a, b
             * The aggregation function check should be pass directly when MV is SPJG.
             */
            if (aggregatedColumnsInQueryOutput == null) {
                continue;
            }
            keyColumnsToExprList(columnToIds, candidateIndexMeta, indexAggColumnExprList);
            // The aggregated columns in query output must be subset of the aggregated columns in view
            if (!aggFunctionsMatchAggColumns(columnToIds, candidateIndexMeta, entry.getKey(),
                    aggregatedColumnsInQueryOutput, indexAggColumnExprList)) {
                iterator.remove();
            }
        }
    }

    private void checkOutputColumns(
            Map<String, Integer> columnToIds,
            Set<Integer> columnNamesInQueryOutput,
            Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta) {
        Preconditions.checkState(columnNamesInQueryOutput != null);
        Iterator<Map.Entry<Long, MaterializedIndexMeta>> iterator = candidateIndexIdToMeta.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MaterializedIndexMeta> entry = iterator.next();
            if (rewriteContexts.containsKey(entry.getKey())) {
                continue;
            }
            Set<Integer> indexColumns = Sets.newHashSet();
            List<Column> candidateIndexSchema = entry.getValue().getSchema();
            candidateIndexSchema.forEach(column -> indexColumns.add(columnToIds.get(column.getName())));
            // The columns in query output must be subset of the columns in SPJ view
            if (!indexColumns.containsAll(columnNamesInQueryOutput)) {
                iterator.remove();
            }
        }
    }

    private void compensateCandidateIndex(Map<Long, MaterializedIndexMeta> candidateIndexIdToMeta, Map<Long,
            MaterializedIndexMeta> allVisibleIndexes, OlapTable table) {
        int keySizeOfBaseIndex = table.getKeyColumnsByIndexId(table.getBaseIndexId()).size();
        for (Map.Entry<Long, MaterializedIndexMeta> index : allVisibleIndexes.entrySet()) {
            long mvIndexId = index.getKey();
            if (table.getKeyColumnsByIndexId(mvIndexId).size() == keySizeOfBaseIndex) {
                candidateIndexIdToMeta.put(mvIndexId, index.getValue());
            }
        }
    }

    private List<CallOperator> mvAggColumnsToExprList(Map<String, Integer> columnToIds, MaterializedIndexMeta mvMeta) {
        List<CallOperator> result = Lists.newArrayList();
        List<Column> schema = mvMeta.getSchema();
        for (Column column : schema) {
            if (column.isAggregated()) {
                ColumnRefOperator columnRef = factory.getColumnRef(columnToIds.get(column.getName()));
                CallOperator fn = new CallOperator(column.getAggregationType().name().toLowerCase(),
                        column.getType(),
                        Lists.newArrayList(columnRef));
                result.add(fn);
            }
        }
        return result;
    }

    private void keyColumnsToExprList(Map<String, Integer> columnToIds, MaterializedIndexMeta mvMeta,
                                      List<CallOperator> result) {
        for (Column column : mvMeta.getSchema()) {
            if (!column.isAggregated()) {
                ColumnRefOperator columnRef = factory.getColumnRef(columnToIds.get(column.getName()));
                for (String function : KEY_COLUMN_FUNCTION_NAMES) {
                    CallOperator fn = new CallOperator(function,
                            column.getType(),
                            Lists.newArrayList(columnRef));
                    result.add(fn);
                }
            }
        }
    }

    private boolean aggFunctionsMatchAggColumns(Map<String, Integer> columnToIds,
                                                MaterializedIndexMeta candidateIndexMeta,
                                                Long indexId, Set<CallOperator> queryExprList,
                                                List<CallOperator> mvColumnExprList) {

        ColumnRefSet aggregateColumns = new ColumnRefSet();
        ColumnRefSet keyColumns = new ColumnRefSet();
        for (Column column : candidateIndexMeta.getSchema()) {
            if (!column.isAggregated()) {
                keyColumns.union(factory.getColumnRef(columnToIds.get(column.getName())));
            } else {
                aggregateColumns.union(factory.getColumnRef(columnToIds.get(column.getName())));
            }
        }

        /*
        If all the columns in the query are key column and materialized view is AGG_KEYS.
        We should skip this query for given materialized view.
        ISSUE-6984: https://github.com/StarRocks/starrocks/issues/6984
        */
        if (candidateIndexMeta.getKeysType() == KeysType.AGG_KEYS && !queryExprList.stream()
                .filter(queryExpr -> {
                    String fnName = queryExpr.getFnName();
                    return !(fnName.equals(FunctionSet.COUNT) || fnName.equals(FunctionSet.SUM))
                            || !keyColumns.containsAll(queryExpr.getUsedColumns());
                }).findAny().isPresent()) {
            return false;
        }

        for (CallOperator queryExpr : queryExprList) {
            boolean match = false;
            for (CallOperator mvExpr : mvColumnExprList) {
                if (isMVMatchAggFunctions(indexId, queryExpr, mvExpr, mvColumnExprList, keyColumns, aggregateColumns)) {
                    match = true;
                    break;
                }
            }

            if (!match) {
                return false;
            }
        }
        return true;
    }

    private static final ImmutableSetMultimap<String, String> COLUMN_AGG_TYPE_MATCH_FN_NAME;

    static {
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.put(FunctionSet.SUM, FunctionSet.SUM);
        builder.put(FunctionSet.MAX, FunctionSet.MAX);
        builder.put(FunctionSet.MIN, FunctionSet.MIN);
        builder.put(FunctionSet.SUM, FunctionSet.COUNT);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION_COUNT);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.MULTI_DISTINCT_COUNT);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION_AGG);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.HLL_RAW_AGG);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.NDV);
        builder.put(FunctionSet.HLL_UNION, FunctionSet.APPROX_COUNT_DISTINCT);
        builder.put(FunctionSet.PERCENTILE_UNION, FunctionSet.PERCENTILE_UNION);
        builder.put(FunctionSet.PERCENTILE_UNION, FunctionSet.PERCENTILE_APPROX);
        COLUMN_AGG_TYPE_MATCH_FN_NAME = builder.build();
    }

    public boolean isMVMatchAggFunctions(Long indexId, CallOperator queryFn, CallOperator mvColumnFn,
                                         List<CallOperator> mvColumnExprList, ColumnRefSet keyColumns,
                                         ColumnRefSet aggregateColumns) {
        String queryFnName = queryFn.getFnName();
        if (queryFn.getFnName().equals(FunctionSet.COUNT) && queryFn.isDistinct()) {
            queryFnName = FunctionSet.MULTI_DISTINCT_COUNT;
        }

        if (!COLUMN_AGG_TYPE_MATCH_FN_NAME.get(mvColumnFn.getFnName())
                .contains(queryFnName)) {
            return false;
        }

        if (!queryFn.getFnName().equals(FunctionSet.COUNT) &&
                queryFn.isDistinct() != mvColumnFn.isDistinct()) {
            return false;
        }
        ScalarOperator queryFnChild0 = queryFn.getChild(0);
        // select x1, sum(cast(x2 as tinyint)) from table;
        // sum(x2) result of materialized view maybe greater x2, like many x2 = 100, cast(part_sum(x2) as tinyint) maybe will
        // overflow, but x2(100) isn't overflow
        if (queryFnChild0 instanceof CastOperator && (queryFnChild0.getType().isDecimalOfAnyVersion() ||
                queryFnChild0.getType().isFloatingPointType() || (queryFnChild0.getChild(0).getType().isNumericType() &&
                queryFnChild0.getType().getTypeSize() >= queryFnChild0.getChild(0).getType().getTypeSize()))) {
            queryFnChild0 = queryFnChild0.getChild(0);
        }
        ScalarOperator mvColumnFnChild0 = mvColumnFn.getChild(0);

        if (!queryFnChild0.isColumnRef()) {
            IsNoCallChildrenValidator validator = new IsNoCallChildrenValidator(keyColumns, aggregateColumns);
            if (!(isSupportScalarOperator(queryFnChild0) && queryFnChild0.accept(validator, null))) {
                ColumnRefOperator mvColumnRef = factory.getColumnRef(mvColumnFnChild0.getUsedColumns().getFirstId());
                Column mvColumn = factory.getColumn(mvColumnRef);
                if (mvColumn.getDefineExpr() != null && mvColumn.getDefineExpr() instanceof FunctionCallExpr &&
                        queryFnChild0 instanceof CallOperator) {
                    CallOperator queryCall = (CallOperator) queryFnChild0;
                    String mvName = ((FunctionCallExpr) mvColumn.getDefineExpr()).getFnName().getFunction();
                    String queryName = queryCall.getFnName();

                    if (!mvName.equalsIgnoreCase(queryName)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        if (queryFnChild0.getUsedColumns().equals(mvColumnFnChild0.getUsedColumns())) {
            return true;
        } else if (isSupportScalarOperator(queryFnChild0)) {
            int[] queryColumnIds = queryFnChild0.getUsedColumns().getColumnIds();
            Set<Integer> mvColumnIdSet = mvColumnExprList.stream()
                    .map(u -> u.getUsedColumns().getFirstId())
                    .collect(Collectors.toSet());
            for (int queryColumnId : queryColumnIds) {
                if (!mvColumnIdSet.contains(queryColumnId)) {
                    return false;
                }
            }
            return true;
        } else {
            ColumnRefOperator queryColumnRef = factory.getColumnRef(queryFnChild0.getUsedColumns().getFirstId());
            Column queryColumn = factory.getColumn(queryColumnRef);
            ColumnRefOperator mvColumnRef = factory.getColumnRef(mvColumnFnChild0.getUsedColumns().getFirstId());
            Column mvColumn = factory.getColumn(mvColumnRef);
            if (factory.getRelationId(queryColumnRef.getId()).equals(factory.getRelationId(mvColumnRef.getId()))) {
                if (mvColumn.getAggregationType() == null) {
                    return false;
                }
                String mvColumnName = MVUtils.getMVColumnName(mvColumn, queryFnName, queryColumn.getName());
                if (mvColumnName.equalsIgnoreCase(mvColumn.getName())) {
                    if (!rewriteContexts.containsKey(indexId)) {
                        rewriteContexts.put(indexId, Lists.newArrayList());
                    }
                    rewriteContexts.get(indexId).add(new RewriteContext(
                            queryFn, queryColumnRef, mvColumnRef, mvColumn));
                    return true;
                }
            }
        }
        return false;
    }

    public static class RewriteContext {
        ColumnRefOperator queryColumnRef;
        ColumnRefOperator mvColumnRef;
        Column mvColumn;
        CallOperator aggCall;

        RewriteContext(CallOperator aggCall,
                       ColumnRefOperator queryColumnRef,
                       ColumnRefOperator mvColumnRef,
                       Column mvColumn) {
            this.aggCall = aggCall;
            this.queryColumnRef = queryColumnRef;
            this.mvColumnRef = mvColumnRef;
            this.mvColumn = mvColumn;
        }
    }

    private boolean isSupportScalarOperator(ScalarOperator operator) {
        if (operator instanceof CaseWhenOperator) {
            return true;
        }

        return operator instanceof CallOperator &&
                FunctionSet.IF.equalsIgnoreCase(((CallOperator) operator).getFnName());
    }
}
