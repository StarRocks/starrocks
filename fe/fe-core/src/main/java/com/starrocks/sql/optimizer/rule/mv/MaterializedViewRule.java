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
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
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
import org.apache.commons.collections4.map.CaseInsensitiveMap;

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

    // each table relation id -> set<column ids>
    private final Map<Integer, Set<Integer>> queryRelIdToColumnIdsInPredicates = Maps.newHashMap();
    // each table relation id -> set<group by column ids>
    private final Map<Integer, Set<Integer>> queryRelIdToGroupByIds = Maps.newHashMap();
    // each table relation id -> set<agg functions>
    private final Map<Integer, Set<CallOperator>> queryRelIdToAggFunctions = Maps.newHashMap();
    // each table relation id -> set<aggregate column ids>
    private final ColumnRefSet queryRelIdToAggregateIds = new ColumnRefSet();
    // each table relation id -> set<column name -> column ref id>
    private final Map<Integer, Set<Integer>> queryRelIdToScanNodeOutputColumnIds = Maps.newHashMap();
    // each table relation id -> map<output column ids>
    private final Map<Integer, Map<String, Integer>> queryRelIdToColumnNameIds = Maps.newHashMap();
    // each query scan operator
    private final List<LogicalOlapScanOperator> queryScanOperators = Lists.newArrayList();

    // record the relation id -> disableSPJGMV flag
    // this can be set to true when query has count(*) or count(1)
    private final Map<Integer, Boolean> queryRelIdToEnableMVRewrite = Maps.newHashMap();
    // record the relation id -> isSPJQuery flag
    private final Map<Integer, Boolean> queryRelIdToIsSPJQuery = Maps.newHashMap();
    // record the scan node relation id which has been accessed.
    private final Set<Integer> visitedQueryRelIds = Sets.newHashSet();

    // mv index id -> each RewriteContext
    private final Map<Long, List<RewriteContext>> mvIdToRewriteContexts = Maps.newHashMap();
    // current optimize context factory
    private ColumnRefFactory factory;

    private void init(OptExpression root) {
        collectAllPredicates(root);
        collectGroupByAndAggFunctions(root);
        collectScanOutputColumns(root);
        for (Integer scanId : visitedQueryRelIds) {
            if (!queryRelIdToIsSPJQuery.containsKey(scanId) && !queryRelIdToAggFunctions.containsKey(scanId) &&
                    !queryRelIdToGroupByIds.containsKey(scanId)) {
                queryRelIdToIsSPJQuery.put(scanId, true);
            }
            if (!queryRelIdToEnableMVRewrite.containsKey(scanId)) {
                queryRelIdToEnableMVRewrite.put(scanId, false);
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

        if (queryScanOperators.stream().anyMatch(LogicalOlapScanOperator::hasTableHints)) {
            return Lists.newArrayList(optExpression);
        }

        for (LogicalOlapScanOperator scan : queryScanOperators) {
            int relationId = factory.getRelationId(scan.getOutputColumns().get(0).getId());
            // clear rewrite context since we are going to handle another scan operator.
            mvIdToRewriteContexts.clear();
            Map<Long, List<Column>> candidateIndexIdToSchema = selectValidMVs(scan, relationId);
            if (candidateIndexIdToSchema.isEmpty()) {
                continue;
            }

            long bestIndex = selectBestMV(scan, candidateIndexIdToSchema, relationId);
            if (bestIndex == scan.getSelectedIndexId()) {
                continue;
            }

            BestIndexRewriter bestIndexRewriter = new BestIndexRewriter(scan);
            optExpression = bestIndexRewriter.rewrite(optExpression, bestIndex);

            if (mvIdToRewriteContexts.containsKey(bestIndex)) {
                List<RewriteContext> rewriteContext = mvIdToRewriteContexts.get(bestIndex);
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

    private Map<Long, List<Column>> selectValidMVs(LogicalOlapScanOperator scanOperator, int relationId) {
        OlapTable table = (OlapTable) scanOperator.getTable();

        List<MaterializedIndexMeta> candidateIndexIdToMeta = table.getVisibleIndexMetas();
        // column name -> column id
        Map<String, Integer> queryScanNodeColumnNameToIds = queryRelIdToColumnNameIds.get(relationId);

        Iterator<MaterializedIndexMeta> iterator = candidateIndexIdToMeta.iterator();
        // Consider all IndexMetas of the base table so can be decided by cost strategy for better performance.
        // eg:
        // tbl1     : <a int, b  string, c string> and column `a` is short key
        // tbl1_mv  : select b, a, c from tbl1 and column `b` is short key
        // Q1: select * from tbl1 where a = 1, should choose tbl1
        // Q2: select * from tbl1 where b = 'a', should choose tbl1_mv
        while (iterator.hasNext()) {
            MaterializedIndexMeta mvMeta = iterator.next();
            long mvIdx = mvMeta.getIndexId();

            // Ignore indexes which cannot be remapping with query by column names.
            List<Column> mvNonAggregatedColumns = mvMeta.getNonAggregatedColumns();
            if (mvNonAggregatedColumns.stream().anyMatch(x -> !queryScanNodeColumnNameToIds.containsKey(x.getName()))) {
                iterator.remove();
                continue;
            }

            // Check whether contains complex expressions, MV with Complex expressions will be used
            // to rewrite query by AggregatedMaterializedViewRewriter.
            if (MVUtils.containComplexExpresses(mvMeta)) {
                iterator.remove();
                continue;
            }

            // Step2: check all columns in compensating predicates are available in the view output
            if (!checkCompensatingPredicates(queryScanNodeColumnNameToIds,
                    queryRelIdToColumnIdsInPredicates.get(relationId), mvMeta)) {
                iterator.remove();
                continue;
            }

            // Step3: group by list in query is the subset of group by list in view or view contains no aggregation
            if (!checkGrouping(queryScanNodeColumnNameToIds, queryRelIdToGroupByIds.get(relationId), mvMeta,
                    queryRelIdToEnableMVRewrite.get(relationId), queryRelIdToIsSPJQuery.get(relationId))) {
                iterator.remove();
                continue;
            }

            // Step4: aggregation functions are available in the view output
            if (!checkAggregationFunction(queryScanNodeColumnNameToIds, queryRelIdToAggFunctions.get(relationId),
                    mvIdx, mvMeta, queryRelIdToEnableMVRewrite.get(relationId), queryRelIdToIsSPJQuery.get(relationId))) {
                iterator.remove();
                continue;
            }

            // Step5: columns required to compute output expr are available in the view output
            if (!checkOutputColumns(queryScanNodeColumnNameToIds, queryRelIdToScanNodeOutputColumnIds.get(relationId),
                    mvIdx, mvMeta)) {
                iterator.remove();
            }
        }

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
            compensateCandidateIndex(candidateIndexIdToMeta, table.getVisibleIndexMetas(),
                    table);

            iterator = candidateIndexIdToMeta.iterator();
            while (iterator.hasNext()) {
                MaterializedIndexMeta mvMeta = iterator.next();
                Long mvIdx = mvMeta.getIndexId();

                if (!checkOutputColumns(queryRelIdToColumnNameIds.get(relationId),
                        queryRelIdToScanNodeOutputColumnIds.get(relationId), mvIdx, mvMeta)) {
                    iterator.remove();
                }
            }
        }

        Map<Long, List<Column>> result = Maps.newHashMap();
        for (MaterializedIndexMeta indexMeta : candidateIndexIdToMeta) {
            result.put(indexMeta.getIndexId(), indexMeta.getSchema());
        }
        return result;
    }

    private Long selectBestMV(LogicalOlapScanOperator scanOperator,
                              Map<Long, List<Column>> candidateIndexIdToSchema,
                              int tableId) {
        // Step1: the candidate indexes that satisfies the most prefix index
        final Set<Integer> equivalenceColumns = Sets.newHashSet();
        final Set<Integer> unEquivalenceColumns = Sets.newHashSet();
        splitScanConjuncts(scanOperator, equivalenceColumns, unEquivalenceColumns);

        Set<Long> indexesMatchingBestPrefixIndex =
                matchBestPrefixIndex(
                        queryRelIdToColumnNameIds.get(tableId),
                        candidateIndexIdToSchema, equivalenceColumns, unEquivalenceColumns);

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
                updateTableToColumns(scalarOperator, queryRelIdToColumnIdsInPredicates);
            }
        }
        if (logicalOperator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.getOnPredicate() != null) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(joinOperator.getOnPredicate());
                for (ScalarOperator conjunct : conjuncts) {
                    updateTableToColumns(conjunct, queryRelIdToColumnIdsInPredicates);
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
                tableToColumns.computeIfAbsent(table, k -> Sets.newHashSet()).add(columnId);
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
            visitedQueryRelIds.add(factory.getRelationId(scanOperator.getOutputColumns().get(0).getId()));
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
        for (Integer scanId : visitedQueryRelIds) {
            if (!queryRelIdToGroupByIds.containsKey(scanId) && !queryRelIdToAggFunctions.containsKey(scanId)) {
                queryRelIdToEnableMVRewrite.put(scanId, true);
            }
        }
    }

    // set table is not SPJ query
    private void disableSPJQueries(int table) {
        if (table != -1) {
            queryRelIdToIsSPJQuery.put(table, false);
        } else {
            for (Integer scanId : visitedQueryRelIds) {
                if (!queryRelIdToIsSPJQuery.containsKey(scanId)) {
                    queryRelIdToIsSPJQuery.put(scanId, false);
                }
            }
        }
    }

    private void collectGroupByAndAggFunction(List<ScalarOperator> groupBys,
                                              List<CallOperator> aggs) {
        if (groupBys.stream().map(ScalarOperator::getUsedColumns).anyMatch(queryRelIdToAggregateIds::isIntersect) ||
                aggs.stream().map(ScalarOperator::getUsedColumns).anyMatch(queryRelIdToAggregateIds::isIntersect)) {
            // Has been collect from other aggregate, only check aggregate node which is closest to scan node
            return;
        }

        for (ScalarOperator groupBy : groupBys) {
            ColumnRefSet columns = groupBy.getUsedColumns();
            for (int columnId : columns.getColumnIds()) {
                int table = factory.getRelationId(columnId);
                if (table != -1) {
                    if (queryRelIdToGroupByIds.containsKey(table)) {
                        queryRelIdToGroupByIds.get(table).add(columnId);
                    } else {
                        queryRelIdToGroupByIds.put(table, Sets.newHashSet(columnId));
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
                    if (queryRelIdToAggFunctions.containsKey(table)) {
                        queryRelIdToAggFunctions.get(table).add(agg);
                    } else {
                        queryRelIdToAggFunctions.put(table, Sets.newHashSet(agg));
                    }
                }
                // This table has aggregation, disable isSPJQuery
                disableSPJQueries(table);
            }
        }

        groupBys.stream().map(ScalarOperator::getUsedColumns).forEach(queryRelIdToAggregateIds::union);
        aggs.stream().map(ScalarOperator::getUsedColumns).forEach(queryRelIdToAggregateIds::union);
    }

    // split scan operators' conjuncts into equal and non-equal column ids
    public void splitScanConjuncts(LogicalOlapScanOperator scanOperator,
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
            queryScanOperators.add(scanOperator);

            int tableId = factory.getRelationId(scanOperator.getOutputColumns().get(0).getId());
            scanOperator.getColumnMetaToColRefMap().entrySet()
                    .stream()
                    .forEach(x -> queryRelIdToColumnNameIds
                            .computeIfAbsent(tableId, k -> new CaseInsensitiveMap())
                            .put(x.getKey().getName(), x.getValue().getId()));
            scanOperator.getColRefToColumnMetaMap().keySet()
                    .stream()
                    .forEach(x -> updateTableToColumns(x, queryRelIdToScanNodeOutputColumnIds));
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

    // Map MV's column to query's column id.
    private int getMVColumnToQueryColumnId(Map<String, Integer> columnToIds, Column mvColumn) {
        // Assume sync mv's column names are mapping with query scan node's columns by column name.
        // We can find the according query column id by the mv column name.
        // Once mv's column name is not the same with scan node, how can we do it?
        List<SlotRef> baseColumnRefs = mvColumn.getRefColumns();
        // To be compatible with old policy, remove this later.
        if (baseColumnRefs == null) {
            return columnToIds.get(mvColumn.getName());
        }
        Preconditions.checkState(baseColumnRefs.size() == 1);
        return columnToIds.get(baseColumnRefs.get(0).getColumnName());
    }

    private Set<Long> matchBestPrefixIndex(
            Map<String, Integer> columnToIds,
            Map<Long, List<Column>> candidateIndexIdToSchema,
            Set<Integer> equivalenceColumns,
            Set<Integer> unEquivalenceColumns) {
        if (equivalenceColumns.size() == 0 && unEquivalenceColumns.size() == 0) {
            return candidateIndexIdToSchema.keySet();
        }

        Set<Long> indexesMatchingBestPrefixIndex = Sets.newHashSet();
        int maxPrefixMatchCount = 0;
        for (Map.Entry<Long, List<Column>> entry : candidateIndexIdToSchema.entrySet()) {
            long indexId = entry.getKey();
            List<Column> indexSchema = entry.getValue();

            int prefixMatchCount = 0;
            for (Column col : indexSchema) {
                Integer columnId = getMVColumnToQueryColumnId(columnToIds, col);
                if (equivalenceColumns.contains(columnId)) {
                    prefixMatchCount++;
                } else if (unEquivalenceColumns.contains(columnId)) {
                    // UnEquivalence predicate's columns can only match first columns in rollup.
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

    private boolean checkCompensatingPredicates(
            Map<String, Integer> queryScanColumnNameToIds,
            Set<Integer> columnsInPredicates,
            MaterializedIndexMeta mvMeta) {
        // When the query statement does not contain any columns in predicates, all candidate index can pass this check
        if (columnsInPredicates == null) {
            return true;
        }

        List<Column> mvNonAggregatedColumns = mvMeta.getNonAggregatedColumns();
        Set<Integer> indexNonAggregatedColumns = Sets.newHashSet();
        mvNonAggregatedColumns
                .forEach(col -> indexNonAggregatedColumns.add(getMVColumnToQueryColumnId(queryScanColumnNameToIds, col)));
        if (!indexNonAggregatedColumns.containsAll(columnsInPredicates)) {
            return false;
        }
        return true;
    }

    /**
     * View      Query        result
     * SPJ       SPJG OR SPJ  pass
     * SPJG      SPJ          fail
     * SPJG      SPJG         pass
     * 1. grouping columns in query is subset of grouping columns in view
     * 2. the empty grouping columns in query is subset of all of views
     */
    private boolean checkGrouping(Map<String, Integer> queryScanColumnNameToIds,
                                  Set<Integer> queryGroupingIds,
                                  MaterializedIndexMeta mvMeta,
                                  boolean disableSPJGMV,
                                  boolean isSPJQuery) {
        List<Column> mvNonAggregatedColumns = mvMeta.getNonAggregatedColumns();
        // Remap mv's non aggregated columns to query based.
        Set<Integer> mvNonAggregatedColumnsBasedQuery = Sets.newHashSet();
        mvNonAggregatedColumns.forEach(column ->
                mvNonAggregatedColumnsBasedQuery.add(getMVColumnToQueryColumnId(queryScanColumnNameToIds, column)));

        // If there is no aggregated column in duplicate index, the index will be SPJ.
        // For example:
        //     duplicate table (k1, k2, v1)
        //     duplicate mv index (k1, v1)
        // When the candidate index is SPJ type, it passes the verification directly
        //
        // If there is no aggregated column in aggregate index, the index will be deduplicate index.
        // For example:
        //     duplicate table (k1, k2, v1 sum)
        //     aggregate mv index (k1, k2)
        // This kind of index is SPJG which same as select k1, k2 from aggregate_table group by k1, k2.
        // It also need to check the grouping column using following steps.
        //
        // ISSUE-3016, MaterializedViewFunctionTest: testDeduplicateQueryInAgg
        List<Column> mvMetaSchema = mvMeta.getSchema();
        if (mvNonAggregatedColumns.size() == mvMetaSchema.size()
                && mvMeta.getKeysType() == KeysType.DUP_KEYS) {
            return true;
        }
        // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
        if (disableSPJGMV || isSPJQuery) {
            return false;
        }
        // The query is SPJG. The candidate index is SPJG too.
        // The grouping columns in query is empty. For example: select sum(A) from T
        if (queryGroupingIds == null) {
            return true;
        }
        // The grouping columns in query must be subset of the grouping columns in view
        if (!mvNonAggregatedColumnsBasedQuery.containsAll(queryGroupingIds)) {
            return false;
        }
        return true;
    }

    private boolean checkAggregationFunction(Map<String, Integer> columnToIds,
                                             Set<CallOperator> aggregatedColumnsInQueryOutput,
                                             Long mvIdx,
                                             MaterializedIndexMeta candidateIndexMeta,
                                             boolean disableSPJGMV, boolean isSPJQuery) {
        List<CallOperator> indexAggColumnExprList = mvAggColumnsToExprList(columnToIds, candidateIndexMeta);
        // When the candidate index is SPJ type, it passes the verification directly
        if (indexAggColumnExprList.size() == 0 && candidateIndexMeta.getKeysType() == KeysType.DUP_KEYS) {
            return true;
        }
        // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
        if (disableSPJGMV || isSPJQuery) {
            return false;
        }
        // The query is SPJG. The candidate index is SPJG too.
        /* Situation1: The query is deduplicate SPJG when aggregatedColumnsInQueryOutput is null.
         * For example: select a , b from table group by a, b
         * The aggregation function check should be pass directly when MV is SPJG.
         */
        if (aggregatedColumnsInQueryOutput == null) {
            return true;
        }
        keyColumnsToExprList(columnToIds, candidateIndexMeta, indexAggColumnExprList);

        // The aggregated columns in query output must be subset of the aggregated columns in view
        if (!aggFunctionsMatchAggColumns(columnToIds, candidateIndexMeta, mvIdx,
                aggregatedColumnsInQueryOutput, indexAggColumnExprList)) {
            return false;
        }
        return true;
    }

    private boolean checkOutputColumns(
            Map<String, Integer> columnToIds,
            Set<Integer> columnNamesInQueryOutput,
            Long mvIdx,
            MaterializedIndexMeta mvMeta) {
        Preconditions.checkState(columnNamesInQueryOutput != null);
        if (mvIdToRewriteContexts.containsKey(mvIdx)) {
            return true;
        }
        Set<Integer> indexColumns = Sets.newHashSet();
        List<Column> candidateIndexSchema = mvMeta.getSchema();
        candidateIndexSchema.forEach(column -> indexColumns.add(getMVColumnToQueryColumnId(columnToIds, column)));

        // The columns in query output must be subset of the columns in SPJ view
        if (!indexColumns.containsAll(columnNamesInQueryOutput)) {
            return false;
        }
        return true;
    }

    private void compensateCandidateIndex(List<MaterializedIndexMeta> candidateIndexIdToMetas,
                                          List<MaterializedIndexMeta> allVisibleIndexes,
                                          OlapTable table) {
        int keySizeOfBaseIndex = table.getKeyColumnsByIndexId(table.getBaseIndexId()).size();
        for (MaterializedIndexMeta index : allVisibleIndexes) {
            long mvIndexId = index.getIndexId();
            if (table.getKeyColumnsByIndexId(mvIndexId).size() == keySizeOfBaseIndex) {
                candidateIndexIdToMetas.add(index);
            }
        }
    }

    private List<CallOperator> mvAggColumnsToExprList(Map<String, Integer> columnToIds,
                                                      MaterializedIndexMeta mvMeta) {
        List<CallOperator> result = Lists.newArrayList();
        List<Column> schema = mvMeta.getSchema();
        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.get(i);
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
                int baseColumnId = getMVColumnToQueryColumnId(columnToIds, column);
                ColumnRefOperator columnRef = factory.getColumnRef(baseColumnId);
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
        Set<Integer> usedBaseColumnIds = Sets.newHashSet();
        for (Column column : candidateIndexMeta.getSchema()) {
            int baseColumnId = getMVColumnToQueryColumnId(columnToIds, column);
            usedBaseColumnIds.add(baseColumnId);
            ColumnRefOperator columnRef = factory.getColumnRef(baseColumnId);
            if (!column.isAggregated()) {
                keyColumns.union(columnRef);
            } else {
                aggregateColumns.union(columnRef);
            }
        }

        /*
        If all the columns in the query are key column and materialized view is AGG_KEYS.
        We should skip this query for given materialized view.
        ISSUE-6984: https://github.com/StarRocks/starrocks/issues/6984
        */
        if (candidateIndexMeta.getKeysType() == KeysType.AGG_KEYS && queryExprList.stream()
                .anyMatch(queryExpr -> {
                    String fnName = queryExpr.getFnName();
                    return (fnName.equals(FunctionSet.COUNT) || fnName.equals(FunctionSet.SUM))
                            && keyColumns.containsAll(queryExpr.getUsedColumns());
                })) {
            return false;
        }

        return queryExprList.stream()
                .allMatch(x -> canRewriteQueryAggFunc(x, mvColumnExprList, indexId,
                    keyColumns, aggregateColumns, usedBaseColumnIds));
    }

    private boolean canRewriteQueryAggFunc(CallOperator queryExpr,
                                           List<CallOperator> mvColumnExprList,
                                           Long indexId,
                                           ColumnRefSet keyColumns,
                                           ColumnRefSet aggregateColumns,
                                           Set<Integer> usedBaseColumnIds) {
        return mvColumnExprList.stream()
                .anyMatch(x -> isMVMatchAggFunctions(indexId, queryExpr, x, keyColumns,
                        aggregateColumns, usedBaseColumnIds));
    }

    private static final ImmutableSetMultimap<String, String> COLUMN_AGG_TYPE_MATCH_FN_NAME;

    static {
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.put(FunctionSet.SUM, FunctionSet.SUM);
        builder.put(FunctionSet.MAX, FunctionSet.MAX);
        builder.put(FunctionSet.MIN, FunctionSet.MIN);
        builder.put(FunctionSet.SUM, FunctionSet.COUNT);
        builder.put(FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION);
        builder.put(FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION_COUNT);
        builder.put(FunctionSet.BITMAP_AGG, FunctionSet.MULTI_DISTINCT_COUNT);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_AGG);
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

    public boolean isMVMatchAggFunctions(Long indexId, CallOperator queryFn,
                                         CallOperator mvColumnFn, ColumnRefSet keyColumns,
                                         ColumnRefSet aggregateColumns, Set<Integer> usedBaseColumnIds) {
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
        }

        if (isSupportScalarOperator(queryFnChild0)) {
            int[] queryColumnIds = queryFnChild0.getUsedColumns().getColumnIds();
            Set<Integer> mvColumnIdSet = usedBaseColumnIds.stream()
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
                String mvFuncName = mvColumn.getAggregationType().name().toLowerCase();
                if (queryFnName.equalsIgnoreCase(FunctionSet.COUNT) && mvColumn.getDefineExpr() instanceof CaseExpr) {
                    mvFuncName = FunctionSet.COUNT;
                }
                String mvColumnName = MVUtils.getMVAggColumnName(mvFuncName, queryColumn.getName());
                if (mvColumnName.equalsIgnoreCase(mvColumn.getName())) {
                    mvIdToRewriteContexts.computeIfAbsent(indexId, k -> Lists.newArrayList())
                            .add(new RewriteContext(queryFn, queryColumnRef, mvColumnRef, mvColumn));
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
