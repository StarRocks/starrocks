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

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.sql.common.PermutationGenerator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.MvNormalizePredicateRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *  This rewriter is for single table or multi table join query rewrite
 */
public class MaterializedViewRewriter {
    protected static final Logger LOG = LogManager.getLogger(MaterializedViewRewriter.class);
    protected final MvRewriteContext mvRewriteContext;
    protected final MaterializationContext materializationContext;

    protected enum MatchMode {
        // all tables and join types match
        COMPLETE,
        // all tables match but join types do not
        PARTIAL,
        // all join types match but query has more tables
        QUERY_DELTA,
        // all join types match but view has more tables
        VIEW_DELTA,
        NOT_MATCH
    }

    public MaterializedViewRewriter(MvRewriteContext mvRewriteContext) {
        this.mvRewriteContext = mvRewriteContext;
        this.materializationContext = mvRewriteContext.getMaterializationContext();
    }

    /**
     * Whether the MV expression can be satisfiable for input query, eg AggregateScanRule require
     * MV expression should contain Aggregation operator.
     *
     * @param expression: MV expression.
     */
    public boolean isValidPlan(OptExpression expression) {
        return MvUtils.isLogicalSPJ(expression);
    }

    private boolean isMVApplicable(OptExpression mvExpression,
                                   List<Table> queryTables,
                                   List<Table> mvTables,
                                   MatchMode matchMode,
                                   OptExpression queryExpression) {
        // Only care MatchMode.COMPLETE and VIEW_DELTA here, QUERY_DELTA also can be supported
        // because optimizer will match MV's pattern which is subset of query opt tree
        // from top-down iteration.
        if (matchMode == MatchMode.COMPLETE) {
            if (!isJoinMatch(queryExpression, mvExpression, queryTables, mvTables)) {
                return false;
            }
        } else if (matchMode == MatchMode.VIEW_DELTA) {
            // only consider query with most common tables to optimize performance
            if (!queryTables.containsAll(materializationContext.getIntersectingTables())) {
                return false;
            }
            if (!MvUtils.getAllJoinOperators(queryExpression).stream().allMatch(joinOperator ->
                    joinOperator.isLeftOuterJoin() || joinOperator.isInnerJoin())) {
                return false;
            }
            List<TableScanDesc> queryTableScanDescs = MvUtils.getTableScanDescs(queryExpression);
            List<TableScanDesc> mvTableScanDescs = MvUtils.getTableScanDescs(mvExpression);
            // there should be at least one same join type in mv scan descs for every query scan desc.
            // to forbid rewrite for:
            // query: a left outer join b
            // mv: a inner join b inner join c
            for (TableScanDesc queryScanDesc : queryTableScanDescs) {
                if (queryScanDesc.getParentJoinType() != null
                        && !mvTableScanDescs.stream().anyMatch(scanDesc -> scanDesc.isMatch(queryScanDesc))) {
                    return false;
                }
            }
        } else {
            return false;
        }

        if (!isValidPlan(mvExpression)) {
            return false;
        }

        // If table lists do not intersect, can not be rewritten
        if (Collections.disjoint(queryTables, mvTables)) {
            return false;
        }
        return true;
    }

    public OptExpression rewrite() {
        final OptExpression queryExpression = mvRewriteContext.getQueryExpression();
        final OptExpression mvExpression = materializationContext.getMvExpression();
        final List<Table> queryTables = mvRewriteContext.getQueryTables();
        final List<Table> mvTables = MvUtils.getAllTables(mvExpression);

        MatchMode matchMode = getMatchMode(queryTables, mvTables);

        // Check whether mv can be applicable for the query.
        if (!isMVApplicable(mvExpression, queryTables, mvTables, matchMode, queryExpression)) {
            return null;
        }

        final ColumnRefFactory mvColumnRefFactory = materializationContext.getMvColumnRefFactory();
        final ReplaceColumnRefRewriter mvColumnRefRewriter =
                MvUtils.getReplaceColumnRefWriter(mvExpression, mvColumnRefFactory);

        ScalarOperator mvPredicate = MvUtils.rewriteOptExprCompoundPredicate(mvExpression, mvColumnRefRewriter);

        if (materializationContext.getMvPartialPartitionPredicate() != null) {
            List<ScalarOperator> partitionPredicates =
                    getPartitionRelatedPredicates(mvPredicate, mvRewriteContext.getMaterializationContext().getMv());
            if (partitionPredicates.stream().noneMatch(p -> p instanceof IsNullPredicateOperator)) {
                // there is no partition column is null predicate
                // add latest partition predicate to mv predicate
                ScalarOperator rewritten =
                        mvColumnRefRewriter.rewrite(materializationContext.getMvPartialPartitionPredicate());
                mvPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(mvPredicate, rewritten));
            }
        }
        final PredicateSplit mvPredicateSplit = PredicateSplit.splitPredicate(mvPredicate);

        if (matchMode == MatchMode.VIEW_DELTA) {
            List<TableScanDesc> mvTableScanDescs = MvUtils.getTableScanDescs(mvExpression);
            List<TableScanDesc> queryTableScanDescs = MvUtils.getTableScanDescs(queryExpression);
            // do not support external table now
            if (queryTableScanDescs.stream().anyMatch(tableScanDesc -> !tableScanDesc.getTable().isNativeTable())) {
                return null;
            }
            if (mvTableScanDescs.stream().anyMatch(tableScanDesc -> !tableScanDesc.getTable().isNativeTable())) {
                return null;
            }

            // NOTE: When queries/mvs have multi same tables in snowflake-schema mode, eg:
            // MV   : B <-> A <-> D <-> C <-> E <-> A <-> B
            // QUERY:  A <-> D <-> C <-> E <-> A <-> B
            // It's not easy to decide which tables are needed to compensate into query,
            // so iterate the possible permutations to tryRewrite.
            Map<Table, List<TableScanDesc>> mvTableToTableScanDecs = Maps.newHashMap();
            for (TableScanDesc mvTableScanDesc : mvTableScanDescs) {
                mvTableToTableScanDecs.computeIfAbsent(mvTableScanDesc.getTable(), x -> Lists.newArrayList())
                        .add(mvTableScanDesc);
            }
            List<List<TableScanDesc>> mvExtraTableScanDescLists = Lists.newArrayList();
            for (TableScanDesc mvTableScanDesc : mvTableScanDescs) {
                if (!queryTableScanDescs.contains(mvTableScanDesc)) {
                    mvExtraTableScanDescLists.add(mvTableToTableScanDecs.get(mvTableScanDesc.getTable()));
                }
            }

            List<Set<TableScanDesc>> mvDistinctScanDescs = Lists.newArrayList();
            // `mvExtraTableScanDescLists` may generate duplicated tables, eg:
            // MV   : B(1) <-> A <-> D <-> C <-> E <-> A <-> B (2)
            // QUERY:  A <-> D <-> C <-> E <-> A
            // `mvExtraTableScanDescLists`: [B1, B2], [B1, B2]
            // `PermutationGenerator` will generate all permutations of input tables:
            // [B1, B1], [B1, B2], [B2, B1], [B2, B1].
            // In the next step, remove some redundant permutations below.
            PermutationGenerator generator = new PermutationGenerator(mvExtraTableScanDescLists);
            while (generator.hasNext()) {
                List<TableScanDesc> mvExtraTableScanDescs = generator.next();
                if (mvExtraTableScanDescs.stream().distinct().count() != mvExtraTableScanDescs.size()) {
                    continue;
                }
                Set<TableScanDesc> mvExtraTableScanDescsSet = new HashSet<>(mvExtraTableScanDescs);
                if (mvDistinctScanDescs.contains(mvExtraTableScanDescsSet)) {
                    continue;
                }
                mvDistinctScanDescs.add(mvExtraTableScanDescsSet);

                OptExpression rewritten = tryRewrite(queryTables, mvTables, matchMode, mvPredicateSplit,
                        mvColumnRefRewriter, mvTableScanDescs, mvExtraTableScanDescs);
                if (rewritten != null) {
                    return rewritten;
                }
            }
        } else {
            return tryRewrite(queryTables, mvTables, matchMode, mvPredicateSplit, mvColumnRefRewriter,
                    null, null);
        }
        return null;
    }

    private OptExpression tryRewrite(List<Table> queryTables,
                                     List<Table> mvTables,
                                     MatchMode matchMode,
                                     PredicateSplit mvPredicateSplit,
                                     ReplaceColumnRefRewriter mvColumnRefRewriter,
                                     List<TableScanDesc> mvTableScanDescs,
                                     List<TableScanDesc> mvExtraTableScanDescs) {
        final OptExpression queryExpression = mvRewriteContext.getQueryExpression();
        final OptExpression mvExpression = materializationContext.getMvExpression();
        final ScalarOperator mvEqualPredicate = mvPredicateSplit.getEqualPredicates();

        final Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns = ArrayListMultimap.create();
        final Map<Table, Set<Integer>> compensationRelations = Maps.newHashMap();
        final Map<Integer, Integer> expectedExtraQueryToMVRelationIds = Maps.newHashMap();
        if (matchMode == MatchMode.VIEW_DELTA) {
            EquivalenceClasses viewEquivalenceClasses = createEquivalenceClasses(mvEqualPredicate);
            if (!compensateViewDelta(viewEquivalenceClasses, mvTableScanDescs, mvExtraTableScanDescs,
                    materializationContext.getQueryRefFactory(), materializationContext.getMvColumnRefFactory(),
                    compensationJoinColumns, compensationRelations, expectedExtraQueryToMVRelationIds)) {
                return null;
            }
        }

        final Map<Integer, Map<String, ColumnRefOperator>> queryRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getQueryRefFactory());
        final Map<Integer, Map<String, ColumnRefOperator>> mvRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getMvColumnRefFactory());
        // for query: A1 join A2 join B, mv: A1 join A2 join B
        // there may be two mapping:
        //    1. A1 -> A1, A2 -> A2, B -> B
        //    2. A1 -> A2, A2 -> A1, B -> B
        List<BiMap<Integer, Integer>> relationIdMappings = generateRelationIdMap(
                materializationContext.getQueryRefFactory(),
                queryTables, queryExpression, materializationContext.getMvColumnRefFactory(),
                mvTables, mvExpression, matchMode, compensationRelations, expectedExtraQueryToMVRelationIds);
        if (relationIdMappings.isEmpty()) {
            return null;
        }

        // used to judge whether query scalar ops can be rewritten
        final List<ColumnRefOperator> scanMvOutputColumns =
                materializationContext.getScanMvOperator().getOutputColumns();
        final Set<ColumnRefOperator> queryColumnSet = queryRelationIdToColumns.values()
                .stream().flatMap(x -> x.values().stream())
                .filter(columnRef -> !scanMvOutputColumns.contains(columnRef))
                .collect(Collectors.toSet());
        final EquivalenceClasses queryEc =
                createEquivalenceClasses(mvRewriteContext.getQueryPredicateSplit().getEqualPredicates());
        final RewriteContext rewriteContext = new RewriteContext(
                queryExpression, mvRewriteContext.getQueryPredicateSplit(), queryEc,
                queryRelationIdToColumns, materializationContext.getQueryRefFactory(),
                mvRewriteContext.getQueryColumnRefRewriter(), mvExpression, mvPredicateSplit, mvRelationIdToColumns,
                materializationContext.getMvColumnRefFactory(), mvColumnRefRewriter,
                materializationContext.getOutputMapping(), queryColumnSet);

        // collect partition and distribution related predicates in mv
        // used to prune partition and buckets after mv rewrite
        ScalarOperator mvPrunePredicate = collectMvPrunePredicate(materializationContext);

        for (BiMap<Integer, Integer> relationIdMapping : relationIdMappings) {
            mvRewriteContext.setMvPruneConjunct(mvPrunePredicate);
            rewriteContext.setQueryToMvRelationIdMapping(relationIdMapping);

            final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
            // for view delta, should add compensation join columns to query ec
            if (matchMode == MatchMode.VIEW_DELTA) {
                final EquivalenceClasses newQueryEc = queryEc.clone();
                // convert mv-based compensation join columns into query based after we get relationId mapping
                if (!addCompensationJoinColumnsIntoEquivalenceClasses(columnRewriter, newQueryEc,
                        compensationJoinColumns)) {
                    return null;
                }
                rewriteContext.setQueryEquivalenceClasses(newQueryEc);
            }

            // construct query based view EC
            final EquivalenceClasses queryBasedViewEqualPredicate =
                    createQueryBasedEquivalenceClasses(columnRewriter, mvEqualPredicate);
            if (queryBasedViewEqualPredicate == null) {
                return null;
            }
            rewriteContext.setQueryBasedViewEquivalenceClasses(queryBasedViewEqualPredicate);

            OptExpression rewrittenExpression = tryRewriteForRelationMapping(rewriteContext, compensationJoinColumns);
            if (rewrittenExpression != null) {
                return rewrittenExpression;
            }
        }
        return null;
    }

    // check whether query can be rewritten by view even though the view has additional tables.
    // In order to do that, we should make sure that the additional joins(which exists in view
    // but not in query) are lossless join.
    // for inner join:
    //      1. join on all key columns
    //      2. one side is foreign key and the other side is unique/primary key
    //      3. foreign key columns are not null
    // for outer join:
    //      1. left outer join
    //      2. join keys in additional table should be unique/primary key
    //
    // return true if it can be rewritten. Further, it will add the missing equal-join predicates to the
    // compensationJoinColumns and compensation table into compensationRelations.
    // return false if it can not be rewritten.
    private boolean compensateViewDelta(
            EquivalenceClasses viewEquivalenceClasses,
            List<TableScanDesc> mvTableScanDescs,
            List<TableScanDesc> mvExtraTableScanDescs,
            ColumnRefFactory queryRefFactory,
            ColumnRefFactory mvRefFactory,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns,
            Map<Table, Set<Integer>> compensationRelations,
            Map<Integer, Integer> expectedExtraQueryToMVRelationIds) {
        // use directed graph to construct foreign key join graph
        MutableGraph<TableScanDesc> mvGraph = GraphBuilder.directed().build();
        Map<TableScanDesc, List<ColumnRefOperator>> extraTableColumns = Maps.newHashMap();
        Multimap<String, TableScanDesc> mvNameToTable = ArrayListMultimap.create();
        for (TableScanDesc mvTableScanDesc : mvTableScanDescs) {
            mvGraph.addNode(mvTableScanDesc);
            mvNameToTable.put(mvTableScanDesc.getName(), mvTableScanDesc);
        }

        // add edges to directed graph by FK-UK
        for (TableScanDesc mvTableScanDesc : mvGraph.nodes()) {
            // now only support OlapTable
            Preconditions.checkState(mvTableScanDesc.getTable() instanceof OlapTable);
            OlapTable mvChildTable = (OlapTable) mvTableScanDesc.getTable();
            List<ForeignKeyConstraint> foreignKeyConstraints = mvChildTable.getForeignKeyConstraints();
            if (foreignKeyConstraints == null) {
                continue;
            }

            for (ForeignKeyConstraint foreignKeyConstraint : foreignKeyConstraints) {
                Collection<TableScanDesc> mvParentTableScanDescs =
                        mvNameToTable.get(foreignKeyConstraint.getParentTableInfo().getTableName());
                if (mvParentTableScanDescs == null || mvParentTableScanDescs.isEmpty()) {
                    continue;
                }
                List<Pair<String, String>> columnPairs = foreignKeyConstraint.getColumnRefPairs();
                List<String> childKeys = columnPairs.stream().map(pair -> pair.first)
                        .map(String::toLowerCase).collect(Collectors.toList());
                List<String> parentKeys = columnPairs.stream().map(pair -> pair.second)
                        .map(String::toLowerCase).collect(Collectors.toList());
                for (TableScanDesc mvParentTableScanDesc : mvParentTableScanDescs) {
                    Multimap<ColumnRefOperator, ColumnRefOperator> constraintCompensationJoinColumns = ArrayListMultimap.create();
                    if (!extraJoinCheck(mvParentTableScanDesc, mvTableScanDesc, columnPairs, childKeys, parentKeys,
                            viewEquivalenceClasses, constraintCompensationJoinColumns)) {
                        continue;
                    }

                    // If `mvParentTableScanDesc` is not included in query's plan, add it
                    // to extraColumns.
                    if (mvExtraTableScanDescs.contains(mvParentTableScanDesc)) {
                        compensationJoinColumns.putAll(constraintCompensationJoinColumns);
                        List<ColumnRefOperator> parentTableCompensationColumns =
                                constraintCompensationJoinColumns.keys().stream().collect(Collectors.toList());
                        extraTableColumns.computeIfAbsent(mvParentTableScanDesc, x -> Lists.newArrayList())
                                .addAll(parentTableCompensationColumns);
                    }
                    if (mvExtraTableScanDescs.contains(mvTableScanDesc)) {
                        compensationJoinColumns.putAll(constraintCompensationJoinColumns);
                        List<ColumnRefOperator> childTableCompensationColumns =
                                constraintCompensationJoinColumns.values().stream().collect(Collectors.toList());
                        extraTableColumns.computeIfAbsent(mvTableScanDesc, k -> Lists.newArrayList())
                                .addAll(childTableCompensationColumns);
                    }

                    // Add an edge (mvTableScanDesc -> mvParentTableScanDesc) into graph
                    if (!mvGraph.successors(mvTableScanDesc).contains(mvParentTableScanDesc)) {
                        mvGraph.putEdge(mvTableScanDesc, mvParentTableScanDesc);
                    }
                }
            }
        }
        if (!graphBasedCheck(mvGraph, mvExtraTableScanDescs)) {
            return false;
        }

        // should add new tables/columnRefs into query ColumnRefFactory if pass test
        // should collect additional tables and the related columns
        getCompensationRelations(extraTableColumns, queryRefFactory, mvRefFactory,
                compensationRelations, expectedExtraQueryToMVRelationIds);
        return true;
    }

    private boolean extraJoinCheck(
            TableScanDesc parentTableScanDesc, TableScanDesc tableScanDesc,
            List<Pair<String, String>> columnPairs, List<String> childKeys, List<String> parentKeys,
            EquivalenceClasses viewEquivalenceClasses,
            Multimap<ColumnRefOperator, ColumnRefOperator> constraintCompensationJoinColumns) {
        // now only OlapTable is supported
        Preconditions.checkState(parentTableScanDesc.getTable() instanceof OlapTable);
        OlapTable parentOlapTable = (OlapTable) parentTableScanDesc.getTable();
        OlapTable childTable = (OlapTable) tableScanDesc.getTable();
        JoinOperator parentJoinType = parentTableScanDesc.getParentJoinType();
        if (parentJoinType.isInnerJoin()) {
            // to check:
            // 1. childKeys should be foreign key
            // 2. childKeys should be not null
            // 3. parentKeys should be unique
            if (!isUniqueKeys(parentOlapTable, parentKeys)) {
                return false;
            }
            // foreign keys are not null
            if (childKeys.stream().anyMatch(column -> childTable.getColumn(column).isAllowNull())) {
                return false;
            }
        } else if (parentJoinType.isLeftOuterJoin()) {
            // make sure that all join keys are in foreign keys
            // the join keys of parent table should be unique
            if (!isUniqueKeys(parentOlapTable, parentKeys)) {
                return false;
            }
        } else {
            // other joins are not supported to rewrite
            return false;
        }
        // foreign keys should be join keys,
        // it means that there should be a join between childTable and parentTable
        for (Pair<String, String> pair : columnPairs) {
            ColumnRefOperator childColumn = getColumnRef(pair.first, tableScanDesc.getScanOperator());
            ColumnRefOperator parentColumn = getColumnRef(pair.second, parentTableScanDesc.getScanOperator());
            if (childColumn == null || parentColumn == null) {
                return false;
            }
            if (viewEquivalenceClasses.getEquivalenceClass(childColumn) == null) {
                return false;
            }
            if (viewEquivalenceClasses.getEquivalenceClass(parentColumn) == null) {
                return false;
            }
            if (!viewEquivalenceClasses.getEquivalenceClass(childColumn).contains(parentColumn)
                    || !viewEquivalenceClasses.getEquivalenceClass(parentColumn).contains(childColumn)) {
                // there is no join between childTable and parentTable
                return false;
            }
            constraintCompensationJoinColumns.put(parentColumn, childColumn);
        }
        return true;
    }

    private boolean graphBasedCheck(MutableGraph<TableScanDesc> graph,
                                    List<TableScanDesc> extraTableScanDescs) {
        // remove one in-degree and zero out-degree's node from graph repeatedly
        boolean done;
        do {
            List<TableScanDesc> nodesToRemove = Lists.newArrayList();
            for (TableScanDesc node : graph.nodes()) {
                if (graph.predecessors(node).size() == 1 && graph.successors(node).size() == 0) {
                    nodesToRemove.add(node);
                }
            }
            done = nodesToRemove.isEmpty();
            nodesToRemove.stream().forEach(node -> graph.removeNode(node));
        } while (!done);

        // If all nodes left after removing the preprocessor's size is 1 and successor's size is zero are
        // disjoint with `extraTables`, means `query` can be view-delta compensated.
        if (Collections.disjoint(graph.nodes(), extraTableScanDescs)) {
            return true;
        }
        return false;
    }

    private void getCompensationRelations(Map<TableScanDesc, List<ColumnRefOperator>> extraTableColumns,
                                          ColumnRefFactory queryRefFactory,
                                          ColumnRefFactory mvRefFactory,
                                          Map<Table, Set<Integer>> compensationRelations,
                                          Map<Integer, Integer> expectedExtraQueryToMVRelationIds) {
        // Extra table should always link with MV's relation id.
        for (Map.Entry<TableScanDesc, List<ColumnRefOperator>> entry : extraTableColumns.entrySet()) {
            int relationId = queryRefFactory.getNextRelationId();
            List<ColumnRefOperator> columnRefOperators = entry.getValue();
            int mvRelationId = -1;
            for (ColumnRefOperator columnRef : columnRefOperators) {
                if (mvRelationId == -1) {
                    mvRelationId = mvRefFactory.getRelationId(columnRef.getId());
                }

                OlapTable olapTable = (OlapTable) entry.getKey().getTable();
                Column column = olapTable.getColumn(columnRef.getName());
                ColumnRefOperator newColumn =
                        queryRefFactory.create(columnRef.getName(), columnRef.getType(), columnRef.isNullable());
                queryRefFactory.updateColumnToRelationIds(newColumn.getId(), relationId);
                queryRefFactory.updateColumnRefToColumns(newColumn, column, olapTable);
            }
            Set<Integer> relationIds =
                    compensationRelations.computeIfAbsent(entry.getKey().getTable(), table -> Sets.newHashSet());
            relationIds.add(relationId);
            expectedExtraQueryToMVRelationIds.put(relationId, mvRelationId);
        }
    }

    private boolean isUniqueKeys(OlapTable table, List<String> lowerCasekeys) {
        KeysType tableKeyType = table.getKeysType();
        Set<String> keySet = Sets.newHashSet(lowerCasekeys);
        if (tableKeyType == KeysType.PRIMARY_KEYS || tableKeyType == KeysType.UNIQUE_KEYS) {
            if (!table.isKeySet(keySet)) {
                return false;
            }
            return true;
        } else if (tableKeyType == KeysType.DUP_KEYS) {
            List<UniqueConstraint> uniqueConstraints = table.getUniqueConstraints();
            if (uniqueConstraints == null || uniqueConstraints.isEmpty()) {
                return false;
            }
            for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                if (uniqueConstraint.isMatch(keySet)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private ColumnRefOperator getColumnRef(String columnName, LogicalScanOperator scanOperator) {
        Optional<ColumnRefOperator> columnRef = scanOperator.getColumnMetaToColRefMap().values()
                .stream().filter(col -> col.getName().equalsIgnoreCase(columnName)).findFirst();
        return columnRef.isPresent() ? columnRef.get() : null;
    }

    private boolean isJoinMatch(OptExpression queryExpression,
                                OptExpression mvExpression,
                                List<Table> queryTables,
                                List<Table> mvTables) {
        boolean isQueryAllEqualInnerJoin = MvUtils.isAllEqualInnerOrCrossJoin(queryExpression);
        boolean isMVAllEqualInnerJoin = MvUtils.isAllEqualInnerOrCrossJoin(mvExpression);
        if (isQueryAllEqualInnerJoin && isMVAllEqualInnerJoin) {
            return true;
        } else {
            // If not all join types are InnerJoin, need to check whether MV's join tables' order
            // matches Query's join tables' order.
            // eg. MV   : a left join b inner join c
            //     Query: b left join a inner join c (cannot rewrite)
            //     Query: a left join b inner join c (can rewrite)
            //     Query: c inner join a left join b (can rewrite)
            // NOTE: Only support all MV's join tables' order exactly match with the query's join tables'
            // order for now.
            // Use traverse order to check whether all joins' order and operator are exactly matched.
            List<JoinOperator> queryJoinOperators = MvUtils.getAllJoinOperators(queryExpression);
            List<JoinOperator> mvJoinOperators = MvUtils.getAllJoinOperators(mvExpression);
            Preconditions.checkState(queryJoinOperators.size() + 1 == queryTables.size());
            Preconditions.checkState(mvJoinOperators.size() + 1 == mvTables.size());
            return queryTables.equals(mvTables) && queryJoinOperators.equals(mvJoinOperators);
        }
    }

    private ScalarOperator collectMvPrunePredicate(MaterializationContext mvContext) {
        final OptExpression mvExpression = mvContext.getMvExpression();
        final List<ScalarOperator> conjuncts = MvUtils.getAllPredicates(mvExpression);
        final ColumnRefSet mvOutputColumnRefSet = mvExpression.getOutputColumns();
        // conjuncts related to partition and distribution
        final List<ScalarOperator> mvPrunePredicates = Lists.newArrayList();

        // Construct partition/distribution key column refs to filter conjunctions which need to retain.
        Set<String> mvPruneKeyColNames = Sets.newHashSet();
        MaterializedView mv = mvContext.getMv();
        DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
        // only hash distribution is supported
        Preconditions.checkState(distributionInfo instanceof HashDistributionInfo);
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
        List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
        distributedColumns.stream().forEach(distKey -> mvPruneKeyColNames.add(distKey.getName()));
        mv.getPartitionColumnNames().stream().forEach(partName -> mvPruneKeyColNames.add(partName));
        final Set<Integer> mvPruneColumnIdSet = mvOutputColumnRefSet.getStream().map(
                id -> mvContext.getMvColumnRefFactory().getColumnRef(id))
                .filter(colRef -> mvPruneKeyColNames.contains(colRef.getName()))
                .map(colRef -> colRef.getId())
                .collect(Collectors.toSet());
        for (ScalarOperator conj : conjuncts) {
            // ignore binary predicates which cannot be used for pruning.
            if (conj instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator conjOp = (BinaryPredicateOperator) conj;
                if (conjOp.getChild(0).isVariable() && conjOp.getChild(1).isVariable()) {
                    continue;
                }
            }
            final List<Integer> conjColumnRefOperators =
                    Utils.extractColumnRef(conj).stream().map(ref -> ref.getId()).collect(Collectors.toList());
            if (mvPruneColumnIdSet.containsAll(conjColumnRefOperators)) {
                mvPrunePredicates.add(conj);
            }
        }
        return Utils.compoundAnd(mvPrunePredicates);
    }

    private OptExpression tryRewriteForRelationMapping(RewriteContext rewriteContext,
                                                       Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
        // the rewritten expression to replace query
        // should copy the op because the op will be modified and reused
        final LogicalOlapScanOperator mvScanOperator = materializationContext.getScanMvOperator();
        final Operator.Builder mvScanBuilder = OperatorBuilderFactory.build(mvScanOperator);
        mvScanBuilder.withOperator(mvScanOperator);

        // Rewrite original mv's predicates into query if needed.
        final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        final Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp = rewriteContext.getMVColumnRefToScalarOp();
        if (mvRewriteContext.getMvPruneConjunct() != null && !mvRewriteContext.getMvPruneConjunct().isTrue()) {
            ScalarOperator rewrittenPrunePredicate = rewriteMVCompensationExpression(rewriteContext, columnRewriter,
                    mvColumnRefToScalarOp, mvRewriteContext.getMvPruneConjunct(), false);
            mvRewriteContext.setMvPruneConjunct(MvUtils.canonizePredicate(rewrittenPrunePredicate));
        }
        OptExpression mvScanOptExpression = OptExpression.create(mvScanBuilder.build());
        deriveLogicalProperty(mvScanOptExpression);

        final PredicateSplit compensationPredicates = getCompensationPredicatesQueryToView(columnRewriter,
                rewriteContext, compensationJoinColumns);
        if (compensationPredicates == null) {
            if (!materializationContext.getOptimizerContext().getSessionVariable()
                    .isEnableMaterializedViewUnionRewrite()) {
                return null;
            }
            return tryUnionRewrite(rewriteContext, mvScanOptExpression, compensationJoinColumns);
        } else {
            // all predicates are now query based
            final ScalarOperator equalPredicates = MvUtils.canonizePredicate(compensationPredicates.getEqualPredicates());
            final ScalarOperator otherPredicates = MvUtils.canonizePredicate(Utils.compoundAnd(
                    compensationPredicates.getRangePredicates(), compensationPredicates.getResidualPredicates()));

            final ScalarOperator compensationPredicate = getMVCompensationPredicate(rewriteContext,
                    columnRewriter, mvColumnRefToScalarOp, equalPredicates, otherPredicates);
            if (compensationPredicate == null) {
                return null;
            }

            if (!compensationPredicate.isTrue()) {
                // NOTE: Keep mv's original predicates which have been already rewritten.
                ScalarOperator finalCompensationPredicate = compensationPredicate;
                if (mvScanOptExpression.getOp().getPredicate() != null) {
                    finalCompensationPredicate = Utils.compoundAnd(finalCompensationPredicate,
                            mvScanOptExpression.getOp().getPredicate());
                }
                final Operator.Builder newScanOpBuilder = OperatorBuilderFactory.build(mvScanOptExpression.getOp());
                newScanOpBuilder.withOperator(mvScanOptExpression.getOp());
                final ScalarOperator pruneFinalCompensationPredicate =
                        MvNormalizePredicateRule.pruneRedundantPredicates(finalCompensationPredicate);
                newScanOpBuilder.setPredicate(pruneFinalCompensationPredicate);
                mvScanOptExpression = OptExpression.create(newScanOpBuilder.build());
                mvScanOptExpression.setLogicalProperty(null);
                deriveLogicalProperty(mvScanOptExpression);
            }

            // add projection
            return viewBasedRewrite(rewriteContext, mvScanOptExpression);
        }
    }

    // Rewrite non-inner/cross join's on-predicates, all on-predicates should not compensated.
    // For non inner/cross join, we must ensure all on-predicates are not compensated, otherwise there may
    // be some correctness bugs.
    private ScalarOperator rewriteJoinOnPredicates(ColumnRewriter columnRewriter,
                                                   Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns,
                                                   List<ScalarOperator> srcJoinOnPredicates,
                                                   List<ScalarOperator> targetJoinOnPredicates,
                                                   boolean isQueryAgainstView) {
        if (srcJoinOnPredicates.isEmpty() && targetJoinOnPredicates.isEmpty()) {
            return ConstantOperator.TRUE;
        }
        if (srcJoinOnPredicates.isEmpty() && !targetJoinOnPredicates.isEmpty()) {
            return ConstantOperator.TRUE;
        }
        if (!srcJoinOnPredicates.isEmpty() && targetJoinOnPredicates.isEmpty()) {
            return null;
        }
        final PredicateSplit srcJoinOnPredicateSplit =
                PredicateSplit.splitPredicate(Utils.compoundAnd(srcJoinOnPredicates));
        final PredicateSplit targetJoinOnPredicateSplit =
                PredicateSplit.splitPredicate(Utils.compoundAnd(targetJoinOnPredicates));

        final EquivalenceClasses sourceEquivalenceClasses =
                createEquivalenceClasses(srcJoinOnPredicateSplit.getEqualPredicates());
        final EquivalenceClasses targetEquivalenceClasses = createQueryBasedEquivalenceClasses(columnRewriter,
                targetJoinOnPredicateSplit.getEqualPredicates());
        if (targetEquivalenceClasses == null) {
            return null;
        }

        // NOTE: For view-delta mode, we still need add extra join-compensations equal predicates.
        if (compensationJoinColumns != null) {
            if (!addCompensationJoinColumnsIntoEquivalenceClasses(columnRewriter,
                    sourceEquivalenceClasses, compensationJoinColumns)) {
                return null;
            }
            if (!addCompensationJoinColumnsIntoEquivalenceClasses(columnRewriter,
                    targetEquivalenceClasses, compensationJoinColumns)) {
                return null;
            }
        }

        final PredicateSplit compensationPredicates = getCompensationPredicates(columnRewriter,
                sourceEquivalenceClasses,
                targetEquivalenceClasses,
                srcJoinOnPredicateSplit,
                targetJoinOnPredicateSplit,
                isQueryAgainstView);
        if (compensationPredicates == null) {
            return null;
        }
        if (!ScalarOperator.isTrue(compensationPredicates.getEqualPredicates())) {
            return null;
        }
        if (!ScalarOperator.isTrue(compensationPredicates.getRangePredicates())) {
            return null;
        }
        if (!ScalarOperator.isTrue(compensationPredicates.getResidualPredicates())) {
            return null;
        }
        return ConstantOperator.TRUE;
    }

    private ScalarOperator getMVCompensationPredicate(RewriteContext rewriteContext,
                                                      ColumnRewriter rewriter,
                                                      Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
                                                      ScalarOperator equalPredicates,
                                                      ScalarOperator otherPredicates) {
        if (!ConstantOperator.TRUE.equals(equalPredicates)) {
            equalPredicates = rewriteMVCompensationExpression(rewriteContext, rewriter,
                    mvColumnRefToScalarOp, equalPredicates, true);
            if (equalPredicates == null) {
                return null;
            }
        }

        if (!ConstantOperator.TRUE.equals(otherPredicates)) {
            otherPredicates = rewriteMVCompensationExpression(rewriteContext, rewriter,
                    mvColumnRefToScalarOp, otherPredicates, false);
            if (otherPredicates == null) {
                return null;
            }
        }

        return MvUtils.canonizePredicate(Utils.compoundAnd(equalPredicates, otherPredicates));
    }

    private ScalarOperator rewriteMVCompensationExpression(RewriteContext rewriteContext,
                                                           ColumnRewriter rewriter,
                                                           Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp,
                                                           ScalarOperator predicate,
                                                           boolean isMVBased) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        // swapped by query based view ec
        List<ScalarOperator> rewrittenConjuncts = conjuncts.stream()
                .map(conjunct -> {
                    if (isMVBased) {
                        return rewriter.rewriteByViewEc(conjunct);
                    } else {
                        return rewriter.rewriteByQueryEc(conjunct);
                    }
                })
                .collect(Collectors.toList());
        if (rewrittenConjuncts.isEmpty()) {
            return null;
        }

        EquationRewriter queryExprToMvExprRewriter =
                buildEquationRewriter(mvColumnRefToScalarOp, rewriteContext, isMVBased);
        List<ScalarOperator> candidates = rewriteScalarOpToTarget(rewrittenConjuncts, queryExprToMvExprRewriter,
                rewriteContext.getOutputMapping(), new ColumnRefSet(rewriteContext.getQueryColumnSet()));
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        return Utils.compoundAnd(candidates);
    }

    private OptExpression tryUnionRewrite(RewriteContext rewriteContext,
                                          OptExpression mvOptExpr,
                                          Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
        final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        final PredicateSplit mvCompensationToQuery = getCompensationPredicatesViewToQuery(columnRewriter,
                rewriteContext, compensationJoinColumns);
        if (mvCompensationToQuery == null) {
            return null;
        }
        Preconditions.checkState(mvCompensationToQuery.getPredicates()
                .stream().anyMatch(predicate -> !ConstantOperator.TRUE.equals(predicate)));

        // construct viewToQueryRefSet
        final Set<ColumnRefOperator> mvColumnRefSet = rewriteContext.getMvRefFactory().getColumnRefToColumns().keySet();
        final Set<ColumnRefOperator> viewToQueryColRefs = mvColumnRefSet.stream()
                .map(columnRef -> (ColumnRefOperator) columnRewriter.rewriteViewToQueryWithViewEc(columnRef))
                .collect(Collectors.toSet());
        final ColumnRefSet mvToQueryRefSet = new ColumnRefSet(viewToQueryColRefs);

        // construct queryScanOutputColRefs
        final List<LogicalScanOperator> queryScanOps = MvUtils.getScanOperator(rewriteContext.getQueryExpression());
        final List<ColumnRefOperator> queryScanOutputColRefs = queryScanOps.stream()
                .map(scan -> scan.getOutputColumns())
                .flatMap(List::stream)
                .collect(Collectors.toList());

        // should exclude the columns that are both in query scan output columns and mv ref set, which are valid columns
        // left columns are only in mv scan output columns
        // if the columns used in compensation predicates exist in the mvRefSets, but not in the scanOutputColumns, it means
        // the predicate can not be rewritten.
        // for example:
        //  predicate: empid:1 < 10
        //  query scan output: name: 2, salary: 3
        //  mvRefSets: empid:1, name: 2, salary: 3
        // the predicate empid:1 < 10 can not be rewritten
        mvToQueryRefSet.except(queryScanOutputColRefs);

        final Map<ColumnRefOperator, ScalarOperator> queryExprMap = MvUtils.getColumnRefMap(
                rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        ScalarOperator mvEqualPreds = MvUtils.canonizePredicate(mvCompensationToQuery.getEqualPredicates());
        ScalarOperator mvOtherPreds = MvUtils.canonizePredicate(Utils.compoundAnd(
                mvCompensationToQuery.getRangePredicates(),
                mvCompensationToQuery.getResidualPredicates()));
        if (!ConstantOperator.TRUE.equals(mvEqualPreds)) {
            mvEqualPreds =
                    rewriteScalarOperatorToTarget(mvEqualPreds, queryExprMap, rewriteContext, mvToQueryRefSet, true);
        }
        if (!ConstantOperator.TRUE.equals(mvOtherPreds)) {
            mvOtherPreds =
                    rewriteScalarOperatorToTarget(mvOtherPreds, queryExprMap, rewriteContext, mvToQueryRefSet, false);
        }
        if (mvEqualPreds == null || mvOtherPreds == null) {
            return null;
        }
        final ScalarOperator mvCompensationPredicates = Utils.compoundAnd(mvEqualPreds, mvOtherPreds);

        // for mv: select a, b from t where a < 10;
        // query: select a, b from t where a < 20;
        // queryBasedRewrite will return the tree of "select a, b from t where a >= 10 and a < 20"
        // which is realized by adding the compensation predicate to original query expression
        final OptExpression queryInput = queryBasedRewrite(rewriteContext,
                mvCompensationPredicates, mvRewriteContext.getQueryExpression());
        if (queryInput == null) {
            return null;
        }

        // viewBasedRewrite will return the tree of "select a, b from t where a < 10" based on mv expression
        final OptExpression viewInput = viewBasedRewrite(rewriteContext, mvOptExpr);
        if (viewInput == null) {
            return null;
        }

        // createUnion will return the union all result of queryInput and viewInput
        //           Union
        //       /          |
        // partial query   view
        return createUnion(queryInput, viewInput, rewriteContext);
    }

    private ScalarOperator rewriteScalarOperatorToTarget(
            ScalarOperator predicate,
            Map<ColumnRefOperator, ScalarOperator> exprMap,
            RewriteContext rewriteContext,
            ColumnRefSet originalRefSet,
            boolean isEqual) {
        final EquationRewriter equationRewriter = isEqual ?
                buildEquationRewriter(exprMap, rewriteContext, false, false) :
                buildEquationRewriter(exprMap, rewriteContext, true, false);
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        final List<ScalarOperator> rewrittens =
                rewriteScalarOpToTarget(conjuncts, equationRewriter, null, originalRefSet);
        if (rewrittens == null || rewrittens.isEmpty()) {
            return null;
        }
        return Utils.compoundAnd(rewrittens);
    }

    protected OptExpression queryBasedRewrite(RewriteContext rewriteContext, ScalarOperator compensationPredicates,
                                              OptExpression queryExpression) {
        // query predicate and (not viewToQueryCompensationPredicate) is the final query compensation predicate
        ScalarOperator queryCompensationPredicate = MvUtils.canonizePredicate(
                Utils.compoundAnd(
                        rewriteContext.getQueryPredicateSplit().toScalarOperator(),
                        CompoundPredicateOperator.not(compensationPredicates)));
        if (!ConstantOperator.TRUE.equals(queryCompensationPredicate)) {
            if (queryExpression.getOp().getProjection() != null) {
                ReplaceColumnRefRewriter rewriter =
                        new ReplaceColumnRefRewriter(queryExpression.getOp().getProjection().getColumnRefMap());
                queryCompensationPredicate = rewriter.rewrite(queryCompensationPredicate);
            }
            queryCompensationPredicate = processNullPartition(rewriteContext, queryCompensationPredicate);
            if (queryCompensationPredicate == null) {
                return null;
            }

            // add filter to op
            Operator.Builder builder = OperatorBuilderFactory.build(queryExpression.getOp());
            builder.withOperator(queryExpression.getOp());
            builder.setPredicate(queryCompensationPredicate);
            Operator newQueryOp = builder.build();
            OptExpression newQueryExpr = OptExpression.create(newQueryOp, queryExpression.getInputs());
            deriveLogicalProperty(newQueryExpr);
            return newQueryExpr;
        }
        return null;
    }

    private List<ScalarOperator> getPartitionRelatedPredicates(ScalarOperator predicate, MaterializedView mv) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return Lists.newArrayList();
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Preconditions.checkState(partitionColumns.size() == 1);
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> partitionRelatedPredicates = conjuncts.stream()
                .filter(p -> isRelatedPredicate(p, partitionColumns.get(0).getName()))
                .collect(Collectors.toList());
        return partitionRelatedPredicates;
    }

    // process null value partition
    // when query select all data from base tables,
    // should add partition column is null predicate into queryCompensationPredicate to get null partition value related data
    private ScalarOperator processNullPartition(RewriteContext rewriteContext, ScalarOperator queryCompensationPredicate) {
        MaterializedView mv = mvRewriteContext.getMaterializationContext().getMv();
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return queryCompensationPredicate;
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Preconditions.checkState(partitionColumns.size() == 1);
        ScalarOperator queryPredicate = rewriteContext.getQueryPredicateSplit().toScalarOperator();
        List<ScalarOperator> queryPartitionRelatedScalars = getPartitionRelatedPredicates(queryPredicate, mv);

        ScalarOperator mvPredicate = rewriteContext.getMvPredicateSplit().toScalarOperator();
        List<ScalarOperator> mvPartitionRelatedScalars = getPartitionRelatedPredicates(mvPredicate, mv);
        if (queryPartitionRelatedScalars.isEmpty() && !mvPartitionRelatedScalars.isEmpty()) {
            // when query has no partition related predicates and mv has,
            // we should consider to add null value predicate into compensation predicates
            ColumnRefOperator partitionColumnRef =
                    getColumnRefFromPredicate(mvPredicate, partitionColumns.get(0).getName());
            Preconditions.checkState(partitionColumnRef != null);
            if (!partitionColumnRef.isNullable()) {
                // only add null value into compensation predicate when partition column is nullable
                return queryCompensationPredicate;
            }
            ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
            partitionColumnRef = columnRewriter.rewriteViewToQuery(partitionColumnRef).cast();
            IsNullPredicateOperator isNullPredicateOperator = new IsNullPredicateOperator(partitionColumnRef);
            IsNullPredicateOperator isNotNullPredicateOperator = new IsNullPredicateOperator(true, partitionColumnRef);
            List<ScalarOperator> predicates = Utils.extractConjuncts(queryCompensationPredicate);
            List<ScalarOperator> partitionRelatedPredicates = predicates.stream()
                    .filter(predicate -> isRelatedPredicate(predicate, partitionColumns.get(0).getName()))
                    .collect(Collectors.toList());
            predicates.removeAll(partitionRelatedPredicates);
            if (partitionRelatedPredicates.contains(isNullPredicateOperator)
                    || partitionRelatedPredicates.contains(isNotNullPredicateOperator)) {
                if (partitionRelatedPredicates.size() != 1) {
                    // has other partition predicates except partition column is null
                    // do not support now
                    // can it happend?
                    return null;
                }
                predicates.addAll(partitionRelatedPredicates);
            } else {
                // add partition column is null into compensation predicates
                ScalarOperator partitionPredicate =
                        Utils.compoundOr(Utils.compoundAnd(partitionRelatedPredicates), isNullPredicateOperator);
                predicates.add(partitionPredicate);
            }
            queryCompensationPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(predicates));
        }
        return queryCompensationPredicate;
    }

    private boolean isRelatedPredicate(ScalarOperator scalarOperator, String name) {
        ScalarOperatorVisitor<Boolean, Void> visitor = new ScalarOperatorVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    boolean ret = child.accept(this, null);
                    if (ret) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Boolean visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                return columnRefOperator.getName().equalsIgnoreCase(name);
            }
        };
        return scalarOperator.accept(visitor, null);
    }

    private ColumnRefOperator getColumnRefFromPredicate(ScalarOperator predicate, String name) {
        ScalarOperatorVisitor<ColumnRefOperator, Void> visitor = new ScalarOperatorVisitor<ColumnRefOperator, Void>() {
            @Override
            public ColumnRefOperator visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    ColumnRefOperator ret = child.accept(this, null);
                    if (ret != null) {
                        return ret;
                    }
                }
                return null;
            }

            @Override
            public ColumnRefOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                if (columnRefOperator.getName().equalsIgnoreCase(name)) {
                    return columnRefOperator;
                }
                return null;
            }
        };
        return predicate.accept(visitor, null);
    }

    protected OptExpression createUnion(OptExpression queryInput, OptExpression viewInput,
                                        RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(queryInput, rewriteContext.getQueryRefFactory());

        // keys of queryColumnRefMap and mvColumnRefMap are the same
        List<ColumnRefOperator> originalOutputColumns =
                queryColumnRefMap.keySet().stream().collect(Collectors.toList());

        // rewrite query
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(materializationContext);
        OptExpression newQueryInput = duplicator.duplicate(queryInput);
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(originalOutputColumns);

        // rewrite viewInput
        // for viewInput, there must be a projection,
        // the keyset of the projection is the same as the keyset of queryColumnRefMap
        Preconditions.checkState(viewInput.getOp().getProjection() != null);
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> mvProjection = viewInput.getOp().getProjection().getColumnRefMap();
        List<ColumnRefOperator> newViewOutputColumns = Lists.newArrayList();
        for (ColumnRefOperator columnRef : originalOutputColumns) {
            ColumnRefOperator newColumn = rewriteContext.getQueryRefFactory().create(
                    columnRef, columnRef.getType(), columnRef.isNullable());
            newViewOutputColumns.add(newColumn);
            newColumnRefMap.put(newColumn, mvProjection.get(columnRef));
        }
        Projection newMvProjection = new Projection(newColumnRefMap);
        viewInput.getOp().setProjection(newMvProjection);
        // reset the logical property, should be derived later again because output columns changed
        viewInput.setLogicalProperty(null);

        List<ColumnRefOperator> unionOutputColumns = originalOutputColumns.stream()
                .map(c -> rewriteContext.getQueryRefFactory().create(c, c.getType(), c.isNullable()))
                .collect(Collectors.toList());

        // generate new projection
        Map<ColumnRefOperator, ScalarOperator> unionProjection = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            unionProjection.put(originalOutputColumns.get(i), unionOutputColumns.get(i));
        }
        Projection projection = new Projection(unionProjection);
        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(unionOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(newQueryOutputColumns, newViewOutputColumns))
                .setProjection(projection)
                .isUnionAll(true)
                .build();
        OptExpression result = OptExpression.create(unionOperator, newQueryInput, viewInput);
        deriveLogicalProperty(result);
        return result;
    }

    protected EquationRewriter buildEquationRewriter(
            Map<ColumnRefOperator, ScalarOperator> viewProjection, RewriteContext rewriteContext, boolean isViewBased) {
        return buildEquationRewriter(viewProjection, rewriteContext, isViewBased, true);
    }

    protected EquationRewriter buildEquationRewriter(
            Map<ColumnRefOperator, ScalarOperator> columnRefMap,
            RewriteContext rewriteContext,
            boolean isViewBased, boolean isQueryAgainstView) {

        EquationRewriter rewriter = new EquationRewriter();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getMvColumnRefRewriter().rewrite(entry.getValue());
            ScalarOperator rewriteScalarOp;
            if (isQueryAgainstView) {
                if (isViewBased) {
                    rewriteScalarOp = columnRewriter.rewriteViewToQueryWithViewEc(rewritten);
                } else {
                    rewriteScalarOp = columnRewriter.rewriteViewToQueryWithQueryEc(rewritten);
                }
                // if rewriteScalarOp == rewritten and !rewritten.getUsedColumns().isEmpty(),
                // it means the rewritten can not be mapped from mv to query.
                // and ColumnRefOperator may conflict between mv and query(same id but not same name),
                // so do not put it into the reversedMap
                if (rewriteScalarOp != rewritten || rewritten.getUsedColumns().isEmpty()) {
                    rewriter.addMapping(rewriteScalarOp, entry.getKey());
                }
            } else {
                if (isViewBased) {
                    rewriteScalarOp = columnRewriter.rewriteByViewEc(rewritten);
                } else {
                    rewriteScalarOp = columnRewriter.rewriteByQueryEc(rewritten);
                }
                rewriter.addMapping(rewriteScalarOp, entry.getKey());
            }

        }
        return rewriter;
    }

    // equalPredicates eg: t1.a = t2.b and t1.c = t2.d
    private EquivalenceClasses createEquivalenceClasses(ScalarOperator equalPredicates) {
        EquivalenceClasses ec = new EquivalenceClasses();
        if (equalPredicates == null) {
            return ec;
        }
        for (ScalarOperator equalPredicate : Utils.extractConjuncts(equalPredicates)) {
            Preconditions.checkState(equalPredicate.getChild(0).isColumnRef());
            ColumnRefOperator left = (ColumnRefOperator) equalPredicate.getChild(0);
            Preconditions.checkState(equalPredicate.getChild(1).isColumnRef());
            ColumnRefOperator right = (ColumnRefOperator) equalPredicate.getChild(1);
            ec.addEquivalence(left, right);
        }
        return ec;
    }

    private EquivalenceClasses createQueryBasedEquivalenceClasses(ColumnRewriter columnRewriter,
                                                                  ScalarOperator equalPredicates) {
        EquivalenceClasses ec = new EquivalenceClasses();
        if (equalPredicates == null) {
            return ec;
        }

        for (ScalarOperator equalPredicate : Utils.extractConjuncts(equalPredicates)) {
            Preconditions.checkState(equalPredicate.getChild(0).isColumnRef());
            ColumnRefOperator left = (ColumnRefOperator) equalPredicate.getChild(0);
            Preconditions.checkState(equalPredicate.getChild(1).isColumnRef());
            ColumnRefOperator right = (ColumnRefOperator) equalPredicate.getChild(1);
            ColumnRefOperator leftTarget = columnRewriter.rewriteViewToQuery(left);
            ColumnRefOperator rightTarget = columnRewriter.rewriteViewToQuery(right);
            if (leftTarget == null || rightTarget == null) {
                return null;
            }
            ec.addEquivalence(leftTarget, rightTarget);
        }
        return ec;
    }

    private boolean addCompensationJoinColumnsIntoEquivalenceClasses(
            ColumnRewriter columnRewriter,
            EquivalenceClasses ec,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
        if (compensationJoinColumns == null || compensationJoinColumns.isEmpty()) {
            return true;
        }

        for (final Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : compensationJoinColumns.entries()) {
            final ColumnRefOperator left = entry.getKey();
            final ColumnRefOperator right = entry.getValue();
            ColumnRefOperator newKey = columnRewriter.rewriteViewToQuery(left);
            ColumnRefOperator newValue = columnRewriter.rewriteViewToQuery(right);
            if (newKey == null || newValue == null) {
                return false;
            }
            ec.addEquivalence(newKey, newValue);
        }
        return true;
    }

    private MatchMode getMatchMode(List<Table> queryTables, List<Table> mvTables) {
        MatchMode matchMode = MatchMode.NOT_MATCH;
        if (queryTables.size() == mvTables.size() && new HashSet<>(queryTables).containsAll(mvTables)) {
            matchMode = MatchMode.COMPLETE;
        } else if (queryTables.size() > mvTables.size() && queryTables.containsAll(mvTables)) {
            // TODO: query delta
            matchMode = MatchMode.QUERY_DELTA;
        } else if (queryTables.size() < mvTables.size() && mvTables.containsAll(queryTables)) {
            matchMode = MatchMode.VIEW_DELTA;
        }
        return matchMode;
    }

    protected void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }

    // TODO: consider no-loss type cast
    protected OptExpression viewBasedRewrite(RewriteContext rewriteContext,
                                             OptExpression mvScanOptExpression) {
        final Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp =
                rewriteContext.getMVColumnRefToScalarOp();

        EquationRewriter queryExprToMvExprRewriter =
                buildEquationRewriter(mvColumnRefToScalarOp, rewriteContext, false);

        return rewriteProjection(rewriteContext, queryExprToMvExprRewriter, mvScanOptExpression);
    }

    protected OptExpression rewriteProjection(RewriteContext rewriteContext,
                                              EquationRewriter queryExprToMvExprRewriter,
                                              OptExpression mvOptExpr) {
        Map<ColumnRefOperator, ScalarOperator> queryMap = MvUtils.getColumnRefMap(
                rewriteContext.getQueryExpression(), rewriteContext.getQueryRefFactory());
        Map<ColumnRefOperator, ScalarOperator> swappedQueryColumnMap = Maps.newHashMap();
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : queryMap.entrySet()) {
            ScalarOperator rewritten = rewriteContext.getQueryColumnRefRewriter().rewrite(entry.getValue().clone());
            ScalarOperator swapped = columnRewriter.rewriteByQueryEc(rewritten);
            swappedQueryColumnMap.put(entry.getKey(), swapped);
        }
        Map<ColumnRefOperator, ScalarOperator> newQueryProjection = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : swappedQueryColumnMap.entrySet()) {
            ScalarOperator rewritten = replaceExprWithTarget(entry.getValue(),
                    queryExprToMvExprRewriter, rewriteContext.getOutputMapping());
            if (rewritten == null) {
                return null;
            }
            if (!isAllExprReplaced(rewritten, new ColumnRefSet(rewriteContext.getQueryColumnSet()))) {
                // it means there is some column that can not be rewritten by outputs of mv
                return null;
            }
            newQueryProjection.put(entry.getKey(), rewritten);
        }
        Projection newProjection = new Projection(newQueryProjection);
        mvOptExpr.getOp().setProjection(newProjection);
        return mvOptExpr;
    }

    protected List<ScalarOperator> rewriteScalarOpToTarget(List<ScalarOperator> exprsToRewrites,
                                                           EquationRewriter queryExprToMvExprRewriter,
                                                           Map<ColumnRefOperator, ColumnRefOperator> outputMapping,
                                                           ColumnRefSet originalColumnSet) {
        List<ScalarOperator> rewrittenExprs = Lists.newArrayList();
        for (ScalarOperator expr : exprsToRewrites) {
            ScalarOperator rewritten = replaceExprWithTarget(expr, queryExprToMvExprRewriter, outputMapping);
            if (expr.isVariable() && expr == rewritten) {
                // it means it can not be rewritten  by target
                return Lists.newArrayList();
            }
            if (originalColumnSet != null && !isAllExprReplaced(rewritten, originalColumnSet)) {
                // it means there is some column that can not be rewritten by outputs of mv
                return Lists.newArrayList();
            }
            rewrittenExprs.add(rewritten);
        }
        return rewrittenExprs;
    }

    protected boolean isAllExprReplaced(ScalarOperator rewritten, ColumnRefSet originalColumnSet) {
        ScalarOperatorVisitor<Void, Void> visitor = new ScalarOperatorVisitor<Void, Void>() {
            @Override
            public Void visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    child.accept(this, null);
                }
                return null;
            }

            @Override
            public Void visitVariableReference(ColumnRefOperator variable, Void context) {
                if (originalColumnSet.contains(variable)) {
                    throw new UnsupportedOperationException("predicate can not be rewritten");
                }
                return null;
            }
        };
        try {
            rewritten.accept(visitor, null);
        } catch (UnsupportedOperationException e) {
            return false;
        }
        return true;
    }

    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr,
                                                   EquationRewriter queryExprToMvExprRewriter,
                                                   Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        queryExprToMvExprRewriter.setOutputMapping(columnMapping);
        return queryExprToMvExprRewriter.replaceExprWithTarget(expr);
    }

    private List<BiMap<Integer, Integer>> generateRelationIdMap(
            ColumnRefFactory queryRefFactory, List<Table> queryTables, OptExpression queryExpression,
            ColumnRefFactory mvRefFactory, List<Table> mvTables, OptExpression mvExpression,
            MatchMode matchMode, Map<Table, Set<Integer>> compensationRelations,
            Map<Integer, Integer> expectedExtraQueryToMVRelationIds) {
        Map<Table, Set<Integer>> queryTableToRelationId =
                getTableToRelationid(queryExpression, queryRefFactory, queryTables);
        if (matchMode == MatchMode.VIEW_DELTA) {
            for (Map.Entry<Table, Set<Integer>> entry : compensationRelations.entrySet()) {
                if (queryTableToRelationId.containsKey(entry.getKey())) {
                    queryTableToRelationId.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    queryTableToRelationId.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Map<Table, Set<Integer>> mvTableToRelationId =
                getTableToRelationid(mvExpression, mvRefFactory, mvTables);
        // `queryTableToRelationId` may not equal to `mvTableToRelationId` when mv/query's columns are not
        // satisfied for the query.
        if (!queryTableToRelationId.keySet().equals(mvTableToRelationId.keySet())) {
            return Lists.newArrayList();
        }
        List<BiMap<Integer, Integer>> result = ImmutableList.of(HashBiMap.create());
        for (Map.Entry<Table, Set<Integer>> queryEntry : queryTableToRelationId.entrySet()) {
            Preconditions.checkState(!queryEntry.getValue().isEmpty());
            Set<Integer> queryTableIds = queryEntry.getValue();
            Set<Integer> mvTableIds = mvTableToRelationId.get(queryEntry.getKey());
            if (queryTableIds.size() == 1) {
                Integer queryTableId = queryTableIds.iterator().next();
                if (mvTableIds.size() == 1) {
                    Integer mvTableId = mvTableIds.iterator().next();
                    for (BiMap<Integer, Integer> m : result) {
                        m.put(queryTableId, mvTableId);
                    }
                } else {
                    // query's table id is one and mv's tables are greater than one:
                    // Query table ids: a
                    // MV    table ids: a1, a2
                    // output:
                    //  a -> a1
                    //  a -> a2
                    ImmutableList.Builder<BiMap<Integer, Integer>> newResult = ImmutableList.builder();
                    Iterator<Integer> mvTableIdsIter = mvTableIds.iterator();
                    while (mvTableIdsIter.hasNext()) {
                        Integer mvTableId = mvTableIdsIter.next();
                        for (BiMap<Integer, Integer> m : result) {
                            final BiMap<Integer, Integer> newQueryToMvMap = HashBiMap.create(m);
                            newQueryToMvMap.put(queryTableId, mvTableId);
                            newResult.add(newQueryToMvMap);
                        }
                    }
                    result = newResult.build();
                }
            } else {
                // query's table ids are greater than one and mv's tables are greater than one:
                // Query table ids: a1, a2
                // MV    table ids: A1, A2
                // output:
                //  a1 -> A1, a2 -> A1 (x)
                //  a1 -> A2, a2 -> A2 (x) : cannot contain same values.
                //  a1 -> A1, a2 -> A2
                //  a1 -> A2, a2 -> A1
                List<Integer> queryList = new ArrayList<>(queryTableIds);
                List<Integer> mvList = new ArrayList<>(mvTableIds);
                List<List<Integer>> mvTableIdPermutations = getPermutationsOfTableIds(mvList, queryList.size());
                ImmutableList.Builder<BiMap<Integer, Integer>> newResult = ImmutableList.builder();
                for (List<Integer> mvPermutation : mvTableIdPermutations) {
                    Preconditions.checkState(mvPermutation.size() == queryList.size());
                    for (BiMap<Integer, Integer> m : result) {
                        final BiMap<Integer, Integer> newQueryToMvMap = HashBiMap.create(m);
                        for (int i = 0; i < queryList.size(); i++) {
                            newQueryToMvMap.put(queryList.get(i), mvPermutation.get(i));
                        }
                        newResult.add(newQueryToMvMap);
                    }
                }
                result = newResult.build();
            }
        }

        if (expectedExtraQueryToMVRelationIds.isEmpty()) {
            return result;
        } else {
            List<BiMap<Integer, Integer>> finalResult = Lists.newArrayList();
            for (BiMap<Integer, Integer> queryToMVRelations : result) {
                // Ignore permutations which not contain expected extra query to mv relation id mappings.
                boolean hasExpectedExtraQueryToMVRelationIds = true;
                for (Map.Entry<Integer, Integer> expect : expectedExtraQueryToMVRelationIds.entrySet()) {
                    if (!queryToMVRelations.get(expect.getKey()).equals(expect.getValue()))  {
                        hasExpectedExtraQueryToMVRelationIds = false;
                        break;
                    }
                }
                if (hasExpectedExtraQueryToMVRelationIds) {
                    finalResult.add(queryToMVRelations);
                }
            }
            return finalResult;
        }
    }

    public static List<List<Integer>> getPermutationsOfTableIds(List<Integer> tableIds, int n) {
        List<Integer> t = new ArrayList<>();
        List<List<Integer>> ret = new ArrayList<>();
        getPermutationsOfTableIds(tableIds, n, t, ret);
        return ret;
    }

    private static void getPermutationsOfTableIds(List<Integer> tableIds, int targetSize,
                                                  List<Integer> tmpResult, List<List<Integer>> ret) {
        if (tmpResult.size() == targetSize) {
            ret.add(new ArrayList<>(tmpResult));
            return;
        }
        for (int i = 0; i < tableIds.size(); i++) {
            if (tmpResult.contains(tableIds.get(i))) {
                continue;
            }
            tmpResult.add(tableIds.get(i));
            getPermutationsOfTableIds(tableIds, targetSize, tmpResult, ret);
            tmpResult.remove(tmpResult.size() - 1);
        }
    }

    private Map<Table, Set<Integer>> getTableToRelationid(
            OptExpression optExpression, ColumnRefFactory refFactory, List<Table> tableList) {
        Map<Table, Set<Integer>> tableToRelationId = Maps.newHashMap();
        List<ColumnRefOperator> validColumnRefs = MvUtils.collectScanColumn(optExpression);
        for (Map.Entry<ColumnRefOperator, Table> entry : refFactory.getColumnRefToTable().entrySet()) {
            if (!tableList.contains(entry.getValue())) {
                continue;
            }
            if (!validColumnRefs.contains(entry.getKey())) {
                continue;
            }
            Set<Integer> relationIds = tableToRelationId.computeIfAbsent(entry.getValue(), k -> Sets.newHashSet());
            Integer relationId = refFactory.getRelationId(entry.getKey().getId());
            relationIds.add(relationId);
        }
        return tableToRelationId;
    }

    private Map<Integer, Map<String, ColumnRefOperator>> getRelationIdToColumns(ColumnRefFactory refFactory) {
        // relationId -> column map
        Map<Integer, Map<String, ColumnRefOperator>> result = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> entry : refFactory.getColumnToRelationIds().entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> Maps.newHashMap());
            ColumnRefOperator columnRef = refFactory.getColumnRef(entry.getKey());
            result.get(entry.getValue()).put(columnRef.getName(), columnRef);
        }
        return result;
    }

    private PredicateSplit getCompensationPredicatesQueryToView(
            ColumnRewriter columnRewriter,
            RewriteContext rewriteContext,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
        final List<ScalarOperator> queryJoinOnPredicates = mvRewriteContext.getQueryJoinOnPredicates();
        final List<ScalarOperator> mvJoinOnPredicates = mvRewriteContext.getMvJoinOnPredicates();
        final ScalarOperator queryJoinOnPredicateCompensations =
                rewriteJoinOnPredicates(columnRewriter, compensationJoinColumns,
                        queryJoinOnPredicates, mvJoinOnPredicates, true);
        if (queryJoinOnPredicateCompensations == null) {
            return null;
        }

        return getCompensationPredicates(columnRewriter,
                rewriteContext.getQueryEquivalenceClasses(),
                rewriteContext.getQueryBasedViewEquivalenceClasses(),
                rewriteContext.getQueryPredicateSplit(),
                rewriteContext.getMvPredicateSplit(),
                true);
    }

    private PredicateSplit getCompensationPredicatesViewToQuery(
            ColumnRewriter columnRewriter,
            RewriteContext rewriteContext,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
        final List<ScalarOperator> queryJoinOnPredicates = mvRewriteContext.getQueryJoinOnPredicates();
        final List<ScalarOperator> mvJoinOnPredicates = mvRewriteContext.getMvJoinOnPredicates();
        final ScalarOperator queryJoinOnPredicateCompensations =
                rewriteJoinOnPredicates(columnRewriter, compensationJoinColumns,
                        mvJoinOnPredicates, queryJoinOnPredicates, false);
        if (queryJoinOnPredicateCompensations == null) {
            return null;
        }

        return getCompensationPredicates(columnRewriter,
                rewriteContext.getQueryBasedViewEquivalenceClasses(),
                rewriteContext.getQueryEquivalenceClasses(),
                rewriteContext.getMvPredicateSplit(),
                rewriteContext.getQueryPredicateSplit(),
                false);
    }

    // when isQueryAgainstView is true, get compensation predicates of query against view
    // or get compensation predicates of view against query
    private PredicateSplit getCompensationPredicates(ColumnRewriter columnRewriter,
                                                     EquivalenceClasses sourceEquivalenceClasses,
                                                     EquivalenceClasses targetEquivalenceClasses,
                                                     PredicateSplit srcPredicateSplit,
                                                     PredicateSplit targetPredicateSplit,
                                                     boolean isQueryAgainstView) {
        // 1. equality join subsumption test
        final ScalarOperator compensationEqualPredicate =
                getCompensationEqualPredicate(sourceEquivalenceClasses, targetEquivalenceClasses);
        if (compensationEqualPredicate == null) {
            return null;
        }

        // 2. range subsumption test
        ScalarOperator srcPr = srcPredicateSplit.getRangePredicates();
        ScalarOperator targetPr = targetPredicateSplit.getRangePredicates();
        ScalarOperator compensationPr =
                getCompensationRangePredicate(srcPr, targetPr, columnRewriter, isQueryAgainstView);
        if (compensationPr == null) {
            return null;
        }

        // 3. residual subsumption test
        ScalarOperator srcPu = srcPredicateSplit.getResidualPredicates();
        ScalarOperator targetPu = targetPredicateSplit.getResidualPredicates();
        if (srcPu == null && targetPu != null) {
            // query: empid < 5
            // mv: empid < 5 or salary > 100
            if (!isQueryAgainstView) {
                // compensationEqualPredicate and compensationPr is based on query, need to change it to view based
                srcPu = Utils.compoundAnd(columnRewriter.rewriteQueryToView(compensationEqualPredicate),
                        columnRewriter.rewriteQueryToView(compensationPr));
            } else {
                srcPu = Utils.compoundAnd(compensationEqualPredicate, compensationPr);
            }
        }
        ScalarOperator compensationPu =
                getCompensationResidualPredicate(srcPu, targetPu, columnRewriter, isQueryAgainstView);
        if (compensationPu == null) {
            return null;
        }

        return PredicateSplit.of(compensationEqualPredicate, compensationPr, compensationPu);
    }

    private ScalarOperator getCompensationResidualPredicate(ScalarOperator srcPu,
                                                            ScalarOperator targetPu,
                                                            ColumnRewriter columnRewriter,
                                                            boolean isQueryAgainstView) {
        ScalarOperator compensationPu;
        if (srcPu == null && targetPu == null) {
            compensationPu = ConstantOperator.createBoolean(true);
        } else if (srcPu == null) {
            return null;
        } else if (targetPu == null) {
            compensationPu = srcPu;
        } else {
            ScalarOperator swappedSrcPu;
            ScalarOperator swappedTargetPu;
            if (isQueryAgainstView) {
                // src is query
                swappedSrcPu = columnRewriter.rewriteByQueryEc(srcPu.clone());
                // target is view
                swappedTargetPu = columnRewriter.rewriteViewToQueryWithQueryEc(targetPu.clone());
            } else {
                // src is view
                swappedSrcPu = columnRewriter.rewriteViewToQueryWithViewEc(srcPu.clone());
                // target is query
                swappedTargetPu = columnRewriter.rewriteByViewEc(targetPu.clone());
            }
            ScalarOperator canonizedSrcPu = MvUtils.canonizePredicateForRewrite(swappedSrcPu);
            ScalarOperator canonizedTargetPu = MvUtils.canonizePredicateForRewrite(swappedTargetPu);

            compensationPu = MvUtils.getCompensationPredicateForDisjunctive(canonizedSrcPu, canonizedTargetPu);
            if (compensationPu == null) {
                compensationPu = getCompensationResidualPredicate(canonizedSrcPu, canonizedTargetPu);
                if (compensationPu == null) {
                    return null;
                }
            }
        }
        compensationPu = MvUtils.canonizePredicate(compensationPu);
        return compensationPu;
    }

    private ScalarOperator getCompensationResidualPredicate(ScalarOperator srcPu, ScalarOperator targetPu) {
        List<ScalarOperator> srcConjuncts = Utils.extractConjuncts(srcPu);
        List<ScalarOperator> targetConjuncts = Utils.extractConjuncts(targetPu);
        if (new HashSet<>(srcConjuncts).containsAll(targetConjuncts)) {
            srcConjuncts.removeAll(targetConjuncts);
            if (srcConjuncts.isEmpty()) {
                return ConstantOperator.createBoolean(true);
            } else {
                return Utils.compoundAnd(srcConjuncts);
            }
        }
        return null;
    }

    private ScalarOperator getCompensationRangePredicate(ScalarOperator srcPr,
                                                         ScalarOperator targetPr,
                                                         ColumnRewriter columnRewriter,
                                                         boolean isQueryAgainstView) {
        if (srcPr == null && targetPr == null) {
            return ConstantOperator.TRUE;
        }

        ScalarOperator compensationPr;
        if (targetPr == null) {
            compensationPr = srcPr;
        } else if (srcPr == null) {
            return null;
        } else {
            // swap column by query EC
            ScalarOperator swappedSrcPr;
            ScalarOperator swappedTargetPr;
            if (isQueryAgainstView) {
                // for query
                swappedSrcPr = columnRewriter.rewriteByQueryEc(srcPr.clone());
                // for view, swap column by relation mapping and query ec
                swappedTargetPr = columnRewriter.rewriteViewToQueryWithQueryEc(targetPr.clone());
            } else {
                // for view
                swappedSrcPr = columnRewriter.rewriteViewToQueryWithViewEc(srcPr.clone());
                // for query
                swappedTargetPr = columnRewriter.rewriteByViewEc(targetPr.clone());
            }
            ScalarOperator canonizedSrcPr = MvUtils.canonizePredicateForRewrite(swappedSrcPr);
            ScalarOperator canonizedTargetPr = MvUtils.canonizePredicateForRewrite(swappedTargetPr);
            compensationPr = getCompensationRangePredicate(canonizedSrcPr, canonizedTargetPr);
        }
        return MvUtils.canonizePredicate(compensationPr);
    }

    private ScalarOperator getCompensationRangePredicate(ScalarOperator srcPr, ScalarOperator targetPr) {
        RangeSimplifier simplifier = new RangeSimplifier(Utils.extractConjuncts(srcPr));
        return simplifier.simplify(Utils.extractConjuncts(targetPr));
    }

    // compute the compensation equality predicates
    // here do the equality join subsumption test
    // if targetEc is not contained in sourceEc, return null
    // if sourceEc equals targetEc, return true literal
    private ScalarOperator getCompensationEqualPredicate(EquivalenceClasses sourceEquivalenceClasses,
                                                         EquivalenceClasses targetEquivalenceClasses) {
        if (sourceEquivalenceClasses.getEquivalenceClasses().isEmpty()
                && targetEquivalenceClasses.getEquivalenceClasses().isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }
        if (sourceEquivalenceClasses.getEquivalenceClasses().isEmpty()
                && !targetEquivalenceClasses.getEquivalenceClasses().isEmpty()) {
            // targetEc must not be contained in sourceEc, just return null
            return null;
        }
        final List<Set<ColumnRefOperator>> sourceEquivalenceClassesList =
                sourceEquivalenceClasses.getEquivalenceClasses();
        final List<Set<ColumnRefOperator>> targetEquivalenceClassesList =
                targetEquivalenceClasses.getEquivalenceClasses();
        // it is a mapping from source to target
        // it may be 1 to n
        final Multimap<Integer, Integer> mapping =
                computeECMapping(sourceEquivalenceClassesList, targetEquivalenceClassesList);
        if (mapping == null) {
            // means that the targetEc can not be contained in sourceEc
            // it means Equal-join subsumption test fails
            return null;
        }
        // compute compensation equality predicate
        // if targetEc equals sourceEc, return true literal, so init to true here
        ScalarOperator compensation = ConstantOperator.createBoolean(true);
        for (int i = 0; i < sourceEquivalenceClassesList.size(); i++) {
            if (!mapping.containsKey(i)) {
                // it means that the targetEc do not have the corresponding mapping ec
                // we should all equality predicates between each column in ec into compensation
                Iterator<ColumnRefOperator> it = sourceEquivalenceClassesList.get(i).iterator();
                ScalarOperator first = it.next();
                while (it.hasNext()) {
                    ScalarOperator equalPredicate = BinaryPredicateOperator.eq(first, it.next());
                    compensation = Utils.compoundAnd(compensation, equalPredicate);
                }
            } else {
                // remove columns exists in target and add remain equality predicate in source into compensation
                for (int j : mapping.get(i)) {
                    Set<ScalarOperator> difference = Sets.newHashSet(sourceEquivalenceClassesList.get(i));
                    Set<ColumnRefOperator> targetColumnRefs = targetEquivalenceClassesList.get(j);
                    difference.removeAll(targetColumnRefs);

                    ScalarOperator targetFirst = targetColumnRefs.iterator().next();
                    for (ScalarOperator remain : difference) {
                        ScalarOperator equalPredicate = BinaryPredicateOperator.eq(remain, targetFirst);
                        compensation = Utils.compoundAnd(compensation, equalPredicate);
                    }
                }
            }
        }
        compensation = MvUtils.canonizePredicate(compensation);
        return compensation;
    }

    // check whether each target equivalence classes is contained in source equivalence classes.
    // if any of target equivalence class cannot be contained, return null
    private Multimap<Integer, Integer> computeECMapping(List<Set<ColumnRefOperator>> sourceEquivalenceClassesList,
                                                        List<Set<ColumnRefOperator>> targetEquivalenceClassesList) {
        Multimap<Integer, Integer> mapping = ArrayListMultimap.create();
        for (int i = 0; i < targetEquivalenceClassesList.size(); i++) {
            final Set<ColumnRefOperator> targetSet = targetEquivalenceClassesList.get(i);
            boolean contained = false;
            for (int j = 0; j < sourceEquivalenceClassesList.size(); j++) {
                final Set<ColumnRefOperator> srcSet = sourceEquivalenceClassesList.get(j);
                // targetSet is converted into the same relationId, so just use containAll
                if (srcSet.containsAll(targetSet)) {
                    mapping.put(j, i);
                    contained = true;
                    // once there is a mapping from src -> target, compensations can be done.
                    break;
                }
            }
            if (!contained) {
                return null;
            }
        }
        return mapping;
    }
}
