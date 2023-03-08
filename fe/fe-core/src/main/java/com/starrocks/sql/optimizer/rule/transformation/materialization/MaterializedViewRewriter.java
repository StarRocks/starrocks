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
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.MaterializationContext;
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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import org.apache.commons.collections4.iterators.PermutationIterator;
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

    public MaterializedViewRewriter(MaterializationContext materializationContext) {
        this.materializationContext = materializationContext;
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
                                   List<Table> mvTables) {
        if (!isValidPlan(mvExpression)) {
            return false;
        }

        // If table lists do not intersect, can not be rewritten
        if (Collections.disjoint(queryTables, mvTables)) {
            return false;
        }
        return true;
    }

    public List<OptExpression> rewrite(ReplaceColumnRefRewriter queryColumnRefRewriter,
                                       PredicateSplit queryPredicateSplit) {
        final OptExpression queryExpression = materializationContext.getQueryExpression();
        final OptExpression mvExpression = materializationContext.getMvExpression();
        final List<Table> queryTables = MvUtils.getAllTables(queryExpression);
        final List<Table> mvTables = MvUtils.getAllTables(mvExpression);

        // Check whether mv can be applicable for the query.
        if (!isMVApplicable(mvExpression, queryTables, mvTables)) {
            return Lists.newArrayList();
        }

        final ColumnRefFactory mvColumnRefFactory = materializationContext.getMvColumnRefFactory();
        final ReplaceColumnRefRewriter mvColumnRefRewriter =
                MvUtils.getReplaceColumnRefWriter(mvExpression, mvColumnRefFactory);
        // Compensate partition predicates and add them into mv predicate,
        // eg: c3 is partition column
        // MV    : select c1, c3, c2 from test_base_part where c3 < 2000
        // Query : select c1, c3, c2 from test_base_part
        // `c3 < 2000` is missed after partition pruning, so `mvPredicate` must add `mvPartitionPredicate`,
        // otherwise query above may be rewritten by mv.
        final ScalarOperator mvPartitionPredicate =
                MvUtils.compensatePartitionPredicate(mvExpression, mvColumnRefFactory);
        if (mvPartitionPredicate == null) {
            return Lists.newArrayList();
        }
        ScalarOperator mvPredicate = MvUtils.rewriteOptExprCompoundPredicate(mvExpression, mvColumnRefRewriter);
        if (!ConstantOperator.TRUE.equals(mvPartitionPredicate)) {
            mvPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(mvPredicate, mvPartitionPredicate));
        }
        final PredicateSplit mvPredicateSplit = PredicateSplit.splitPredicate(mvPredicate);

        // Only care MatchMode.COMPLETE and VIEW_DELTA here, QUERY_DELTA also can be supported
        // because optimizer will match MV's pattern which is subset of query opt tree
        // from top-down iteration.
        MatchMode matchMode = getMatchMode(queryTables, mvTables);
        Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns = ArrayListMultimap.create();
        Map<Table, Set<Integer>> compensationRelations = Maps.newHashMap();
        if (matchMode == MatchMode.COMPLETE) {
            if (!isJoinMatch(queryExpression, mvExpression, queryTables, mvTables)) {
                return Lists.newArrayList();
            }
        } else if (matchMode == MatchMode.VIEW_DELTA) {
            ScalarOperator viewEqualPredicate = mvPredicateSplit.getEqualPredicates();
            EquivalenceClasses viewEquivalenceClasses = createEquivalenceClasses(viewEqualPredicate);
            if (!compensateViewDelta(viewEquivalenceClasses, queryExpression, mvExpression,
                    materializationContext.getQueryRefFactory(), compensationJoinColumns, compensationRelations)) {
                return Lists.newArrayList();
            }
        } else {
            return Lists.newArrayList();
        }

        Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getQueryRefFactory());
        final Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns =
                getRelationIdToColumns(materializationContext.getMvColumnRefFactory());

        // for query: A1 join A2 join B, mv: A1 join A2 join B
        // there may be two mapping:
        //    1. A1 -> A1, A2 -> A2, B -> B
        //    2. A1 -> A2, A2 -> A1, B -> B
        List<BiMap<Integer, Integer>> relationIdMappings = generateRelationIdMap(
                materializationContext.getQueryRefFactory(),
                queryTables, queryExpression, materializationContext.getMvColumnRefFactory(),
                mvTables, mvExpression, matchMode, compensationRelations);
        if (relationIdMappings.isEmpty()) {
            return Lists.newArrayList();
        }
        // used to judge whether query scalar ops can be rewritten
        final List<ColumnRefOperator> scanMvOutputColumns =
                materializationContext.getScanMvOperator().getOutputColumns();
        final Set<ColumnRefOperator> queryColumnSet = queryRelationIdToColumns.values()
                .stream().flatMap(List::stream)
                .filter(columnRef -> !scanMvOutputColumns.contains(columnRef))
                .collect(Collectors.toSet());
        final EquivalenceClasses queryEc = createEquivalenceClasses(queryPredicateSplit.getEqualPredicates());
        RewriteContext rewriteContext = new RewriteContext(queryExpression, queryPredicateSplit, queryEc,
                queryRelationIdToColumns, materializationContext.getQueryRefFactory(),
                queryColumnRefRewriter, mvExpression, mvPredicateSplit, mvRelationIdToColumns,
                materializationContext.getMvColumnRefFactory(), mvColumnRefRewriter,
                materializationContext.getOutputMapping(), queryColumnSet);

        List<OptExpression> results = Lists.newArrayList();
        for (BiMap<Integer, Integer> relationIdMapping : relationIdMappings) {
            rewriteContext.setQueryToMvRelationIdMapping(relationIdMapping);
            // for view delta, should add compensation join columns to query ec
            if (matchMode == MatchMode.VIEW_DELTA) {
                EquivalenceClasses newQueryEc = new EquivalenceClasses(queryEc);
                ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
                // convert mv-based compensation join columns into query based after we get relationId mapping
                for (Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : compensationJoinColumns.entries()) {
                    ColumnRefOperator newKey = (ColumnRefOperator) columnRewriter.rewriteViewToQuery(entry.getKey());
                    ColumnRefOperator newValue = (ColumnRefOperator) columnRewriter.rewriteViewToQuery(entry.getValue());
                    newQueryEc.addEquivalence(newKey, newValue);
                }
                rewriteContext.setQueryEquivalenceClasses(newQueryEc);
            }

            OptExpression rewrittenExpression = tryRewriteForRelationMapping(rewriteContext);
            if (rewrittenExpression == null) {
                continue;
            }
            results.add(rewrittenExpression);
        }

        return results;
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
    // return true if it can be rewritten. Further, it will add the missing equi-join predicates to the
    // compensationJoinColumns and compensation table into compensationRelations.
    // return false if it can not be rewritten.
    private boolean compensateViewDelta(
            EquivalenceClasses viewEquivalenceClasses,
            OptExpression queryExpression,
            OptExpression mvExpression,
            ColumnRefFactory queryRefFactory,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns,
            Map<Table, Set<Integer>> compensationRelations) {
        // use directed graph to construct foreign key join graph
        MutableGraph<TableScanDesc> graph = GraphBuilder.directed().build();
        List<TableScanDesc> extraTableScanDescs = Lists.newArrayList();

        List<TableScanDesc> queryTableScanDescs = MvUtils.getTableScanDescs(queryExpression);
        List<TableScanDesc> mvTableScanDescs = MvUtils.getTableScanDescs(mvExpression);
        // do not support external table now
        if (queryTableScanDescs.stream().anyMatch(tableScanDesc -> !tableScanDesc.getTable().isNativeTable())) {
            return false;
        }
        if (mvTableScanDescs.stream().anyMatch(tableScanDesc -> !tableScanDesc.getTable().isNativeTable())) {
            return false;
        }

        Multimap<String, TableScanDesc> nameToTable = ArrayListMultimap.create();
        for (TableScanDesc tableScanDesc : mvTableScanDescs) {
            graph.addNode(tableScanDesc);
            nameToTable.put(tableScanDesc.getName(), tableScanDesc);
            if (!queryTableScanDescs.contains(tableScanDesc)) {
                extraTableScanDescs.add(tableScanDesc);
            }
        }
        Map<TableScanDesc, List<ColumnRefOperator>> extraTableColumns = Maps.newHashMap();

        // add edges to directed graph by FK-UK
        for (TableScanDesc tableScanDesc : graph.nodes()) {
            // now only support OlapTable
            Preconditions.checkState(tableScanDesc.getTable() instanceof OlapTable);
            OlapTable childTable = (OlapTable) tableScanDesc.getTable();
            List<ForeignKeyConstraint> foreignKeyConstraints = childTable.getForeignKeyConstraints();
            if (foreignKeyConstraints == null) {
                continue;
            }

            for (ForeignKeyConstraint foreignKeyConstraint : foreignKeyConstraints) {
                Collection<TableScanDesc> parentTableScanDescs =
                        nameToTable.get(foreignKeyConstraint.getParentTableInfo().getTableName());
                if (parentTableScanDescs == null || parentTableScanDescs.isEmpty()) {
                    continue;
                }
                List<Pair<String, String>> columnPairs = foreignKeyConstraint.getColumnRefPairs();
                List<String> childKeys = columnPairs.stream().map(pair -> pair.first).collect(Collectors.toList());
                List<String> parentKeys = columnPairs.stream().map(pair -> pair.second).collect(Collectors.toList());
                for (TableScanDesc parentTableScanDesc : parentTableScanDescs) {
                    List<ColumnRefOperator> tableCompensationColumns = Lists.newArrayList();
                    Multimap<ColumnRefOperator, ColumnRefOperator> tmpCompensationJoinColumns = ArrayListMultimap.create();
                    if (!extraJoinCheck(parentTableScanDesc, tableScanDesc, columnPairs, childKeys, parentKeys,
                            viewEquivalenceClasses, tableCompensationColumns, tmpCompensationJoinColumns)) {
                        continue;
                    }

                    if (extraTableScanDescs.contains(parentTableScanDesc)) {
                        extraTableColumns.put(parentTableScanDesc, tableCompensationColumns);
                        compensationJoinColumns.putAll(tmpCompensationJoinColumns);
                    }
                    // add an edge to graph
                    if (!graph.successors(tableScanDesc).contains(parentTableScanDesc)) {
                        graph.putEdge(tableScanDesc, parentTableScanDesc);
                    }
                }
            }
        }
        if (!graphBasedCheck(graph, extraTableScanDescs)) {
            return false;
        }

        // should add new tables/columnRefs into query ColumnRefFactory if pass test
        // should collect additional tables and the related columns
        getCompensationRelations(extraTableColumns, queryRefFactory, compensationRelations);
        return true;
    }

    private boolean extraJoinCheck(
            TableScanDesc parentTableScanDesc, TableScanDesc tableScanDesc,
            List<Pair<String, String>> columnPairs, List<String> childKeys, List<String> parentKeys,
            EquivalenceClasses viewEquivalenceClasses,
            List<ColumnRefOperator> tableCompensationColumns,
            Multimap<ColumnRefOperator, ColumnRefOperator> compensationJoinColumns) {
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
            tableCompensationColumns.add(parentColumn);
            compensationJoinColumns.put(childColumn, parentColumn);
        }
        return true;
    }

    private boolean graphBasedCheck(MutableGraph<TableScanDesc> graph, List<TableScanDesc> extraTableScanDescs) {
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

        if (!Collections.disjoint(graph.nodes(), extraTableScanDescs)) {
            return false;
        }
        return true;
    }

    private void getCompensationRelations(Map<TableScanDesc, List<ColumnRefOperator>> extraTableColumns,
                                          ColumnRefFactory queryRefFactory,
                                          Map<Table, Set<Integer>> compensationRelations) {
        for (Map.Entry<TableScanDesc, List<ColumnRefOperator>> entry : extraTableColumns.entrySet()) {
            int relationId = queryRefFactory.getNextRelationId();
            for (ColumnRefOperator columnRef : entry.getValue()) {
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
        }
    }

    private boolean isUniqueKeys(OlapTable table, List<String> keys) {
        KeysType tableKeyType = table.getKeysType();
        if (tableKeyType == KeysType.PRIMARY_KEYS || tableKeyType == KeysType.UNIQUE_KEYS) {
            List<String> tableKeyColumns = table.getKeyColumns()
                    .stream().map(column -> column.getName()).collect(Collectors.toList());
            if (!keys.containsAll(tableKeyColumns) || !tableKeyColumns.containsAll(keys)) {
                return false;
            }
        } else if (tableKeyType == KeysType.DUP_KEYS) {
            List<UniqueConstraint> uniqueConstraints = table.getUniqueConstraints();
            if (uniqueConstraints == null || uniqueConstraints.isEmpty()) {
                return false;
            }
            for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
                if (uniqueConstraint.getUniqueColumns().equals(keys)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private ColumnRefOperator getColumnRef(String columnName, LogicalScanOperator scanOperator) {
        Optional<ColumnRefOperator> columnRef = scanOperator.getColumnMetaToColRefMap().values()
                .stream().filter(col -> col.getName().equals(columnName)).findFirst();
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
            return queryTables.equals(mvTables) && queryJoinOperators.equals(mvJoinOperators);
        }
    }

    private OptExpression tryRewriteForRelationMapping(RewriteContext rewriteContext) {
        // the rewritten expression to replace query
        // should copy the op because the op will be modified and reused
        final LogicalOlapScanOperator mvScanOperator = materializationContext.getScanMvOperator();
        final Operator.Builder mvScanBuilder = OperatorBuilderFactory.build(mvScanOperator);
        mvScanBuilder.withOperator(mvScanOperator);

        // Rewrite original mv's predicates into query if needed.
        final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        final Map<ColumnRefOperator, ScalarOperator> mvColumnRefToScalarOp = rewriteContext.getMVColumnRefToScalarOp();
        ScalarOperator mvOriginalPredicates = mvScanOperator.getPredicate();
        if (mvOriginalPredicates != null && !ConstantOperator.TRUE.equals(mvOriginalPredicates)) {
            mvOriginalPredicates = rewriteMVCompensationExpression(rewriteContext, columnRewriter,
                    mvColumnRefToScalarOp, mvOriginalPredicates, false);
            if (!ConstantOperator.TRUE.equals(mvOriginalPredicates)) {
                mvScanBuilder.setPredicate(mvOriginalPredicates);
            }
        }
        OptExpression mvScanOptExpression = OptExpression.create(mvScanBuilder.build());
        deriveLogicalProperty(mvScanOptExpression);

        // construct query based view EC
        EquivalenceClasses queryBaseViewEc = new EquivalenceClasses();
        if (rewriteContext.getMvPredicateSplit().getEqualPredicates() != null) {
            ScalarOperator equalPredicates = rewriteContext.getMvPredicateSplit().getEqualPredicates();
            ScalarOperator queryBasedViewEqualPredicate = columnRewriter.rewriteViewToQuery(equalPredicates);
            queryBaseViewEc = createEquivalenceClasses(queryBasedViewEqualPredicate);
        }
        rewriteContext.setQueryBasedViewEquivalenceClasses(queryBaseViewEc);

        final PredicateSplit compensationPredicates = getCompensationPredicates(rewriteContext, true);
        if (compensationPredicates == null) {
            if (!materializationContext.getOptimizerContext().getSessionVariable().isEnableMaterializedViewUnionRewrite()) {
                return null;
            }
            return tryUnionRewrite(rewriteContext, mvScanOptExpression);
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
            // NOTE: Keeps mv's original predicates which have been already rewritten.
            final List<ScalarOperator> finalCompensationPredicates =
                    Lists.newArrayList(compensationPredicate, mvScanOptExpression.getOp().getPredicate())
                            .stream().filter(op -> op != null && !ConstantOperator.TRUE.equals(op))
                            .collect(Collectors.toList());

            if (!finalCompensationPredicates.isEmpty()) {
                final Operator.Builder newScanOpBuilder = OperatorBuilderFactory.build(mvScanOptExpression.getOp());
                newScanOpBuilder.withOperator(mvScanOptExpression.getOp());
                newScanOpBuilder.setPredicate(Utils.compoundAnd(finalCompensationPredicates));
                mvScanOptExpression = OptExpression.create(newScanOpBuilder.build());
                mvScanOptExpression.setLogicalProperty(null);
                deriveLogicalProperty(mvScanOptExpression);
            }

            // add projection
            return viewBasedRewrite(rewriteContext, mvScanOptExpression);
        }
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
                .map(conjunct ->  {
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

        Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                normalizeAndReverseProjection(mvColumnRefToScalarOp, rewriteContext, isMVBased);
        List<ScalarOperator> candidates = rewriteScalarOpToTarget(rewrittenConjuncts, normalizedMap,
                rewriteContext.getOutputMapping(), new ColumnRefSet(rewriteContext.getQueryColumnSet()));
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        return Utils.compoundAnd(candidates);
    }

    private OptExpression tryUnionRewrite(RewriteContext rewriteContext, OptExpression mvOptExpr) {
        final PredicateSplit mvCompensationToQuery = getCompensationPredicates(rewriteContext, false);
        if (mvCompensationToQuery == null) {
            return null;
        }
        Preconditions.checkState(mvCompensationToQuery.getPredicates()
                .stream().anyMatch(predicate -> !ConstantOperator.TRUE.equals(predicate)));

        final ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
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
            mvEqualPreds = rewriteScalarOperatorToTarget(mvEqualPreds, queryExprMap, rewriteContext, mvToQueryRefSet, true);
        }
        if (!ConstantOperator.TRUE.equals(mvOtherPreds)) {
            mvOtherPreds = rewriteScalarOperatorToTarget(mvOtherPreds, queryExprMap, rewriteContext, mvToQueryRefSet, false);
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
                mvCompensationPredicates, materializationContext.getQueryExpression());
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
        final Multimap<ScalarOperator, ColumnRefOperator> normalizedMap = isEqual ?
                normalizeAndReverseProjection(exprMap, rewriteContext, false, false) :
                normalizeAndReverseProjection(exprMap, rewriteContext, true, false);
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        final List<ScalarOperator> rewrittens = rewriteScalarOpToTarget(conjuncts, normalizedMap, null, originalRefSet);
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

    protected OptExpression createUnion(OptExpression queryInput, OptExpression viewInput, RewriteContext rewriteContext) {
        Map<ColumnRefOperator, ScalarOperator> queryColumnRefMap =
                MvUtils.getColumnRefMap(queryInput, rewriteContext.getQueryRefFactory());

        // keys of queryColumnRefMap and mvColumnRefMap are the same
        List<ColumnRefOperator> originalOutputColumns = queryColumnRefMap.keySet().stream().collect(Collectors.toList());

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

    protected Multimap<ScalarOperator, ColumnRefOperator> normalizeAndReverseProjection(
            Map<ColumnRefOperator, ScalarOperator> viewProjection, RewriteContext rewriteContext, boolean isViewBased) {
        return normalizeAndReverseProjection(viewProjection, rewriteContext, isViewBased, true);
    }

    protected Multimap<ScalarOperator, ColumnRefOperator> normalizeAndReverseProjection(
            Map<ColumnRefOperator, ScalarOperator> columnRefMap,
            RewriteContext rewriteContext,
            boolean isViewBased, boolean isQueryAgainstView) {
        Multimap<ScalarOperator, ColumnRefOperator> reversedMap = ArrayListMultimap.create();
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
                reversedMap.put(rewriteScalarOp, entry.getKey());
                // if rewriteScalarOp == rewritten, it means the rewritten can not be mapped from mv to query
                // and ColumnRefOperator may conflict between mv and query(same id but not same name),
                // so do not put it into the reversedMap
                if (rewriteScalarOp != rewritten) {
                    reversedMap.put(rewriteScalarOp, entry.getKey());
                }
            } else {
                if (isViewBased) {
                    rewriteScalarOp = columnRewriter.rewriteByViewEc(rewritten);
                } else {
                    rewriteScalarOp = columnRewriter.rewriteByQueryEc(rewritten);
                }
                reversedMap.put(rewriteScalarOp, entry.getKey());
            }
        }
        return reversedMap;
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

    private MatchMode getMatchMode(List<Table> queryTables, List<Table> mvTables) {
        MatchMode matchMode = MatchMode.NOT_MATCH;
        if (queryTables.size() == mvTables.size() && new HashSet<>(queryTables).containsAll(mvTables)) {
            matchMode = MatchMode.COMPLETE;
        } else if (queryTables.size() > mvTables.size() && queryTables.containsAll(mvTables)) {
            // TODO: query delta
            matchMode = MatchMode.QUERY_DELTA;
        } else if (queryTables.size() < mvTables.size() && mvTables.containsAll(queryTables)) {
            // TODO: view delta
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

        // normalize view projection by query relation and ec
        Multimap<ScalarOperator, ColumnRefOperator> normalizedMap =
                normalizeAndReverseProjection(mvColumnRefToScalarOp, rewriteContext, false);

        return rewriteProjection(rewriteContext, normalizedMap, mvScanOptExpression);
    }

    protected OptExpression rewriteProjection(RewriteContext rewriteContext,
                                    Multimap<ScalarOperator, ColumnRefOperator> normalizedViewMap,
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
                    normalizedViewMap, rewriteContext.getOutputMapping());
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
                                                           Multimap<ScalarOperator, ColumnRefOperator> reversedViewProjection,
                                                           Map<ColumnRefOperator, ColumnRefOperator> outputMapping,
                                                           ColumnRefSet originalColumnSet) {
        List<ScalarOperator> rewrittenExprs = Lists.newArrayList();
        for (ScalarOperator expr : exprsToRewrites) {
            ScalarOperator rewritten = replaceExprWithTarget(expr, reversedViewProjection, outputMapping);
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

    // TODO: support rewrite query by using mv's binary predicate later:
    // MV   : deptno + 1
    // Query: deptno > 10
    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr,
                                                   Multimap<ScalarOperator, ColumnRefOperator> exprMap,
                                                   Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitBinaryPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitCall(CallOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitCall(predicate, context);
            }

            @Override
            public ScalarOperator visitCastOperator(CastOperator cast, Void context) {
                ScalarOperator tmp = replace(cast);
                return tmp != null ? tmp : super.visitCastOperator(cast, context);
            }

            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                ScalarOperator tmp = replace(variable);
                return tmp != null ? tmp : super.visitVariableReference(variable, context);
            }

            ScalarOperator replace(ScalarOperator scalarOperator) {
                if (exprMap.containsKey(scalarOperator)) {
                    Optional<ColumnRefOperator> mappedColumnRef = exprMap.get(scalarOperator).stream().findFirst();
                    if (!mappedColumnRef.isPresent()) {
                        return null;
                    }
                    if (columnMapping != null) {
                        ColumnRefOperator replaced = columnMapping.get(mappedColumnRef.get());
                        if (replaced == null) {
                            return null;
                        }
                        return replaced.clone();
                    } else {
                        return mappedColumnRef.get().clone();
                    }
                }
                return null;
            }
        };
        return expr.accept(shuttle, null);
    }

    private List<BiMap<Integer, Integer>> generateRelationIdMap(
            ColumnRefFactory queryRefFactory, List<Table> queryTables, OptExpression queryExpression,
            ColumnRefFactory mvRefFactory, List<Table> mvTables, OptExpression mvExpression,
            MatchMode matchMode, Map<Table, Set<Integer>> compensationRelations) {
        Map<Table, Set<Integer>> queryTableToRelationId = getTableToRelationid(queryExpression, queryRefFactory, queryTables);
        if (matchMode == MatchMode.VIEW_DELTA) {
            for (Map.Entry<Table, Set<Integer>> entry : compensationRelations.entrySet()) {
                if (queryTableToRelationId.containsKey(entry.getKey())) {
                    queryTableToRelationId.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    queryTableToRelationId.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Map<Table, Set<Integer>> mvTableToRelationId = getTableToRelationid(mvExpression, mvRefFactory, mvTables);
        // `queryTableToRelationId` may not equal to `mvTableToRelationId` when mv/query's columns are not
        // satisfied for the query.
        if (!queryTableToRelationId.keySet().equals(mvTableToRelationId.keySet())) {
            return Lists.newArrayList();
        }
        List<BiMap<Integer, Integer>> result = ImmutableList.of(HashBiMap.create());
        for (Map.Entry<Table, Set<Integer>> queryEntry : queryTableToRelationId.entrySet()) {
            Preconditions.checkState(!queryEntry.getValue().isEmpty());
            if (queryEntry.getValue().size() == 1) {
                Integer src = queryEntry.getValue().iterator().next();
                Integer target = mvTableToRelationId.get(queryEntry.getKey()).iterator().next();
                for (BiMap<Integer, Integer> m : result) {
                    m.put(src, target);
                }
            } else {
                ImmutableList.Builder<BiMap<Integer, Integer>> newResult = ImmutableList.builder();
                PermutationIterator<Integer> permutationIterator =
                        new PermutationIterator<>(mvTableToRelationId.get(queryEntry.getKey()));
                List<Integer> queryList = new ArrayList<>(queryEntry.getValue());
                while (permutationIterator.hasNext()) {
                    List<Integer> permutation = permutationIterator.next();
                    for (BiMap<Integer, Integer> m : result) {
                        final BiMap<Integer, Integer> newM = HashBiMap.create(m);
                        for (int i = 0; i < queryList.size(); i++) {
                            newM.put(queryList.get(i), permutation.get(i));
                        }
                        newResult.add(newM);
                    }
                }
                result = newResult.build();
            }
        }
        return result;
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

    private Map<Integer, List<ColumnRefOperator>> getRelationIdToColumns(ColumnRefFactory refFactory) {
        // relationId -> column list
        Map<Integer, List<ColumnRefOperator>> result = Maps.newHashMap();
        for (Map.Entry<Integer, Integer> entry : refFactory.getColumnToRelationIds().entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> Lists.newArrayList());
            ColumnRefOperator columnRef = refFactory.getColumnRef(entry.getKey());
            result.get(entry.getValue()).add(columnRef);
        }
        return result;
    }

    // when isQueryAgainstView is true, get compensation predicates of query against view
    // or get compensation predicates of view against query
    private PredicateSplit getCompensationPredicates(RewriteContext rewriteContext, boolean isQueryAgainstView) {
        // 1. equality join subsumption test
        EquivalenceClasses sourceEquivalenceClasses = isQueryAgainstView ?
                rewriteContext.getQueryEquivalenceClasses() : rewriteContext.getQueryBasedViewEquivalenceClasses();
        EquivalenceClasses targetEquivalenceClasses = isQueryAgainstView ?
                rewriteContext.getQueryBasedViewEquivalenceClasses() : rewriteContext.getQueryEquivalenceClasses();
        final ScalarOperator compensationEqualPredicate =
                getCompensationEqualPredicate(sourceEquivalenceClasses, targetEquivalenceClasses);
        if (compensationEqualPredicate == null) {
            // means source equal predicates cannot be rewritten by target
            return null;
        }

        // 2. range subsumption test
        ColumnRewriter columnRewriter = new ColumnRewriter(rewriteContext);
        ScalarOperator srcPr = isQueryAgainstView ? rewriteContext.getQueryPredicateSplit().getRangePredicates()
                : rewriteContext.getMvPredicateSplit().getRangePredicates();
        ScalarOperator targetPr = isQueryAgainstView ? rewriteContext.getMvPredicateSplit().getRangePredicates()
                : rewriteContext.getQueryPredicateSplit().getRangePredicates();
        ScalarOperator compensationPr = getCompensationRangePredicate(srcPr, targetPr, columnRewriter, isQueryAgainstView);
        if (compensationPr == null) {
            return null;
        }

        // 3. residual subsumption test
        ScalarOperator srcPu = isQueryAgainstView ? rewriteContext.getQueryPredicateSplit().getResidualPredicates()
                : rewriteContext.getMvPredicateSplit().getResidualPredicates();
        ScalarOperator targetPu = isQueryAgainstView ? rewriteContext.getMvPredicateSplit().getResidualPredicates()
                : rewriteContext.getQueryPredicateSplit().getResidualPredicates();
        if (srcPu == null && targetPu != null) {
            // query: empid < 5
            // mv: empid < 5 or salary > 100
            srcPu = Utils.compoundAnd(compensationEqualPredicate, compensationPr);
        }
        ScalarOperator compensationPu = getCompensationResidualPredicate(srcPu, targetPu, columnRewriter, isQueryAgainstView);
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
            ScalarOperator canonizedSrcPu = MvUtils.canonizePredicateForRewrite(srcPu.clone());
            ScalarOperator canonizedTargetPu = MvUtils.canonizePredicateForRewrite(targetPu.clone());
            ScalarOperator swappedSrcPu;
            ScalarOperator swappedTargetPu;
            if (isQueryAgainstView) {
                // src is query
                swappedSrcPu = columnRewriter.rewriteByQueryEc(canonizedSrcPu);
                // target is view
                swappedTargetPu = columnRewriter.rewriteViewToQueryWithQueryEc(canonizedTargetPu);
            } else {
                // src is view
                swappedSrcPu = columnRewriter.rewriteViewToQueryWithViewEc(canonizedSrcPu);
                // target is query
                swappedTargetPu = columnRewriter.rewriteByViewEc(canonizedTargetPu);
            }

            compensationPu = MvUtils.getCompensationPredicateForDisjunctive(swappedSrcPu, swappedTargetPu);
            if (compensationPu == null) {
                compensationPu = getCompensationResidualPredicate(swappedSrcPu, swappedTargetPu);
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
        ScalarOperator compensationPr;
        if (srcPr == null && targetPr == null) {
            compensationPr = ConstantOperator.createBoolean(true);
        } else if (srcPr == null) {
            return null;
        } else {
            ScalarOperator canonizedSrcPr = MvUtils.canonizePredicateForRewrite(srcPr.clone());
            ScalarOperator canonizedTargetPr = targetPr == null ? null : MvUtils.canonizePredicateForRewrite(targetPr.clone());

            // swap column by query EC
            ScalarOperator swappedSrcPr;
            ScalarOperator swappedTargetPr;
            if (isQueryAgainstView) {
                // for query
                swappedSrcPr = columnRewriter.rewriteByQueryEc(canonizedSrcPr);
                // for view, swap column by relation mapping and query ec
                swappedTargetPr = columnRewriter.rewriteViewToQueryWithQueryEc(canonizedTargetPr);
            } else {
                // for view
                swappedSrcPr = columnRewriter.rewriteViewToQueryWithViewEc(canonizedSrcPr);
                // for query
                swappedTargetPr = columnRewriter.rewriteByViewEc(canonizedTargetPr);
            }
            compensationPr = getCompensationRangePredicate(swappedSrcPr, swappedTargetPr);
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
        final List<Set<ColumnRefOperator>> sourceEquivalenceClassesList = sourceEquivalenceClasses.getEquivalenceClasses();
        final List<Set<ColumnRefOperator>> targetEquivalenceClassesList = targetEquivalenceClasses.getEquivalenceClasses();
        // it is a mapping from source to target
        // it may be 1 to n
        final Multimap<Integer, Integer> mapping = computeECMapping(sourceEquivalenceClassesList, targetEquivalenceClassesList);
        if (mapping == null) {
            // means that the targetEc can not be contained in sourceEc
            // it means Equijoin subsumption test fails
            return null;
        }
        // compute compensation equality predicate
        // if targetEc equals sourceEc, return true literal, so init to true here
        ScalarOperator compensation = ConstantOperator.createBoolean(true);
        for (int i = 0; i < sourceEquivalenceClassesList.size(); i++) {
            if (!mapping.containsKey(i)) {
                // it means that the targeEc do not have the corresponding mapping ec
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
                    difference.removeAll(targetEquivalenceClassesList.get(j));
                    Iterator<ColumnRefOperator> it = targetEquivalenceClassesList.get(j).iterator();
                    ScalarOperator targetFirst = it.next();
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
                }
            }
            if (!contained) {
                return null;
            }
        }
        return mapping;
    }
}
