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
package com.starrocks.scheduler.mv;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MVPCTRefreshPlanBuilder {
    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshPlanBuilder.class);
    private final MaterializedView mv;
    private final MvTaskRunContext mvContext;
    private final MVPCTRefreshPartitioner mvRefreshPartitioner;
    private final boolean isEnableInsertStrict;

    public MVPCTRefreshPlanBuilder(MaterializedView mv,
                                   MvTaskRunContext mvContext,
                                   MVPCTRefreshPartitioner mvRefreshPartitioner) {
        this.mv = mv;
        this.mvContext = mvContext;
        this.mvRefreshPartitioner = mvRefreshPartitioner;
        this.isEnableInsertStrict = mvContext.getCtx().getSessionVariable().getEnableInsertStrict();
    }

    public InsertStmt analyzeAndBuildInsertPlan(InsertStmt insertStmt,
                                                Map<String, Set<String>> refTableRefreshPartitions,
                                                ConnectContext ctx) throws AnalysisException {
        Analyzer.analyze(insertStmt, ctx);
        InsertStmt newInsertStmt = buildInsertPlan(insertStmt, refTableRefreshPartitions);
        return newInsertStmt;
    }

    private InsertStmt buildInsertPlan(InsertStmt insertStmt,
                                       Map<String, Set<String>> refTableRefreshPartitions) throws AnalysisException {
        // if the refTableRefreshPartitions is empty(not partitioned mv), no need to generate partition predicate
        if (refTableRefreshPartitions.isEmpty()) {
            LOG.info("There is no ref table partitions to refresh, skip to generate partition predicates");
            return insertStmt;
        }

        // after analyze, we could get the table meta info of the tableRelation.
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        // try to push down into query relation so can push down filter into both sides
        // NOTE: it's safe here to push the partition predicate into query relation directly because
        // partition predicates always belong to the relation output expressions and can be resolved
        // by the query analyzer.
        QueryRelation queryRelation = queryStatement.getQueryRelation();
        List<Expr> extraPartitionPredicates = Lists.newArrayList();
        Multimap<String, TableRelation> tableRelations = AnalyzerUtils.collectAllTableRelation(queryStatement);
        Map<Table, SlotRef> mvRefBaseTablePartitionSlotRefs = mv.getRefBaseTablePartitionSlots();
        if (CollectionUtils.sizeIsEmpty(mvRefBaseTablePartitionSlotRefs)) {
            throw new AnalysisException(String.format("MV refresh cannot generate partition predicates " +
                    "because of mv %s contains no ref base table's partitions", mv.getName()));
        }

        Set<String> uniqueTableNames = tableRelations.keySet().stream().collect(Collectors.toSet());
        int numOfPushDownIntoTables = 0;
        boolean hasGenerateNonPushDownPredicates = false;
        SessionVariable sessionVariable = mvContext.getCtx().getSessionVariable();
        boolean isEnableMVRefreshQueryRewrite = sessionVariable.isEnableMaterializedViewRewriteForInsert();
        for (String tblName : uniqueTableNames) {
            // skip to generate partition predicate for non-ref base tables
            if (!refTableRefreshPartitions.containsKey(tblName)) {
                LOG.warn("Skip to generate partition predicate to refresh because it's not ref " +
                                "base table, table: {}, mv:{}, refTableRefreshPartitions:{}", tblName, mv.getName(),
                        refTableRefreshPartitions);
                continue;
            }
            // set partition names for ref base table
            Set<String> tablePartitionNames = refTableRefreshPartitions.get(tblName);
            Collection<TableRelation> relations = tableRelations.get(tblName);
            TableRelation tableRelation = relations.iterator().next();
            Table table = tableRelation.getTable();
            if (table == null) {
                throw new AnalysisException(String.format("Optimize materialized view %s refresh task, generate table relation " +
                        "%s failed: table is null", mv.getName(), tableRelation.getName()));
            }
            // skip it table is not ref base table.
            if (!mvRefBaseTablePartitionSlotRefs.containsKey(table)) {
                LOG.warn("Skip to generate partition predicate because it's mv direct ref base table:{}, mv:{}, " +
                        "refBaseTableAndCol: {}", table, mv.getName(), mvRefBaseTablePartitionSlotRefs);
                continue;
            }
            SlotRef refTablePartitionSlotRef = mvRefBaseTablePartitionSlotRefs.get(table);
            if (refTablePartitionSlotRef == null) {
                throw new AnalysisException(String.format("Generate partition predicate failed: " +
                        "cannot find partition slot ref %s from query relation"));
            }

            // If there are multiple table relations, don't push down partition predicate into table relation
            // If `enable_mv_refresh_query_rewrite` is enabled, table relation should not set partition names
            // since it will deduce `hasTableHints` to true and causes rewrite failed.
            boolean isPushDownBelowTable = (relations.size() == 1);
            if (isPushDownBelowTable) {
                boolean ret = pushDownPartitionPredicates(table, tableRelation, refTablePartitionSlotRef,
                        tablePartitionNames, isEnableMVRefreshQueryRewrite);
                if (ret) {
                    numOfPushDownIntoTables += 1;
                } else {
                    LOG.warn("Generate push down partition predicate failed, table:{}", table);
                }
            } else {
                LOG.warn("Ref base table contains self join, cannot push down partition predicates, table:{}",
                        table.getName());
                // For non-push-down predicates, it only needs to be generated only once since we can only use mv's partition
                // info ref column to generate incremental partition predicates.
                // eg:
                // mv:  create mv xx as select dt as dt1, a from t1 join t2 on t1.dt = t2.dt;
                // non-push-down predicate: where dt1 in ('2024-07-15')
                if (hasGenerateNonPushDownPredicates) {
                    continue;
                }
                // Use the mv's partition info ref column to generate incremental partition predicates rather than ref base
                // table's slot ref since ref base table's partition column may be aliased in the query relation.
                String mvPartitionInfoRefColName = getMVPartitionInfoRefColumnName();
                // if it hasn't pushed down into table, add it into the query relation's predicate
                Expr mvPartitionOutputExpr = getPartitionOutputExpr(queryStatement, mvPartitionInfoRefColName);
                if (mvPartitionOutputExpr == null) {
                    LOG.warn("Fail to generate partition predicates for self-join table because output expr is null, " +
                            "table: {}, refTablePartitionSlotRef:{}", table.getName(), refTablePartitionSlotRef);
                    continue;
                }
                Expr partitionPredicate = generatePartitionPredicate(table, tablePartitionNames, mvPartitionOutputExpr);
                if (partitionPredicate == null) {
                    LOG.warn("Fail to generate partition predicates for self-join table, " +
                            "table: {}, refTablePartitionSlotRef:{}", table.getName(), refTablePartitionSlotRef);
                    continue;
                }
                hasGenerateNonPushDownPredicates = true;
                extraPartitionPredicates.add(partitionPredicate);
            }
        }
        if (extraPartitionPredicates.isEmpty()) {
            doIfNoPushDownPredicates(numOfPushDownIntoTables, refTableRefreshPartitions);
            LOG.info("Generate partition extra predicates empty, mv:{}, numOfPushDownIntoTables:{}",
                    mv.getName(), numOfPushDownIntoTables);
            return insertStmt;
        }
        if (queryRelation instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) queryRelation;
            extraPartitionPredicates.add(selectRelation.getWhereClause());
            Expr finalPredicate = Expr.compoundAnd(extraPartitionPredicates);
            selectRelation.setWhereClause(finalPredicate);
            LOG.info("Optimize materialized view {} refresh task, generate insert stmt final " +
                    "predicate(select relation):{} ", mv.getName(), finalPredicate.toSql());
        } else {
            // support to generate partition predicate for other query relation types
            LOG.warn("MV Refresh cannot push down partition predicate since " +
                    "the query relation is not select relation, mv:{}", mv.getName());
            List<SelectListItem> items = queryRelation.getOutputExpression().stream()
                    .map(x -> new SelectListItem(x, null)).collect(Collectors.toList());
            SelectList selectList = new SelectList(items, false);
            SelectRelation selectRelation = new SelectRelation(selectList, queryRelation,
                    Expr.compoundAnd(extraPartitionPredicates), null, null);
            selectRelation.setWhereClause(Expr.compoundAnd(extraPartitionPredicates));
            QueryStatement newQueryStatement = new QueryStatement(selectRelation);
            insertStmt.setQueryStatement(newQueryStatement);
            new QueryAnalyzer(mvContext.getCtx()).analyze(newQueryStatement);
        }
        return insertStmt;
    }

    private boolean pushDownPartitionPredicates(Table table,
                                                TableRelation tableRelation,
                                                SlotRef refBaseTablePartitionSlot,
                                                Set<String> tablePartitionNames,
                                                boolean isEnableMVRefreshQueryRewrite) throws AnalysisException {
        if (isEnableMVRefreshQueryRewrite) {
            // When `isEnableMVRefreshQueryRewrite` is true, disable push down partition names into scan node since
            // mv rewrite will disable rewrite if table scan contains table partitions/tablets hint.
            return pushDownByPredicate(table, tableRelation, refBaseTablePartitionSlot, tablePartitionNames);
        } else {
            if (pushDownByPartitionNames(table, tableRelation, tablePartitionNames)) {
                return true;
            }
            if (pushDownByPredicate(table, tableRelation, refBaseTablePartitionSlot, tablePartitionNames)) {
                return true;
            }
            return false;
        }
    }

    private boolean pushDownByPartitionNames(Table table,
                                             TableRelation tableRelation,
                                             Set<String> tablePartitionNames) {
        if (table.isExternalTableWithFileSystem()) {
            return false;
        }
        // external table doesn't support query with partitionNames
        LOG.info("Optimize materialized view {} refresh task, push down partition names into table " +
                        "relation {}, filtered partition names:{} ",
                mv.getName(), tableRelation.getName(), Joiner.on(",").join(tablePartitionNames));
        tableRelation.setPartitionNames(
                new PartitionNames(false, new ArrayList<>(tablePartitionNames)));
        return true;
    }

    private boolean pushDownByPredicate(Table table,
                                        TableRelation tableRelation,
                                        SlotRef refBaseTablePartitionSlot,
                                        Set<String> tablePartitionNames) throws AnalysisException {
        // generate partition predicate for the select relation, so can generate partition predicates
        // for non-ref base tables.
        // eg:
        //  mv: create mv mv1 partition by t1.dt
        //  as select  * from t1 join t2 on t1.dt = t2.dt.
        //  ref-base-table      : t1.dt
        //  non-ref-base-table  : t2.dt
        // so add partition predicates for select relation when refresh partitions incrementally(eg: dt=20230810):
        // (select * from t1 join t2 on t1.dt = t2.dt) where t1.dt=20230810
        SlotRef cloned = (SlotRef) refBaseTablePartitionSlot.clone();
        cloned.setTblName(null);
        Expr partitionPredicate = generatePartitionPredicate(table,
                tablePartitionNames, cloned);
        if (partitionPredicate == null) {
            LOG.warn("Generate partition predicate failed, table:{}, tablePartitionNames:{}, outputMRefVPartitionExpr:{}",
                    table, tablePartitionNames, cloned);
            return false;
        }
        // try to push down into table relation
        final List<SlotRef> slots = ImmutableList.of(cloned);
        Scope tableRelationScope = tableRelation.getScope();
        if (!canResolveSlotsInTheScope(slots, tableRelationScope)) {
            throw new AnalysisException(String.format("Cannot generate partition predicate " +
                            "because cannot find partition slot ref in ref table's scope, refBaseTable:%s, " +
                            "refBaseTablePartitionSlot:%s, tablePartitionNames:%s",
                    table, cloned, tablePartitionNames));
        }
        LOG.info("Optimize materialized view {} refresh task, push down partition predicate into table " +
                        "relation {},  partition predicate:{} ",
                mv.getName(), tableRelation.getName(), partitionPredicate.toSql());
        tableRelation.setPartitionPredicate(partitionPredicate);
        return true;
    }

    /**
     * This is only used to self-joins table for now and to be compatible with before.
     */
    @Deprecated
    private Expr getPartitionOutputExpr(QueryStatement queryStatement, String mvPartitionInfoRefColName) {
        if (mvPartitionInfoRefColName == null) {
            LOG.warn("Generate partition predicate failed: " +
                    "mv partition info ref column is null, mv:{}", mv.getName());
            return null;
        }
        Expr outputPartitionSlot = findPartitionOutputExpr(queryStatement, mvPartitionInfoRefColName);
        if (outputPartitionSlot == null) {
            LOG.warn("Generate partition predicate failed: " +
                    "cannot find partition slot ref {} from query relation", mvPartitionInfoRefColName);
            return null;
        }
        return outputPartitionSlot;
    }

    private Expr findPartitionOutputExpr(QueryStatement queryStatement, String mvPartitionInfoRefColName) {
        List<String> columnOutputNames = queryStatement.getQueryRelation().getColumnOutputNames();
        List<Expr> outputExpressions = queryStatement.getQueryRelation().getOutputExpression();
        for (int i = 0; i < outputExpressions.size(); ++i) {
            Expr expr = outputExpressions.get(i);
            if (columnOutputNames.get(i).equalsIgnoreCase(mvPartitionInfoRefColName)) {
                return expr;
            } else if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)
                        && functionCallExpr.getChild(0) instanceof SlotRef) {
                    SlotRef slot = functionCallExpr.getChild(0).cast();
                    if (slot.getColumnName().equalsIgnoreCase(mvPartitionInfoRefColName)) {
                        return slot;
                    }
                }
            } else {
                // alias name.
                SlotRef slotRef = expr.unwrapSlotRef();
                if (slotRef != null && slotRef.getColumnName().equals(mvPartitionInfoRefColName)) {
                    return outputExpressions.get(i);
                }
            }
        }
        return null;
    }

    /**
     * Get the partition column name of the mv's partition info.
     * eg:
     *  table1: partition by dt
     *  mv: create mv as select dt as dt1, key1 from table1;
     * then mv partition info ref column name is dt1 rather than dt.
     */
    private String getMVPartitionInfoRefColumnName() {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isExprRangePartitioned()) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            List<Expr> exprs = expressionRangePartitionInfo.getPartitionExprs(mv.getIdToColumn());
            Preconditions.checkState(exprs.size() == 1);
            List<SlotRef> slotRefs = Lists.newArrayList();
            exprs.get(0).collect(SlotRef.class, slotRefs);
            // if partitionExpr is FunctionCallExpr, get first SlotRef
            Preconditions.checkState(slotRefs.size() == 1);
            return slotRefs.get(0).getColumnName();
        } else if (partitionInfo.isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            List<Column> partitionColumns = listPartitionInfo.getPartitionColumns(mv.getIdToColumn());
            Preconditions.checkState(partitionColumns.size() == 1);
            return partitionColumns.get(0).getName();
        }
        return null;
    }

    /**
     * Generate partition predicates to refresh the materialized view so can be refreshed by the incremental partitions.
     *
     * @param tablePartitionNames : the need pruned partition tables of the ref base table
     * @return
     * @throws AnalysisException
     */
    private Expr generatePartitionPredicate(Table table, Set<String> tablePartitionNames,
                                            Expr mvPartitionOutputExpr)
            throws AnalysisException {
        if (tablePartitionNames.isEmpty()) {
            // If the updated partition names are empty, it means that the table should not be refreshed.
            return new BoolLiteral(false);
        }
        return mvRefreshPartitioner.generatePartitionPredicate(table, tablePartitionNames, mvPartitionOutputExpr);
    }

    private void doIfNoPushDownPredicates(int numOfPushDownIntoTables,
                                          Map<String, Set<String>> refTableRefreshPartitions) throws AnalysisException {
        int refBaseTableSize = refTableRefreshPartitions.size();
        if (numOfPushDownIntoTables == refBaseTableSize) {
            return;
        }
        LOG.warn("Cannot generate partition predicate for mv refresh {} and there " +
                        "are no predicate push down tables, refBaseTableSize:{}, numOfPushDownIntoTables:{}", mv.getName(),
                refBaseTableSize, numOfPushDownIntoTables);
        if (isEnableInsertStrict) {
            throw new AnalysisException(String.format("Cannot generate partition predicate for mv refresh %s",
                    mv.getName()));
        }
    }

    /**
     * Check whether to push down predicate expr with the slot refs into the scope.
     *
     * @param slots : slot refs that are contained in the predicate expr
     * @param scope : scope that try to push down into.
     * @return
     */
    private boolean canResolveSlotsInTheScope(List<SlotRef> slots, Scope scope) {
        return slots.stream().allMatch(s -> scope.tryResolveField(s).isPresent());
    }
}