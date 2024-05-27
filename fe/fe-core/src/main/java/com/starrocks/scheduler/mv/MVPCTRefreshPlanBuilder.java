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
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getStr2DateExpr;

public class MVPCTRefreshPlanBuilder {
    private static final Logger LOG = LogManager.getLogger(MVPCTRefreshPlanBuilder.class);
    private final MaterializedView mv;
    private final MvTaskRunContext mvContext;

    public MVPCTRefreshPlanBuilder(MaterializedView mv, MvTaskRunContext mvContext) {
        this.mv = mv;
        this.mvContext = mvContext;
    }

    public InsertStmt analyzeAndBuildInsertPlan(InsertStmt insertStmt,
                                                Map<String, Set<String>> refTableRefreshPartitions,
                                                ConnectContext ctx) throws AnalysisException {
        // analyze the insert stmt
        Analyzer.analyze(insertStmt, ctx);
        // if the refTableRefreshPartitions is empty(not partitioned mv), no need to generate partition predicate
        if (refTableRefreshPartitions.isEmpty()) {
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
        for (String tblName : tableRelations.keys()) {
            // skip to generate partition predicate for non-ref base tables
            if (!refTableRefreshPartitions.containsKey(tblName) || !tableRelations.containsKey(tblName)) {
                continue;
            }
            // set partition names for ref base table
            Set<String> tablePartitionNames = refTableRefreshPartitions.get(tblName);
            Collection<TableRelation> relations = tableRelations.get(tblName);
            TableRelation tableRelation = relations.iterator().next();

            // if there are multiple table relations, don't push down partition predicate into table relation
            boolean isPushDownBelowTable = (relations.size() == 1);
            Table table = tableRelation.getTable();
            if (table == null) {
                LOG.warn("Optimize materialized view {} refresh task, generate table relation {} failed: " +
                                "table is null", mv.getName(), tableRelation.getName());
                continue;
            }
            // external table doesn't support query with partitionNames
            if (isPushDownBelowTable && !table.isExternalTableWithFileSystem()) {
                LOG.info("Optimize materialized view {} refresh task, generate table relation {} target partition names:{} ",
                        mv.getName(), tableRelation.getName(), Joiner.on(",").join(tablePartitionNames));
                tableRelation.setPartitionNames(
                        new PartitionNames(false, new ArrayList<>(tablePartitionNames)));
            }

            Pair<Table, Column> refBaseTableAndCol = mv.getDirectTableAndPartitionColumn();
            if (refBaseTableAndCol == null || !refBaseTableAndCol.first.equals(table)) {
                continue;
            }
            // generate partition predicate for the select relation, so can generate partition predicates
            // for non-ref base tables.
            // eg:
            //  mv: create mv mv1 partition by t1.dt
            //  as select  * from t1 join t2 on t1.dt = t2.dt.
            //  ref-base-table      : t1.dt
            //  non-ref-base-table  : t2.dt
            // so add partition predicates for select relation when refresh partitions incrementally(eg: dt=20230810):
            // (select * from t1 join t2 on t1.dt = t2.dt) where t1.dt=20230810
            Expr partitionPredicate = generatePartitionPredicate(table, tablePartitionNames,
                    queryStatement, mv.getPartitionInfo());
            if (partitionPredicate == null) {
                continue;
            }
            // try to push down into table relation
            List<SlotRef> slots = Lists.newArrayList();
            partitionPredicate.collect(SlotRef.class, slots);
            Scope tableRelationScope = tableRelation.getScope();
            if (isPushDownBelowTable && canResolveSlotsInTheScope(slots, tableRelationScope)) {
                LOG.info("Optimize materialized view {} refresh task, generate table relation {} " +
                                "partition predicate:{} ",
                        mv.getName(), tableRelation.getName(), partitionPredicate.toSql());
                tableRelation.setPartitionPredicate(partitionPredicate);
            }
            extraPartitionPredicates.add(partitionPredicate);
        }
        if (extraPartitionPredicates.isEmpty()) {
            return insertStmt;
        }

        if (queryRelation instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) queryRelation;
            extraPartitionPredicates.add(selectRelation.getWhereClause());
            Expr finalPredicate = Expr.compoundAnd(extraPartitionPredicates);
            selectRelation.setWhereClause(finalPredicate);
            LOG.info("Optimize materialized view {} refresh task, generate insert stmt final " +
                            "predicate(select relation):{} ", mv.getName(), finalPredicate.toSql());
        }
        return insertStmt;
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

    /**
     * Generate partition predicates to refresh the materialized view so can be refreshed by the incremental partitions.
     *
     * @param tablePartitionNames : the need pruned partition tables of the ref base table
     * @param queryStatement      : the materialized view's defined query statement
     * @param mvPartitionInfo     : the materialized view's partition information
     * @return
     * @throws AnalysisException
     */
    private Expr generatePartitionPredicate(Table table, Set<String> tablePartitionNames,
                                            QueryStatement queryStatement, PartitionInfo mvPartitionInfo)
            throws AnalysisException {
        SlotRef partitionSlot = MaterializedView.getRefBaseTablePartitionSlotRef(mv);
        List<String> columnOutputNames = queryStatement.getQueryRelation().getColumnOutputNames();
        List<Expr> outputExpressions = queryStatement.getQueryRelation().getOutputExpression();
        Expr outputPartitionSlot = null;
        for (int i = 0; i < outputExpressions.size(); ++i) {
            if (columnOutputNames.get(i).equalsIgnoreCase(partitionSlot.getColumnName())) {
                outputPartitionSlot = outputExpressions.get(i);
                break;
            } else if (outputExpressions.get(i) instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) outputExpressions.get(i);
                if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)
                        && functionCallExpr.getChild(0) instanceof SlotRef) {
                    SlotRef slot = functionCallExpr.getChild(0).cast();
                    if (slot.getColumnName().equalsIgnoreCase(partitionSlot.getColumnName())) {
                        outputPartitionSlot = slot;
                        break;
                    }
                }
            } else {
                // alias name.
                SlotRef slotRef = outputExpressions.get(i).unwrapSlotRef();
                if (slotRef != null && slotRef.getColumnName().equals(partitionSlot.getColumnName())) {
                    outputPartitionSlot = outputExpressions.get(i);
                    break;
                }
            }
        }

        if (outputPartitionSlot == null) {
            LOG.warn("Generate partition predicate failed: " +
                    "cannot find partition slot ref {} from query relation", partitionSlot);
            return null;
        }

        if (mvPartitionInfo.isRangePartition()) {
            List<Range<PartitionKey>> sourceTablePartitionRange = Lists.newArrayList();
            for (String partitionName : tablePartitionNames) {
                sourceTablePartitionRange.add(mvContext.getRefBaseTableRangePartitionMap()
                        .get(table).get(partitionName));
            }
            sourceTablePartitionRange = MvUtils.mergeRanges(sourceTablePartitionRange);
            // for nested mv, the base table may be another mv, which is partition by str2date(dt, '%Y%m%d')
            // here we should convert date into '%Y%m%d' format
            Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
            Pair<Table, Column> partitionTableAndColumn = mv.getDirectTableAndPartitionColumn();
            boolean isConvertToDate = PartitionUtil.isConvertToDate(partitionExpr, partitionTableAndColumn.second);
            if (isConvertToDate && partitionExpr instanceof FunctionCallExpr
                    && !sourceTablePartitionRange.isEmpty() && MvUtils.isDateRange(sourceTablePartitionRange.get(0))) {
                Optional<FunctionCallExpr> functionCallExprOpt = getStr2DateExpr(partitionExpr);
                if (!functionCallExprOpt.isPresent()) {
                    LOG.warn("invalid partition expr:{}", partitionExpr);
                    return null;
                }
                FunctionCallExpr functionCallExpr = functionCallExprOpt.get();
                Preconditions.checkState(
                        functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE));
                String dateFormat = ((StringLiteral) functionCallExpr.getChild(1)).getStringValue();
                List<Range<PartitionKey>> converted = Lists.newArrayList();
                for (Range<PartitionKey> range : sourceTablePartitionRange) {
                    Range<PartitionKey> varcharPartitionKey = MvUtils.convertToVarcharRange(range, dateFormat);
                    converted.add(varcharPartitionKey);
                }
                sourceTablePartitionRange = converted;
            }
            List<Expr> partitionPredicates =
                    MvUtils.convertRange(outputPartitionSlot, sourceTablePartitionRange);
            // range contains the min value could be null value
            Optional<Range<PartitionKey>> nullRange = sourceTablePartitionRange.stream().
                    filter(range -> range.lowerEndpoint().isMinValue()).findAny();
            if (nullRange.isPresent()) {
                Expr isNullPredicate = new IsNullPredicate(outputPartitionSlot, false);
                partitionPredicates.add(isNullPredicate);
            }

            return Expr.compoundOr(partitionPredicates);
        } else if (mvPartitionInfo.getType() == PartitionType.LIST) {
            Map<String, List<List<String>>> baseListPartitionMap = mvContext.getRefBaseTableListPartitionMap();
            Type partitionType = mvContext.getRefBaseTablePartitionColumn().getType();
            List<LiteralExpr> sourceTablePartitionList = Lists.newArrayList();
            for (String tablePartitionName : tablePartitionNames) {
                List<List<String>> values = baseListPartitionMap.get(tablePartitionName);
                for (List<String> value : values) {
                    LiteralExpr partitionValue = new PartitionValue(value.get(0)).getValue(partitionType);
                    sourceTablePartitionList.add(partitionValue);
                }
            }
            List<Expr> partitionPredicates = MvUtils.convertList(outputPartitionSlot, sourceTablePartitionList);
            return Expr.compoundOr(partitionPredicates);
        } else {
            LOG.warn("Generate partition predicate failed: " +
                    "partition slot {} is not supported yet: {}", partitionSlot, mvPartitionInfo);
            return null;
        }
    }
}
