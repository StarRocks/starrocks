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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.connector.PartitionUtil.generateMVPartitionName;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.Operator.OP_UNION_ALL_BIT;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.mergeRanges;

/**
 * This class represents all partition compensations for partition predicates in a materialized view.
 */
public class MvPartitionCompensator {
    private static final Logger LOG = LogManager.getLogger(MvPartitionCompensator.class);

    /**
     * External scan operators should be supported if it has been supported in
     * {@link com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner}
     */
    public static final ImmutableSet<OperatorType> SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES =
            ImmutableSet.<OperatorType>builder()
                    .add(OperatorType.LOGICAL_HIVE_SCAN)
                    .add(OperatorType.LOGICAL_ICEBERG_SCAN)
                    .build();

    public static final ImmutableSet<OperatorType> SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES =
            ImmutableSet.<OperatorType>builder()
                    .add(OperatorType.LOGICAL_OLAP_SCAN)
                    .addAll(SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES)
                    .build();

    /**
     * Whether the table is supported to compensate extra partition predicates.
     * @param t: input table
     * @return: true if the table is supported to compensate extra partition predicates, otherwise false.
     */
    public static boolean isTableSupportedPartitionCompensate(Table t) {
        if (t == null) {
            return false;
        }
        if (t.isNativeTableOrMaterializedView() || t.isIcebergTable() || t.isHiveTable()) {
            return true;
        }
        return false;
    }

    /**
     * Determine whether to compensate extra partition predicates to query plan for the mv,
     * - if it needs compensate, use `selectedPartitionIds` to compensate complete partition ranges
     *  with lower and upper bound.
     * - if not compensate, use original pruned partition predicates as the compensated partition
     *  predicates.
     * @param queryPlan : query opt expression
     * @param mvContext : materialized view context
     * @return Optional<Boolean>: if `queryPlan` contains ref table, Optional is set and return whether mv can satisfy
     * query plan's freshness, other Optional.empty() is returned.
     */
    public static MVCompensation getMvCompensation(OptExpression queryPlan,
                                                   MaterializationContext mvContext) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        MvUpdateInfo mvUpdateInfo = mvContext.getMvUpdateInfo();
        Set<String> mvPartitionNameToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
        // If mv contains no partitions to refresh, no need compensate
        if (Objects.isNull(mvPartitionNameToRefresh) || mvPartitionNameToRefresh.isEmpty()) {
            logMVRewrite(mvContext, "MV has no partitions to refresh, no need compensate");
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        // If no partition table and columns, no need compensate
        MaterializedView mv = mvContext.getMv();
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            logMVRewrite(mvContext, "MV's partition table and columns is null, unknown state");
            return MVCompensation.createUnkownState(sessionVariable);
        }
        Table refBaseTable = partitionTableAndColumns.first;

        // If ref table contains no partitions to refresh, no need compensate.
        // If the mv is partitioned and non-ref table need refresh, then all partitions need to be refreshed,
        // it can not be a candidate.
        Set<String> refTablePartitionNameToRefresh = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
        if (refTablePartitionNameToRefresh == null) {
            logMVRewrite(mvContext, "MV's ref base to refresh partition is null, unknown state");
            return MVCompensation.createUnkownState(sessionVariable);
        }
        if (refTablePartitionNameToRefresh.isEmpty()) {
            logMVRewrite(mvContext, "MV's ref base to refresh partition is empty, no need compensate");
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlan);
        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return MVCompensation.createUnkownState(sessionVariable);
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        // only set this when `queryExpression` contains ref table, otherwise the cached value maybe dirty.
        LogicalScanOperator refScanOperator = getRefBaseTableScanOperator(scanOperators, refBaseTable);
        if (refScanOperator == null) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        Table table = refScanOperator.getTable();
        // If table's not partitioned, no need compensate
        if (table.isUnPartitioned()) {
            // TODO: Support this later.
            logMVRewrite(mvContext, "MV's is un-partitioned, unknown state");
            return MVCompensation.createUnkownState(sessionVariable);
        }

        if (refScanOperator instanceof LogicalOlapScanOperator) {
            return getMvCompensationForOlap(sessionVariable, refBaseTable, refTablePartitionNameToRefresh,
                    (LogicalOlapScanOperator) refScanOperator);
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(refScanOperator.getOpType())) {
            return getMvCompensationForExternal(sessionVariable, refTablePartitionNameToRefresh, refScanOperator);
        } else {
            return MVCompensation.createUnkownState(sessionVariable);
        }
    }

    private static MVCompensation getMvCompensationForOlap(SessionVariable sessionVariable,
                                                           Table refBaseTable,
                                                           Set<String> refTablePartitionNameToRefresh,
                                                           LogicalOlapScanOperator olapScanOperator) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

        List<Long> selectPartitionIds = olapScanOperator.getSelectedPartitionId();
        if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        // if any of query's select partition ids has not been refreshed, then no rewrite with this mv.
        if (selectPartitionIds.stream()
                .map(id -> olapTable.getPartition(id))
                .noneMatch(part -> refTablePartitionNameToRefresh.contains(part.getName()))) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        // if mv's to refresh partitions contains any of query's select partition ids, then rewrite with compensate.
        List<Long> toRefreshRefTablePartitions = getMvRefTableCompensatePartitionsForOlap(refTablePartitionNameToRefresh,
                refBaseTable, olapScanOperator);
        if (toRefreshRefTablePartitions == null) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        if (Sets.newHashSet(toRefreshRefTablePartitions).containsAll(selectPartitionIds)) {
            return MVCompensation.createNoRewriteState(sessionVariable);
        }

        return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE,
                toRefreshRefTablePartitions, null);
    }

    private static MVCompensation getMvCompensationForExternal(SessionVariable sessionVariable,
                                                               Set<String> refTablePartitionNamesToRefresh,
                                                               LogicalScanOperator refScanOperator) {
        try {
            ScanOperatorPredicates scanOperatorPredicates = refScanOperator.getScanOperatorPredicates();
            Collection<Long> selectPartitionIds = scanOperatorPredicates.getSelectedPartitionIds();
            if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
                // see OptExternalPartitionPruner#computePartitionInfo:
                // it's not the same meaning when selectPartitionIds is null and empty for hive and other tables
                if (refScanOperator.getOpType() == OperatorType.LOGICAL_HIVE_SCAN) {
                    return MVCompensation.createNoCompensateState(sessionVariable);
                } else {
                    return MVCompensation.createUnkownState(sessionVariable);
                }
            }
            List<PartitionKey> selectPartitionKeys = scanOperatorPredicates.getSelectedPartitionKeys();
            if (selectPartitionKeys.stream()
                    .map(PartitionUtil::generateMVPartitionName)
                    .noneMatch(x -> refTablePartitionNamesToRefresh.contains(x))) {
                return MVCompensation.createNoCompensateState(sessionVariable);
            }
            // if mv's to refresh partitions contains any of query's select partition ids, then rewrite with compensate.
            List<PartitionKey> toRefreshRefTablePartitions = getMvRefTableCompensatePartitionsForExternal(
                    refTablePartitionNamesToRefresh, refScanOperator);
            if (toRefreshRefTablePartitions == null) {
                return MVCompensation.createUnkownState(sessionVariable);
            }
            if (Sets.newHashSet(toRefreshRefTablePartitions).containsAll(selectPartitionKeys)) {
                return MVCompensation.createNoRewriteState(sessionVariable);
            }
            return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, null,
                    toRefreshRefTablePartitions);
        } catch (AnalysisException e) {
            return MVCompensation.createUnkownState(sessionVariable);
        }
    }

    /**
     * Get mv's compensate partitions for ref table(olap table).
     * @param refBaseTable: materialized view's ref base table
     * @param refScanOperator: ref base table's scan operator.
     * @return: need to compensate partition ids of the materialized view.
     */
    private static List<Long> getMvRefTableCompensatePartitionsForOlap(Set<String> refTablePartitionNameToRefresh,
                                                                       Table refBaseTable,
                                                                       LogicalScanOperator refScanOperator) {

        LogicalOlapScanOperator olapScanOperator = ((LogicalOlapScanOperator) refScanOperator);
        if (olapScanOperator.getSelectedPartitionId() == null) {
            return null;
        }
        List<Long> refTableCompensatePartitionIds = Lists.newArrayList();
        List<Long> selectPartitionIds = olapScanOperator.getSelectedPartitionId();
        for (Long selectPartitionId : selectPartitionIds) {
            Partition partition = refBaseTable.getPartition(selectPartitionId);
            // If this partition has updated, add it into compensate partition ids.
            if (refTablePartitionNameToRefresh.contains(partition.getName())) {
                refTableCompensatePartitionIds.add(selectPartitionId);
            }
        }
        return refTableCompensatePartitionIds;
    }

    private static List<PartitionKey> getMvRefTableCompensatePartitionsForExternal(Set<String> refTablePartitionNamesToRefresh,
                                                                                   LogicalScanOperator refScanOperator)
            throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = null;
        try {
            scanOperatorPredicates = refScanOperator.getScanOperatorPredicates();
        } catch (Exception e) {
            return null;
        }
        if (scanOperatorPredicates == null) {
            return null;
        }
        List<PartitionKey> refTableCompensatePartitionKeys = Lists.newArrayList();
        List<PartitionKey> selectPartitionKeys = scanOperatorPredicates.getSelectedPartitionKeys();
        // different behavior for different external table types
        if (selectPartitionKeys.isEmpty() && refScanOperator.getOpType() != OperatorType.LOGICAL_HIVE_SCAN) {
            return null;
        }
        for (PartitionKey partitionKey : selectPartitionKeys) {
            String partitionName = generateMVPartitionName(partitionKey);
            if (refTablePartitionNamesToRefresh.contains(partitionName)) {
                refTableCompensatePartitionKeys.add(partitionKey);
            }
        }
        return refTableCompensatePartitionKeys;
    }

    /**
     * Get all refreshed partitions' plan of the materialized view.
     * @param mvContext: materialized view context
     * @return:  a pair of compensated mv scan plan(refreshed partitions) and its output columns
     */
    private static Pair<OptExpression, List<ColumnRefOperator>> getMvScanPlan(MaterializationContext mvContext) {
        // NOTE: mv's scan operator has already been partition pruned by filtering refreshed partitions,
        // see MvRewritePreprocessor#createScanMvOperator.
        final LogicalOlapScanOperator mvScanOperator = mvContext.getScanMvOperator();
        final MaterializedView mv = mvContext.getMv();
        final LogicalOlapScanOperator.Builder mvScanBuilder = OperatorBuilderFactory.build(mvScanOperator);
        final List<ColumnRefOperator> orgMvScanOutputColumns = MvUtils.getMvScanOutputColumnRefs(mv,
                mvScanOperator);
        mvScanBuilder.withOperator(mvScanOperator);
        OptExpression mvScanOptExpression = OptExpression.create(mvScanBuilder.build());
        deriveLogicalProperty(mvScanOptExpression);
        // duplicate mv's plan and output columns
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression newMvScanPlan = duplicator.duplicate(mvScanOptExpression);
        // output columns order by mv's columns
        List<ColumnRefOperator> mvScanOutputColumns = duplicator.getMappedColumns(orgMvScanOutputColumns);
        newMvScanPlan.getOp().setOpRuleMask(OP_UNION_ALL_BIT);
        return Pair.create(newMvScanPlan, mvScanOutputColumns);
    }

    /**
     * Get all to-refreshed partitions' plan of the materialized view.
     * @param mvContext: materialized view context
     * @param mvCompensation: materialized view's compensation info
     * @return:  a pair of compensated mv query scan plan(to-refreshed partitions) and its output columns
     */
    private static Pair<OptExpression, List<ColumnRefOperator>> getMvQueryPlan(MaterializationContext mvContext,
                                                                               MVCompensation mvCompensation) {
        final OptExpression mvQueryPlan = mvContext.getMvExpression();
        OptExpression compensateMvQueryPlan = getMvCompensateQueryPlan(mvContext, mvCompensation, mvQueryPlan);
        if (compensateMvQueryPlan == null) {
            logMVRewrite(mvContext, "Get mv compensate query plan failed");
            return null;
        }
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression newMvQueryPlan = duplicator.duplicate(compensateMvQueryPlan);
        if (newMvQueryPlan == null) {
            logMVRewrite(mvContext, "Duplicate compensate query plan failed");
            return null;
        }
        deriveLogicalProperty(newMvQueryPlan);
        List<ColumnRefOperator> orgMvQueryOutputColumnRefs = mvContext.getMvOutputColumnRefs();
        List<ColumnRefOperator> mvQueryOutputColumnRefs = duplicator.getMappedColumns(orgMvQueryOutputColumnRefs);
        newMvQueryPlan.getOp().setOpRuleMask(OP_UNION_ALL_BIT);
        return Pair.create(newMvQueryPlan, mvQueryOutputColumnRefs);
    }

    public static OptExpression getMvCompensateQueryPlan(MaterializationContext mvContext,
                                                         MVCompensation mvCompensation,
                                                         OptExpression mvQueryPlan) {
        MaterializedView mv = mvContext.getMv();
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }
        // only set this when `queryExpression` contains ref table, otherwise the cached value maybe dirty.
        Table refBaseTable = partitionTableAndColumns.first;
        LogicalScanOperator mvRefScanOperator = getRefBaseTableScanOperator(mvQueryPlan, refBaseTable);
        if (mvRefScanOperator == null) {
            return null;
        }

        if (mvRefScanOperator instanceof LogicalOlapScanOperator) {
            List<Long> refTableCompensatePartitionIds = mvCompensation.getOlapCompensatePartitionIds();
            if (refTableCompensatePartitionIds == null) {
                logMVRewrite(mvContext, "Get ref olap base table's compensation partition ids is null");
                return null;
            }
            return MVPartitionPruner.getOlapTableCompensatePlan(mvQueryPlan, mvRefScanOperator, refTableCompensatePartitionIds);
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(mvRefScanOperator.getOpType())) {
            List<PartitionKey> refTableCompensatePartitionKeys = mvCompensation.getExternalCompensatePartitionKeys();
            if (refTableCompensatePartitionKeys == null) {
                logMVRewrite(mvContext, "Get ref external base table's compensation partition ids is null");
                return null;
            }

            // NOTE: This is necessary because iceberg's physical plan will not use selectedPartitionIds to
            // prune partitions.
            Column mvPartitionColumn = partitionTableAndColumns.second;
            ColumnRefOperator partitionColumnRef = mvRefScanOperator.getColumnReference(mvPartitionColumn);
            Preconditions.checkState(partitionColumnRef != null);
            ScalarOperator partitionPredicates = convertPartitionKeysToListPredicate(partitionColumnRef,
                    refTableCompensatePartitionKeys);
            Preconditions.checkState(partitionPredicates != null);
            partitionPredicates.setRedundant(true);
            return MVPartitionPruner.getExternalTableCompensatePlan(mvQueryPlan, mvRefScanOperator,
                    partitionPredicates);
        } else {
            logMVRewrite(mvContext, "Unsupported ref base table's scan operator type:{}",
                    mvRefScanOperator.getOpType().name());
            return null;
        }
    }

    /**
     * <p> What's the transparent plan of mv? </p>
     *
     * Transparent MV: select * from t(all partition refreshed) union all select * from mv's defined query
     *  (all to-refresh partitions)
     *
     * eg:
     * MV       : select col1, col2 from t;
     * MV refreshed partition: dt = '2023-11-01'
     *
     * Query    : select col1, col2 from t where dt in ('2023-11-01', '2023-11-02')
     * Rewritten: select col1, col2 from <Transparent MV> where dt in ('2023-11-01', '2023-11-02')
     *
     * Transparent MV : Refreshed MV Partitions union all To-Refresh MV Partitions
     * = select * from mv where dt='2023-11-01' union all select * from t where dt = '2023-11-02'
     */
    public static OptExpression getMvTransparentPlan(MaterializationContext mvContext,
                                                     MVCompensation mvCompensation,
                                                     List<ColumnRefOperator> expectOutputColumns) {
        Preconditions.checkState(mvCompensation.getState().isCompensate());
        final LogicalOlapScanOperator mvScanOperator = mvContext.getScanMvOperator();
        final MaterializedView mv = mvContext.getMv();
        final List<ColumnRefOperator> originalOutputColumns = expectOutputColumns == null ?
                MvUtils.getMvScanOutputColumnRefs(mv, mvScanOperator) : expectOutputColumns;

        Pair<OptExpression, List<ColumnRefOperator>> mvScanPlans = getMvScanPlan(mvContext);
        if (mvScanPlans == null) {
            logMVRewrite(mvContext, "Get mv scan transparent plan failed");
            return null;
        }
        Pair<OptExpression, List<ColumnRefOperator>> mvQueryPlans = getMvQueryPlan(mvContext, mvCompensation);
        if (mvQueryPlans == null) {
            logMVRewrite(mvContext, "Get mv query transparent plan failed");
            return null;
        }
        LogicalUnionOperator unionOperator = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(originalOutputColumns)
                .setChildOutputColumns(Lists.newArrayList(mvScanPlans.second, mvQueryPlans.second))
                .isUnionAll(true)
                .build();
        OptExpression result = OptExpression.create(unionOperator, mvScanPlans.first, mvQueryPlans.first);
        deriveLogicalProperty(result);
        return result;
    }

    /**
     * - if `isCompensate` is true, use `selectedPartitionIds` to compensate complete partition ranges
     *  with lower and upper bound.
     * - otherwise use original pruned partition predicates as the compensated partition
     *  predicates.
     * NOTE: When MV has enough partitions for the query, no need to compensate anymore for both mv and the query's plan.
     *       A query can be rewritten just by the original SQL.
     * NOTE: It's not safe if `isCompensate` is always false:
     *      - partitionPredicate is null if olap scan operator cannot prune partitions.
     *      - partitionPredicate is not exact even if olap scan operator has pruned partitions.
     * eg:
     *      t1:
     *       PARTITION p1 VALUES [("0000-01-01"), ("2020-01-01")), has data
     *       PARTITION p2 VALUES [("2020-01-01"), ("2020-02-01")), has data
     *       PARTITION p3 VALUES [("2020-02-01"), ("2020-03-01")), has data
     *       PARTITION p4 VALUES [("2020-03-01"), ("2020-04-01")), no data
     *       PARTITION p5 VALUES [("2020-04-01"), ("2020-05-01")), no data
     *
     *      query1 : SELECT k1, sum(v1) as sum_v1 FROM t1 group by k1;
     *      `partitionPredicate` : null
     *
     *      query2 : SELECT k1, sum(v1) as sum_v1 FROM t1 where k1>='2020-02-01' group by k1;
     *      `partitionPredicate` : k1>='2020-02-11'
     *      however for mv  we need: k1>='2020-02-11' and k1 < "2020-03-01"
     */
    public static ScalarOperator compensateQueryPartitionPredicate(MaterializationContext mvContext,
                                                                   ColumnRefFactory columnRefFactory,
                                                                   OptExpression queryExpression) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        if (scanOperators.isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }

        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        MVCompensation mvCompensation = mvContext.getMvCompensation();
        if (mvCompensation.getState().isNoRewrite()) {
            return null;
        }
        boolean isCompensatePartition = mvCompensation.isCompensatePartitionPredicate();
        // Compensate partition predicates and add them into query predicate.
        Map<Pair<LogicalScanOperator, Boolean>, List<ScalarOperator>> scanOperatorScalarOperatorMap =
                mvContext.getScanOpToPartitionCompensatePredicates();
        MaterializedView mv = mvContext.getMv();
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (!SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES.contains(scanOperator.getOpType())) {
                // If the scan operator is not supported, then return null when compensate type is not NO_COMPENSATE
                // which means the query cannot be rewritten only when all partitions are refreshed.
                // Change query_rewrite_consistency=loose to no check query rewrite consistency.
                if (!mvCompensation.isCompensatePartitionPredicate()) {
                    continue;
                }
                return null;
            }
            List<ScalarOperator> partitionPredicate = scanOperatorScalarOperatorMap
                    .computeIfAbsent(Pair.create(scanOperator, isCompensatePartition), x -> {
                        return isCompensatePartition ? getCompensatePartitionPredicates(mvContext, columnRefFactory,
                                scanOperator) : getScanOpPrunedPartitionPredicates(mv, scanOperator);
                    });
            if (partitionPredicate == null) {
                logMVRewrite(mvContext.getMv().getName(), "Compensate partition failed for scan {}",
                        scanOperator.getTable().getName());
                return null;
            }
            partitionPredicates.addAll(partitionPredicate);
        }
        ScalarOperator compensatePredicate = partitionPredicates.isEmpty() ? ConstantOperator.createBoolean(true) :
                Utils.compoundAnd(partitionPredicates);
        logMVRewrite(mvContext.getMv().getName(), "Query Compensate partition predicate:{}", compensatePredicate);
        return compensatePredicate;
    }

    private static List<ScalarOperator> getCompensatePartitionPredicates(MaterializationContext mvContext,
                                                                         ColumnRefFactory columnRefFactory,
                                                                         LogicalScanOperator scanOperator) {
        List<ScalarOperator> partitionPredicate = null;
        if (scanOperator instanceof LogicalOlapScanOperator) {
            partitionPredicate = compensatePartitionPredicateForOlapScan((LogicalOlapScanOperator) scanOperator,
                    columnRefFactory);
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
            partitionPredicate = compensatePartitionPredicateForExternalTables(mvContext, scanOperator);
        } else {
            logMVRewrite(mvContext.getMv().getName(), "Compensate partition failed: unsupported scan " +
                    "operator type {} for {}", scanOperator.getOpType(), scanOperator.getTable().getName());
            return null;
        }
        return partitionPredicate;
    }

    private static boolean supportCompensatePartitionPredicateForHiveScan(List<PartitionKey> partitionKeys) {
        for (PartitionKey partitionKey : partitionKeys) {
            // only support one partition column now.
            if (partitionKey.getKeys().size() != 1) {
                return false;
            }
            LiteralExpr e = partitionKey.getKeys().get(0);
            // Only support date/int type
            if (!(e instanceof DateLiteral || e instanceof IntLiteral)) {
                return false;
            }
        }
        return true;
    }

    private static List<ScalarOperator> compensatePartitionPredicateForExternalTables(MaterializationContext mvContext,
                                                                                      LogicalScanOperator scanOperator) {
        ScanOperatorPredicates scanOperatorPredicates = null;
        try {
            scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
        } catch (AnalysisException e) {
            return null;
        }
        if (scanOperatorPredicates == null) {
            return null;
        }

        Table baseTable = scanOperator.getTable();
        if (baseTable.isUnPartitioned()) {
            return Lists.newArrayList();
        }

        OperatorType operatorType = scanOperator.getOpType();
        // only used for hive
        if (operatorType == OperatorType.LOGICAL_HIVE_SCAN && scanOperatorPredicates.getSelectedPartitionIds().size()
                == scanOperatorPredicates.getIdToPartitionKey().size()) {
            return Lists.newArrayList();
        }
        // do not support compensate partition predicate for hive scan if partition key has more than one column,
        // we could return empty list here because queryConjuncts in queryExpression for external table will add all conjuncts
        if (operatorType == OperatorType.LOGICAL_HIVE_SCAN &&
                !supportCompensatePartitionPredicateForHiveScan(scanOperatorPredicates.getSelectedPartitionKeys())) {
            return Lists.newArrayList();
        }

        List<Range<PartitionKey>> ranges = Lists.newArrayList();
        MaterializedView mv = mvContext.getMv();
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }
        Column partitionColumn = partitionTableAndColumns.second;
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mv.getFirstPartitionRefTableExpr(), partitionColumn);
        for (PartitionKey selectedPartitionKey : scanOperatorPredicates.getSelectedPartitionKeys()) {
            try {
                LiteralExpr literalExpr = selectedPartitionKey.getKeys().get(0);
                if (isConvertToDate) {
                    literalExpr = PartitionUtil.convertToDateLiteral(literalExpr);
                    if (literalExpr == null) {
                        return null;
                    }
                }
                PartitionUtil.DateTimeInterval interval = PartitionUtil.getDateTimeInterval(baseTable,
                        baseTable.getPartitionColumns().get(0));
                LiteralExpr expr = PartitionUtil.addOffsetForLiteral(literalExpr, 1, interval);
                PartitionKey partitionKey = new PartitionKey(ImmutableList.of(expr), selectedPartitionKey.getTypes());
                ranges.add(Range.closedOpen(selectedPartitionKey, partitionKey));
            } catch (AnalysisException e) {
                logMVRewrite(mv.getName(), "Compute partition key range failed:{}", DebugUtil.getStackTrace(e));
                return null;
            }
        }

        List<Range<PartitionKey>> mergedRanges = mergeRanges(ranges);
        ColumnRefOperator partitionColumnRef = scanOperator.getColumnReference(baseTable.getPartitionColumns().get(0));
        ScalarOperator partitionPredicate = convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
        if (partitionPredicate == null) {
            return null;
        }
        return ImmutableList.of(partitionPredicate);
    }

    /**
     * Compensate olap table's partition predicates from olap scan operator which may be pruned by optimizer before or not.
     *
     * @param olapScanOperator   : olap scan operator that needs to compensate partition predicates.
     * @param columnRefFactory   : column ref factory that used to generate new partition predicate epxr.
     * @return
     */
    private static List<ScalarOperator> compensatePartitionPredicateForOlapScan(LogicalOlapScanOperator olapScanOperator,
                                                                                ColumnRefFactory columnRefFactory) {
        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        Preconditions.checkState(olapScanOperator.getTable().isNativeTableOrMaterializedView());
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

        // compensate nothing for single partition table
        if (olapTable.getPartitionInfo() instanceof SinglePartitionInfo) {
            return partitionPredicates;
        }

        // compensate nothing if selected partitions are the same with the total partitions.
        if (olapScanOperator.getSelectedPartitionId() != null
                && olapScanOperator.getSelectedPartitionId().size() == olapTable.getPartitions().size()) {
            return partitionPredicates;
        }

        // if no partitions are selected, return pruned partition predicates directly.
        if (olapScanOperator.getSelectedPartitionId().isEmpty()) {
            return olapScanOperator.getPrunedPartitionPredicates();
        }

        if (olapTable.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo partitionInfo =
                    (ExpressionRangePartitionInfo) olapTable.getPartitionInfo();
            Expr partitionExpr = partitionInfo.getPartitionExprs().get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionExpr.collect(SlotRef.class, slotRefs);
            Preconditions.checkState(slotRefs.size() == 1);
            Optional<ColumnRefOperator> partitionColumn =
                    olapScanOperator.getColRefToColumnMetaMap().keySet().stream()
                            .filter(columnRefOperator -> columnRefOperator.getName()
                                    .equals(slotRefs.get(0).getColumnName()))
                            .findFirst();
            // compensate nothing if there is no partition column predicate in the scan node.
            if (!partitionColumn.isPresent()) {
                return null;
            }

            ExpressionMapping mapping =
                    new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
            mapping.put(slotRefs.get(0), partitionColumn.get());
            ScalarOperator partitionScalarOperator =
                    SqlToScalarOperatorTranslator.translate(partitionExpr, mapping, columnRefFactory);
            List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
            // compensate selected partition ranges from selected partition id
            for (long pid : olapScanOperator.getSelectedPartitionId()) {
                selectedRanges.add(partitionInfo.getRange(pid));
            }

            // normalize selected partition ranges
            List<Range<PartitionKey>> mergedRanges = mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionScalarOperator, mergedRanges);
            if (partitionPredicate == null) {
                return null;
            }
            partitionPredicates.add(partitionPredicate);
        } else if (olapTable.getPartitionInfo() instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            if (partitionColumns.size() != 1) {
                // now do not support more than one partition columns
                return null;
            }
            List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
            for (long pid : olapScanOperator.getSelectedPartitionId()) {
                selectedRanges.add(rangePartitionInfo.getRange(pid));
            }
            ColumnRefOperator partitionColumnRef = olapScanOperator.getColumnReference(partitionColumns.get(0));
            List<Range<PartitionKey>> mergedRanges = mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
            if (partitionPredicate == null) {
                return null;
            }
            partitionPredicates.add(partitionPredicate);
        } else {
            return null;
        }

        return partitionPredicates;
    }

    public static ScalarOperator convertPartitionRangesToListPredicate(ScalarOperator partitionColRef,
                                                                       List<Range<PartitionKey>> partitionRanges) {
        List<PartitionKey> keys = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }

            // see `convertToDateRange`
            if (range.hasLowerBound()) {
                // partition range must have lower bound and upper bound
                keys.add(range.lowerEndpoint());
            } else if (range.hasUpperBound()) {
                keys.add(range.upperEndpoint());
            } else {
                return null;
            }
        }
        return convertPartitionKeysToListPredicate(partitionColRef, keys);
    }

    public static ScalarOperator convertPartitionKeysToListPredicate(ScalarOperator partitionColRef,
                                                                     Collection<PartitionKey> partitionRanges) {
        List<ScalarOperator> inArgs = Lists.newArrayList();
        inArgs.add(partitionColRef);
        for (PartitionKey partitionKey : partitionRanges) {
            LiteralExpr literalExpr = partitionKey.getKeys().get(0);
            ConstantOperator upperBound = (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
            inArgs.add(upperBound);
        }
        if (inArgs.size() == 1) {
            return ConstantOperator.TRUE;
        } else {
            return new InPredicateOperator(false, inArgs);
        }
    }

    private static ScalarOperator convertPartitionKeysToPredicate(ScalarOperator partitionColumn,
                                                                  List<Range<PartitionKey>> partitionKeys) {
        // NOTE: For string type partition column, it should be list partition rather than range partition.
        boolean isListPartition = partitionColumn.getType().isStringType();
        if (isListPartition) {
            return MvPartitionCompensator.convertPartitionRangesToListPredicate(partitionColumn, partitionKeys);
        } else {
            List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionColumn, partitionKeys);
            return Utils.compoundOr(rangePredicates);
        }
    }

    // try to get partial partition predicates of partitioned mv.
    // eg, mv1's base partition table is t1, partition column is k1 and has two partition:
    // p1:[2022-01-01, 2022-01-02), p1 is updated(refreshed),
    // p2:[2022-01-02, 2022-01-03), p2 is outdated,
    // then this function will return predicate:
    // k1 >= "2022-01-01" and k1 < "2022-01-02"
    // NOTE: This method can be only used in query rewrite and cannot be used in insert routine.
    public static ScalarOperator getMvPartialPartitionPredicates(
            MaterializedView mv,
            OptExpression mvPlan,
            Set<String> mvPartitionNamesToRefresh) throws AnalysisException {
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }

        Table refBaseTable = partitionTableAndColumns.first;
        List<Range<PartitionKey>> refreshedRefBaseTableRanges = getMvRefreshedPartitionRange(refBaseTable,
                partitionTableAndColumns.second, mv, mvPartitionNamesToRefresh);
        if (refreshedRefBaseTableRanges == null) {
            return null;
        }

        if (refreshedRefBaseTableRanges.isEmpty()) {
            // if there isn't an updated partition, do not rewrite
            return ConstantOperator.TRUE;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(mvPlan);
        for (LogicalScanOperator scanOperator : scanOperators) {
            // Since mv's plan disabled partition prune, no need to compensate partition predicate for non ref base tables.
            if (!isRefBaseTable(scanOperator, refBaseTable)) {
                continue;
            }
            final Optional<ColumnRefOperator> columnRefOption;
            if (scanOperator instanceof LogicalViewScanOperator) {
                LogicalViewScanOperator viewScanOperator = scanOperator.cast();
                columnRefOption = Optional.ofNullable(viewScanOperator.getExpressionMapping(partitionExpr));
            } else {
                columnRefOption = Optional.ofNullable(scanOperator.getColumnReference(partitionColumn));
            }
            if (!columnRefOption.isPresent()) {
                continue;
            }
            return convertPartitionKeysToPredicate(columnRefOption.get(), refreshedRefBaseTableRanges);
        }
        return null;
    }

    // convert varchar date to date type
    @VisibleForTesting
    public static Range<PartitionKey> convertToDateRange(Range<PartitionKey> from) throws AnalysisException {
        if (from.hasLowerBound() && from.hasUpperBound()) {
            StringLiteral lowerString = (StringLiteral) from.lowerEndpoint().getKeys().get(0);
            LocalDateTime lowerDateTime = DateUtils.parseDatTimeString(lowerString.getStringValue());
            PartitionKey lowerPartitionKey = PartitionKey.ofDate(lowerDateTime.toLocalDate());

            StringLiteral upperString = (StringLiteral) from.upperEndpoint().getKeys().get(0);
            LocalDateTime upperDateTime = DateUtils.parseDatTimeString(upperString.getStringValue());
            PartitionKey upperPartitionKey = PartitionKey.ofDate(upperDateTime.toLocalDate());
            return Range.range(lowerPartitionKey, from.lowerBoundType(), upperPartitionKey, from.upperBoundType());
        } else if (from.hasUpperBound()) {
            StringLiteral upperString = (StringLiteral) from.upperEndpoint().getKeys().get(0);
            LocalDateTime upperDateTime = DateUtils.parseDatTimeString(upperString.getStringValue());
            PartitionKey upperPartitionKey = PartitionKey.ofDate(upperDateTime.toLocalDate());
            return Range.upTo(upperPartitionKey, from.upperBoundType());
        } else if (from.hasLowerBound()) {
            StringLiteral lowerString = (StringLiteral) from.lowerEndpoint().getKeys().get(0);
            LocalDateTime lowerDateTime = DateUtils.parseDatTimeString(lowerString.getStringValue());
            PartitionKey lowerPartitionKey = PartitionKey.ofDate(lowerDateTime.toLocalDate());
            return Range.downTo(lowerPartitionKey, from.lowerBoundType());
        }
        return Range.all();
    }

    /**
     * Return the refreshed partition key ranges of the ref base table.
     *
     * NOTE: This method can be only used in query rewrite and cannot be used to insert routine.
     * @param partitionByTable          : the base table of the mv
     * @param partitionColumn           : the partition column of the base table
     * @param mv                        : the materialized view
     * @param mvPartitionNamesToRefresh : the updated partition names  of the materialized view
     * @return
     */
    private static List<Range<PartitionKey>> getMvRefreshedPartitionRange(
            Table partitionByTable,
            Column partitionColumn,
            MaterializedView mv,
            Set<String> mvPartitionNamesToRefresh) throws AnalysisException {
        // materialized view latest partition ranges except to-refresh partitions
        List<Range<PartitionKey>> mvRanges = getLatestPartitionRangeForNativeTable(mv, mvPartitionNamesToRefresh);

        List<Range<PartitionKey>> refBaseTableRanges = Lists.newArrayList();
        try {
            // todo: support list partition mv
            refBaseTableRanges = Lists.newArrayList(PartitionUtil.getPartitionKeyRange(partitionByTable, partitionColumn,
                    MaterializedView.getPartitionExpr(mv)).values());
        } catch (UserException e) {
            LOG.warn("Materialized view Optimizer compute partition range failed.", e);
            return Lists.newArrayList();
        }

        // date to varchar range
        Map<Range<PartitionKey>, Range<PartitionKey>> baseRangeMapping = null;
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mv.getFirstPartitionRefTableExpr(), partitionColumn);
        if (isConvertToDate) {
            baseRangeMapping = Maps.newHashMap();
            // convert varchar range to date range
            List<Range<PartitionKey>> baseTableDateRanges = Lists.newArrayList();
            for (Range<PartitionKey> range : refBaseTableRanges) {
                Range<PartitionKey> datePartitionRange = convertToDateRange(range);
                baseTableDateRanges.add(datePartitionRange);
                baseRangeMapping.put(datePartitionRange, range);
            }
            refBaseTableRanges = baseTableDateRanges;
        }

        List<Range<PartitionKey>> latestBaseTableRanges = Lists.newArrayList();
        for (Range<PartitionKey> range : refBaseTableRanges) {
            // if materialized view's partition range can enclose the ref base table range, we think that
            // the materialized view's partition has been refreshed and should be compensated into the materialized
            // view's partition predicate.
            if (mvRanges.stream().anyMatch(mvRange -> mvRange.encloses(range))) {
                latestBaseTableRanges.add(range);
            }
        }
        if (isConvertToDate) {
            // treat string type partition as list, so no need merge
            List<Range<PartitionKey>> tmpRangeList = Lists.newArrayList();
            for (Range<PartitionKey> range : latestBaseTableRanges) {
                tmpRangeList.add(baseRangeMapping.get(range));
            }
            return tmpRangeList;
        } else {
            return MvUtils.mergeRanges(latestBaseTableRanges);
        }
    }

    private static List<Range<PartitionKey>> getLatestPartitionRangeForNativeTable(OlapTable partitionTable,
                                                                                   Set<String> modifiedPartitionNames) {
        // partitions that will be excluded
        Set<Long> filteredIds = Sets.newHashSet();
        for (Partition p : partitionTable.getPartitions()) {
            if (modifiedPartitionNames.contains(p.getName()) || !p.hasData()) {
                filteredIds.add(p.getId());
            }
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionTable.getPartitionInfo();
        return rangePartitionInfo.getRangeList(filteredIds, false);
    }

    private static List<ScalarOperator> getScanOpPrunedPartitionPredicates(MaterializedView mv,
                                                                           LogicalScanOperator scanOperator) {
        if (scanOperator instanceof LogicalOlapScanOperator) {
            return ((LogicalOlapScanOperator) scanOperator).getPrunedPartitionPredicates();
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
            try {
                ScanOperatorPredicates scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
                return scanOperatorPredicates.getPrunedPartitionConjuncts();
            } catch (AnalysisException e) {
                logMVRewrite(mv.getName(), "Compensate partition predicate for mv {} failed: {}",
                        mv.getName(), DebugUtil.getStackTrace(e));
                return null;
            }
        } else {
            // Cannot decide whether it has been pruned or not, return null for now.
            logMVRewrite(mv.getName(), "Compensate partition predicate for mv {} failed: unsupported scan " +
                            "operator type {} for {}", mv.getName(),
                    scanOperator.getOpType(), scanOperator.getTable().getName());
            return null;
        }
    }

    /**
     * Find scan operator which contains the ref-base-table of the materialized view.
     * @param scanOperators: scan operators extracted from query plan.
     * @param refBaseTable: ref-base-table of the materialized view.
     * @return: the first scan operator which contains the ref-base-table of the materialized view if existed,
     * otherwise null is returned.
     */
    private static LogicalScanOperator getRefBaseTableScanOperator(List<LogicalScanOperator> scanOperators,
                                                                   Table refBaseTable) {
        Optional<LogicalScanOperator> optRefScanOperator =
                scanOperators.stream().filter(x -> isRefBaseTable(x, refBaseTable)).findFirst();
        if (!optRefScanOperator.isPresent()) {
            return null;
        }
        return optRefScanOperator.get();
    }

    private static LogicalScanOperator getRefBaseTableScanOperator(OptExpression queryPlan,
                                                                   Table refBaseTable) {

        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlan);
        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return null;
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return null;
        }

        LogicalScanOperator refScanOperator = getRefBaseTableScanOperator(scanOperators, refBaseTable);
        if (refScanOperator == null) {
            return null;
        }

        Table table = refScanOperator.getTable();
        // If table's not partitioned, no need compensate
        if (table.isUnPartitioned()) {
            return null;
        }
        return refScanOperator;
    }

    private static boolean isRefBaseTable(LogicalScanOperator scanOperator, Table refBaseTable) {
        Table scanTable = scanOperator.getTable();
        if (scanTable.isNativeTableOrMaterializedView() && !scanTable.equals(refBaseTable)) {
            return false;
        }
        if (scanOperator instanceof LogicalViewScanOperator) {
            return true;
        }
        if (!scanTable.isNativeTableOrMaterializedView() && !scanTable.getTableIdentifier().equals(
                refBaseTable.getTableIdentifier())) {
            return false;
        }
        return true;
    }
}
