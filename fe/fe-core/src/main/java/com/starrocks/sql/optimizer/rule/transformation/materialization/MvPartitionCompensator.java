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
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
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

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

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
    public static Optional<Boolean> isNeedCompensatePartitionPredicate(OptExpression queryPlan,
                                                                       MaterializationContext mvContext) {
        Set<String> mvPartitionNameToRefresh = mvContext.getMvPartitionNamesToRefresh();
        // If mv contains no partitions to refresh, no need compensate
        if (Objects.isNull(mvPartitionNameToRefresh) || mvPartitionNameToRefresh.isEmpty()) {
            return Optional.of(false);
        }

        // If ref table contains no partitions to refresh, no need compensate.
        // If the mv is partitioned and non-ref table need refresh, then all partitions need to be refreshed,
        // it can not be a candidate.
        Set<String> refTablePartitionNameToRefresh = mvContext.getRefTableUpdatePartitionNames();
        if (Objects.isNull(refTablePartitionNameToRefresh) || refTablePartitionNameToRefresh.isEmpty()) {
            // NOTE: This should not happen: `mvPartitionNameToRefresh` is not empty, so `refTablePartitionNameToRefresh`
            // should not empty. Return true in the situation to avoid bad cases.
            return Optional.of(true);
        }

        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlan);
        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return Optional.of(false);
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return Optional.of(true);
        }

        // If no partition table and columns, no need compensate
        MaterializedView mv = mvContext.getMv();
<<<<<<< HEAD
        Pair<Table, Column> partitionTableAndColumns = mv.getBaseTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return Optional.of(false);
=======
        if (mv.getRefBaseTablePartitionColumns() == null) {
            return null;
        }
        return OptCompensator.getMVCompensatePlan(mvContext.getOptimizerContext(),
                mv, mvCompensation, mvQueryPlan);
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
                                                     List<ColumnRefOperator> originalOutputColumns) {
        Preconditions.checkArgument(originalOutputColumns != null);
        Preconditions.checkState(mvCompensation.getState().isCompensate());

        Pair<OptExpression, List<ColumnRefOperator>> mvScanPlans = getMvScanPlan(mvContext);
        if (mvScanPlans == null) {
            logMVRewrite(mvContext, "Get mv scan transparent plan failed");
            return null;
>>>>>>> 8f4c6aadff ([BugFix] Ensure mv rewrite plan output column refs unique (#54863))
        }

        // only set this when `queryExpression` contains ref table, otherwise the cached value maybe dirty.
        Table refBaseTable = partitionTableAndColumns.first;
        LogicalScanOperator refScanOperator = getRefBaseTableScanOperator(scanOperators, refBaseTable);
        if (refScanOperator == null) {
            return Optional.empty();
        }

        Table table = refScanOperator.getTable();
        // If table's not partitioned, no need compensate
        if (table.isUnPartitioned()) {
            return Optional.of(false);
        }

        if (refScanOperator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) refScanOperator;
            OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

            List<Long> selectPartitionIds = olapScanOperator.getSelectedPartitionId();
            if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
                return Optional.of(false);
            }

            // determine whether query's partitions can be satisfied by materialized view.
            for (Long selectPartitionId : selectPartitionIds) {
                Partition partition = olapTable.getPartition(selectPartitionId);
                if (partition != null && refTablePartitionNameToRefresh.contains(partition.getName())) {
                    return Optional.of(true);
                }
            }
            return Optional.of(false);
        } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(refScanOperator.getOpType())) {
            try {
                ScanOperatorPredicates scanOperatorPredicates = refScanOperator.getScanOperatorPredicates();
                if (scanOperatorPredicates == null) {
                    return Optional.of(true);
                }

                Collection<Long> selectPartitionIds = scanOperatorPredicates.getSelectedPartitionIds();
                if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
                    // see OptExternalPartitionPruner#computePartitionInfo:
                    // it's not the same meaning when selectPartitionIds is null and empty for hive and other tables
                    if (refScanOperator.getOpType() == OperatorType.LOGICAL_HIVE_SCAN) {
                        return Optional.of(false);
                    } else {
                        return Optional.of(true);
                    }
                }
                // determine whether query's partitions can be satisfied by materialized view.
                List<PartitionKey> selectPartitionKeys = scanOperatorPredicates.getSelectedPartitionKeys();
                for (PartitionKey partitionKey : selectPartitionKeys) {
                    String mvPartitionName = PartitionUtil.generateMVPartitionName(partitionKey);
                    if (refTablePartitionNameToRefresh.contains(mvPartitionName)) {
                        return Optional.of(true);
                    }
                }
                return Optional.of(false);
            } catch (AnalysisException e) {
                return Optional.of(true);
            }
        } else {
            return Optional.of(true);
        }
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
        boolean isCompensatePartition = mvContext.getOrInitCompensatePartitionPredicate(queryExpression);
        // Compensate partition predicates and add them into query predicate.
        Map<Pair<LogicalScanOperator, Boolean>, List<ScalarOperator>> scanOperatorScalarOperatorMap =
                mvContext.getScanOpToPartitionCompensatePredicates();
        MaterializedView mv = mvContext.getMv();
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (!SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES.contains(scanOperator.getOpType())) {
                continue;
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
        Pair<Table, Column> partitionTableAndColumns = mv.getBaseTableAndPartitionColumn();
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

        List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(ranges);
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
            List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionScalarOperator, mergedRanges);
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
            List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
            if (partitionPredicate != null) {
                partitionPredicates.add(partitionPredicate);
            }
        } else {
            return null;
        }

        return partitionPredicates;
    }

    public static ScalarOperator convertListPartitionKeys(ScalarOperator partitionColRef,
                                                          List<Range<PartitionKey>> partitionRanges) {
        List<ScalarOperator> inArgs = Lists.newArrayList();
        inArgs.add(partitionColRef);
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }

            // see `convertToDateRange`
            if (range.hasLowerBound()) {
                // partition range must have lower bound and upper bound
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                inArgs.add(lowerBound);
            } else if (range.hasUpperBound()) {
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                Preconditions.checkState(upperBound.getType().isStringType());
            } else {
                return null;
            }
        }
        if (inArgs.size() == 1) {
            return ConstantOperator.TRUE;
        } else {
            return new InPredicateOperator(false, inArgs);
        }
    }

    private static ScalarOperator convertPartitionKeysToPredicate(ScalarOperator partitionColumn,
                                                                  List<Range<PartitionKey>> partitionKeys) {
        boolean isListPartition = partitionColumn.getType().isStringType();
        // NOTE: For string type partition column, it should be list partition rather than range partition.
        if (isListPartition) {
            return convertListPartitionKeys(partitionColumn, partitionKeys);
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
        Pair<Table, Column> partitionTableAndColumns = mv.getBaseTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }

        Table refBaseTable = partitionTableAndColumns.first;
        List<Range<PartitionKey>> latestBaseTableRanges =
                getLatestPartitionRangeForTable(refBaseTable, partitionTableAndColumns.second,
                        mv, mvPartitionNamesToRefresh);
        if (latestBaseTableRanges.isEmpty()) {
            // if there isn't an updated partition, do not rewrite
            return null;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(mvPlan);
        for (LogicalScanOperator scanOperator : scanOperators) {
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
            return convertPartitionKeysToPredicate(columnRefOption.get(), latestBaseTableRanges);
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
     * Return the updated partition key ranges of the specific table.
     *
     * NOTE: This method can be only used in query rewrite and cannot be used to insert routine.
     * @param partitionByTable          : the base table of the mv
     * @param partitionColumn           : the partition column of the base table
     * @param mv                        : the materialized view
     * @param mvPartitionNamesToRefresh : the updated partition names  of the materialized view
     * @return
     */
    private static List<Range<PartitionKey>> getLatestPartitionRangeForTable(
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
