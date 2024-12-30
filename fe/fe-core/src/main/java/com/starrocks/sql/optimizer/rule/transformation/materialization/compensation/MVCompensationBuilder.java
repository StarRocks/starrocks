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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

/**
 * MVCompensationBuilder is used to build a mv compensation for materialized view in mv rewrite.
 */
public class MVCompensationBuilder {
    private static final Logger LOG = LogManager.getLogger(MVCompensationBuilder.class);

    private final MaterializationContext mvContext;
    private final MvUpdateInfo mvUpdateInfo;
    private final MaterializedView mv;

    public MVCompensationBuilder(MaterializationContext mvContext,
                                 MvUpdateInfo mvUpdateInfo) {
        this.mvContext = mvContext;
        this.mvUpdateInfo = mvUpdateInfo;
        this.mv = mvContext.getMv();
    }

    /**
     * Get mv compensation info by mv's update info.
     */
    public MVCompensation buildMvCompensation(Optional<OptExpression> queryPlanOpt) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        List<Pair<Optional<LogicalScanOperator>, Table>> refBaseTables;
        if (queryPlanOpt.isPresent()) {
            List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlanOpt.get());
            // If no partition to refresh, return directly.
            Set<String> mvToRefreshPartitionNames = mvUpdateInfo.getMvToRefreshPartitionNames();
            if (CollectionUtils.isEmpty(mvToRefreshPartitionNames)) {
                return MVCompensation.noCompensate(sessionVariable);
            }

            // If no scan operator, no need compensate
            if (scanOperators.isEmpty() || scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
                return MVCompensation.unknown(sessionVariable);
            }
            refBaseTables = scanOperators.stream()
                    .map(scan -> Pair.create(Optional.of(scan), scan.getTable()))
                    .collect(Collectors.toList());
<<<<<<< HEAD
            return ofOlapTableCompensation(refBaseTable, refTablePartitionIdsToRefresh);
        } else if (MvPartitionCompensator.isTableSupportedPartitionCompensate(refBaseTable)) {
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo =
                    mvUpdateInfo.getBaseTableUpdateInfos().get(refBaseTable);
            if (mvBaseTableUpdateInfo == null) {
                return null;
            }
            PartitionInfo partitionInfo = mvContext.getMv().getPartitionInfo();
            if (partitionInfo.isRangePartition()) {
                Map<String, Range<PartitionKey>> refTablePartitionNameWithRanges =
                        mvBaseTableUpdateInfo.getPartitionNameWithRanges();
                List<PRangeCell> partitionKeys = Lists.newArrayList();
                try {
                    for (String partitionName : refTablePartitionNamesToRefresh) {
                        Preconditions.checkState(refTablePartitionNameWithRanges.containsKey(partitionName));
                        Range<PartitionKey> partitionKeyRange = refTablePartitionNameWithRanges.get(partitionName);
                        partitionKeys.add(PRangeCell.of(partitionKeyRange));
                    }
                } catch (Exception e) {
                    logMVRewrite("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                            DebugUtil.getStackTrace(e));
                    return MVCompensation.createUnkownState(sessionVariable);
                }
                return ofExternalTableCompensation(refBaseTable, partitionKeys);
            } else {
                Preconditions.checkArgument(partitionInfo.isListPartition());
                Map<String, PListCell> partitionNameWithLists = mvBaseTableUpdateInfo.getPartitionNameWithLists();
                List<PRangeCell> partitionKeys = Lists.newArrayList();
                try {
                    List<Column> partitionCols = refBaseTable.getPartitionColumns();
                    for (String partitionName : refTablePartitionNamesToRefresh) {
                        Preconditions.checkState(partitionNameWithLists.containsKey(partitionName));
                        // TODO: we are assuming PListCell's cells' order is by partition's columns order, we may introduce
                        // partition columns in PListCell.
                        partitionNameWithLists.get(partitionName)
                                .toPartitionKeys(partitionCols)
                                .stream()
                                .map(PRangeCell::of)
                                .forEach(partitionKeys::add);
                    }
                } catch (Exception e) {
                    logMVRewrite("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                            DebugUtil.getStackTrace(e));
                    return MVCompensation.createUnkownState(sessionVariable);
                }
                return ofExternalTableCompensation(refBaseTable, partitionKeys);
            }
=======
>>>>>>> 6c1b836ff ([Refactor] Refactor mv partition compensate (#54387))
        } else {
            refBaseTables = mvUpdateInfo.getBaseTableUpdateInfos()
                    .keySet()
                    .stream()
                    .map(table -> Pair.create(Optional.<LogicalScanOperator>empty(), table))
                    .collect(Collectors.toList());
        }

<<<<<<< HEAD
        // if mv's to refresh partitions contains any of query's select partition ids, then rewrite with compensate.
        List<Long> toRefreshRefTablePartitions = getMVCompensatePartitionsOfOlap(partitionNamesToRefresh,
                refBaseTable, olapScanOperator);
        if (toRefreshRefTablePartitions == null) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        Set<Long> toRefreshPartitionIds = Sets.newHashSet(toRefreshRefTablePartitions);
        if (toRefreshPartitionIds.containsAll(selectPartitionIds)) {
            logMVRewrite(mvContext, "All olap table {}'s selected partitions {} need to refresh, no rewrite",
                    refBaseTable.getName(), selectPartitionIds);
            return MVCompensation.createNoRewriteState(sessionVariable);
        }
        return ofOlapTableCompensation(refBaseTable, toRefreshRefTablePartitions);
    }

    private MVCompensation ofOlapTableCompensation(Table refBaseTable,
                                                   List<Long> toRefreshRefTablePartitions) {
        BaseCompensation<Long> compensation = new OlapTableCompensation(toRefreshRefTablePartitions);
        Map<Table, BaseCompensation<?>> compensationMap = Collections.singletonMap(refBaseTable, compensation);
        return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, compensationMap);
    }

    private MVCompensation ofExternalTableCompensation(Table refBaseTable,
                                                       List<PRangeCell> toRefreshRefTablePartitions) {
        Map<Table, BaseCompensation<?>> compensationMap = Collections.singletonMap(refBaseTable,
                new ExternalTableCompensation(toRefreshRefTablePartitions));
        return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, compensationMap);
    }

    private MVCompensation ofBaseTableCompensations(Map<Table, BaseCompensation<?>> compensations) {
        if (compensations.isEmpty()) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        } else {
            return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, compensations);
        }
    }
    private MVCompensation getMVCompensationForExternal(Table refBaseTable,
                                                        Set<String> refTablePartitionNamesToRefresh,
                                                        LogicalScanOperator refScanOperator) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        MaterializedView mv = mvContext.getMv();
        try {
            ScanOperatorPredicates scanOperatorPredicates = refScanOperator.getScanOperatorPredicates();
            Collection<Long> selectPartitionIds = scanOperatorPredicates.getSelectedPartitionIds();
            // NOTE: ref base table's partition keys may contain multi columns, but mv may only contain one column.
            List<Integer> colIndexes = PartitionUtil.getRefBaseTablePartitionColumIndexes(mv, refBaseTable);
            if (colIndexes == null) {
                return MVCompensation.createUnkownState(sessionVariable);
            }
            List<PartitionKey> selectPartitionKeys = scanOperatorPredicates.getSelectedPartitionKeys()
                    .stream()
                    .map(partitionKey -> PartitionUtil.getSelectedPartitionKey(partitionKey, colIndexes))
                    .collect(Collectors.toList());
            // For scan operator which support prune partitions with OptExternalPartitionPruner,
            // we could only compensate partitions which selected partitions need to refresh.
            if (SUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES.contains(refScanOperator.getOpType())) {
                if (CollectionUtils.isEmpty(selectPartitionIds)) {
                    // see OptExternalPartitionPruner#computePartitionInfo:
                    // it's not the same meaning when selectPartitionIds is null and empty for hive and other tables
                    if (refScanOperator.getOpType() == OperatorType.LOGICAL_HIVE_SCAN) {
                        return MVCompensation.createNoCompensateState(sessionVariable);
                    } else {
                        return MVCompensation.createUnkownState(sessionVariable);
                    }
                }

                // NOTE: ref base table's partition keys may contain multi columns, but mv may only contain one column.
                List<PartitionKey> newPartitionKeys = selectPartitionKeys.stream()
                        .map(partitionKey -> PartitionUtil.getSelectedPartitionKey(partitionKey, colIndexes))
                        .collect(Collectors.toList());
                Set<String> selectPartitionNames = newPartitionKeys.stream()
                        .map(PartitionUtil::generateMVPartitionName)
                        .collect(Collectors.toSet());
                if (selectPartitionNames.stream().noneMatch(refTablePartitionNamesToRefresh::contains)) {
                    return MVCompensation.createNoCompensateState(sessionVariable);
                }
                // if all partitions need to refresh, no need rewrite.
                if (refTablePartitionNamesToRefresh.containsAll(selectPartitionNames)) {
                    return MVCompensation.createNoRewriteState(sessionVariable);
                }
            }
            // if mv's to refresh partitions contains any of query's select partition ids, then rewrite with compensation.
            List<PRangeCell> toRefreshRefTablePartitions = getMVCompensatePartitionsOfExternal(refBaseTable,
                    selectPartitionKeys, refTablePartitionNamesToRefresh, refScanOperator);
            if (toRefreshRefTablePartitions == null) {
                return MVCompensation.createUnkownState(sessionVariable);
            }

            Table table = refScanOperator.getTable();
            if (SUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES.contains(refScanOperator.getOpType())) {
                if (Sets.newHashSet(toRefreshRefTablePartitions).containsAll(selectPartitionKeys)) {
                    logMVRewrite(mvContext, "All external table {}'s selected partitions {} need to refresh, no rewrite",
                            table.getName(), selectPartitionIds);
                    return MVCompensation.createNoRewriteState(sessionVariable);
                }
            }
            return ofExternalTableCompensation(table, toRefreshRefTablePartitions);
        } catch (AnalysisException e) {
            return MVCompensation.createUnkownState(sessionVariable);
=======
        MaterializedView mv = mvContext.getMv();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (CollectionUtils.sizeIsEmpty(refBaseTablePartitionColumns)) {
            logMVRewrite("MV's not partitioned, failed to get partition keys: {}", mv.getName());
            return MVCompensation.unknown(sessionVariable);
>>>>>>> 6c1b836ff ([Refactor] Refactor mv partition compensate (#54387))
        }

        Map<Table, TableCompensation> compensations = Maps.newHashMap();
        for (Pair<Optional<LogicalScanOperator>, Table> pair : refBaseTables) {
            Table refBaseTable = pair.second;
            if (!refBaseTablePartitionColumns.containsKey(refBaseTable)) {
                continue;
            }
            // If the ref table contains no partitions to refresh, no need compensate.
            // If the mv is partitioned and non-ref table needs refresh, then all partitions need to be refreshed;
            // it cannot be a candidate.
            TableCompensation tableCompensation = getRefBaseTableCompensation(refBaseTable, pair.first);
            // NPE check
            if (tableCompensation == null) {
                logMVRewrite(mvContext, "MV's ref base table {} to compensation is null, unknown state",
                        refBaseTable.getName());
                return MVCompensation.unknown(sessionVariable);
            }
            // if the table is unnecessary to compensate, skip
            if (tableCompensation.isNoCompensate()) {
                continue;
            }
            // if the table is not rewritable, return unknown state
            if (tableCompensation.isUncompensable()) {
                MVTransparentState state = tableCompensation.getState();
                return MVCompensation.withState(sessionVariable, state);
            }
            compensations.put(refBaseTable, tableCompensation);
        }
        return MVCompensation.compensate(sessionVariable, compensations);
    }

    public TableCompensation getRefBaseTableCompensation(Table refBaseTable,
                                                         Optional<LogicalScanOperator> scanOperatorOpt) {
        // if query consistency is not `force_mv`, use the old compensation logic.
        return getRefBaseTableCompensationByPartitionKeys(refBaseTable, scanOperatorOpt);
    }

    /**
     * Get the compensation for the ref base table which has no input query plan but to compensate a consistent mv.
     */
<<<<<<< HEAD
    private List<Long> getMVCompensatePartitionsOfOlap(Set<String> partitionNamesToRefresh,
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
            if (partitionNamesToRefresh.contains(partition.getName())) {
                refTableCompensatePartitionIds.add(selectPartitionId);
            }
        }
        return refTableCompensatePartitionIds;
    }

    private List<PRangeCell> getMVCompensatePartitionsOfExternal(Table refBaseTable,
                                                                   List<PartitionKey> selectPartitionKeys,
                                                                   Set<String> refTablePartitionNamesToRefresh,
                                                                   LogicalScanOperator refScanOperator)
            throws AnalysisException {
        if (SUPPORTED_PARTITION_PRUNE_EXTERNAL_SCAN_TYPES.contains(refScanOperator.getOpType())) {
            // For external table which support partition prune with OptExternalPartitionPruner,
            // could use selectPartitionKeys to get the compensate partitions.
            return getMVCompensatePartitionsOfExternalWithPartitionPruner(selectPartitionKeys,
                    refTablePartitionNamesToRefresh, refScanOperator);
=======
    public TableCompensation getRefBaseTableCompensationByPartitionKeys(Table refBaseTable,
                                                                        Optional<LogicalScanOperator> scanOperatorOpt) {
        if (refBaseTable.isNativeTableOrMaterializedView()) {
            return OlapTableCompensation.build(refBaseTable, mvUpdateInfo, scanOperatorOpt);
        } else if (MvPartitionCompensator.isSupportPartitionCompensate(refBaseTable)) {
            return ExternalTableCompensation.build(refBaseTable, mvUpdateInfo, scanOperatorOpt);
>>>>>>> 6c1b836ff ([Refactor] Refactor mv partition compensate (#54387))
        } else {
            logMVRewrite(mv.getName(), "Unsupported ref base table type: {}", refBaseTable.getName());
            return TableCompensation.unknown();
        }
    }
<<<<<<< HEAD

    private List<PRangeCell> getMVCompensatePartitionsOfExternalWithPartitionPruner(
            List<PartitionKey> selectPartitionKeys,
            Set<String> refTablePartitionNamesToRefresh,
            LogicalScanOperator refScanOperator) {
        List<PRangeCell> refTableCompensatePartitionKeys = Lists.newArrayList();
        ScanOperatorPredicates scanOperatorPredicates = null;
        try {
            scanOperatorPredicates = refScanOperator.getScanOperatorPredicates();
        } catch (Exception e) {
            return null;
        }
        if (scanOperatorPredicates == null) {
            return null;
        }
        // different behavior for different external table types
        if (selectPartitionKeys.isEmpty() && refScanOperator.getOpType() != OperatorType.LOGICAL_HIVE_SCAN) {
            return null;
        }
        Table refBaseTable = refScanOperator.getTable();
        List<Integer> colIndexes = PartitionUtil.getRefBaseTablePartitionColumIndexes(mvContext.getMv(), refBaseTable);
        if (colIndexes == null) {
            return null;
        }
        for (PartitionKey partitionKey : selectPartitionKeys) {
            PartitionKey newPartitionKey = PartitionUtil.getSelectedPartitionKey(partitionKey, colIndexes);
            String partitionName = generateMVPartitionName(newPartitionKey);
            if (refTablePartitionNamesToRefresh.contains(partitionName)) {
                refTableCompensatePartitionKeys.add(PRangeCell.of(partitionKey));
            }
        }
        return refTableCompensatePartitionKeys;
    }

    private List<PRangeCell> getMVCompensatePartitionsOfExternalWithoutPartitionPruner(
            Table refBaseTable,
            Set<String> refTablePartitionNamesToRefresh) {
        MvBaseTableUpdateInfo baseTableUpdateInfo = mvUpdateInfo.getBaseTableUpdateInfos().get(refBaseTable);
        if (baseTableUpdateInfo == null) {
            return null;
        }
        // use update info's partition to cells since it's accurate.
        Map<String, PCell> nameToPartitionKeys = baseTableUpdateInfo.getPartitonToCells();
        List<PRangeCell> partitionKeys = Lists.newArrayList();
        try {
            for (String partitionName : refTablePartitionNamesToRefresh) {
                if (!nameToPartitionKeys.containsKey(partitionName)) {
                    return null;
                }
                PCell pCell = nameToPartitionKeys.get(partitionName);
                if (pCell instanceof PRangeCell) {
                    partitionKeys.add(((PRangeCell) pCell));
                } else if (pCell instanceof PListCell) {
                    List<Column> partitionColumns = refBaseTable.getPartitionColumns();
                    List<PartitionKey> keys = ((PListCell) pCell).toPartitionKeys(partitionColumns);
                    keys.stream().forEach(key -> partitionKeys.add(PRangeCell.of(key)));
                }
            }
        } catch (Exception e) {
            logMVRewrite("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                    DebugUtil.getStackTrace(e));
            return null;
        }
        return partitionKeys;
    }
}
=======
}
>>>>>>> 6c1b836ff ([Refactor] Refactor mv partition compensate (#54387))
