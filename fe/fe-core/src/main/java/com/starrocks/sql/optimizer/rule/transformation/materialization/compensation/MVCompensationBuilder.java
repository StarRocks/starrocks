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

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.generateMVPartitionName;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

/**
 * MVCompensationBuilder is used to build a mv compensation for materialized view in mv rewrite.
 */
public class MVCompensationBuilder {
    private static final Logger LOG = LogManager.getLogger(MVCompensationBuilder.class);

    private final MaterializationContext mvContext;
    private final MvUpdateInfo mvUpdateInfo;
    private final SessionVariable sessionVariable;

    public MVCompensationBuilder(MaterializationContext mvContext,
                                 MvUpdateInfo mvUpdateInfo) {
        this.mvContext = mvContext;
        this.mvUpdateInfo = mvUpdateInfo;
        this.sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
    }

    /**
     * Get mv compensation info by mv's update info.
     */
    public MVCompensation buildMvCompensation(OptExpression queryPlan) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlan);
        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return MVCompensation.createUnkownState(sessionVariable);
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        // If no partition to refresh, return directly.
        Set<String> mvToRefreshPartitionNames = mvUpdateInfo.getMvToRefreshPartitionNames();
        if (CollectionUtils.isEmpty(mvToRefreshPartitionNames)) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return MVCompensation.createUnkownState(sessionVariable);
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        MaterializedView mv = mvContext.getMv();
        Map<Table, Column> refBaseTableAndColumns = mv.getRefBaseTablePartitionColumns();
        if (CollectionUtils.sizeIsEmpty(refBaseTableAndColumns)) {
            logMVRewrite("MV's not partitioned, failed to get partition keys: {}", mv.getName());
            return MVCompensation.createUnkownState(sessionVariable);
        }

        Map<Table, BaseCompensation<?>> compensations = Maps.newHashMap();
        for (LogicalScanOperator scanOperator : scanOperators) {
            Table refBaseTable = scanOperator.getTable();
            if (!refBaseTableAndColumns.containsKey(refBaseTable)) {
                continue;
            }
            // If the ref table contains no partitions to refresh, no need compensate.
            // If the mv is partitioned and non-ref table needs refresh, then all partitions need to be refreshed;
            // it cannot be a candidate.
            Set<String> partitionNamesToRefresh = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
            if (partitionNamesToRefresh == null) {
                logMVRewrite(mvContext, "MV's ref base table {} to refresh partition is null, unknown state",
                        refBaseTable.getName());
                return MVCompensation.createUnkownState(sessionVariable);
            }
            if (partitionNamesToRefresh.isEmpty()) {
                logMVRewrite(mvContext, "MV's ref base table {} to refresh partition is empty, no need compensate",
                        refBaseTable.getName());
                continue;
            }
            MVCompensation subCompensation = getMVCompensationOfTable(refBaseTable, partitionNamesToRefresh, scanOperator);
            if (subCompensation.getState().isNoCompensate()) {
                continue;
            }
            if (!subCompensation.getState().isCompensate()) {
                return subCompensation;
            }
            compensations.putAll(subCompensation.getCompensations());
        }
        return ofBaseTableCompensations(compensations);
    }

    /**
     * Build compensation for transparent mv which has no input query plan but to compensate a consistent mv.
     */
    public MVCompensation buildMvCompensation() {
        // If no partition to refresh, return directly.
        Set<String> mvToRefreshPartitionNames = mvUpdateInfo.getMvToRefreshPartitionNames();
        if (CollectionUtils.isEmpty(mvToRefreshPartitionNames)) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        MaterializedView mv = mvContext.getMv();
        Map<Table, Column> refBaseTableAndColumns = mv.getRefBaseTablePartitionColumns();
        if (CollectionUtils.sizeIsEmpty(refBaseTableAndColumns)) {
            logMVRewrite("MV's not partitioned, failed to get partition keys: {}", mv.getName());
            return MVCompensation.createUnkownState(sessionVariable);
        }
        Map<Table, BaseCompensation<?>> compensations = Maps.newHashMap();
        Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfoMap = mvUpdateInfo.getBaseTableUpdateInfos();
        for (Map.Entry<Table, MvBaseTableUpdateInfo> e : baseTableUpdateInfoMap.entrySet()) {
            Table refBaseTable = e.getKey();
            if (!refBaseTableAndColumns.containsKey(refBaseTable)) {
                continue;
            }
            // If the ref table contains no partitions to refresh, no need compensate.
            // If the mv is partitioned and non-ref table needs refresh, then all partitions need to be refreshed;
            // it cannot be a candidate.
            Set<String> partitionNamesToRefresh = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
            if (partitionNamesToRefresh == null) {
                logMVRewrite(mvContext, "MV's ref base table {} to refresh partition is null, unknown state",
                        refBaseTable.getName());
                return MVCompensation.createUnkownState(sessionVariable);
            }
            if (partitionNamesToRefresh.isEmpty()) {
                logMVRewrite(mvContext, "MV's ref base table {} to refresh partition is empty, no need compensate",
                        refBaseTable.getName());
                continue;
            }

            MVCompensation subCompensation = getMVCompensationOfTable(refBaseTable, partitionNamesToRefresh);
            if (subCompensation.getState().isNoCompensate()) {
                continue;
            }

            if (!subCompensation.getState().isCompensate()) {
                return subCompensation;
            }
            compensations.putAll(subCompensation.getCompensations());
        }
        return ofBaseTableCompensations(compensations);
    }

    private MVCompensation getMVCompensationOfTable(Table refBaseTable,
                                                    Set<String> refTablePartitionNamesToRefresh) {
        if (refBaseTable.isNativeTableOrMaterializedView()) {
            // What if nested mv?
            List<Long> refTablePartitionIdsToRefresh = refTablePartitionNamesToRefresh.stream()
                    .map(name -> refBaseTable.getPartition(name))
                    .filter(Objects::nonNull)
                    .map(p -> p.getId())
                    .collect(Collectors.toList());
            return ofOlapTableCompensation(refBaseTable, refTablePartitionIdsToRefresh);
        } else if (MvPartitionCompensator.isTableSupportedPartitionCompensate(refBaseTable)) {
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo =
                    mvUpdateInfo.getBaseTableUpdateInfos().get(refBaseTable);
            if (mvBaseTableUpdateInfo == null) {
                return null;
            }
            Map<String, Range<PartitionKey>> refTablePartitionNameWithRanges =
                    mvBaseTableUpdateInfo.getPartitionNameWithRanges();
            List<PartitionKey> partitionKeys = Lists.newArrayList();
            try {
                for (String partitionName : refTablePartitionNamesToRefresh) {
                    Preconditions.checkState(refTablePartitionNameWithRanges.containsKey(partitionName));
                    Range<PartitionKey> partitionKeyRange = refTablePartitionNameWithRanges.get(partitionName);
                    partitionKeys.add(partitionKeyRange.lowerEndpoint());
                }
            } catch (Exception e) {
                logMVRewrite("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                        DebugUtil.getStackTrace(e));
                return MVCompensation.createUnkownState(sessionVariable);
            }
            return ofExternalTableCompensation(refBaseTable, partitionKeys);
        } else {
            return MVCompensation.createUnkownState(sessionVariable);
        }
    }

    private MVCompensation getMVCompensationOfTable(Table refBaseTable,
                                                    Set<String> partitionNamesToRefresh,
                                                    LogicalScanOperator scanOperator) {
        if (scanOperator instanceof LogicalOlapScanOperator) {
            return getMVCompensationOfOlapTable(refBaseTable, partitionNamesToRefresh,
                    (LogicalOlapScanOperator) scanOperator);
        } else if (MvPartitionCompensator.isTableSupportedPartitionCompensate(refBaseTable)) {
            return getMVCompensationForExternal(partitionNamesToRefresh, scanOperator);
        } else {
            SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
            return MVCompensation.createUnkownState(sessionVariable);
        }
    }

    private MVCompensation getMVCompensationOfOlapTable(Table refBaseTable,
                                                        Set<String> partitionNamesToRefresh,
                                                        LogicalOlapScanOperator olapScanOperator) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        OlapTable olapTable = (OlapTable) refBaseTable;
        List<Long> selectPartitionIds = olapScanOperator.getSelectedPartitionId();
        if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }
        // if any of query's select partition ids has not been refreshed, then no rewrite with this mv.
        if (selectPartitionIds.stream()
                .map(id -> olapTable.getPartition(id))
                .noneMatch(part -> partitionNamesToRefresh.contains(part.getName()))) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

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
                                                       List<PartitionKey> toRefreshRefTablePartitions) {
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

    private MVCompensation getMVCompensationForExternal(Set<String> refTablePartitionNamesToRefresh,
                                                        LogicalScanOperator refScanOperator) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
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
            List<PartitionKey> toRefreshRefTablePartitions = getMVCompensatePartitionsOfExternal(
                    refTablePartitionNamesToRefresh, refScanOperator);
            if (toRefreshRefTablePartitions == null) {
                return MVCompensation.createUnkownState(sessionVariable);
            }
            Table table = refScanOperator.getTable();
            if (Sets.newHashSet(toRefreshRefTablePartitions).containsAll(selectPartitionKeys)) {
                logMVRewrite(mvContext, "All external table {}'s selected partitions {} need to refresh, no rewrite",
                        table.getName(), selectPartitionIds);
                return MVCompensation.createNoRewriteState(sessionVariable);
            }
            return ofExternalTableCompensation(table, toRefreshRefTablePartitions);
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

    private List<PartitionKey> getMVCompensatePartitionsOfExternal(Set<String> refTablePartitionNamesToRefresh,
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
}
