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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TableSnapshotInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MVVersionManager is used to update materialized view version info when base table partition changes after mv refresh finished.
 */
public class MVVersionManager {
    private final Logger logger;
    private final MaterializedView mv;
    private final MvTaskRunContext mvTaskRunContext;

    public MVVersionManager(MaterializedView mv,
                            MvTaskRunContext mvTaskRunContext) {
        this.mv = mv;
        this.mvTaskRunContext = mvTaskRunContext;
        this.logger = MVTraceUtils.getLogger(mv, MVVersionManager.class);
    }

    /**
     * Update materialized view version info when base table partition changes after mv refresh finished.
     * @param snapshotBaseTables base tables snapshot info that has been refreshed
     * @param mvRefreshedPartitions mv refreshed partitions
     * @param refBaseTableIds  mv's ref base table ids
     * @param refTableAndPartitionNames mv's ref base table and partition names
     */
    public void updateMVVersionInfo(Map<Long, TableSnapshotInfo> snapshotBaseTables,
                                    Set<String> mvRefreshedPartitions,
                                    Set<Long> refBaseTableIds,
                                    Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        MaterializedView.MvRefreshScheme mvRefreshScheme = mv.getRefreshScheme();
        MaterializedView.AsyncRefreshContext refreshContext = mvRefreshScheme.getAsyncRefreshContext();
        // update materialized view partition to ref base table partition names meta
        updateAssociatedPartitionMeta(refreshContext, mvRefreshedPartitions, refTableAndPartitionNames);
        // Update meta information for OLAP tables and external tables
        Map<Boolean, List<TableSnapshotInfo>> snapshotInfoSplits = snapshotBaseTables.values()
                .stream()
                .collect(Collectors.partitioningBy(s -> s.getBaseTable().isNativeTableOrMaterializedView()));
        List<TableSnapshotInfo> olapTables = snapshotInfoSplits.getOrDefault(true, List.of());
        List<TableSnapshotInfo> externalTables = snapshotInfoSplits.getOrDefault(false, List.of());
        boolean isOlapTableRefreshed = updateMetaForOlapTable(refreshContext, olapTables, refBaseTableIds);
        boolean isExternalTableRefreshed = updateMetaForExternalTable(refreshContext, externalTables, refBaseTableIds);

        if (!isOlapTableRefreshed && !isExternalTableRefreshed) {
            return;
        }
        Collection<Map<String, MaterializedView.BasePartitionInfo>> allChangedPartitionInfos =
                snapshotBaseTables.values()
                        .stream()
                        .map(snapshot -> snapshot.getRefreshedPartitionInfos())
                        .collect(Collectors.toList());
        long maxChangedTableRefreshTime = MvUtils.getMaxTablePartitionInfoRefreshTime(allChangedPartitionInfos);
        mv.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
        updateEditLogAfterVersionMetaChanged(mv, maxChangedTableRefreshTime);

        // trigger timeless info event since mv version changed
        GlobalStateMgr.getCurrentState().getMaterializedViewMgr().triggerTimelessInfoEvent(mv,
                MVTimelinessMgr.MVChangeEvent.MV_REFRESHED);
    }

    private boolean updateMetaForOlapTable(MaterializedView.AsyncRefreshContext refreshContext,
                                           List<TableSnapshotInfo> changedTablePartitionInfos,
                                           Set<Long> refBaseTableIds) {
        if (changedTablePartitionInfos.isEmpty()) {
            return false;
        }
        logger.info("Update meta for mv {} with olap tables:{}, refBaseTableIds:{}", mv.getName(),
                changedTablePartitionInfos, refBaseTableIds);
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableVisibleVersionMap();
        boolean hasNextPartitionToRefresh = mvTaskRunContext.hasNextBatchPartition();
        // update version map of materialized view
        boolean isOlapTableRefreshed = false;
        for (TableSnapshotInfo snapshotInfo : changedTablePartitionInfos) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            // Non-ref-base-tables should be update meta at the last refresh, otherwise it may
            // cause wrong results for rewrite or refresh.
            // eg:
            // tblA : partition table, has partitions: p0, p1, p2
            // tblB : non-partition table
            // MV: tblA a join tblB b on a.dt=b.dt
            // case: tblB has been updated,
            // run1: tblA(p0) + tblB, (X)
            // run2: tblA(p1) + tblB, (X)
            // run3: tblA(p2) + tblB, (Y)
            // In the run1/run2 should only update the tblA's partition info, but tblB's partition
            // info meta should be updated at the last refresh.
            if (hasNextPartitionToRefresh && !refBaseTableIds.contains(snapshotTable.getId())) {
                logger.info("Skip update meta for olap base table {} with partitions info: {}, " +
                                "because it is not a ref base table of materialized view {}",
                        snapshotTable.getName(), snapshotInfo.getRefreshedPartitionInfos(), mv.getName());
                continue;
            }
            Long tableId = snapshotTable.getId();
            currentVersionMap.computeIfAbsent(tableId, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.get(tableId);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = snapshotInfo.getRefreshedPartitionInfos();
            logger.debug("Update materialized view {} meta for base table {} with partitions info: {}, old partition infos:{}",
                    mv.getName(), snapshotTable.getName(), partitionInfoMap, currentTablePartitionInfo);
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // FIXME: If base table's partition has been dropped, should drop the according version partition too?
            // remove partition info of not-exist partition for snapshot table from version map
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                currentTablePartitionInfo.keySet().removeIf(partitionName ->
                        !snapshotOlapTable.getVisiblePartitionNames().contains(partitionName));
            }
            isOlapTableRefreshed = true;
        }
        return isOlapTableRefreshed;
    }

    private boolean updateMetaForExternalTable(MaterializedView.AsyncRefreshContext refreshContext,
                                               List<TableSnapshotInfo> changedTablePartitionInfos,
                                               Set<Long> refBaseTableIds) {
        if (changedTablePartitionInfos.isEmpty()) {
            return false;
        }
        logger.info("Update meta for mv {} with external tables:{}, refBaseTableIds:{}", mv.getName(),
                changedTablePartitionInfos, refBaseTableIds);
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> currentVersionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        boolean hasNextBatchPartition = mvTaskRunContext.hasNextBatchPartition();
        // update version map of materialized view
        for (TableSnapshotInfo snapshotInfo : changedTablePartitionInfos) {
            BaseTableInfo baseTableInfo = snapshotInfo.getBaseTableInfo();
            Table snapshotTable = snapshotInfo.getBaseTable();
            // Non-ref-base-tables should be update meta at the last refresh, otherwise it may
            // cause wrong results for rewrite or refresh.
            // eg:
            // tblA : partition table, has partitions: p0, p1, p2
            // tblB : non-partition table
            // MV: tblA a join tblB b on a.dt=b.dt
            // case: tblB has been updated,
            // run1: tblA(p0) + tblB, (X)
            // run2: tblA(p1) + tblB, (X)
            // run3: tblA(p2) + tblB, (Y)
            // In the run1/run2 should only update the tblA's partition info, but tblB's partition
            // info meta should be updated at the last refresh.
            if (hasNextBatchPartition && !refBaseTableIds.contains(snapshotTable.getId())) {
                logger.info("Skip update meta for external base table {} with partitions info: {}, " +
                                "because it is not a ref base table of materialized view {}",
                        snapshotTable.getName(), snapshotInfo.getRefreshedPartitionInfos(), mv.getName());
                continue;
            }
            currentVersionMap.computeIfAbsent(baseTableInfo, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo = currentVersionMap.get(baseTableInfo);
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = snapshotInfo.getRefreshedPartitionInfos();
            logger.debug("Update materialized view {} meta for external base table {} with partitions info: {}, " +
                            "old partition infos:{}", mv.getName(), snapshotTable.getName(),
                    partitionInfoMap, currentTablePartitionInfo);
            // overwrite old partition names
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // FIXME: If base table's partition has been dropped, should drop the according version partition too?
            // remove partition info of not-exist partition for snapshot table from version map
            Set<String> partitionNames = Sets.newHashSet(PartitionUtil.getPartitionNames(snapshotTable));
            currentTablePartitionInfo.keySet().removeIf(partitionName -> !partitionNames.contains(partitionName));
        }
        return true;
    }

    /**
     * Update materialized view partition to ref base table partition names meta, this is used in base table's partition
     * changes.
     * eg:
     * base table has dropped one partitioned, we can only drop the vesion map of associated materialized view's
     * partitions rather than the whole table.
     */
    private void updateAssociatedPartitionMeta(MaterializedView.AsyncRefreshContext refreshContext,
                                               Set<String> mvRefreshedPartitions,
                                               Map<TableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
        Map<String, Map<Table, Set<String>>> mvToBaseNameRefs = mvTaskRunContext.getMvRefBaseTableIntersectedPartitions();
        if (Objects.isNull(mvToBaseNameRefs) || Objects.isNull(refTableAndPartitionNames) ||
                refTableAndPartitionNames.isEmpty()) {
            return;
        }

        try {
            Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                    refreshContext.getMvPartitionNameRefBaseTablePartitionMap();
            for (String mvRefreshedPartition : mvRefreshedPartitions) {
                Map<Table, Set<String>> mvToBaseNameRef = mvToBaseNameRefs.get(mvRefreshedPartition);
                for (TableSnapshotInfo snapshotInfo : refTableAndPartitionNames.keySet()) {
                    Table refBaseTable = snapshotInfo.getBaseTable();
                    if (!mvToBaseNameRef.containsKey(refBaseTable)) {
                        continue;
                    }
                    Set<String> realBaseTableAssociatedPartitions = Sets.newHashSet();
                    for (String refBaseTableAssociatedPartition : mvToBaseNameRef.get(refBaseTable)) {
                        realBaseTableAssociatedPartitions.addAll(
                                mvTaskRunContext.getExternalTableRealPartitionName(refBaseTable,
                                        refBaseTableAssociatedPartition));
                    }
                    mvPartitionNameRefBaseTablePartitionMap
                            .put(mvRefreshedPartition, realBaseTableAssociatedPartitions);
                }
            }
        } catch (Exception e) {
            logger.warn("Update materialized view {} with the associated ref base table partitions failed: ",
                    mv.getName(), e);
        }
    }

    /**
     * Sync meta changes to followers by edit log after version meta changed.
     * @param mv  mv that need to update
     * @param maxChangedTableRefreshTime max changed table refresh time
     */
    public static void updateEditLogAfterVersionMetaChanged(MaterializedView mv,
                                                            long maxChangedTableRefreshTime) {
        mv.getRefreshScheme().setLastRefreshTime(maxChangedTableRefreshTime);
        ChangeMaterializedViewRefreshSchemeLog changeRefreshSchemeLog =
                new ChangeMaterializedViewRefreshSchemeLog(mv);
        GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
    }
}
