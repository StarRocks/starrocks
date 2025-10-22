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
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MVVersionManager is used to update materialized view version info when base table partition changes after mv refresh finished.
 */
public class MVVersionManager {
    // only used in the static methods
    private static final Logger LOG = LogManager.getLogger(MVVersionManager.class);

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
     * @param tempMvTvrVersionRangeMap temporary tvr version range map for each base table which is used for ivm refresh
     */
    public void updateMVVersionInfo(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables,
                                    Set<String> mvRefreshedPartitions,
                                    Set<Long> refBaseTableIds,
                                    Map<BaseTableSnapshotInfo, Set<String>> refTableAndPartitionNames,
                                    Map<BaseTableInfo, TvrVersionRange> tempMvTvrVersionRangeMap) {
        MaterializedView.MvRefreshScheme mvRefreshScheme = mv.getRefreshScheme();
        MaterializedView.AsyncRefreshContext refreshContext = mvRefreshScheme.getAsyncRefreshContext();
        // update materialized view partition to ref base table partition names meta
        updateAssociatedPartitionMeta(refreshContext, mvRefreshedPartitions, refTableAndPartitionNames);
        // Update meta information for OLAP tables and external tables
        Map<Boolean, List<BaseTableSnapshotInfo>> snapshotInfoSplits = snapshotBaseTables.values()
                .stream()
                .collect(Collectors.partitioningBy(s -> s.getBaseTable().isNativeTableOrMaterializedView()));
        List<BaseTableSnapshotInfo> olapTables = snapshotInfoSplits.getOrDefault(true, List.of());
        List<BaseTableSnapshotInfo> externalTables = snapshotInfoSplits.getOrDefault(false, List.of());
        boolean isOlapTableRefreshed = updateMetaForOlapTable(refreshContext, olapTables, refBaseTableIds);
        boolean isExternalTableRefreshed = updateMetaForExternalTable(refreshContext, externalTables, refBaseTableIds);

        if (!isOlapTableRefreshed && !isExternalTableRefreshed) {
            return;
        }
        if (tempMvTvrVersionRangeMap != null) {
            // update the tvr version range map in mv context
            final Map<BaseTableInfo, TvrVersionRange> mvTvrVersionRangeMap =
                    mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
            for (Map.Entry<BaseTableInfo, TvrVersionRange> entry : tempMvTvrVersionRangeMap.entrySet()) {
                mvTvrVersionRangeMap.put(entry.getKey(), entry.getValue());
            }
        }

        long maxChangedTableRefreshTime = 0L;
        for (BaseTableSnapshotInfo snapshot : snapshotBaseTables.values()) {
            if (snapshot instanceof PCTTableSnapshotInfo) {
                Map<String, MaterializedView.BasePartitionInfo> partitionInfos =
                        ((PCTTableSnapshotInfo) snapshot).getRefreshedPartitionInfos();
                long refreshTime = MvUtils.getMaxTablePartitionInfoRefreshTime(partitionInfos);
                if (refreshTime > maxChangedTableRefreshTime) {
                    maxChangedTableRefreshTime = refreshTime;
                }
            }
        }
        mvRefreshScheme.setLastRefreshTime(maxChangedTableRefreshTime);
        updateEditLogAfterVersionMetaChanged(mv, maxChangedTableRefreshTime);

        // trigger timeless info event since mv version changed
        GlobalStateMgr.getCurrentState().getMaterializedViewMgr().triggerTimelessInfoEvent(mv,
                MVTimelinessMgr.MVChangeEvent.MV_REFRESHED);
    }

    private boolean updateMetaForOlapTable(MaterializedView.AsyncRefreshContext refreshContext,
                                           List<BaseTableSnapshotInfo> changedTablePartitionInfos,
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
        for (BaseTableSnapshotInfo snapshotInfo : changedTablePartitionInfos) {
            if (!(snapshotInfo instanceof PCTTableSnapshotInfo)) {
                logger.warn("Skip update meta for base table {}, because it is not a PCTTableSnapshotInfo",
                        snapshotInfo.getBaseTable().getName());
                continue;
            }
            PCTTableSnapshotInfo pctTableSnapshotInfo = (PCTTableSnapshotInfo) snapshotInfo;
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
                        snapshotTable.getName(), pctTableSnapshotInfo.getRefreshedPartitionInfos(), mv.getName());
                continue;
            }
            Long tableId = snapshotTable.getId();
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.computeIfAbsent(tableId, (v) -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = pctTableSnapshotInfo.getRefreshedPartitionInfos();
            logger.debug("Update materialized view {} meta for base table {} with partitions info: {}, old partition infos:{}",
                    mv.getName(), snapshotTable.getName(), partitionInfoMap, currentTablePartitionInfo);
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // FIXME: If base table's partition has been dropped, should drop the according version partition too?
            // remove partition info of not-exist partition for snapshot table from version map
            if (snapshotTable.isOlapOrCloudNativeTable()) {
                OlapTable snapshotOlapTable = (OlapTable) snapshotTable;
                Set<String> visiblePartitionNames = snapshotOlapTable.getVisiblePartitionNames();
                // Use iterator to avoid creating intermediate collections and for better performance
                Iterator<String> keyIter = currentTablePartitionInfo.keySet().iterator();
                while (keyIter.hasNext()) {
                    String partitionName = keyIter.next();
                    if (!visiblePartitionNames.contains(partitionName)) {
                        keyIter.remove();
                    }
                }
            }
            isOlapTableRefreshed = true;
        }
        return isOlapTableRefreshed;
    }

    private boolean updateMetaForExternalTable(MaterializedView.AsyncRefreshContext refreshContext,
                                               List<BaseTableSnapshotInfo> changedTablePartitionInfos,
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
        for (BaseTableSnapshotInfo snapshotInfo : changedTablePartitionInfos) {
            if (!(snapshotInfo instanceof PCTTableSnapshotInfo)) {
                logger.warn("Skip update meta for base table {}, because it is not a PCTTableSnapshotInfo",
                        snapshotInfo.getBaseTable().getName());
                continue;
            }
            PCTTableSnapshotInfo pctTableSnapshotInfo = (PCTTableSnapshotInfo) snapshotInfo;
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
                        snapshotTable.getName(), pctTableSnapshotInfo.getRefreshedPartitionInfos(), mv.getName());
                continue;
            }

            // Use computeIfAbsent to avoid unnecessary map creation
            Map<String, MaterializedView.BasePartitionInfo> currentTablePartitionInfo =
                    currentVersionMap.computeIfAbsent(baseTableInfo, v -> Maps.newConcurrentMap());
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = pctTableSnapshotInfo.getRefreshedPartitionInfos();
            logger.debug("Update materialized view {} meta for external base table {} with partitions info: {}, " +
                            "old partition infos:{}", mv.getName(), snapshotTable.getName(),
                    partitionInfoMap, currentTablePartitionInfo);
            // overwrite old partition names
            currentTablePartitionInfo.putAll(partitionInfoMap);

            // Remove partition info for partitions that no longer exist in the snapshot table
            List<String> partitionNamesList = PartitionUtil.getPartitionNames(snapshotTable);
            if (!partitionNamesList.isEmpty()) {
                Set<String> partitionNames = Sets.newHashSetWithExpectedSize(partitionNamesList.size());
                partitionNames.addAll(partitionNamesList);
                // Remove using iterator for better performance on concurrent maps
                Iterator<String> iter = currentTablePartitionInfo.keySet().iterator();
                while (iter.hasNext()) {
                    String partitionName = iter.next();
                    if (!partitionNames.contains(partitionName)) {
                        iter.remove();
                    }
                }
            } else {
                // If no partitions exist, clear all
                currentTablePartitionInfo.clear();
            }
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
                                               Map<BaseTableSnapshotInfo, Set<String>> refTableAndPartitionNames) {
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
                for (BaseTableSnapshotInfo snapshotInfo : refTableAndPartitionNames.keySet()) {
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
        LOG.info("Update materialized view {} refresh scheme, " +
                        "last refresh time: {}, version meta changed",
                mv.getName(), maxChangedTableRefreshTime);
        GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(changeRefreshSchemeLog);
    }
}
