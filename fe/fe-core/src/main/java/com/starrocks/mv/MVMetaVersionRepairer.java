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

package com.starrocks.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.scheduler.mv.MVVersionManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MVMetaVersionRepairer {
    private static final Logger LOG = LogManager.getLogger(MVMetaVersionRepairer.class);

    /**
     * Repair base table version changes for all related materialized views when table has no data changed but only version
     * changes which happens in cloud-native environment for background compaction.
     * @param db table's database
     * @param table changed table
     * @param partitionRepairInfos table's changed partition infos
     */
    public static void repairBaseTableVersionChanges(Database db, Table table,
                                                     List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        if (db == null || table == null) {
            return;
        }
        if (!table.isNativeTableOrMaterializedView()) {
            LOG.warn("table {} is not a native table or materialized view", table.getName());
            return;
        }

        Set<MvId> mvIds = table.getRelatedMaterializedViews();
        for (MvId mvId : mvIds) {
            Database mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
            if (mvDb == null) {
                LOG.warn("mv db {} not found", mvId.getDbId());
                continue;
            }
            MaterializedView mv = (MaterializedView) mvDb.getTable(mvId.getId());
            if (mv == null) {
                LOG.warn("mv {} not found", mvId.getId());
                continue;
            }

            // acquire db write lock to modify meta of mv
            Locker locker = new Locker();
            if (!locker.lockDatabaseAndCheckExist(db, mv, LockType.WRITE)) {
                continue;
            }
            try {
                repairBaseTableTableVersionChange(mv, table, partitionRepairInfos);
            } finally {
                locker.unLockTableWithIntensiveDbLock(db, mv, LockType.WRITE);
            }
        }
    }

    private static void repairBaseTableTableVersionChange(MaterializedView mv,
                                                          Table table,
                                                          List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        // check table existed in mv's version map
        MaterializedView.AsyncRefreshContext asyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVersionMap =
                asyncRefreshContext.getBaseTableVisibleVersionMap();
        List<MVRepairHandler.PartitionRepairInfo> needToUpdatePartitionInfos =
                filterToRepairPartitionInfos(mv, table, baseTableVersionMap, partitionRepairInfos);
        if (needToUpdatePartitionInfos.isEmpty()) {
            return;
        }
        // repair base table's version map directly
        Map<String, MaterializedView.BasePartitionInfo> changedVersions = toBasePartitionInfoMap(needToUpdatePartitionInfos);
        baseTableVersionMap.computeIfAbsent(table.getId(), k -> Maps.newHashMap())
                .putAll(changedVersions);
        LOG.info("repair base table {} version changes for mv {}, changed versions:{}",
                table.getName(), mv.getName(), changedVersions);
        // update edit log
        long maxChangedTableRefreshTime =
                MvUtils.getMaxTablePartitionInfoRefreshTime(Lists.newArrayList(changedVersions));
        MVVersionManager.updateEditLogAfterVersionMetaChanged(mv, maxChangedTableRefreshTime);
    }

    /**
     * Only repair partition infos which version and version time are matched with base table's version map.
     * @param mv mv to repair
     * @param table base table that has been changed(schema change or compactions)
     * @param baseTableVersionMap base table's version map in mv's version map
     * @param partitionRepairInfos partition infos to repair
     * @return partition infos that need to be updated
     */
    private static List<MVRepairHandler.PartitionRepairInfo> filterToRepairPartitionInfos(
            MaterializedView mv,
            Table table,
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVersionMap,
            List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        List<MVRepairHandler.PartitionRepairInfo> needToUpdatePartitionInfos = Lists.newArrayList();
        for (MVRepairHandler.PartitionRepairInfo info : partitionRepairInfos) {
            if (!baseTableVersionMap.containsKey(table.getId())) {
                LOG.info("Base table {} not found in mv {}'s version map, skip to repair", table.getName(), mv.getName());
                continue;
            }
            Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = baseTableVersionMap.get(table.getId());
            if (!partitionInfoMap.containsKey(info.getPartitionName())) {
                LOG.info("Partition {} not found in base table {}'s version map, skip to repair", info.getPartitionName(),
                        table.getName());
                continue;
            }
            MaterializedView.BasePartitionInfo curBasePartitionInfo = partitionInfoMap.get(info.getPartitionName());
            if (curBasePartitionInfo.getId() != info.getPartitionId()
                    || curBasePartitionInfo.getVersion() != info.getLastVersion()) {
                LOG.info("Base table {} partition {} version not match, id {}(mv)/{}(table), " +
                                "version {}(mv)/{}(table), version time {}(mv), skip to repair",
                        table.getName(), info.getPartitionName(), curBasePartitionInfo.getId(),
                        info.getPartitionId(), curBasePartitionInfo.getVersion(), info.getLastVersion(),
                        curBasePartitionInfo.getLastRefreshTime());
                continue;
            }
            needToUpdatePartitionInfos.add(info);
        }
        return needToUpdatePartitionInfos;
    }

    private static Map<String, MaterializedView.BasePartitionInfo> toBasePartitionInfoMap(
            List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = Maps.newHashMap();
        for (MVRepairHandler.PartitionRepairInfo partitionRepairInfo : partitionRepairInfos) {
            MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                    partitionRepairInfo.getPartitionId(), partitionRepairInfo.getNewVersion(),
                    partitionRepairInfo.getNewVersionTime());
            partitionInfoMap.put(partitionRepairInfo.getPartitionName(), basePartitionInfo);
        }
        return partitionInfoMap;
    }
}
