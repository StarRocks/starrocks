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
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.connector.PartitionUtil;
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
            repairBaseTableTableVersionChange(mv, table, partitionRepairInfos);
        }
    }

    private static void repairBaseTableTableVersionChange(MaterializedView mv,
                                                          Table table,
                                                          List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        // check table existed in mv's version map
        MaterializedView.AsyncRefreshContext asyncRefreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVersionMap =
                asyncRefreshContext.getBaseTableVisibleVersionMap();
        // repair base table's version map directly
        Map<String, MaterializedView.BasePartitionInfo> changedVersions = toBasePartitionInfoMap(partitionRepairInfos);
        baseTableVersionMap.computeIfAbsent(table.getId(), k -> Maps.newHashMap())
                .putAll(changedVersions);
        LOG.info("repair base table {} version changes for mv {}, changed versions:{}",
                table.getName(), mv.getName(), changedVersions);
        // update edit log
        long maxChangedTableRefreshTime =
                MvUtils.getMaxTablePartitionInfoRefreshTime(Lists.newArrayList(changedVersions));
        MVVersionManager.updateEditLogAfterVersionMetaChanged(mv, maxChangedTableRefreshTime);
    }

    private static Map<String, MaterializedView.BasePartitionInfo> toBasePartitionInfoMap(
            List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = Maps.newHashMap();
        for (MVRepairHandler.PartitionRepairInfo partitionRepairInfo : partitionRepairInfos) {
            MaterializedView.BasePartitionInfo basePartitionInfo = new MaterializedView.BasePartitionInfo(
                    partitionRepairInfo.getPartitionId(), partitionRepairInfo.getVersion(),
                    partitionRepairInfo.getVersionTime());
            partitionInfoMap.put(partitionRepairInfo.getPartitionName(), basePartitionInfo);
        }
        return partitionInfoMap;
    }

    /**
     * Repair mv's base table info if base table has been dropped and recreated and base table info is changed.
     * @param mv mv to repair
     * @param oldBaseTableInfo old base table info
     * @param newTable new table meta data
     * @param updatedPartitionNames updated partition names
     */
    public static void repairExternalBaseTableInfo(MaterializedView mv, BaseTableInfo oldBaseTableInfo,
                                                   Table newTable, List<String> updatedPartitionNames) {

        if (oldBaseTableInfo.isInternalCatalog()) {
            return;
        }

        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap = mv.getBaseTableRefreshInfo(oldBaseTableInfo);
        Map<String, MaterializedView.BasePartitionInfo> newPartitionInfoMap = Maps.newHashMap();
        for (Map.Entry<String, MaterializedView.BasePartitionInfo> entry : partitionInfoMap.entrySet()) {
            if (updatedPartitionNames.contains(entry.getKey())) {
                newPartitionInfoMap.put(entry.getKey(), entry.getValue());
            } else {
                List<String> baseTablePartitionNames = Lists.newArrayList(partitionInfoMap.keySet());
                Map<String, com.starrocks.connector.PartitionInfo> newPartitionInfos =
                        PartitionUtil.getPartitionNameWithPartitionInfo(newTable, baseTablePartitionNames);
                if (newPartitionInfos.containsKey(entry.getKey())) {
                    MaterializedView.BasePartitionInfo oldBasePartitionInfo = entry.getValue();
                    com.starrocks.connector.PartitionInfo newPartitionInfo = newPartitionInfos.get(entry.getKey());
                    MaterializedView.BasePartitionInfo newBasePartitionInfo = new MaterializedView.BasePartitionInfo(
                            entry.getValue().getId(), newPartitionInfo.getModifiedTime(), newPartitionInfo.getModifiedTime());
                    newBasePartitionInfo.setExtLastFileModifiedTime(oldBasePartitionInfo.getExtLastFileModifiedTime());
                    newBasePartitionInfo.setFileNumber(oldBasePartitionInfo.getFileNumber());
                    newPartitionInfoMap.put(entry.getKey(), newBasePartitionInfo);
                } else {
                    // if the partition does not exist in new table,
                    // keep the partition's last modified time as old
                    // which will be refreshed
                    newPartitionInfoMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> baseTableInfoMapMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap();
        // create new base table info with newTable.getTableIdentifier()
        BaseTableInfo newBaseTableInfo = new BaseTableInfo(
                oldBaseTableInfo.getCatalogName(),
                oldBaseTableInfo.getDbName(),
                oldBaseTableInfo.getTableName(), newTable.getTableIdentifier());
        baseTableInfoMapMap.remove(oldBaseTableInfo);
        baseTableInfoMapMap.put(newBaseTableInfo, newPartitionInfoMap);

        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        baseTableInfos.remove(oldBaseTableInfo);
        baseTableInfos.add(newBaseTableInfo);
        // reset mv's state after repair
        mv.resetMetadataCache();

        ConnectorTableInfo connectorTableInfo = GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr()
                .getConnectorTableInfo(oldBaseTableInfo.getCatalogName(), oldBaseTableInfo.getDbName(),
                        oldBaseTableInfo.getTableIdentifier());
        ConnectorTableInfo newConnectorTableInfo = ConnectorTableInfo.builder()
                .setRelatedMaterializedViews(connectorTableInfo.getRelatedMaterializedViews())
                .build();
        GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().removeConnectorTableInfo(
                oldBaseTableInfo.getCatalogName(), oldBaseTableInfo.getDbName(),
                oldBaseTableInfo.getTableIdentifier(), connectorTableInfo);
        GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().addConnectorTableInfo(
                newBaseTableInfo.getCatalogName(), newBaseTableInfo.getDbName(),
                newBaseTableInfo.getTableIdentifier(), newConnectorTableInfo);
        // TODO: update edit log for followers' fe
    }
}
