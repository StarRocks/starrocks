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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
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
}
