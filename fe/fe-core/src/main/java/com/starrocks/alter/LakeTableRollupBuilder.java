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

package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.Util;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LakeTableRollupBuilder extends AlterJobV2Builder {
    private static final Logger LOG = LogManager.getLogger(LakeTableRollupBuilder.class);
    private final OlapTable olapTable;

    public LakeTableRollupBuilder(OlapTable table) {
        Preconditions.checkArgument(table.isCloudNativeTableOrMaterializedView());
        this.olapTable = table;
    }

    @Override
    public AlterJobV2 build() throws StarRocksException {
        /*
         * create all rollup indexes. and set state.
         * After setting, Tables' state will be ROLLUP
         */
        int baseSchemaHash = olapTable.getSchemaHashByIndexId(baseIndexId);
        // mvSchemaVersion will keep same with the src MaterializedIndex
        int mvSchemaVersion = olapTable.getIndexMetaByIndexId(baseIndexId).getSchemaVersion();
        int mvSchemaHash = Util.schemaHash(0 /* init schema version */, rollupColumns, olapTable.getBfColumnNames(),
                olapTable.getBfFpp());

        AlterJobV2 mvJob = new LakeRollupJob(jobId, dbId, olapTable.getId(), olapTable.getName(), timeoutMs,
                baseIndexId, rollupIndexId, baseIndexName, rollupIndexName, mvSchemaVersion,
                rollupColumns, whereClause, baseSchemaHash, mvSchemaHash,
                rollupKeysType, rollupShortKeyColumnCount, origStmt, viewDefineSql, isColocateMVIndex);
        mvJob.setWarehouseId(warehouseId);

        for (Partition partition : olapTable.getPartitions()) {
            long partitionId = partition.getId();
            TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long physicalPartitionId = physicalPartition.getId();
                // create shard group
                long shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().
                        createShardGroup(dbId, olapTable.getId(), partitionId, rollupIndexId);
                // index state is SHADOW
                MaterializedIndex mvIndex = new MaterializedIndex(rollupIndexId,
                        MaterializedIndex.IndexState.SHADOW, shardGroupId);
                MaterializedIndex baseIndex = physicalPartition.getIndex(baseIndexId);

                // create shard
                Map<String, String> shardProperties = new HashMap<>();
                shardProperties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(olapTable.getId()));
                shardProperties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                shardProperties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(rollupIndexId));

                List<Tablet> originTablets = physicalPartition.getIndex(baseIndexId).getTablets();
                WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Optional<Long> workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(
                        ConnectContext.get().getCurrentWarehouseId());
                if (workerGroupId.isEmpty()) {
                    Warehouse warehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
                    ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, warehouse.getName());
                }
                List<Long> originTableIds = originTablets.stream()
                        .map(tablet -> tablet.getId())
                        .collect(Collectors.toList());
                List<Long> shadowTabletIds = GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                        originTablets.size(),
                        olapTable.getPartitionFilePathInfo(partitionId),
                        olapTable.getPartitionFileCacheInfo(partitionId),
                        shardGroupId, originTableIds, shardProperties, workerGroupId.get());
                Preconditions.checkState(originTablets.size() == shadowTabletIds.size());

                TabletMeta shadowTabletMeta =
                        new TabletMeta(dbId, olapTable.getId(), physicalPartitionId, rollupIndexId, 0, medium, true);
                for (int i = 0; i < originTablets.size(); i++) {
                    Tablet originTablet = originTablets.get(i);
                    Tablet shadowTablet = new LakeTablet(shadowTabletIds.get(i));
                    mvIndex.addTablet(shadowTablet, shadowTabletMeta);
                    mvJob.addTabletIdMap(physicalPartitionId, shadowTablet.getId(), originTablet.getId());
                }

                mvJob.addMVIndex(physicalPartitionId, mvIndex);
                LOG.debug("create materialized view index {} based on index {} in partition {}:{}",
                        rollupIndexId, baseIndexId, partitionId, physicalPartitionId);
            } // end for baseTablets
        }
        return mvJob;
    }
}
