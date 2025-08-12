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

package com.starrocks.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import com.staros.client.StarClientException;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TTableReplicationRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LakeReplicationJob extends ReplicationJob {
    private static final Logger LOG = LogManager.getLogger(LakeReplicationJob.class);
    private long virtualTabletId;
    static Map<String, Long> storageVolumeNameToShardIdMap = new ConcurrentHashMap<>();

    @SerializedName(value = "srcDatabaseId")
    protected long srcDatabaseId;

    @SerializedName(value = "srcTableId")
    protected long srcTableId;

    @TestOnly
    public LakeReplicationJob() {}

    @TestOnly
    public LakeReplicationJob(String jobId, long virtualTabletId, long srcDatabaseId, long srcTableId, long databaseId,
                              OlapTable table, OlapTable srcTable, SystemInfoService srcSystemInfoService) {
        super(jobId, null, databaseId, table, srcTable, srcSystemInfoService);
        this.srcDatabaseId = srcDatabaseId;
        this.srcTableId = srcTableId;
        this.virtualTabletId = virtualTabletId;
    }

    public LakeReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        super(request);
        this.srcDatabaseId = request.src_database_id;
        this.srcTableId = request.src_table_id;
        this.virtualTabletId = getVirtualTabletId(request.src_storage_volume_name, request.src_service_id);
    }

    @VisibleForTesting
    public synchronized long getVirtualTabletId(String svName, String srcServiceId) {
        return storageVolumeNameToShardIdMap.computeIfAbsent(svName,
                storageVolumeName -> getOrCreateVirtualTabletId(storageVolumeName, srcServiceId));
    }

    public long getOrCreateVirtualTabletId(String storageVolumeName, String srcServiceId) {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        StorageVolume storageVolume = storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
        if (storageVolume == null) {
            throw new RuntimeException("Unknown src storage volume while creating virtual tablet: " + storageVolumeName);
        }

        long vTabletId = storageVolume.getUniqueId();
        // missing storage volume unique id, reset it
        if (vTabletId == -1) {
            try {
                vTabletId = storageVolumeMgr.resetStorageVolumeUniqueId(storageVolumeName);
            } catch (DdlException e) {
                LOG.error("Failed to reset storage volume unique id for {}", storageVolumeName);
                throw new RuntimeException("Failed to reset storage volume unique id for " + storageVolumeName);
            }
        }

        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        try {
            ShardInfo shardInfo = starOSAgent.getShardInfo(vTabletId, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            if (shardInfo != null) {
                assert shardInfo.getShardId() == vTabletId;
                LOG.info("Shard {} already exists, skip creating it", vTabletId);
                return vTabletId;
            }
        } catch (StarClientException e) {
            // skip as shard not found
        }

        try {
            FilePathInfo pathInfo = starOSAgent.allocateFilePath(storageVolume.getId(), srcServiceId);
            FileCacheInfo cacheInfo =
                    FileCacheInfo.newBuilder().setEnableCache(false).setTtlSeconds(-1).setAsyncWriteBack(false).build();
            // assume each shard group has only one shard
            long shardGroupId = starOSAgent.createShardGroupForVirtualTablet();
            Map<String, String> properties = new HashMap<>();
            starOSAgent.createShardWithVirtualTabletId(pathInfo, cacheInfo, shardGroupId, properties, vTabletId,
                    WarehouseManager.DEFAULT_RESOURCE);
            LOG.info("Created shard for storage volume: {}, vTablet id: {}, group id: {}, src service id: {}",
                    storageVolumeName, vTabletId, shardGroupId, srcServiceId);
            return vTabletId;
        } catch (Exception e) {
            LOG.error("Failed to create shard for storage volume {} ", storageVolumeName, e);
            throw new RuntimeException("Failed to create shard for storage volume: " + storageVolumeName + ", " +
                    "error: " + e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        try {
            if (super.getState().equals(ReplicationJobState.INITIALIZING)) {
                beginTransaction();
                sendReplicateLakeRemoteStorageTasks();
                setState(ReplicationJobState.REPLICATING);
            } else if (super.getState().equals(ReplicationJobState.REPLICATING)) {
                if (isTransactionAborted()) {
                    setState(ReplicationJobState.ABORTED);
                } else if (isCrashRecovery()) {
                    sendReplicateLakeRemoteStorageTasks();
                    LOG.info("Lake replication job recovered, state: {}, database id: {}, table id: {}, transaction id: {}",
                            super.getState(), super.getDatabaseId(), super.getTableId(), super.getTransactionId());
                } else if (isAllTaskFinished()) {
                    commitTransaction();
                    setState(ReplicationJobState.COMMITTED);
                }
            }
        } catch (Exception e) {
            LOG.warn("Lake replication job exception, state(): {}, database id: {}, table id: {}, transaction id: {}, ",
                    super.getState(), super.getDatabaseId(), super.getTableId(), super.getTransactionId(), e);
            abortTransaction(e.getMessage());
            setState(ReplicationJobState.ABORTED);
        }
    }

    private void sendReplicateLakeRemoteStorageTasks() throws Exception {
        runningTasks.clear();
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        byte[] encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();
        for (PartitionInfo partitionInfo : super.getPartitionInfos().values()) {
            for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                for (TabletInfo tabletInfo : indexInfo.getTabletInfos().values()) {
                    Long computeNodeId = warehouseMgr
                            .getComputeNodeId(WarehouseManager.DEFAULT_RESOURCE, tabletInfo.getTabletId());
                    if (computeNodeId == null) {
                        throw new RuntimeException("Send lake replicate task failed, no compute node found for tablet: "
                                + tabletInfo.getTabletId());
                    }
                    ReplicateSnapshotTask task = new ReplicateSnapshotTask(computeNodeId, super.getDatabaseId(),
                            super.getTableId(), partitionInfo.getPartitionId(), indexInfo.getIndexId(),
                            tabletInfo.getTabletId(), getTabletType(super.getTableType()), super.getTransactionId(),
                            indexInfo.getSchemaHash(), partitionInfo.getVersion(),
                            partitionInfo.getDataVersion(), tabletInfo.getSrcTabletId(),
                            getTabletType(super.getSrcTableType()),
                            indexInfo.getSrcSchemaHash(), partitionInfo.getSrcVersion(), encryptionMeta,
                            virtualTabletId, srcDatabaseId, srcTableId, partitionInfo.getSrcPartitionId());
                    LOG.info("Add lake replicate snapshot task, tablet id: {}, txn id: {}, src partition info: {}/{}/{}",
                            tabletInfo.getTabletId(), super.getTransactionId(), srcDatabaseId, srcTableId,
                            partitionInfo.getSrcPartitionId());
                    runningTasks.put(task, task);
                }
            }
        }
        taskNum = runningTasks.size();
        LOG.info("Send lake replicate snapshot task, task num: {}, database id: {}, table id: {}, transaction id: {}",
                taskNum, super.getDatabaseId(), super.getTableId(), super.getTransactionId());
        sendRunningTasks();
    }

    @Override
    public long getReplicationReplicaCount() {
        return 1;
    }

}
