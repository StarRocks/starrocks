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

import com.google.gson.annotations.SerializedName;
import com.staros.client.StarClientException;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TTableReplicationRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class LakeReplicationJob extends ReplicationJob {
    private static final Logger LOG = LogManager.getLogger(LakeReplicationJob.class);
    private final long fakedShardId;

    @SerializedName(value = "srcDatabaseId")
    protected final long srcDatabaseId;

    @SerializedName(value = "srcTableId")
    protected final long srcTableId;

    public LakeReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        super(request);
        this.srcDatabaseId = request.src_database_id;
        this.srcTableId = request.src_table_id;
        this.fakedShardId = getFakedShardId(request.src_storage_volume_name, request.src_service_id);
    }

    private synchronized long getFakedShardId(String svName, String srcServiceId) {
        ReplicationMgr replicationMgr = GlobalStateMgr.getCurrentState().getReplicationMgr();
        long existed = replicationMgr.getStorageVolumeNameToShardIdMap().computeIfAbsent(svName,
                storageVolumeName -> {
                    long uniqueId = GlobalStateMgr.getCurrentState().getNextId();
                    createFakeShard(uniqueId, storageVolumeName, srcServiceId);
                    return uniqueId;
                });

        // double check to prevent fake shard not existed in starmgr
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        ShardInfo shardInfo = null;
        try {
            shardInfo = starOSAgent.getShardInfo(existed, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        } catch (StarClientException e) {
            // skip
        }
        if (shardInfo != null) {
            assert existed == shardInfo.getShardId();
            return existed;
        }
        LOG.info("Fake shard {} not found, try to create it again", existed);
        createFakeShard(existed, svName, srcServiceId);
        return existed;
    }
    
    private void createFakeShard(long fakedShardId, String storageVolumeName, String srcServiceId) {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        StorageVolume storageVolume = storageVolumeMgr.getStorageVolumeByName(storageVolumeName);

        LOG.info("Start creating fake shard for storage volume: {}, src service id (as root dir): {}",
                storageVolumeName, srcServiceId);

        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        try {
            FilePathInfo pathInfo = starOSAgent.allocateFilePath(storageVolume.getId(), srcServiceId);
            FileCacheInfo cacheInfo =
                    FileCacheInfo.newBuilder().setEnableCache(false).setTtlSeconds(-1).setAsyncWriteBack(false).build();
            // assume each shard group has only one shard
            long shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createFakeShardGroup();
            Map<String, String> properties = new HashMap<>();
            starOSAgent.createFakeShardWithId(pathInfo, cacheInfo, shardGroupId, properties, fakedShardId,
                    WarehouseManager.DEFAULT_RESOURCE);
            LOG.info("Created fake shard for storage volume: {}, shard id: {}, group id: {}",
                    storageVolumeName, fakedShardId, shardGroupId);
        } catch (Throwable e) {
            LOG.error("Failed to create shard for storage volume: " + storageVolume, e);
            throw new RuntimeException("Failed to create shard for storage volume: " + storageVolume, e);
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
                    LOG.info("Lake replication job recovered, super.getState(): {}, database id: {}, table id: {}, transaction id: {}",
                            super.getState(), super.getDatabaseId(), super.getTableId(), super.getTransactionId());
                } else if (isAllTaskFinished()) {
                    commitTransaction();
                    setState(ReplicationJobState.COMMITTED);
                }
            }
        } catch (Exception e) {
            LOG.warn("Lake replication job exception, super.getState(): {}, database id: {}, table id: {}, transaction id: {}, ",
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
                    Long computeNodeId = warehouseMgr.getComputeNodeId(WarehouseManager.DEFAULT_RESOURCE,
                            tabletInfo.getTabletId());
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
                            fakedShardId, srcDatabaseId, srcTableId, partitionInfo.getSrcPartitionId());
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
