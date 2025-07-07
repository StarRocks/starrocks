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

import com.google.common.base.Preconditions;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LakeReplicationJob extends ReplicationJob {
    private static final Logger LOG = LogManager.getLogger(LakeReplicationJob.class);
    private final long fakedShardId;
    static Map<String, Long> storageVolumeNameToShardIdMap = new ConcurrentHashMap<>();

    public LakeReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        super(request);
        this.fakedShardId = getFakedShardId(request.src_storage_volume_name, request.src_service_id);
    }

    private synchronized long getFakedShardId(String svName, String srcServiceId) {
        return storageVolumeNameToShardIdMap.computeIfAbsent(svName,
                storageVolumeName -> getOrCreateFakeShard(storageVolumeName, srcServiceId));
    }

    // reuse storage volume id as shard id, if not existed yet, create it manually
    private long getOrCreateFakeShard(String storageVolumeName, String srcServiceId) {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        StorageVolume storageVolume = storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
        // use negative value as shard id to prevent conflict with real shard id
        long shardId = -1 * storageVolume.getId().hashCode();
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        try {
            ShardInfo shardInfo = starOSAgent.getShardInfo(shardId, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            if (shardInfo != null) {
                LOG.info("Shard {} already exists, skip creating it", shardId);
                return shardId;
            }
        } catch (StarClientException e) {
            // skip as shard not found
        }
        LOG.info("Start creating shard for storage volume: {}, shard id: {}, src service id (as root dir): {}",
                storageVolume.getName(), shardId, srcServiceId);
        try {
            FilePathInfo pathInfo = starOSAgent.allocateFilePath(storageVolume.getId(), srcServiceId);
            FileCacheInfo cacheInfo =
                    FileCacheInfo.newBuilder().setEnableCache(false).setTtlSeconds(-1).setAsyncWriteBack(false).build();
            // assume each shard group has only one shard
            long shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup();
            Map<String, String> properties = new HashMap<>();
            List<Long> shards = starOSAgent.createShards(1, pathInfo,
                    cacheInfo, shardGroupId, null, properties, WarehouseManager.DEFAULT_RESOURCE, shardId);
            Preconditions.checkState(shards.size() == 1);
            LOG.info("Created shard for storage volume: {}, shard id: {}, group id: {}",
                    storageVolumeName, shardId, shardGroupId);
            return shards.get(0);
        } catch (Throwable e) {
            LOG.error("Failed to create shard for storage volume: " + storageVolume, e);
            throw new RuntimeException("Failed to create shard for storage volume: " + storageVolume, e);
        }
    }

    @Override
    public void run() {
        try {
            if (state.equals(ReplicationJobState.INITIALIZING)) {
                beginTransaction();
                sendReplicateLakeRemoteStorageTasks();
                setState(ReplicationJobState.REPLICATING);
            } else if (state.equals(ReplicationJobState.REPLICATING)) {
                if (isTransactionAborted()) {
                    setState(ReplicationJobState.ABORTED);
                } else if (isCrashRecovery()) {
                    sendReplicateLakeRemoteStorageTasks();
                    LOG.info("Lake replication job recovered, state: {}, database id: {}, table id: {}, transaction id: {}",
                            state, databaseId, tableId, transactionId);
                } else if (isAllTaskFinished()) {
                    commitTransaction();
                    setState(ReplicationJobState.COMMITTED);
                }
            }
        } catch (Exception e) {
            LOG.warn("Lake replication job exception, state: {}, database id: {}, table id: {}, transaction id: {}, ",
                    state, databaseId, tableId, transactionId, e);
            abortTransaction(e.getMessage());
            setState(ReplicationJobState.ABORTED);
        }
    }

    private void sendReplicateLakeRemoteStorageTasks() throws Exception {
        runningTasks.clear();
        byte[] encryptionMeta = GlobalStateMgr.getCurrentState().getKeyMgr().getCurrentKEKAsEncryptionMeta();
        for (PartitionInfo partitionInfo : partitionInfos.values()) {
            for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                for (TabletInfo tabletInfo : indexInfo.getTabletInfos().values()) {
                    Long computeNodeId = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                            .getComputeNodeId(WarehouseManager.DEFAULT_RESOURCE, tabletInfo.getTabletId());
                    if (computeNodeId == null) {
                        throw new RuntimeException("Send replicate task failed, no compute node found for tablet: "
                                + tabletInfo.getTabletId());
                    }
                    ReplicateSnapshotTask task = new ReplicateSnapshotTask(computeNodeId, databaseId, tableId,
                            partitionInfo.getPartitionId(), indexInfo.getIndexId(),
                            tabletInfo.getTabletId(), getTabletType(tableType), transactionId,
                            indexInfo.getSchemaHash(), partitionInfo.getVersion(),
                            partitionInfo.getDataVersion(), tabletInfo.getSrcTabletId(),
                            getTabletType(srcTableType),
                            indexInfo.getSrcSchemaHash(), partitionInfo.getSrcVersion(), encryptionMeta,
                            fakedShardId, srcDatabaseId, srcTableId, partitionInfo.getSrcPartitionId());
                    LOG.info("Add replicateLakeRemoteStorageTask, tablet id: {}, txn id: {}, src partition info: {}/{}/{}",
                            tabletInfo.getTabletId(), transactionId, srcDatabaseId, srcTableId,
                            partitionInfo.getSrcPartitionId());
                    runningTasks.put(task, task);
                }
            }
        }
        taskNum = runningTasks.size();
        LOG.info("Send replicate lake remote storage tasks, task num: {}, database id: {}, table id: {}," +
                        " transaction id: {}", taskNum, databaseId, tableId, transactionId);
        sendRunningTasks();
    }

    @Override
    public long getReplicationReplicaCount() {
        return 1;
    }

}
