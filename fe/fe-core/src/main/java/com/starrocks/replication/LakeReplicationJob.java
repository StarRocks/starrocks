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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.ReplicateSnapshotTask;
import com.starrocks.thrift.TTableReplicationRequest;
import org.apache.arrow.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;

import java.util.Map;

public class LakeReplicationJob extends ReplicationJob {
    private static final Logger LOG = LogManager.getLogger(LakeReplicationJob.class);

    @SerializedName(value = "virtualTabletId")
    private final long virtualTabletId;

    @SerializedName(value = "srcDatabaseId")
    protected long srcDatabaseId;

    @SerializedName(value = "srcTableId")
    protected long srcTableId;

    // Partitioned prefix configuration for source cluster storage volume
    @SerializedName(value = "srcEnablePartitionedPrefix")
    private final boolean srcEnablePartitionedPrefix;

    @SerializedName(value = "srcNumPartitionedPrefix")
    private final int srcNumPartitionedPrefix;

    @TestOnly
    public LakeReplicationJob(String jobId, long virtualTabletId, long srcDatabaseId, long srcTableId, long databaseId,
                              OlapTable table, OlapTable srcTable, SystemInfoService srcSystemInfoService) {
        super(jobId, null, databaseId, table, srcTable, srcSystemInfoService);
        this.srcDatabaseId = srcDatabaseId;
        this.srcTableId = srcTableId;
        this.virtualTabletId = virtualTabletId;
        this.srcEnablePartitionedPrefix = false;
        this.srcNumPartitionedPrefix = 0;
    }

    @TestOnly
    public LakeReplicationJob(String jobId, long virtualTabletId, long srcDatabaseId, long srcTableId, long databaseId,
                              OlapTable table, OlapTable srcTable, SystemInfoService srcSystemInfoService,
                              boolean srcEnablePartitionedPrefix, int srcNumPartitionedPrefix) {
        super(jobId, null, databaseId, table, srcTable, srcSystemInfoService);
        this.srcDatabaseId = srcDatabaseId;
        this.srcTableId = srcTableId;
        this.virtualTabletId = virtualTabletId;
        this.srcEnablePartitionedPrefix = srcEnablePartitionedPrefix;
        this.srcNumPartitionedPrefix = srcNumPartitionedPrefix;
    }

    public LakeReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        super(request);
        Preconditions.checkArgument(request.src_database_id > 0 && request.src_table_id > 0);
        this.srcDatabaseId = request.src_database_id;
        this.srcTableId = request.src_table_id;
        this.virtualTabletId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getOrCreateVirtualTabletId(request.src_storage_volume_name, request.src_service_id);

        // Get partitioned prefix configuration from source cluster storage volume
        boolean enablePartitionedPrefix = false;
        int numPartitionedPrefix = 0;
        StorageVolume srcStorageVolume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeByName(request.src_storage_volume_name);
        if (srcStorageVolume != null) {
            Map<String, String> props = srcStorageVolume.getProperties();
            enablePartitionedPrefix = Boolean.parseBoolean(
                    props.getOrDefault(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "false"));
            if (enablePartitionedPrefix) {
                numPartitionedPrefix = Integer.parseInt(
                        props.getOrDefault(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "256"));
            }
        }
        this.srcEnablePartitionedPrefix = enablePartitionedPrefix;
        this.srcNumPartitionedPrefix = numPartitionedPrefix;

        LOG.info("LakeReplicationJob created, src_storage_volume: {}, enable_partitioned_prefix: {}, " +
                        "num_partitioned_prefix: {}", request.src_storage_volume_name, enablePartitionedPrefix,
                numPartitionedPrefix);
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
            LOG.warn("Lake replication job exception, state: {}, database id: {}, table id: {}, transaction id: {}",
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
                            virtualTabletId, srcDatabaseId, srcTableId, partitionInfo.getSrcPartitionId(),
                            srcEnablePartitionedPrefix, srcNumPartitionedPrefix);
                    runningTasks.put(task, task);
                }
            }
        }
        taskNum = runningTasks.size();
        LOG.info("Send lake replicate snapshot task, task num: {}, database id: {}, table id: {}, transaction id: {}, " +
                        "enable_partitioned_prefix: {}, num_partitioned_prefix: {}",
                taskNum, super.getDatabaseId(), super.getTableId(), super.getTransactionId(),
                srcEnablePartitionedPrefix, srcNumPartitionedPrefix);
        sendRunningTasks();
    }

    @Override
    public long getReplicationReplicaCount() {
        long count = 0;
        for (PartitionInfo partitionInfo : getPartitionInfos().values()) {
            for (IndexInfo indexInfo : partitionInfo.getIndexInfos().values()) {
                count += indexInfo.getTabletInfos().size();
            }
        }
        return count;
    }
}
