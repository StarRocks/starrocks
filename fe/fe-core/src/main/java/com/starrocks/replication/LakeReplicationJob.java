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
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.lake.StarOSAgent;
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

public class LakeReplicationJob extends ReplicationJob {
    private static final Logger LOG = LogManager.getLogger(LakeReplicationJob.class);

    @SerializedName(value = "virtualTabletId")
    private final long virtualTabletId;

    @SerializedName(value = "srcDatabaseId")
    protected long srcDatabaseId;

    @SerializedName(value = "srcTableId")
    protected long srcTableId;

    // Source table's FilePathInfo for calculating partition paths with partitioned prefix support
    private transient FilePathInfo srcTableFilePathInfo;

    @TestOnly
    public LakeReplicationJob(String jobId, long virtualTabletId, long srcDatabaseId, long srcTableId, long databaseId,
                              OlapTable table, OlapTable srcTable, SystemInfoService srcSystemInfoService) {
        super(jobId, null, databaseId, table, srcTable, srcSystemInfoService);
        this.srcDatabaseId = srcDatabaseId;
        this.srcTableId = srcTableId;
        this.virtualTabletId = virtualTabletId;
        this.srcTableFilePathInfo = null;
    }

    @TestOnly
    public LakeReplicationJob(String jobId, long virtualTabletId, long srcDatabaseId, long srcTableId, long databaseId,
                              OlapTable table, OlapTable srcTable, SystemInfoService srcSystemInfoService,
                              FilePathInfo srcTableFilePathInfo) {
        super(jobId, null, databaseId, table, srcTable, srcSystemInfoService);
        this.srcDatabaseId = srcDatabaseId;
        this.srcTableId = srcTableId;
        this.virtualTabletId = virtualTabletId;
        this.srcTableFilePathInfo = srcTableFilePathInfo;
    }

    public LakeReplicationJob(TTableReplicationRequest request) throws MetaNotFoundException {
        super(request);
        Preconditions.checkArgument(request.src_database_id > 0 && request.src_table_id > 0);
        this.srcDatabaseId = request.src_database_id;
        this.srcTableId = request.src_table_id;
        this.virtualTabletId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getOrCreateVirtualTabletId(request.src_storage_volume_name, request.src_service_id);

        // Get source table's FilePathInfo for partition path calculation
        // This FilePathInfo contains partitioned prefix configuration from storage volume
        try {
            StorageVolume srcStorageVolume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                    .getStorageVolumeByName(request.src_storage_volume_name);
            if (srcStorageVolume == null) {
                throw new MetaNotFoundException("Source storage volume not found: " + request.src_storage_volume_name);
            }
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            this.srcTableFilePathInfo = starOSAgent.allocateFilePathFromOtherService(
                    srcStorageVolume.getId(), srcDatabaseId, srcTableId, request.src_service_id);
            LOG.info("LakeReplicationJob created, src_storage_volume: {}, src_table_file_path: {}",
                    request.src_storage_volume_name, srcTableFilePathInfo.getFullPath());
        } catch (DdlException e) {
            throw new MetaNotFoundException("Failed to allocate file path for source table: " + e.getMessage());
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
            // Get S3 full path if applicable (only for S3 storage type)
            // For non-S3 storage types, BE will use RemoteStarletLocationProvider to construct the path
            String srcPartitionFullPath = getS3PartitionFullPath(partitionInfo.getSrcPartitionId());
            
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
                            srcPartitionFullPath);
                    runningTasks.put(task, task);
                }
            }
        }
        taskNum = runningTasks.size();
        LOG.info("Send lake replicate snapshot task, task num: {}, database id: {}, table id: {}, transaction id: {}",
                taskNum, super.getDatabaseId(), super.getTableId(), super.getTransactionId());
        sendRunningTasks();
    }

    /**
     * Get the full path of the source partition for S3 storage type only.
     * 
     * Design rationale:
     * - S3 storage type: FE provides the full S3 path (supports partitioned prefix feature).
     *   BE will use raw path mode (s3.use_raw_path_with_scheme=true) to access the files directly.
     * - Non-S3 storage types (OSS/Azure/HDFS/GFS): FE returns null.
     *   BE will use RemoteStarletLocationProvider to construct the relative path,
     *   and starlet will use normalize_path to build the final path.
     * 
     * The partitioned prefix feature is ONLY supported for S3 storage type.
     * 
     * @param physicalPartitionId the physical partition ID
     * @return the full S3 path if storage type is S3, null otherwise
     */
    private String getS3PartitionFullPath(long physicalPartitionId) {
        if (srcTableFilePathInfo == null || !srcTableFilePathInfo.hasFsInfo()) {
            return null;
        }
        
        // Only provide full path for S3 storage type
        if (!srcTableFilePathInfo.getFsInfo().hasS3FsInfo()) {
            // Non-S3 storage type (OSS/Azure/HDFS/GFS) - return null
            // BE will use RemoteStarletLocationProvider to construct the path
            LOG.debug("Non-S3 storage type, BE will use RemoteStarletLocationProvider for partition: {}",
                    physicalPartitionId);
            return null;
        }
        
        // S3 storage type - provide full path
        if (srcTableFilePathInfo.getFsInfo().getS3FsInfo().getPartitionedPrefixEnabled()) {
            // S3 with partitioned prefix enabled - use special path calculation
            FilePathInfo partitionPathInfo = StarOSAgent.allocatePartitionFilePathInfo(
                    srcTableFilePathInfo, physicalPartitionId);
            String fullPath = partitionPathInfo.getFullPath();
            LOG.debug("S3 with partitioned prefix enabled, partition full path: {} for partition: {}", 
                    fullPath, physicalPartitionId);
            return fullPath;
        }
        
        // S3 without partitioned prefix - simple concatenation
        String fullPath = srcTableFilePathInfo.getFullPath() + "/" + physicalPartitionId;
        LOG.debug("S3 without partitioned prefix, partition full path: {} for partition: {}",
                fullPath, physicalPartitionId);
        return fullPath;
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
