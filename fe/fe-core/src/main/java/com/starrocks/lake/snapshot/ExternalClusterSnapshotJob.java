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

package com.starrocks.lake.snapshot;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.ExternalClusterSnapshotTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TComputeNodeTablets;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ExternalClusterSnapshotJob implements external cluster snapshot including
 * both
 * metadata and data.
 */
public class ExternalClusterSnapshotJob extends ClusterSnapshotJob {
    public static final Logger LOG = LogManager.getLogger(ExternalClusterSnapshotJob.class);

    @SerializedName(value = "snapshotDiff")
    private ClusterSnapshotDiff snapshotDiff;

    @SerializedName(value = "cleaningCompleted")
    private boolean cleaningCompleted = true;

    private AgentBatchTask lakeSnapshotBatchTask = new AgentBatchTask();

    // for deserialization
    public ExternalClusterSnapshotJob() {
        super(0, "", "", 0);
    }

    private long feImageCreatedTimeMs = 0;

    public ExternalClusterSnapshotJob(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        super(id, snapshotName, storageVolumeName, createdTimeMs);
    }

    @Override
    protected ClusterSnapshot createClusterSnapshot(long id, String snapshotName, String storageVolumeName,
            long createdTimeMs) {
        return new ClusterSnapshot(id, snapshotName, ClusterSnapshot.ClusterSnapshotType.AUTO_FULL,
                storageVolumeName, createdTimeMs, -1, 0, 0);
    }

    @Override
    public boolean needClusterSnapshotInfo() {
        return true;
    }

    public AgentBatchTask getLakeSnapshotBatchTask() {
        if (lakeSnapshotBatchTask == null) {
            lakeSnapshotBatchTask = new AgentBatchTask();
        }
        return lakeSnapshotBatchTask;
    }

    @Override
    protected void runSnapshottingJob(SnapshotJobContext context) throws StarRocksException {
        LOG.debug("begin to snapshot cluster snapshot job. job: {}", getId());
        // Record the time when FE image was created
        feImageCreatedTimeMs = System.currentTimeMillis();
        ClusterSnapshotInfo newClusterSnapshotInfo = createImagesAndGetSnapshotInfo(context);
        setClusterSnapshotInfo(newClusterSnapshotInfo);

        ClusterSnapshotInfo preClusterSnapshotInfo = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                .getLastSuccFullSnapshotInfo();
        // Calculate diff between prev and new ClusterSnapshotInfo
        snapshotDiff = calculateClusterSnapshotDiff(preClusterSnapshotInfo, newClusterSnapshotInfo);
        LOG.info(
                "Cluster snapshot diff calculated. Added partitions: {}, Changed partitions: {}, Deleted partitions: {}",
                snapshotDiff.getAddedPartitions().size(), snapshotDiff.getChangedPartitions().size(),
                snapshotDiff.getDeletedPartitions().size());

        // create data snapshot tasks
        createUploadClusterSnapshotTasks();
        setState(ClusterSnapshotJobState.UPLOADING);
        logJob();
    }

    @Override
    protected void runUploadingJob(SnapshotJobContext context) throws StarRocksException {
        AgentBatchTask batchTask = getLakeSnapshotBatchTask();
        if (!batchTask.isFinished()) {
            LOG.info("data snapshot tasks not finished. job: {}", getId());
            List<AgentTask> tasks = batchTask.getUnfinishedTasks(2000);
            AgentTask task = tasks.stream().filter(t -> t.getFailedTimes() >= 3).findAny()
                    .orElse(null);
            if (task != null) {
                throw new StarRocksException("data snapshot task failed after try three times: " + task.getErrorMsg());
            } else if (System.currentTimeMillis()
                    - getCreatedTimeMs() > Config.automated_cluster_snapshot_timeout_seconds * 1000) {
                LOG.warn("data snapshot tasks not finished after timeout. job: {}, timeout: {}", getId(),
                        Config.automated_cluster_snapshot_timeout_seconds);
                throw new StarRocksException("data snapshot tasks not finished after timeout. job: " + getId() +
                        ", timeout: " + Config.automated_cluster_snapshot_timeout_seconds + " seconds");
            }
            return;
        }
        // check upload status
        List<AgentTask> allTasks = batchTask.getAllTasks();
        AgentTask failedTask = allTasks.stream().filter(t -> t.getFailedTimes() >= 3).findAny()
                .orElse(null);
        if (failedTask != null) {
            throw new StarRocksException(
                    "data snapshot task failed after try three times: " + failedTask.getErrorMsg());
        }
        LOG.info("Finish upload data file, begin to upload snapshot meta file. job: {}", getId());
        try {
            ClusterSnapshotUtils.uploadClusterSnapshotToRemote(this);
        } catch (StarRocksException e) {
            throw new StarRocksException("upload image failed, err msg: " + e.getMessage());
        }
        ClusterSnapshot snapshot = getSnapshot();
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                .setLastSuccFullSnapshotInfo(snapshot.getClusterSnapshotInfo());
        setState(ClusterSnapshotJobState.CLEANING);
        logJob();
        LOG.info("Finish upload snapshot meta file for Cluster Snapshot, job: {}", getId());
    }

    @Override
    protected void runCleaningJob(SnapshotJobContext context) throws StarRocksException {
        try {
            createDeleteClusterSnasphotTasks();
            cleaningCompleted = false;
            setState(ClusterSnapshotJobState.FINISHED);
            logJob();
            // Update metric with the FE image creation time when snapshot job finishes successfully
            if (feImageCreatedTimeMs > 0) {
                MetricRepo.GAUGE_EXTERNAL_LAST_SUCCESS_SNAPSHOT_TIME.setValue(feImageCreatedTimeMs);
            }
            // Update success counter for external snapshot job
            MetricRepo.COUNTER_EXTERNAL_SNAPSHOT_JOB_SUCCESS.increase(1L);
        } catch (StarRocksException e) {
            LOG.warn("failed to create delete cluster snapshot tasks when run finished job {}", getId(), e);
            setState(ClusterSnapshotJobState.ERROR);
            logJob();
            return;
        }
    }

    public boolean isCleaningCompleted() {
        return cleaningCompleted;
    }

    public void setCleaningCompleted(boolean cleaningCompleted) {
        this.cleaningCompleted = cleaningCompleted;
    }

    public ClusterSnapshotDiff getSnapshotDiff() {
        return snapshotDiff;
    }

    @Override
    public void replay() {
        switch (getState()) {
            case INITIALIZING:
            case SNAPSHOTING:
                break;
            case FINISHED:
                ClusterSnapshot snapshot = getSnapshot();
                if (snapshot != null && snapshot.getClusterSnapshotInfo() != null) {
                    GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                            .setLastSuccFullSnapshotInfo(snapshot.getClusterSnapshotInfo());
                    LOG.info("Restored lastSuccFullSnapshotInfo from FINISHED job {} during replay", getId());
                }
                break;
            case EXPIRED:
            case DELETED:
            case ERROR:
                break;
            case UPLOADING:
                replayUploadingJob();
                break;
            case CLEANING:
                replayCleaningJob();
                break;
            default:
                return;
        }
    }

    @Override
    public void finishSnapshotTask(ExternalClusterSnapshotTask task, TFinishTaskRequest request) {
        // TODO(zhangqiang)
        // handle failed tablets
        if (request.getTask_status().getStatus_code() == TStatusCode.OK) {
            task.setFinished(true);
        } else {
            task.failed();
            task.setFailed(true);
            String errorMsg = "Unknown error";
            if (request.getTask_status().isSetError_msgs() && !request.getTask_status().getError_msgs().isEmpty()) {
                errorMsg = request.getTask_status().getError_msgs().get(0);
            }
            task.setErrorMsg(errorMsg);
            LOG.warn("Cluster snapshot task failed, task: {}, error: {}", task, errorMsg);
        }
    }

    public void replayUploadingJob() {
        Preconditions.checkState(getState() == ClusterSnapshotJobState.UPLOADING, getState());
        LOG.info("begin to replay cluster snapshot job. job: {}", getId());
        // resend the cluster snapshot tasks
        if (snapshotDiff == null) {
            LOG.warn("snapshot diff is null when replay external snapshot job {}", getId());
            setState(ClusterSnapshotJobState.ERROR);
            return;
        }
        try {
            if (GlobalStateMgr.isCheckpointThread()
                    || !GlobalStateMgr.getCurrentState().isLeader()) {
                return;
            }
            createUploadClusterSnapshotTasks();
        } catch (StarRocksException e) {
            LOG.warn("failed to create cluster snapshot tasks when replay external snapshot job {}", getId(), e);
            setState(ClusterSnapshotJobState.ERROR);
            return;
        }
    }

    public void replayCleaningJob() {
        Preconditions.checkState(getState() == ClusterSnapshotJobState.CLEANING, getState());
        LOG.info("begin to replay cluster snapshot job. job: {}", getId());
        if (snapshotDiff == null) {
            LOG.warn("snapshot diff is null when replay external snapshot job {}", getId());
            setState(ClusterSnapshotJobState.FINISHED);
            return;
        }
        try {
            if (GlobalStateMgr.isCheckpointThread()
                    || !GlobalStateMgr.getCurrentState().isLeader()) {
                return;
            }
            runCleaningJob(null);
        } catch (StarRocksException e) {
            LOG.warn("failed to run cleaning job when replay external snapshot job {}", getId(), e);
            return;
        }
    }

    public long getVirtualTabletId() throws StarRocksException {
        String svName = getStorageVolumeName();
        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeByName(svName);
        if (sv == null) {
            throw new StarRocksException("storage volume not found: " + svName);
        }
        if (sv.getVTabletId() == -1L) {
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            FilePathInfo pathInfo = null;
            try {
                pathInfo = starOSAgent.allocateFilePath(sv.getId(), "");
            } catch (Exception e) {
                throw new StarRocksException("failed to allocate file path for storage volume: " + svName, e);
            }
            FileCacheInfo cacheInfo =
                    FileCacheInfo.newBuilder().setEnableCache(false).setTtlSeconds(-1).setAsyncWriteBack(false).build();

            long shardGroupId = starOSAgent.createShardGroupForVirtualTablet();
            Map<String, String> properties = new HashMap<>();
            // create a new id as tablet id
            long vTabletId = GlobalStateMgr.getCurrentState().getNextId();
            starOSAgent.createShardWithVirtualTabletId(pathInfo, cacheInfo, shardGroupId, properties, vTabletId,
                            getComputeResource());
            sv.setVTabletGroupId(shardGroupId);
            sv.setVTabletId(vTabletId);
            GlobalStateMgr.getCurrentState().getStorageVolumeMgr().
                            updateStorageVolumeVTabletMapping(svName, vTabletId, shardGroupId);
            return vTabletId;
        } else {
            return sv.getVTabletId();
        }
    }

    public ComputeResource getComputeResource() {
        String warehouseName = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getWarehouseName();
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        // if warehouse is not found, `getWarehouse` will throw exception
        Warehouse warehouse = warehouseManager.getWarehouse(warehouseName);
        return warehouseManager.acquireComputeResource(warehouse.getId());
    }

    private void createUploadClusterSnapshotTasks() throws StarRocksException {
        long vTabletId = getVirtualTabletId();
        lakeSnapshotBatchTask = new AgentBatchTask();
        for (PartitionVersionInfo partition : snapshotDiff.getAddedPartitions()) {
            // try to reuse the aggregator node id if possible
            long aggregatorNodeId = chooseAggregatorNodeId(partition.getAggregatorNodeId());
            if (aggregatorNodeId == 0) {
                throw new StarRocksException("failed to choose aggregator node for cluster snapshot task");
            }
            partition.setAggregatorNodeId(aggregatorNodeId);
            List<TComputeNodeTablets> computeNodeTablets = collectComputeNodeTablets(partition.getTabletIds());

            PartitionKey partitionKey = partition.getPartitionKey();
            ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(aggregatorNodeId, partitionKey.getDbId(),
                    partitionKey.getTableId(), partitionKey.getPartId(), partitionKey.getPhysicalPartId(), getId(), -1,
                    partition.getVersion(), partition.isFileBundling(), false, vTabletId,
                    GlobalStateMgr.getCurrentState().getNextId());
            task.setComputeNodeTablets(computeNodeTablets);
            lakeSnapshotBatchTask.addTask(task);
        }

        for (PartitionVersionChangeInfo partition : snapshotDiff.getChangedPartitions()) {
            // try to reuse the aggregator node id if possible
            long aggregatorNodeId = chooseAggregatorNodeId(partition.getCurrentPartitionInfo().getAggregatorNodeId());
            if (aggregatorNodeId == 0) {
                throw new StarRocksException("failed to choose aggregator node for cluster snapshot task");
            }
            partition.getCurrentPartitionInfo().setAggregatorNodeId(aggregatorNodeId);
            List<TComputeNodeTablets> computeNodeTablets = 
                            collectComputeNodeTablets(partition.getCurrentPartitionInfo().getTabletIds());

            PartitionKey partitionKey = partition.getCurrentPartitionInfo().getPartitionKey();
            ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(aggregatorNodeId, partitionKey.getDbId(),
                    partitionKey.getTableId(), partitionKey.getPartId(), partitionKey.getPhysicalPartId(),
                    getId(), partition.getPrevVersion(), partition.getCurrentPartitionInfo().getVersion(), 
                    partition.getCurrentPartitionInfo().isFileBundling(), false, vTabletId,
                    GlobalStateMgr.getCurrentState().getNextId());
            task.setComputeNodeTablets(computeNodeTablets);
            lakeSnapshotBatchTask.addTask(task);
        }

        AgentTaskQueue.addBatchTask(lakeSnapshotBatchTask);
        AgentTaskExecutor.submit(lakeSnapshotBatchTask);
        LOG.debug("Finish create cluster snapshot tasks. job: {}, vTabletId: {}, task count: {}", getId(), vTabletId,
                 lakeSnapshotBatchTask.getAllTasks().size());
    }

    void createDeleteClusterSnasphotTasks() throws StarRocksException {
        long vTabletId = getVirtualTabletId();
        lakeSnapshotBatchTask = new AgentBatchTask();
        for (PartitionVersionInfo partition : snapshotDiff.getDeletedPartitions()) {
            long aggregatorNodeId = chooseAggregatorNodeId(partition.getAggregatorNodeId());
            if (aggregatorNodeId == 0) {
                throw new StarRocksException("failed to choose aggregator node for cluster snapshot task");
            }
            partition.setAggregatorNodeId(aggregatorNodeId);
            PartitionKey partitionKey = partition.getPartitionKey();
            ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(aggregatorNodeId, partitionKey.getDbId(),
                    partitionKey.getTableId(), partitionKey.getPartId(), partitionKey.getPhysicalPartId(), getId(),
                    -1, -1, true, true, vTabletId, GlobalStateMgr.getCurrentState().getNextId());
            lakeSnapshotBatchTask.addTask(task);
        }

        for (PartitionVersionChangeInfo partition : snapshotDiff.getChangedPartitions()) {
            long aggregatorNodeId = chooseAggregatorNodeId(partition.getCurrentPartitionInfo().getAggregatorNodeId());
            if (aggregatorNodeId == 0) {
                throw new StarRocksException("failed to choose aggregator node for cluster snapshot task");
            }
            partition.getCurrentPartitionInfo().setAggregatorNodeId(aggregatorNodeId);
            PartitionKey partitionKey = partition.getCurrentPartitionInfo().getPartitionKey();
            ExternalClusterSnapshotTask task = new ExternalClusterSnapshotTask(aggregatorNodeId, partitionKey.getDbId(),
                    partitionKey.getTableId(), partitionKey.getPartId(), partitionKey.getPhysicalPartId(), getId(),
                    -1, -1, partition.isPreviousFileBundling(), false, vTabletId, GlobalStateMgr.getCurrentState().getNextId());
            List<TComputeNodeTablets> computeNodeTablets =
                            collectComputeNodeTablets(partition.getCurrentPartitionInfo().getTabletIds());
            task.setComputeNodeTablets(computeNodeTablets);
            lakeSnapshotBatchTask.addTask(task);
        }

        AgentTaskQueue.addBatchTask(lakeSnapshotBatchTask);
        AgentTaskExecutor.submit(lakeSnapshotBatchTask);
        LOG.debug("Finish create delete cluster snapshot tasks. job: {}, vTabletId: {}, task count: {}", getId(), vTabletId,
                  lakeSnapshotBatchTask.getAllTasks().size());
    }

    private long chooseAggregatorNodeId(long preAggregatorNodeId) {
        if (preAggregatorNodeId != 0) {
            if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .checkComputeNodeAlive(preAggregatorNodeId)) {
                return preAggregatorNodeId;
            }
        }
        ComputeNode computeNode = LakeAggregator.chooseAggregatorNode(getComputeResource(), null);
        if (computeNode == null) {
            return 0;
        }
        return computeNode.getId();
    }

    private List<TComputeNodeTablets> collectComputeNodeTablets(List<Long> tabletIds) throws StarRocksException {
        Map<ComputeNode, List<Long>> nodeToTablets = Maps.newHashMap();

        for (Long tabletId : tabletIds) {
            ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getComputeNodeAssignedToTablet(getComputeResource(), tabletId);
            if (computeNode == null) {
                throw new StarRocksException("failed to get compute node for tablet " + tabletId);
            }

            List<Long> tabletsOnNode = nodeToTablets.get(computeNode);
            if (tabletsOnNode == null) {
                tabletsOnNode = Lists.newArrayList();
                nodeToTablets.put(computeNode, tabletsOnNode);
            }
            tabletsOnNode.add(tabletId);
        }

        List<TComputeNodeTablets> computeNodeTablets = Lists.newArrayList();
        for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
            ComputeNode computeNode = entry.getKey();
            TBackend backend = new TBackend(computeNode.getHost(), computeNode.getBrpcPort(),
                    computeNode.getHttpPort());
            TComputeNodeTablets singleCNTablets = new TComputeNodeTablets();
            singleCNTablets.setCompute_node(backend);
            singleCNTablets.setTablets(entry.getValue());
            computeNodeTablets.add(singleCNTablets);
        }
        return computeNodeTablets;
    }

    /**
     * Calculate the diff between prev and new ClusterSnapshotInfo
     * 
     * @param prevClusterSnapshotInfo previous ClusterSnapshotInfo
     * @param newClusterSnapshotInfo  new ClusterSnapshotInfo
     * @return ClusterSnapshotDiff containing added, changed, and deleted partitions
     */
    private ClusterSnapshotDiff calculateClusterSnapshotDiff(
            ClusterSnapshotInfo prevClusterSnapshotInfo,
            ClusterSnapshotInfo newClusterSnapshotInfo) {
        ClusterSnapshotDiff diff = new ClusterSnapshotDiff();

        // Handle null cases
        if (prevClusterSnapshotInfo == null || prevClusterSnapshotInfo.isEmpty()) {
            // All partitions in new are added
            if (newClusterSnapshotInfo != null && !newClusterSnapshotInfo.isEmpty()) {
                collectAllPartitions(newClusterSnapshotInfo, diff.getAddedPartitions());
            }
            return diff;
        }
        Preconditions.checkNotNull(newClusterSnapshotInfo, "newClusterSnapshotInfo is null");
        // Build maps for quick lookup: key is (dbId, tableId, partId, physicalPartId)
        Map<PartitionKey, PartitionVersionInfo> prevPartitionVersions = Maps.newHashMap();
        Map<PartitionKey, PartitionVersionInfo> newPartitionVersions = Maps.newHashMap();

        collectPartitionVersions(prevClusterSnapshotInfo, prevPartitionVersions);
        collectPartitionVersions(newClusterSnapshotInfo, newPartitionVersions);

        for (Map.Entry<PartitionKey, PartitionVersionInfo> entry : newPartitionVersions.entrySet()) {
            PartitionKey key = entry.getKey();
            // Find added partitions (in new but not in prev)
            if (!prevPartitionVersions.containsKey(key)) {
                diff.getAddedPartitions().add(entry.getValue());
                continue;
            }
            // Find changed partitions (in both but version changed)
            PartitionVersionInfo prevVersionInfo = prevPartitionVersions.get(key);
            Long prevVersion = prevVersionInfo.getVersion();
            boolean isPreviousFileBundling = prevVersionInfo.isFileBundling();
            Long newVersion = entry.getValue().getVersion();
            if (prevVersion != null && !prevVersion.equals(newVersion)) {
                diff.getChangedPartitions().add(
                    new PartitionVersionChangeInfo(prevVersion, isPreviousFileBundling, entry.getValue()));
            }

        }

        // Find deleted partitions (in prev but not in new)
        for (Map.Entry<PartitionKey, PartitionVersionInfo> entry : prevPartitionVersions.entrySet()) {
            PartitionKey key = entry.getKey();
            if (!newPartitionVersions.containsKey(key)) {
                diff.getDeletedPartitions().add(entry.getValue());
            }
        }

        return diff;
    }

    /**
     * Collect all partition versions from ClusterSnapshotInfo
     */
    private void collectPartitionVersions(ClusterSnapshotInfo clusterSnapshotInfo,
            Map<PartitionKey, PartitionVersionInfo> partitionInfos) {
        try {
            Map<Long, DatabaseSnapshotInfo> dbInfos = clusterSnapshotInfo.getDbInfos();

            if (dbInfos == null) {
                return;
            }

            for (Map.Entry<Long, DatabaseSnapshotInfo> dbEntry : dbInfos.entrySet()) {
                long dbId = dbEntry.getKey();
                DatabaseSnapshotInfo dbInfo = dbEntry.getValue();
                if (dbInfo == null || dbInfo.tableInfos == null) {
                    continue;
                }

                for (Map.Entry<Long, TableSnapshotInfo> tableEntry : dbInfo.tableInfos.entrySet()) {
                    long tableId = tableEntry.getKey();
                    TableSnapshotInfo tableInfo = tableEntry.getValue();
                    if (tableInfo == null || tableInfo.partInfos == null) {
                        continue;
                    }
                    boolean isFileBundling = tableInfo.isFileBundling;
                    for (Map.Entry<Long, PartitionSnapshotInfo> partEntry : tableInfo.partInfos.entrySet()) {
                        long partId = partEntry.getKey();
                        PartitionSnapshotInfo partInfo = partEntry.getValue();
                        if (partInfo == null || partInfo.physicalPartInfos == null) {
                            continue;
                        }
                        for (Map.Entry<Long, PhysicalPartitionSnapshotInfo> physicalPartEntry : partInfo.physicalPartInfos
                                .entrySet()) {
                            long physicalPartId = physicalPartEntry.getKey();
                            PhysicalPartitionSnapshotInfo physicalPartInfo = physicalPartEntry.getValue();
                            if (physicalPartInfo == null || physicalPartInfo.indexInfos == null) {
                                continue;
                            }
                            long metadataSwitchVersion = physicalPartInfo.metadataSwitchVersion;
                            List<Long> tabletIds = Lists.newArrayList();
                            for (Map.Entry<Long, MaterializedIndexSnapshotInfo> indexEntry : physicalPartInfo.indexInfos
                                    .entrySet()) {
                                long indexId = indexEntry.getKey();
                                MaterializedIndexSnapshotInfo indexInfo = indexEntry.getValue();
                                if (indexInfo == null || indexInfo.tabletIds == null) {
                                    continue;
                                }
                                tabletIds.addAll(indexInfo.tabletIds);
                            }
                            long version = physicalPartInfo.visibleVersion;
                            boolean isFileBundlingVersion = isFileBundling;
                            if (metadataSwitchVersion != 0 && version < metadataSwitchVersion) {
                                isFileBundlingVersion = !isFileBundlingVersion;
                            }

                            PartitionKey key = new PartitionKey(dbId, tableId, partId, physicalPartId);
                            partitionInfos.put(key,
                                new PartitionVersionInfo(key, physicalPartInfo.visibleVersion, isFileBundlingVersion, 
                                    tabletIds));
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to collect partition versions", e);
        }
    }

    /**
     * Collect all partitions for added/deleted list
     */
    private void collectAllPartitions(ClusterSnapshotInfo clusterSnapshotInfo,
            List<PartitionVersionInfo> partitionList) {
        Map<PartitionKey, PartitionVersionInfo> partitionVersions = Maps.newHashMap();
        collectPartitionVersions(clusterSnapshotInfo, partitionVersions);
        for (Map.Entry<PartitionKey, PartitionVersionInfo> entry : partitionVersions.entrySet()) {
            partitionList.add(entry.getValue());
        }
    }

    /**
     * Key to identify a partition: (dbId, tableId, partId, physicalPartId)
     */
    private static class PartitionKey {
        @SerializedName(value = "dbId")
        private final long dbId;
        @SerializedName(value = "tableId")
        private final long tableId;
        @SerializedName(value = "partId")
        private final long partId;
        @SerializedName(value = "physicalPartId")
        private final long physicalPartId;

        public PartitionKey(long dbId, long tableId, long partId, long physicalPartId) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partId = partId;
            this.physicalPartId = physicalPartId;
        }

        long getDbId() {
            return dbId;
        }

        long getTableId() {
            return tableId;
        }

        long getPartId() {
            return partId;
        }

        long getPhysicalPartId() {
            return physicalPartId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionKey that = (PartitionKey) o;
            return dbId == that.dbId &&
                    tableId == that.tableId &&
                    partId == that.partId &&
                    physicalPartId == that.physicalPartId;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(dbId) * 31 * 31 * 31 +
                    Long.hashCode(tableId) * 31 * 31 +
                    Long.hashCode(partId) * 31 +
                    Long.hashCode(physicalPartId);
        }

        @Override
        public String toString() {
            return String.format("PartitionKey(dbId=%d, tableId=%d, partId=%d, physicalPartId=%d)",
                    dbId, tableId, partId, physicalPartId);
        }
    }

    /**
     * Information about a partition with its version
     */
    private static class PartitionVersionInfo {
        @SerializedName(value = "partitionKey")
        private final PartitionKey partitionKey;
        @SerializedName(value = "version")
        private final long version;
        @SerializedName(value = "isFileBundling")
        private final boolean isFileBundling;
        @SerializedName(value = "tabletIds")
        private final List<Long> tabletIds;
        @SerializedName(value = "aggregatorNodeId")
        private long aggregatorNodeId = 0;


        public PartitionVersionInfo(PartitionKey partitionKey, long version, boolean isFileBundling, 
                                    List<Long> tabletIds) {
            this.partitionKey = partitionKey;
            this.version = version;
            this.isFileBundling = isFileBundling;
            this.tabletIds = tabletIds;
            this.aggregatorNodeId = 0;
        }

        public PartitionKey getPartitionKey() {
            return partitionKey;
        }

        public long getVersion() {
            return version;
        }

        public boolean isFileBundling() {
            return isFileBundling;
        }

        public List<Long> getTabletIds() {
            return tabletIds;
        }

        public long getAggregatorNodeId() {
            return aggregatorNodeId;
        }

        public void setAggregatorNodeId(long aggregatorNodeId) {
            this.aggregatorNodeId = aggregatorNodeId;
        }
    }

    /**
     * Information about a partition with version change
     */
    private static class PartitionVersionChangeInfo {
        private final PartitionVersionInfo currentPartitionInfo;
        private long prevVersion;
        private boolean isPreviousFileBundling;

        public PartitionVersionChangeInfo(long prevVersion, boolean isPreviousFileBundling, 
                                          PartitionVersionInfo currentPartitionInfo) {
            this.prevVersion = prevVersion;
            this.isPreviousFileBundling = isPreviousFileBundling;
            this.currentPartitionInfo = currentPartitionInfo;
        }

        public PartitionVersionInfo getCurrentPartitionInfo() {
            return currentPartitionInfo;
        }

        public long getPrevVersion() {
            return prevVersion;
        }

        public boolean isPreviousFileBundling() {
            return isPreviousFileBundling;
        }
    }

    /**
     * Inner class to store cluster snapshot diff results
     */
    private static class ClusterSnapshotDiff {
        @SerializedName(value = "addedPartitions")
        private final List<PartitionVersionInfo> addedPartitions = Lists.newArrayList();
        @SerializedName(value = "changedPartitions")
        private final List<PartitionVersionChangeInfo> changedPartitions = Lists.newArrayList();
        @SerializedName(value = "deletedPartitions")
        private final List<PartitionVersionInfo> deletedPartitions = Lists.newArrayList();

        public List<PartitionVersionInfo> getAddedPartitions() {
            return addedPartitions;
        }

        public List<PartitionVersionChangeInfo> getChangedPartitions() {
            return changedPartitions;
        }

        public List<PartitionVersionInfo> getDeletedPartitions() {
            return deletedPartitions;
        }
    }
}
