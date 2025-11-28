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
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.leader.CheckpointController;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.ComputeNode;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.ClusterSnapshotTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FullClusterSnapshotJob extends ClusterSnapshotJob {
    public static final Logger LOG = LogManager.getLogger(FullClusterSnapshotJob.class);

    @SerializedName(value = "snapshotDiff")
    protected ClusterSnapshotDiff snapshotDiff;

    private AgentBatchTask lakeSnapshotBatchTask = new AgentBatchTask();

    public FullClusterSnapshotJob(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        super(id, snapshotName, storageVolumeName, createdTimeMs);
    }

    public AgentBatchTask getLakeSnapshotBatchTask() {
        return lakeSnapshotBatchTask;
    }

    @Override
    protected void runInitializingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.INITIALIZING, state);
        LOG.info("begin to initialize cluster snapshot job. job: {}", getId());

        Pair<Long, Long> consistentIds = context.captureConsistentCheckpointIdBetweenFEAndStarMgr();
        if (consistentIds == null) {
            throw new StarRocksException("failed to capture consistent journal id for checkpoint");
        }
        setJournalIds(consistentIds.first, consistentIds.second);
        LOG.info(
                "Successful capture consistent journal id, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                consistentIds.first, consistentIds.second);

        setState(ClusterSnapshotJobState.SNAPSHOTING);
        logJob();
    }

    protected void runSnapshottingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.SNAPSHOTING, state);
        LOG.info("begin to snapshot cluster snapshot job. job: {}", getId());

        CheckpointController feController = context.getFeController();
        CheckpointController starMgrController = context.getStarMgrController();
        // back prev ClusterSnapshotInfo
        ClusterSnapshotInfo preClusterSnapshotInfo = feController.getClusterSnapshotInfo();
        long feCheckpointJournalId = getFeJournalId();
        long starMgrCheckpointJournalId = getStarMgrJournalId();

        long feImageJournalId = feController.getImageJournalId();
        if (feImageJournalId < feCheckpointJournalId) {
            Pair<Boolean, String> createFEImageRet = feController.runCheckpointControllerWithIds(feImageJournalId,
                    feCheckpointJournalId, true);
            if (!createFEImageRet.first) {
                throw new StarRocksException("checkpoint failed for FE image: " + createFEImageRet.second);
            }
        } else if (feImageJournalId > feCheckpointJournalId) {
            throw new StarRocksException("checkpoint journal id for FE is smaller than image version");
        }
        LOG.info("Finished create image for FE image, version: {}", feCheckpointJournalId);

        long starMgrImageJournalId = starMgrController.getImageJournalId();
        if (starMgrImageJournalId < starMgrCheckpointJournalId) {
            Pair<Boolean, String> createStarMgrImageRet = starMgrController
                    .runCheckpointControllerWithIds(starMgrImageJournalId, starMgrCheckpointJournalId, false);
            if (!createStarMgrImageRet.first) {
                throw new StarRocksException("checkpoint failed for starMgr image: " + createStarMgrImageRet.second);
            }
        } else if (starMgrImageJournalId > starMgrCheckpointJournalId) {
            throw new StarRocksException("checkpoint journal id for starMgr is smaller than image version");
        }
        ClusterSnapshotInfo newClusterSnapshotInfo = feController.getClusterSnapshotInfo();
        setClusterSnapshotInfo(newClusterSnapshotInfo);

        // Calculate diff between prev and new ClusterSnapshotInfo
        snapshotDiff = calculateClusterSnapshotDiff(preClusterSnapshotInfo, newClusterSnapshotInfo);

        // Log the diff results
        LOG.info(
                "Cluster snapshot diff calculated. Added partitions: {}, Changed partitions: {}, Deleted partitions: {}",
                snapshotDiff.getAddedPartitions().size(), snapshotDiff.getChangedPartitions().size(),
                snapshotDiff.getDeletedPartitions().size());
        
        // create data snapshot tasks
        createClusterSnapshotTasks();
        setState(ClusterSnapshotJobState.UPLOADING);
        logJob();
        LOG.info("Finished create image for starMgr image, version: {}", starMgrCheckpointJournalId);
    }

    @Override
    protected void runUploadingJob(SnapshotJobContext context) throws StarRocksException {
        //TODO(zhangqiang): remove timeout job
        if (!lakeSnapshotBatchTask.isFinished()) {
            LOG.info("data snapshot tasks not finished. job: {}", getId());
            List<AgentTask> tasks = lakeSnapshotBatchTask.getUnfinishedTasks(2000);
            AgentTask task = tasks.stream().filter(t -> (t.isFailed() || t.getFailedTimes() >= 3)).findAny().orElse(null);
            if (task != null) {
                throw new StarRocksException("data snapshot task failed after try three times: " + task.getErrorMsg());
            } else {
                return;
            }
        }
        // check upload status
        List<AgentTask> allTasks = lakeSnapshotBatchTask.getAllTasks();
        AgentTask failedTask = allTasks.stream().filter(t -> (t.isFailed() || t.getFailedTimes() >= 3)).findAny()
                .orElse(null);
        if (failedTask != null) {
            setState(ClusterSnapshotJobState.ERROR);
            throw new StarRocksException("data snapshot task failed after try three times: " + failedTask.getErrorMsg());
        }
        LOG.info("Finish upload data file, begin to upload snapshot meta file. job: {}", getId());
        try {
            ClusterSnapshotUtils.uploadClusterSnapshotToRemote(this);
        } catch (StarRocksException e) {
            throw new StarRocksException("upload image failed, err msg: " + e.getMessage());
        }
        setState(ClusterSnapshotJobState.FINISHED);
        logJob();
        LOG.info("Finish upload snapshot meta file for Cluster Snapshot, job: {}", getId());
    }

    @Override
    protected void runFinishedJob() throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.FINISHED, state);
        LOG.info("begin to finished cluster snapshot job. job: {}", getId());

        //TODO(zhangqiang)
        // send delete expire partition tasks
    }

    @Override
    public void finishSnapshotTask(ClusterSnapshotTask task, TFinishTaskRequest request) {
        // TODO(zhangqiang)
        // handle failed tablets
        if (request.getTask_status().getStatus_code() == TStatusCode.OK) {
            task.setFinished(true);
        } else {
            task.setFailed(true);
            task.setErrorMsg(request.getTask_status().getError_msgs().get(0));
            LOG.warn("Cluster snapshot task failed, task: {}, error: {}", task, task.getErrorMsg());
        }
    }

    private long getVirtualTabletId() throws StarRocksException {
        String svName = getStorageVolumeName();
        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeByName(svName);
        if (sv == null) {
            throw new StarRocksException("storage volume not found: " + svName);
        }
        if (sv.getVTabletId() == -1) {
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            FilePathInfo pathInfo = null;
            try {
                pathInfo = starOSAgent.allocateFilePath(sv.getId());
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
                            WarehouseManager.DEFAULT_RESOURCE);
            sv.setVTabletGroupId(shardGroupId);
            sv.setVTabletId(vTabletId);
            return vTabletId;
        } else {
            return sv.getVTabletId();
        }
    }

    private void createClusterSnapshotTasks() throws StarRocksException {
        long vTabletId = getVirtualTabletId();
        for (PartitionVersionInfo partition : snapshotDiff.getAddedPartitions()) {
            Map<TBackend, List<Long>> nodeToTablets = Maps.newHashMap();
            long aggregatorNodeId = chooseAggregatorNodeId();
            if (aggregatorNodeId == 0) {
                throw new StarRocksException("failed to choose aggregator node for cluster snapshot task");
            }
            collectNodeToTablets(partition.getTabletIds(), nodeToTablets);
            
            LOG.info("collect node to tablets for added partition, aggregatorNodeId: {}", aggregatorNodeId); 
            PartitionKey partitionKey = partition.getPartitionKey();
            ClusterSnapshotTask task = new ClusterSnapshotTask(aggregatorNodeId, partitionKey.getDbId(), 
                    partitionKey.getTableId(), partitionKey.getPartId(), partitionKey.getPhysicalPartId(), getId(), -1, 
                    partition.getVersion(), vTabletId);
            task.setNodeToTablets(nodeToTablets);
            lakeSnapshotBatchTask.addTask(task);
        }

        for (PartitionVersionChangeInfo partition : snapshotDiff.getChangedPartitions()) {
            Map<TBackend, List<Long>> nodeToTablets = Maps.newHashMap();
            long aggregatorNodeId = chooseAggregatorNodeId();
            if (aggregatorNodeId == 0) {
                throw new StarRocksException("failed to choose aggregator node for cluster snapshot task");
            }
            collectNodeToTablets(partition.getCurrentPartitionInfo().getTabletIds(), nodeToTablets);

            LOG.info("collect node to tablets for changed partition, aggregatorNodeId: {}", aggregatorNodeId); 
            PartitionKey partitionKey = partition.getCurrentPartitionInfo().getPartitionKey();
            ClusterSnapshotTask task = new ClusterSnapshotTask(aggregatorNodeId, partitionKey.getDbId(), 
                    partitionKey.getTableId(), partitionKey.getPartId(), partitionKey.getPhysicalPartId(),
                    getId(), partition.getPrevVersion(), partition.getCurrentPartitionInfo().getVersion(), vTabletId);
            task.setNodeToTablets(nodeToTablets);
            lakeSnapshotBatchTask.addTask(task);
        }

        AgentTaskQueue.addBatchTask(lakeSnapshotBatchTask);
        AgentTaskExecutor.submit(lakeSnapshotBatchTask);
        LOG.info("Finish create cluster snapshot tasks. job: {}, vTabletId: {}, task count: {}", getId(), vTabletId, 
                lakeSnapshotBatchTask.getAllTasks().size());
    }

    private long chooseAggregatorNodeId() {
        LakeAggregator lakeAggregator = new LakeAggregator();
        ComputeNode computeNode = lakeAggregator.chooseAggregatorNode(WarehouseManager.DEFAULT_RESOURCE);
        if (computeNode == null) {
            return 0;
        }
        return computeNode.getId();
    }

    private void collectNodeToTablets(List<Long> tabletIds, Map<TBackend, List<Long>> nodeToTablets) {
        for (Long tabletId : tabletIds) {
            ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getComputeNodeAssignedToTablet(WarehouseManager.DEFAULT_RESOURCE, tabletId);
            TBackend backend = new TBackend(computeNode.getHost(), computeNode.getBrpcPort(), computeNode.getHttpPort());
            nodeToTablets.computeIfAbsent(backend, k -> Lists.newArrayList()).add(tabletId);
        }
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
            LOG.info("prevClusterSnapshotInfo is null or empty");
            // All partitions in new are added
            if (newClusterSnapshotInfo != null && !newClusterSnapshotInfo.isEmpty()) {
                collectAllPartitions(newClusterSnapshotInfo, diff.getAddedPartitions());
            }
            return diff;
        }

        // Build maps for quick lookup: key is (dbId, tableId, partId, physicalPartId)
        Map<PartitionKey, PartitionVersionInfo> prevPartitionVersions = Maps.newHashMap();
        Map<PartitionKey, PartitionVersionInfo> newPartitionVersions = Maps.newHashMap();

        collectPartitionVersions(prevClusterSnapshotInfo, prevPartitionVersions);
        collectPartitionVersions(newClusterSnapshotInfo, newPartitionVersions);

        // Find added partitions (in new but not in prev)
        for (Map.Entry<PartitionKey, PartitionVersionInfo> entry : newPartitionVersions.entrySet()) {
            PartitionKey key = entry.getKey();
            Long newVersion = entry.getValue().getVersion();
            List<Long> tabletIds = entry.getValue().getTabletIds();
            if (!prevPartitionVersions.containsKey(key)) {
                diff.getAddedPartitions().add(new PartitionVersionInfo(key, newVersion, tabletIds));
            }
        }

        // Find changed partitions (in both but version changed)
        for (Map.Entry<PartitionKey, PartitionVersionInfo> entry : newPartitionVersions.entrySet()) {
            if (prevPartitionVersions.containsKey(entry.getKey())) {
                PartitionVersionInfo prevVersionInfo = prevPartitionVersions.get(entry.getKey());
                Long prevVersion = prevVersionInfo.getVersion();
                Long newVersion = entry.getValue().getVersion();
                List<Long> tabletIds = entry.getValue().getTabletIds();
                if (prevVersion != null && !prevVersion.equals(newVersion)) {
                    diff.getChangedPartitions().add(new PartitionVersionChangeInfo(prevVersion, 
                                    new PartitionVersionInfo(entry.getKey(), newVersion, tabletIds)));
                }
            }
        }

        // Find deleted partitions (in prev but not in new)
        for (Map.Entry<PartitionKey, PartitionVersionInfo> entry : prevPartitionVersions.entrySet()) {
            PartitionKey key = entry.getKey();
            if (!newPartitionVersions.containsKey(key)) {
                diff.getDeletedPartitions().add(new PartitionVersionInfo(key, -1, null));
            }
        }

        return diff;
    }

    /**
     * Collect all partition versions from ClusterSnapshotInfo
     */
    private void collectPartitionVersions(ClusterSnapshotInfo clusterSnapshotInfo,
            Map<PartitionKey, PartitionVersionInfo> partitionInfos) {
        // Use reflection to access private dbInfos field
        try {
            java.lang.reflect.Field dbInfosField = ClusterSnapshotInfo.class.getDeclaredField("dbInfos");
            dbInfosField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Long, DatabaseSnapshotInfo> dbInfos = (Map<Long, DatabaseSnapshotInfo>) dbInfosField
                    .get(clusterSnapshotInfo);

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

                    for (Map.Entry<Long, PartitionSnapshotInfo> partEntry : tableInfo.partInfos.entrySet()) {
                        long partId = partEntry.getKey();
                        PartitionSnapshotInfo partInfo = partEntry.getValue();
                        if (partInfo == null || partInfo.physicalPartInfos == null) {
                            continue;
                        }

                        for (Map.Entry<Long, PhysicalPartitionSnapshotInfo> physicalPartEntry : 
                                partInfo.physicalPartInfos.entrySet()) {
                            long physicalPartId = physicalPartEntry.getKey();
                            PhysicalPartitionSnapshotInfo physicalPartInfo = physicalPartEntry.getValue();
                            if (physicalPartInfo == null) {
                                continue;
                            }
                            List<Long> tabletIds = Lists.newArrayList();
                            for (Map.Entry<Long, MaterializedIndexSnapshotInfo> indexEntry : 
                                    physicalPartInfo.indexInfos.entrySet()) {
                                long indexId = indexEntry.getKey();
                                MaterializedIndexSnapshotInfo indexInfo = indexEntry.getValue();
                                if (indexInfo == null) {
                                    continue;
                                }
                                tabletIds.addAll(indexInfo.tabletIds);
                            }

                            PartitionKey key = new PartitionKey(dbId, tableId, partId, physicalPartId);
                            partitionInfos.put(key, new PartitionVersionInfo(key, physicalPartInfo.visibleVersion, tabletIds));
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
        private final long dbId;
        private final long tableId;
        private final long partId;
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
        private final PartitionKey partitionKey;
        private final long version;
        private final List<Long> tabletIds;

        public PartitionVersionInfo(PartitionKey partitionKey, long version, List<Long> tabletIds) {
            this.partitionKey = partitionKey;
            this.version = version;
            this.tabletIds = tabletIds;
        }

        public PartitionKey getPartitionKey() {
            return partitionKey;
        }

        public long getVersion() {
            return version;
        }

        public List<Long> getTabletIds() {
            return tabletIds;
        }
    }

    /**
     * Information about a partition with version change
     */
    private static class PartitionVersionChangeInfo {
        private final PartitionVersionInfo currentPartitionInfo;
        private final long prevVersion;
        // TODO(zhangqiang)
        // tablet split/merge may delete tablets

        public PartitionVersionChangeInfo(long prevVersion, PartitionVersionInfo currentPartitionInfo) {
            this.prevVersion = prevVersion;
            this.currentPartitionInfo = currentPartitionInfo;
        }

        public PartitionVersionInfo getCurrentPartitionInfo() {
            return currentPartitionInfo;
        }

        public long getPrevVersion() {
            return prevVersion;
        }
    }

    /**
     * Inner class to store cluster snapshot diff results
     */
    private static class ClusterSnapshotDiff {
        private final List<PartitionVersionInfo> addedPartitions = Lists.newArrayList();
        private final List<PartitionVersionChangeInfo> changedPartitions = Lists.newArrayList();
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