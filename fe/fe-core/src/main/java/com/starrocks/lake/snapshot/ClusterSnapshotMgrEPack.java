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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.persist.ManualClusterSnapshotLog;
import com.starrocks.lake.restore.RestoreHandler;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateClusterSnapshotStmt;
import com.starrocks.sql.ast.DropClusterSnapshotStmt;
import com.starrocks.sql.ast.RestoreTableFromSnapshotStmt;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ClusterSnapshotMgrEPack extends ClusterSnapshotMgr {
    @SerializedName(value = "manualClusterSnapshotRequestQueue")
    private Queue<ManualClusterSnapshotRequest> manualClusterSnapshotRequestQueue = new LinkedBlockingQueue<>();
    @SerializedName(value = "manualClusterSnapshotJobs")
    private NavigableMap<Long, ManualClusterSnapshotJob> manualClusterSnapshotJobs = new ConcurrentSkipListMap<>();

    private final RestoreHandler tableSnapshotRestoreHandler = new RestoreHandler();

    public ClusterSnapshotMgrEPack() {
    }

    public void createClusterSnapshot(CreateClusterSnapshotStmt stmt) {
        String snapshotName = stmt.getClusterSnapshotName();
        String storageVolumeName = stmt.getStorageVolumeName();
        if (isManualClusterSnapshotNameValid(snapshotName)) {
            if (stmt.isIfNotExists()) {
                LOG.warn("Manual Cluster Snapshot Job has existed, snapshot name: " + snapshotName);
                return;
            } else {
                throw new SemanticException("Manual Cluster Snapshot Job has existed, snapshot name: %s", snapshotName);
            }
        }

        // Existing manual snapshots = pending requests not yet scheduled + jobs already created (in any state).
        int existingManualSnapshotCount = manualClusterSnapshotRequestQueue.size() + manualClusterSnapshotJobs.size();
        if (existingManualSnapshotCount >= Config.max_manual_cluster_snapshot_jobs) {
            throw new SemanticException(
                    "Cannot create manual cluster snapshot '%s': the number of existing manual snapshots (%d) " +
                            "has reached the limit (max_manual_cluster_snapshot_jobs=%d). " +
                            "Please drop some snapshots first.",
                    snapshotName, existingManualSnapshotCount, Config.max_manual_cluster_snapshot_jobs);
        }

        ManualClusterSnapshotRequest request = new ManualClusterSnapshotRequest(snapshotName, storageVolumeName);
        addManualClusterSnapshotRequest(request);

        ManualClusterSnapshotLog log = new ManualClusterSnapshotLog();
        log.setAddManualRequest(request);

        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logManualClusterSnapshotLog(log);

        LOG.info("Add manual cluster snapshot request successfully, snapshot name: {}", stmt.getClusterSnapshotName());
    }

    public void dropClusterSnapshot(DropClusterSnapshotStmt stmt) {
        String snapshotName = stmt.getSnapshotName();
        ClusterSnapshotJob job = getClusterSnapshotJobByName(stmt.getSnapshotName());
        if (job == null) {
            if (stmt.getIfExists()) {
                LOG.warn("Cluster snapshot does not exist, snapshot name: " + snapshotName);
                return;
            } else {
                throw new SemanticException("Manual Snapshot: %s doest not exist", snapshotName);
            }
        }

        // Reject drop request if the snapshot job is still running
        if (job.isUnFinishedState()) {
            throw new SemanticException(
                    "Cannot drop cluster snapshot '%s' because snapshot job is still running with state: %s. " +
                            "Please wait for the job to complete.",
                    snapshotName, job.getState().name());
            // TODO: Support CANCEL CLUSTER SNAPSHOT statement to allow users to cancel
            // running snapshot jobs
        }

        try {
            ClusterSnapshotUtils.clearClusterSnapshotFromRemote(job);
            removeClusterSnapshotJobByName(snapshotName);
        } catch (StarRocksException e) {
            LOG.warn("Cluster Snapshot delete failed, ", e);
            return;
        }

        // log when the manual snapshot is dropped successfully
        ManualClusterSnapshotLog log = new ManualClusterSnapshotLog();
        log.setDropManualJob(snapshotName);
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logManualClusterSnapshotLog(log);

        LOG.info("Drop cluster snapshot successfully, snapshot name: {}", snapshotName);
    }

    public void submitTableSnapshotRestore(RestoreTableFromSnapshotStmt stmt, ConnectContext context)
            throws StarRocksException {
        tableSnapshotRestoreHandler.submitTableSnapshotRestore(stmt, context);
    }

    void removeClusterSnapshotJobByName(String snapshotName) {
        ClusterSnapshotJob job = getClusterSnapshotJobByName(snapshotName);
        if (job != null) {
            if (snapshotName.startsWith(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX)) {
                automatedSnapshotJobs.remove(job.getId());
            } else {
                manualClusterSnapshotJobs.remove(job.getId());
            }
        }
    }

    public Queue<ManualClusterSnapshotRequest> getManualClusterSnapshotRequestQueue() {
        return this.manualClusterSnapshotRequestQueue;
    }

    public NavigableMap<Long, ManualClusterSnapshotJob> getManualClusterSnapshotJobs() {
        return this.manualClusterSnapshotJobs;
    }

    @Override
    public ClusterSnapshotJob getClusterSnapshotJobByName(String snapshotName) {
        ClusterSnapshotJob job = super.getClusterSnapshotJobByName(snapshotName);
        if (job != null) {
            return job;
        }
        for (ManualClusterSnapshotJob manualJob : manualClusterSnapshotJobs.values()) {
            if (manualJob.getSnapshotName().equals(snapshotName)) {
                return manualJob;
            }
        }
        return null;
    }

    @Override
    public ClusterSnapshotJob getUnfinishedClusterSnapshotJob() {
        ClusterSnapshotJob job = super.getUnfinishedClusterSnapshotJob();
        if (job != null) {
            return job;
        }
        Entry<Long, ManualClusterSnapshotJob> entry = manualClusterSnapshotJobs.lastEntry();
        if (entry != null && entry.getValue().isUnFinishedState()) {
            return entry.getValue();
        }
        return null;
    }

    public boolean isManualClusterSnapshotNameValid(String snapshotName) {
        return !snapshotName.startsWith(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX) &&
                manualClusterSnapshotRequestQueue.stream().anyMatch(
                        request -> request.getSnapshotName().equals(snapshotName)) ||
                getClusterSnapshotJobByName(snapshotName) != null;
    }

    public void addManualClusterSnapshotRequest(ManualClusterSnapshotRequest request) {
        manualClusterSnapshotRequestQueue.add(request);
    }

    public void removeManualClusterSnapshotRequestByName(String snapshotName) {
        if (!manualClusterSnapshotRequestQueue.isEmpty() && snapshotName != null) {
            ManualClusterSnapshotRequest peekRequest = manualClusterSnapshotRequestQueue.peek();
            if (peekRequest != null && peekRequest.getSnapshotName() != null &&
                    peekRequest.getSnapshotName().equals(snapshotName)) {
                manualClusterSnapshotRequestQueue.remove();
            }
        }
    }

    public void addManualClusterSnapshotJob(ManualClusterSnapshotJob job) {
        manualClusterSnapshotJobs.put(job.getId(), job);
    }

    public void replayManualLog(ManualClusterSnapshotLog log) {
        ManualClusterSnapshotLog.ManualClusterSnapshotLogType logType = log.getType();
        switch (logType) {
            case ADD_MANUAL_REQUEST: {
                ManualClusterSnapshotRequest request = log.getManualSnapshotRequest();
                addManualClusterSnapshotRequest(request);
                break;
            }
            case DROP_MANUAL_JOB: {
                removeClusterSnapshotJobByName(log.getDropClusterSnapshotName());
                break;
            }
            case UPDATE_SNAPSHOT_JOB: {
                ManualClusterSnapshotJob job = log.getManualSnapshotJob();
                ClusterSnapshotJobState state = job.getState();

                switch (state) {
                    case INITIALIZING: {
                        removeManualClusterSnapshotRequestByName(job.getSnapshotName());
                        // fall-through, do not break
                    }
                    case SNAPSHOTING:
                    case UPLOADING:
                    case FINISHED:
                    case EXPIRED:
                    case DELETED:
                    case ERROR: {
                        addManualClusterSnapshotJob(job);
                        break;
                    }
                    default: {
                        LOG.warn("Invalid Cluster Snapshot Job state {}", state);
                    }
                }
            }
            default: {
                LOG.warn("Invalid Cluster Snapshot Log Type {}", logType);
            }
        }
    }

    @Override
    public boolean canScheduleNextJob(long lastAutomatedJobStartTimeMs) {
        /* manual cluster snapshot has the higher priority for scheduling */
        return !manualClusterSnapshotRequestQueue.isEmpty() || super.canScheduleNextJob(lastAutomatedJobStartTimeMs);
    }

    @Override
    public ClusterSnapshotJob getNextCluterSnapshotJob() {
        if (!manualClusterSnapshotRequestQueue.isEmpty()) {
            ManualClusterSnapshotJob manualJob = manualClusterSnapshotRequestQueue.poll().toManualClusterSnapshotJob();
            manualJob.logJob();
            addManualClusterSnapshotJob(manualJob);
            return manualJob;
        }

        return super.getNextCluterSnapshotJob();
    }

    @Override
    public long getSafeDeletionTimeMs() {
        long runningManualClusterSnapshotCreatedTimsMs = Long.MAX_VALUE;
        ClusterSnapshotJob job = clusterSnapshotJobScheduler.runningJob;
        if (job != null && job instanceof ManualClusterSnapshotJob) {
            runningManualClusterSnapshotCreatedTimsMs = job.getCreatedTimeMs();
        }

        return Math.min(runningManualClusterSnapshotCreatedTimsMs, super.getSafeDeletionTimeMs());
    }

    public List<ClusterSnapshotInfo> getAllClusterSnapshotInfo() {
        List<ClusterSnapshotInfo> clusterSnapshotInfos = Lists.newArrayList();
        for (ManualClusterSnapshotJob manualJob : manualClusterSnapshotJobs.values()) {
            ClusterSnapshot clusterSnapshot = manualJob.getSnapshot();
            ClusterSnapshotInfo clusterSnapshotInfo = clusterSnapshot.getClusterSnapshotInfo();
            clusterSnapshotInfos.add(clusterSnapshotInfo);
        }
        return clusterSnapshotInfos;
    }

    @Override
    public List<Long> getVacuumRetainVersions(long dbId, long tableId, long partId, long physicalPartId) {
        List<Long> versions = Lists.newArrayList();
        List<ClusterSnapshotInfo> infos = getAllClusterSnapshotInfo();
        infos.addAll(super.getRetainExternalClusterSnapshotInfo());
        for (ClusterSnapshotInfo info : infos) {
            long version = info.getVersion(dbId, tableId, partId, physicalPartId);
            if (version != 0) {
                versions.add(version);
            }
            versions.addAll(info.getCommittedVersionsAfterVisible(dbId, tableId, partId, physicalPartId));
        }
        return versions;
    }

    @Override
    public boolean isDbInClusterSnapshotInfo(long dbId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsDb(dbId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isTableInClusterSnapshotInfo(long dbId, long tableId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsTable(dbId, tableId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isPartitionInClusterSnapshotInfo(long dbId, long tableId, long partId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsPartition(dbId, tableId, partId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isMaterializedIndexInClusterSnapshotInfo(long dbId, long tableId, long partId, long indexId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsMaterializedIndex(dbId, tableId, partId, indexId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isMaterializedIndexInClusterSnapshotInfo(
            long dbId, long tableId, long partId, long physicalPartId, long indexId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsMaterializedIndex(dbId, tableId, partId, physicalPartId, indexId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isShardGroupIdInClusterSnapshotInfo(long dbId, long tableId, long partId, long shardGroupId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsShardGroupId(dbId, tableId, partId, shardGroupId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isShardGroupIdInClusterSnapshotInfo(
            long dbId, long tableId, long partId, long physicalPartId, long shardGroupId) {
        for (ClusterSnapshotInfo info : getAllClusterSnapshotInfo()) {
            if (info.containsShardGroupId(dbId, tableId, partId, physicalPartId, shardGroupId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TClusterSnapshotJobsResponse getAllSnapshotJobsInfo() {
        TClusterSnapshotJobsResponse response = super.getAllSnapshotJobsInfo();
        for (ManualClusterSnapshotJob job : manualClusterSnapshotJobs.values()) {
            response.addToItems(job.getInfo());
        }
        return response;
    }

    @Override
    public TClusterSnapshotsResponse getAllSnapshotsInfo() {
        TClusterSnapshotsResponse response = super.getAllSnapshotsInfo();
        for (ManualClusterSnapshotJob job : manualClusterSnapshotJobs.values()) {
            if (job.isFinished()) {
                response.addToItems(job.getSnapshot().getInfo());
            }
        }
        return response;
    }

    @Override
    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        ClusterSnapshotMgrEPack data = reader.readJson(ClusterSnapshotMgrEPack.class);

        storageVolumeName = data.getAutomatedSnapshotSvName();
        automatedSnapshotJobs = data.getAutomatedSnapshotJobs();
        manualClusterSnapshotRequestQueue = data.getManualClusterSnapshotRequestQueue();
        manualClusterSnapshotJobs = data.getManualClusterSnapshotJobs();
        properties = data.getProperties();
        lastSuccFullSnapshotInfo = data.getLastSuccFullSnapshotInfo();
    }
}
