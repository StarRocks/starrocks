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
import com.starrocks.alter.AlterJobV2;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

// only used for AUTOMATED snapshot for now
public class ClusterSnapshotMgr implements GsonPostProcessable {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotMgr.class);
    public static final String AUTOMATED_NAME_PREFIX = "automated_cluster_snapshot";

    @SerializedName(value = "storageVolumeName")
    private volatile String storageVolumeName;
    @SerializedName(value = "automatedSnapshotJobs")
    private NavigableMap<Long, ClusterSnapshotJob> automatedSnapshotJobs = new ConcurrentSkipListMap<>();

    private ClusterSnapshotCheckpointScheduler clusterSnapshotCheckpointScheduler;

    public ClusterSnapshotMgr() {
    }

    // Turn on automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOn(AdminSetAutomatedSnapshotOnStmt stmt) {
        String storageVolumeName = stmt.getStorageVolumeName();
        setAutomatedSnapshotOn(storageVolumeName);

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotOn(storageVolumeName);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName) {
        this.storageVolumeName = storageVolumeName;
    }

    public String getAutomatedSnapshotSvName() {
        return storageVolumeName;
    }

    public boolean isAutomatedSnapshotOn() {
        return RunMode.isSharedDataMode() && storageVolumeName != null;
    }

    // Turn off automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOff(AdminSetAutomatedSnapshotOffStmt stmt) {
        clearFinishedAutomatedClusterSnapshot(null);

        setAutomatedSnapshotOff();

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotOff();
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        storageVolumeName = null;
    }

    protected void clearFinishedAutomatedClusterSnapshot(String keepSnapshotName) {
        for (Map.Entry<Long, ClusterSnapshotJob> entry : automatedSnapshotJobs.entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (!job.isFinished() && !job.isExpired() && !job.isError()) {
                continue;
            }

            if (keepSnapshotName != null && job.getSnapshotName().equals(keepSnapshotName)) {
                continue;
            }

            if (job.isFinished()) {
                job.setState(ClusterSnapshotJobState.EXPIRED);
                job.logJob();
            }

            try {
                ClusterSnapshotUtils.clearAutomatedSnapshotFromRemote(job.getSnapshotName());
                if (job.isExpired()) {
                    job.setState(ClusterSnapshotJobState.DELETED);
                    job.logJob();
                }
            } catch (StarRocksException e) {
                LOG.warn("Cluster Snapshot delete failed, ", e);
            }
        }
    }

    public ClusterSnapshotJob createAutomatedSnapshotJob() {
        long createTimeMs = System.currentTimeMillis();
        long id = GlobalStateMgr.getCurrentState().getNextId();
        String snapshotName = AUTOMATED_NAME_PREFIX + '_' + String.valueOf(createTimeMs);
        ClusterSnapshotJob job = new ClusterSnapshotJob(id, snapshotName, storageVolumeName, createTimeMs);
        job.logJob();

        addSnapshotJob(job);

        LOG.info("Create automated cluster snapshot job successfully, job id: {}, snapshot name: {}", id, snapshotName);

        return job;
    }

    public StorageVolume getAutomatedSnapshotStorageVolume() {
        if (storageVolumeName == null) {
            return null;
        }

        return GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeByName(storageVolumeName);
    }

    public ClusterSnapshotJob getLastFinishedAutomatedClusterSnapshotJob() {
        for (Map.Entry<Long, ClusterSnapshotJob> entry : automatedSnapshotJobs.descendingMap().entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (job.isFinished()) {
                return job;
            }
        }
        return null;
    }

    public ClusterSnapshot getAutomatedSnapshot() {
        ClusterSnapshotJob job = getLastFinishedAutomatedClusterSnapshotJob();
        if (job == null) {
            return null;
        }

        return job.getSnapshot();
    }

    public void addSnapshotJob(ClusterSnapshotJob job) {
        automatedSnapshotJobs.put(job.getId(), job);

        int maxSize = Math.max(Config.max_historical_automated_cluster_snapshot_jobs, 2);
        if (automatedSnapshotJobs.size() > maxSize) {
            removeAutomatedFinalizeJobs(automatedSnapshotJobs.size() - maxSize);
        }
    }

    public long getSafeDeletionTimeMs() {
        if (!isAutomatedSnapshotOn()) {
            return Long.MAX_VALUE;
        }

        boolean meetFirstFinished = false;
        long previousAutomatedSnapshotCreatedTimsMs = 0;
        for (Map.Entry<Long, ClusterSnapshotJob> entry : automatedSnapshotJobs.descendingMap().entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (meetFirstFinished && (job.isFinished() || job.isExpired() || job.isDeleted())) {
                previousAutomatedSnapshotCreatedTimsMs = job.getCreatedTimeMs();
                break;
            }

            if (job.isFinished()) {
                meetFirstFinished = true;
            }
        }

        return previousAutomatedSnapshotCreatedTimsMs;
    }

    public boolean isTableSafeToDeleteTablet(long tableId) {
        if (!isAutomatedSnapshotOn()) {
            return true;
        }

        boolean safe = true;
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        alterJobs.putAll(GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2());
        for (Map.Entry<Long, AlterJobV2> entry : alterJobs.entrySet()) {
            AlterJobV2 alterJob = entry.getValue();
            if (alterJob.getTableId() == tableId) {
                safe = (alterJob.getFinishedTimeMs() < getSafeDeletionTimeMs());
                break;
            }
        }
        return safe;
    }

    public boolean isDeletionSafeToExecute(long deletionCreatedTimeMs) {
        return deletionCreatedTimeMs < getSafeDeletionTimeMs();
    }

    public NavigableMap<Long, ClusterSnapshotJob> getAutomatedSnapshotJobs() {
        return automatedSnapshotJobs;
    }

    public void resetAutomatedJobsStateForTheFirstRun() {
        resetLastUnFinishedAutomatedSnapshotJob();
        clearFinishedAutomatedClusterSnapshotExceptLastFinished();
    }

    public void clearFinishedAutomatedClusterSnapshotExceptLastFinished() {
        ClusterSnapshotJob lastFinishedJob = getLastFinishedAutomatedClusterSnapshotJob();
        if (lastFinishedJob != null) {
            clearFinishedAutomatedClusterSnapshot(lastFinishedJob.getSnapshotName());
        }
    }

    public void resetLastUnFinishedAutomatedSnapshotJob() {
        if (!automatedSnapshotJobs.isEmpty()) {
            ClusterSnapshotJob job = automatedSnapshotJobs.lastEntry().getValue();
            if (job.isUnFinishedState()) {
                job.setErrMsg("Snapshot job has been failed because of FE restart or leader change");
                job.setState(ClusterSnapshotJobState.ERROR);
                job.logJob();
            }
        }
    }

    public void removeAutomatedFinalizeJobs(int removeCount) {
        if (removeCount <= 0) {
            return;
        }

        List<Long> removeIds = Lists.newArrayList();
        for (Map.Entry<Long, ClusterSnapshotJob> entry : automatedSnapshotJobs.entrySet()) {
            long id = entry.getKey();
            ClusterSnapshotJob job = entry.getValue();

            if (job.isFinalState()) {
                removeIds.add(id);
                --removeCount;
            }

            if (removeCount <= 0) {
                break;
            }
        }

        for (Long removeId : removeIds) {
            automatedSnapshotJobs.remove(removeId);
        }
    }

    public void start() {
        if (RunMode.isSharedDataMode() && clusterSnapshotCheckpointScheduler == null) {
            clusterSnapshotCheckpointScheduler = new ClusterSnapshotCheckpointScheduler(
                    GlobalStateMgr.getCurrentState().getCheckpointController(),
                    StarMgrServer.getCurrentState().getCheckpointController());
            clusterSnapshotCheckpointScheduler.start();
        }
    }

    public TClusterSnapshotJobsResponse getAllSnapshotJobsInfo() {
        TClusterSnapshotJobsResponse response = new TClusterSnapshotJobsResponse();
        for (ClusterSnapshotJob job : automatedSnapshotJobs.values()) {
            response.addToItems(job.getInfo());
        }
        return response;
    }

    public TClusterSnapshotsResponse getAllSnapshotsInfo() {
        TClusterSnapshotsResponse response = new TClusterSnapshotsResponse();
        ClusterSnapshot automatedSnapshot = getAutomatedSnapshot();
        if (isAutomatedSnapshotOn() && automatedSnapshot != null) {
            response.addToItems(automatedSnapshot.getInfo());
        }
        return response;
    }

    public void replayLog(ClusterSnapshotLog log) {
        ClusterSnapshotLog.ClusterSnapshotLogType logType = log.getType();
        switch (logType) {
            case AUTOMATED_SNAPSHOT_ON: {
                String storageVolumeName = log.getStorageVolumeName();
                setAutomatedSnapshotOn(storageVolumeName);
                break;
            }
            case AUTOMATED_SNAPSHOT_OFF: {
                setAutomatedSnapshotOff();
                break;
            }
            case UPDATE_SNAPSHOT_JOB: {
                ClusterSnapshotJob job = log.getSnapshotJob();
                ClusterSnapshotJobState state = job.getState();

                switch (state) {
                    case INITIALIZING: {
                        addSnapshotJob(job);
                        break;
                    }
                    case SNAPSHOTING:
                    case UPLOADING:
                    case FINISHED:
                    case EXPIRED:
                    case DELETED:
                    case ERROR: {
                        automatedSnapshotJobs.put(job.getId(), job);
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

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.CLUSTER_SNAPSHOT_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        ClusterSnapshotMgr data = reader.readJson(ClusterSnapshotMgr.class);

        storageVolumeName = data.getAutomatedSnapshotSvName();
        automatedSnapshotJobs = data.getAutomatedSnapshotJobs();
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }
}
