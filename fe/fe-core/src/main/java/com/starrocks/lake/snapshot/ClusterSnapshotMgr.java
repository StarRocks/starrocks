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

import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.leader.CheckpointController;
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
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

// only used for AUTOMATED snapshot for now
public class ClusterSnapshotMgr implements GsonPostProcessable {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotMgr.class);
    public static final String AUTOMATED_NAME_PREFIX = "automated_cluster_snapshot";

    @SerializedName(value = "automatedSnapshotSvName")
    private String automatedSnapshotSvName = "";
    @SerializedName(value = "historyAutomatedSnapshotJobs")
    private TreeMap<Long, ClusterSnapshotJob> historyAutomatedSnapshotJobs = new TreeMap<>();

    private ClusterSnapshotCheckpointScheduler clusterSnapshotCheckpointScheduler;

    public ClusterSnapshotMgr() {}

    // Turn on automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOn(AdminSetAutomatedSnapshotOnStmt stmt) {
        String storageVolumeName = stmt.getStorageVolumeName();
        setAutomatedSnapshotOn(storageVolumeName);

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setCreateSnapshotNamePrefix(AUTOMATED_NAME_PREFIX, storageVolumeName);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName) {
        automatedSnapshotSvName = storageVolumeName;
    }

    public String getAutomatedSnapshotSvName() {
        return automatedSnapshotSvName;
    }

    public boolean isAutomatedSnapshotOn() {
        return RunMode.isSharedDataMode() && automatedSnapshotSvName != null && !automatedSnapshotSvName.isEmpty();
    }

    // Turn off automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOff(AdminSetAutomatedSnapshotOffStmt stmt) {
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setDropSnapshot(AUTOMATED_NAME_PREFIX);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);

        clearFinishedAutomatedClusterSnapshot(null);

        setAutomatedSnapshotOff();
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        automatedSnapshotSvName = "";
    }

    protected void clearFinishedAutomatedClusterSnapshot(String snapshotName) {
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (!job.isFinished() && !job.isExpired() && !job.isError()) {
                continue;
            }

            if (snapshotName != null && job.getSnapshotName().equals(snapshotName)) {
                continue;
            }

            boolean succ = true;
            job.setState(ClusterSnapshotJobState.EXPIRED);
            job.logJob();
            try {
                ClusterSnapshotUtils.clearAutomatedSnapshotFromRemote(job.getSnapshotName());
            } catch (StarRocksException e) {
                succ = false;
                LOG.warn("Cluster Snapshot delete failed, err msg: {}", e.getMessage());
            }

            if (succ) {
                job.setState(ClusterSnapshotJobState.DELETED);
                job.logJob();
            }
        }
    }

    public ClusterSnapshotJob createAutomatedSnapshotJob() {
        long createTimeMs = System.currentTimeMillis();
        long id = GlobalStateMgr.getCurrentState().getNextId();
        String snapshotName = AUTOMATED_NAME_PREFIX + '_' + String.valueOf(createTimeMs);
        String storageVolumeName = automatedSnapshotSvName;
        ClusterSnapshotJob job = new ClusterSnapshotJob(id, snapshotName, storageVolumeName, createTimeMs);
        job.logJob();

        addJob(job);
    
        LOG.info("Create automated cluster snapshot job successfully, job id: {}, snapshot name: {}", id, snapshotName);

        return job;
    }

    public StorageVolume getAutomatedSnapshotSv() {
        if (automatedSnapshotSvName.isEmpty()) {
            return null;
        }

        return GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeByName(automatedSnapshotSvName);
    }

    public ClusterSnapshotJob getLatestAutomatedFinishedClusterSnapshotJob() {
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.descendingMap().entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (job.isFinished()) {
                return job;
            }
        }
        return null;
    }

    public ClusterSnapshot getAutomatedSnapshot() {
        ClusterSnapshotJob job = getLatestAutomatedFinishedClusterSnapshotJob();
        if (job == null) {
            return null;
        }

        return job.getSnapshot();
    }

    public synchronized void addJob(ClusterSnapshotJob job) {
        int maxSize = Math.max(Config.max_historical_automated_cluster_snapshot_jobs, 2);
        if (historyAutomatedSnapshotJobs.size() == maxSize) {
            removeLastestAutomatedFinalizeJob();
        }
        historyAutomatedSnapshotJobs.put(job.getId(), job);
    }

    public synchronized long getValidDeletionTimeMsByAutomatedSnapshot() {
        if (!isAutomatedSnapshotOn()) {
            return Long.MAX_VALUE;
        }

        boolean meetFirstFinished = false;
        long previousAutomatedSnapshotCreatedTimsMs = 0;
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.descendingMap().entrySet()) {
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

    public synchronized boolean checkValidDeletionForTableFromAlterJob(long tableId) {
        if (!isAutomatedSnapshotOn()) {
            return true;
        }

        boolean valid = true;
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        alterJobs.putAll(GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2());
        for (Map.Entry<Long, AlterJobV2> entry : alterJobs.entrySet()) {
            AlterJobV2 alterJob = entry.getValue();
            if (alterJob.getTableId() == tableId) {
                valid = (alterJob.getFinishedTimeMs() < getValidDeletionTimeMsByAutomatedSnapshot());
                break;
            }
        }
        return valid;
    }

    public TreeMap<Long, ClusterSnapshotJob> getHistoryAutomatedSnapshotJobs() {
        return historyAutomatedSnapshotJobs;
    }

    public void resetAutomatedJobsStateForTheFirstRun() {
        resetAllAutomatedFinishedJobsIntoExpiredExceptLastest();
        resetLastUnFinishedAutomatedSnapshotJob();
    }

    public void resetAllAutomatedFinishedJobsIntoExpiredExceptLastest() {
        boolean meetFirstFinished = false;
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.descendingMap().entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (job.isFinished()) {
                if (meetFirstFinished) {
                    job.setState(ClusterSnapshotJobState.EXPIRED);
                    continue;
                }
                meetFirstFinished = true;
            }
        }
    }

    public void resetLastUnFinishedAutomatedSnapshotJob() {
        if (!historyAutomatedSnapshotJobs.isEmpty()) {
            ClusterSnapshotJob job = historyAutomatedSnapshotJobs.lastEntry().getValue();
            if (job.isUnFinishedState()) {
                job.setErrMsg("Snapshot job has been failed because of FE restart or leader change");
                job.setState(ClusterSnapshotJobState.ERROR);
                job.logJob();
            }
        }
    }

    public void removeLastestAutomatedFinalizeJob() {
        long removeId = -1;
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.descendingMap().entrySet()) {
            long id = entry.getKey();
            ClusterSnapshotJob job = entry.getValue();

            if (job.isFinalizeState()) {
                removeId = id;
                break;
            }
        }

        if (removeId != -1) {
            historyAutomatedSnapshotJobs.remove(removeId);
        }
    }

    public void startCheckpointScheduler(CheckpointController feController, CheckpointController starMgrController) {
        if (RunMode.isSharedDataMode() && clusterSnapshotCheckpointScheduler == null) {
            clusterSnapshotCheckpointScheduler = new ClusterSnapshotCheckpointScheduler(feController, starMgrController);
            clusterSnapshotCheckpointScheduler.start();
        }
    }

    public TClusterSnapshotJobsResponse getAllJobsInfo() {
        TClusterSnapshotJobsResponse response = new TClusterSnapshotJobsResponse();
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.entrySet()) {
            response.addToItems(entry.getValue().getInfo());
        }
        return response;
    }

    public TClusterSnapshotsResponse getAllInfo() {
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
            case CREATE_SNAPSHOT_PREFIX: {
                String createSnapshotNamePrefix = log.getCreateSnapshotNamePrefix();
                String storageVolumeName = log.getStorageVolumeName();
                if (createSnapshotNamePrefix.equals(AUTOMATED_NAME_PREFIX)) {
                    setAutomatedSnapshotOn(storageVolumeName);
                }
                break;
            }
            case DROP_SNAPSHOT: {
                String dropSnapshotName = log.getDropSnapshotName();
                if (dropSnapshotName.equals(AUTOMATED_NAME_PREFIX)) {
                    setAutomatedSnapshotOff();
                }
                break;
            }
            case UPDATE_SNAPSHOT_JOB: {
                ClusterSnapshotJob job = log.getSnapshotJob();
                ClusterSnapshotJobState state = job.getState();

                switch (state) {
                    case INITIALIZING: {
                        addJob(job);
                        break;
                    }
                    case SNAPSHOTING:
                    case UPLOADING:
                    case FINISHED:
                    case ERROR:
                    case EXPIRED:
                    case DELETED: {
                        if (historyAutomatedSnapshotJobs.containsKey(job.getId())) {
                            historyAutomatedSnapshotJobs.remove(job.getId());
                            historyAutomatedSnapshotJobs.put(job.getId(), job);
                        }
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

        automatedSnapshotSvName = data.getAutomatedSnapshotSvName();
        historyAutomatedSnapshotJobs = data.getHistoryAutomatedSnapshotJobs();
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }
}
