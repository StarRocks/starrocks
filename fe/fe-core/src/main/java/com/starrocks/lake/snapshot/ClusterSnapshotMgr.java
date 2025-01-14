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
    @SerializedName(value = "automatedSnapshot")
    private ClusterSnapshot automatedSnapshot = null;
    @SerializedName(value = "historyAutomatedSnapshotJobs")
    private TreeMap<Long, ClusterSnapshotJob> historyAutomatedSnapshotJobs = new TreeMap<>();

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
        setAutomatedSnapshotOff();

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setDropSnapshot(AUTOMATED_NAME_PREFIX);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);

        // avoid network communication when replay log
        if (automatedSnapshot != null) {
            try {
                ClusterSnapshotUtils.clearAutomatedSnapshotFromRemote(automatedSnapshot.getSnapshotName());
            } catch (StarRocksException e) {
                LOG.warn("Cluster Snapshot: {} delete failed, err msg: {}", automatedSnapshot.getSnapshotName(), e.getMessage());
            }
        }
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        automatedSnapshotSvName = "";
        automatedSnapshot = null;
    }

    protected void addAutomatedClusterSnapshot(ClusterSnapshot newAutomatedClusterSnapshot) {
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setCreateSnapshot(newAutomatedClusterSnapshot);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);

        if (automatedSnapshot != null && automatedSnapshot.getSnapshotName().startsWith(AUTOMATED_NAME_PREFIX)) {
            try {
                ClusterSnapshotUtils.clearAutomatedSnapshotFromRemote(automatedSnapshot.getSnapshotName());
            } catch (StarRocksException e) {
                LOG.warn("Cluster Snapshot: {} delete failed, err msg: {}", automatedSnapshot.getSnapshotName(), e.getMessage());
            }
        }

        automatedSnapshot = newAutomatedClusterSnapshot;
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

    public ClusterSnapshot getAutomatedSnapshot() {
        return automatedSnapshot;
    }

    public boolean containsAutomatedSnapshot() {
        return getAutomatedSnapshot() != null;
    }

    public synchronized void addJob(ClusterSnapshotJob job) {
        int maxSize = Math.max(Config.max_historical_automated_cluster_snapshot_jobs, 2);
        if (historyAutomatedSnapshotJobs.size() == maxSize) {
            historyAutomatedSnapshotJobs.pollFirstEntry();
        }
        historyAutomatedSnapshotJobs.put(job.getId(), job);
    }

    public synchronized long getValidDeletionTimeMsByAutomatedSnapshot() {
        if (!isAutomatedSnapshotOn()) {
            return Long.MAX_VALUE;
        }

        boolean findLastSuccess = false;
        long previousAutomatedSnapshotCreatedTimsMs = 0;
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.descendingMap().entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (job.isSuccess()) {
                if (findLastSuccess) {
                    previousAutomatedSnapshotCreatedTimsMs = job.getCreatedTimeMs();
                    break;
                }

                findLastSuccess = true;
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
        for (Map.Entry<Long, AlterJobV2> entry : alterJobs.entrySet()) {
            AlterJobV2 alterJob = entry.getValue();
            if (alterJob.getTableId() == tableId) {
                valid = (alterJob.getFinishedTimeMs() < getValidDeletionTimeMsByAutomatedSnapshot());
                break;
            }
        }
        return valid;
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
        if (automatedSnapshot != null) {
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
            case CREATE_SNAPSHOT: {
                ClusterSnapshot snapshot = log.getSnapshot();
                automatedSnapshot = snapshot;
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
                    case ERROR: {
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

                // if a job do not finished/error but fe restart, we should reset the state as error
                // when replaying the log during FE restart. Because the job is unretryable after restart
                if (!GlobalStateMgr.getServingState().isReady() && job.isUnFinishedState()) {
                    job.setState(ClusterSnapshotJobState.ERROR);
                    job.setErrMsg("Snapshot job has been failed");
                }
                break;
            }
            default: {
                LOG.warn("Invalid Cluster Snapshot Log Type {}", logType);
            }
        }
    }

    public void resetLastUnFinishedAutomatedSnapshotJob() {
        if (!historyAutomatedSnapshotJobs.isEmpty()) {
            ClusterSnapshotJob job = historyAutomatedSnapshotJobs.lastEntry().getValue();
            if (job.isUnFinishedState()) {
                job.setErrMsg("Snapshot job has been failed");
                job.setState(ClusterSnapshotJobState.ERROR);
                job.logJob();
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
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }
}
