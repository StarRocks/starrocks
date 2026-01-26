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
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.TimeUtils;
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
import com.starrocks.sql.ast.AdminAlterAutomatedSnapshotIntervalStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

// only used for AUTOMATED snapshot for now
public class ClusterSnapshotMgr implements GsonPostProcessable {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotMgr.class);
    public static final String AUTOMATED_NAME_PREFIX = "automated_cluster_snapshot_";

    @SerializedName(value = "storageVolumeName")
    protected volatile String storageVolumeName;
    @SerializedName(value = "automatedSnapshotIntervalSeconds")
    protected volatile long automatedSnapshotIntervalSeconds = 0;
    @SerializedName(value = "automatedSnapshotJobs")
    protected NavigableMap<Long, ClusterSnapshotJob> automatedSnapshotJobs = new ConcurrentSkipListMap<>();

    protected ClusterSnapshotJobScheduler clusterSnapshotJobScheduler;

    public ClusterSnapshotMgr() {
    }

    // Turn on automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOn(AdminSetAutomatedSnapshotOnStmt stmt) {
        String storageVolumeName = stmt.getStorageVolumeName();
        long intervalSeconds = stmt.getIntervalSeconds();
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotOn(storageVolumeName, intervalSeconds);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            setAutomatedSnapshotOn(storageVolumeName, intervalSeconds);
        });
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName, long intervalSeconds) {
        this.storageVolumeName = storageVolumeName;
        this.automatedSnapshotIntervalSeconds = intervalSeconds;
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName) {
        setAutomatedSnapshotOn(storageVolumeName, 0);
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

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotOff();
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            setAutomatedSnapshotOff();
        });
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        storageVolumeName = null;
    }

    public void setAutomatedSnapshotInterval(AdminAlterAutomatedSnapshotIntervalStmt stmt) {
        long intervalSeconds = stmt.getIntervalSeconds();

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotInterval(intervalSeconds);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            setAutomatedSnapshotInterval(intervalSeconds);
        });
    }

    protected void setAutomatedSnapshotInterval(long intervalSeconds) {
        this.automatedSnapshotIntervalSeconds = intervalSeconds;
    }

    public long getAutomatedSnapshotIntervalSeconds() {
        return automatedSnapshotIntervalSeconds;
    }

    public long getEffectiveAutomatedSnapshotIntervalSeconds() {
        if (automatedSnapshotIntervalSeconds > 0) {
            return automatedSnapshotIntervalSeconds;
        }
        return Config.automated_cluster_snapshot_interval_seconds;
    }

    public long getNextAutomatedSnapshotTimeMs() {
        if (!isAutomatedSnapshotOn()) {
            return -1L;
        }
        long intervalSeconds = getEffectiveAutomatedSnapshotIntervalSeconds();
        if (intervalSeconds <= 0) {
            return -1L;
        }

        long lastStartTimeMs = 0L;
        ClusterSnapshotJobScheduler scheduler = clusterSnapshotJobScheduler;
        if (scheduler != null) {
            lastStartTimeMs = scheduler.getLastAutomatedJobStartTimeMs();
        }
        if (lastStartTimeMs <= 0L) {
            ClusterSnapshotJob lastFinishedJob = getLastFinishedAutomatedClusterSnapshotJob();
            if (lastFinishedJob != null) {
                lastStartTimeMs = lastFinishedJob.getCreatedTimeMs();
            }
        }
        if (lastStartTimeMs <= 0L) {
            return -1L;
        }
        return lastStartTimeMs + intervalSeconds * 1000L;
    }

    public List<List<String>> getAutomatedSnapshotShowResult() {
        List<List<String>> rows = Lists.newArrayList();
        if (!isAutomatedSnapshotOn()) {
            rows.add(Lists.newArrayList("false", FeConstants.NULL_STRING, FeConstants.NULL_STRING,
                    FeConstants.NULL_STRING, FeConstants.NULL_STRING));
            return rows;
        }

        String interval = IntervalLiteral.formatIntervalSeconds(getEffectiveAutomatedSnapshotIntervalSeconds());
        if (interval == null) {
            interval = FeConstants.NULL_STRING;
        }
        String storageVolume = storageVolumeName == null ? FeConstants.NULL_STRING : storageVolumeName;
        ClusterSnapshot snapshot = getAutomatedSnapshot();
        String lastSnapshotTime = snapshot == null ? FeConstants.NULL_STRING
                : TimeUtils.longToTimeString(snapshot.getCreatedTimeMs());
        String nextSnapshotTime = TimeUtils.longToTimeString(getNextAutomatedSnapshotTimeMs());

        rows.add(Lists.newArrayList("true", interval, storageVolume, lastSnapshotTime, nextSnapshotTime));
        return rows;
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
                ClusterSnapshotJob.persistStateChange(job, ClusterSnapshotJobState.EXPIRED);
            }

            try {
                ClusterSnapshotUtils.clearClusterSnapshotFromRemote(job);
                if (job.isExpired()) {
                    ClusterSnapshotJob.persistStateChange(job, ClusterSnapshotJobState.DELETED);
                }
            } catch (StarRocksException e) {
                LOG.warn("Cluster Snapshot delete failed, ", e);
            }
        }
    }

    public boolean canScheduleNextJob(long lastAutomatedJobStartTimeMs) {
        return isAutomatedSnapshotOn() && (System.currentTimeMillis()
                - lastAutomatedJobStartTimeMs >= getEffectiveAutomatedSnapshotIntervalSeconds() * 1000L);
    }

    public ClusterSnapshotJob getNextCluterSnapshotJob() {
        return createAutomatedSnapshotJob();
    }

    public ClusterSnapshotJob createAutomatedSnapshotJob() {
        long createTimeMs = System.currentTimeMillis();
        long id = GlobalStateMgr.getCurrentState().getNextId();
        String snapshotName = AUTOMATED_NAME_PREFIX + createTimeMs;
        ClusterSnapshotJob job = new ClusterSnapshotJob(id, snapshotName, storageVolumeName, createTimeMs);
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            addSnapshotJob(job);
        });

        LOG.info("Create automated cluster snapshot job successfully, job id: {}, snapshot name: {}", id, snapshotName);
        return job;
    }

    public StorageVolume getStorageVolumeBySnapshotJob(ClusterSnapshotJob job) {
        if (job == null) {
            return null;
        }

        return GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeByName(job.getStorageVolumeName());
    }

    public ClusterSnapshotJob getClusterSnapshotJobByName(String snapshotName) {
        for (ClusterSnapshotJob job : automatedSnapshotJobs.values()) {
            if (job.getSnapshotName().equals(snapshotName)) {
                return job;
            }
        }
        return null;
    }

    public ClusterSnapshotJob getUnfinishedClusterSnapshotJob() {
        Entry<Long, ClusterSnapshotJob> entry = automatedSnapshotJobs.lastEntry();
        if (entry != null && entry.getValue().isUnFinishedState()) {
            return entry.getValue();
        }
        return null;
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

    public void resetSnapshotJobsStateAfterRestarted(RestoredSnapshotInfo restoredSnapshotInfo) {
        setJobFinishedIfRestoredFromIt(restoredSnapshotInfo);
        abortUnfinishedClusterSnapshotJob();
        clearFinishedAutomatedClusterSnapshotExceptLast();
    }

    public void setJobFinishedIfRestoredFromIt(RestoredSnapshotInfo restoredSnapshotInfo) {
        if (restoredSnapshotInfo == null) {
            return;
        }

        String restoredSnapshotName = restoredSnapshotInfo.getSnapshotName();
        long feJournalId = restoredSnapshotInfo.getFeJournalId();
        long starMgrJournalId = restoredSnapshotInfo.getStarMgrJournalId();
        ClusterSnapshotJob job = null;
        if (restoredSnapshotName != null) {
            job = getClusterSnapshotJobByName(restoredSnapshotName);
        } else {
            job = getUnfinishedClusterSnapshotJob();
        }
        // snapshot job may in init state, because it does not include the
        // editlog for the state transtition after ClusterSnapshotJobState.INITIALIZING
        if (job != null && job.isInitializing()) {
            job.setJournalIds(feJournalId, starMgrJournalId);
            job.setDetailInfo("Finished time was reset after cluster restored");
            ClusterSnapshotJob.persistStateChange(job, ClusterSnapshotJobState.FINISHED);
        }
    }

    public void abortUnfinishedClusterSnapshotJob() {
        ClusterSnapshotJob lastUnfinishedJob = getUnfinishedClusterSnapshotJob();
        if (lastUnfinishedJob != null) {
            lastUnfinishedJob.setErrMsg("Snapshot job has been failed because of FE restart or leader change");
            lastUnfinishedJob.setState(ClusterSnapshotJobState.ERROR);
            ClusterSnapshotJob.persistStateChange(lastUnfinishedJob, ClusterSnapshotJobState.ERROR);
        }
    }

    public void clearFinishedAutomatedClusterSnapshotExceptLast() {
        ClusterSnapshotJob lastFinishedJob = getLastFinishedAutomatedClusterSnapshotJob();
        if (lastFinishedJob != null) {
            clearFinishedAutomatedClusterSnapshot(lastFinishedJob.getSnapshotName());
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

    // keep this interface and do not remove it
    public List<Long> getVacuumRetainVersions(long dbId, long tableId, long partId, long physicalPartId) {
        List<Long> versions = Lists.newArrayList();
        return versions;
    }

    // keep this interface and do not remove it
    public boolean isDbInClusterSnapshotInfo(long dbId) {
        return false;
    }

    // keep this interface and do not remove it
    public boolean isTableInClusterSnapshotInfo(long dbId, long tableId) {
        return false;
    }

    // keep this interface and do not remove it
    public boolean isPartitionInClusterSnapshotInfo(long dbId, long tableId, long partId) {
        return false;
    }

    // keep this interface and do not remove it
    public boolean isMaterializedIndexInClusterSnapshotInfo(long dbId, long tableId, long partId, long indexId) {
        return false;
    }

    // keep this interface and do not remove it
    public boolean isMaterializedIndexInClusterSnapshotInfo(
            long dbId, long tableId, long partId, long physicalPartId, long indexId) {
        return false;
    }

    // keep this interface and do not remove it
    public boolean isShardGroupIdInClusterSnapshotInfo(long dbId, long tableId, long partId, long shardGroupId) {
        return false;
    }

    // keep this interface and do not remove it
    public boolean isShardGroupIdInClusterSnapshotInfo(
            long dbId, long tableId, long partId, long physicalPartId, long shardGroupId) {
        return false;
    }

    public void start() {
        if (RunMode.isSharedDataMode() && clusterSnapshotJobScheduler == null) {
            clusterSnapshotJobScheduler = new ClusterSnapshotJobScheduler(
                    GlobalStateMgr.getCurrentState().getCheckpointController(),
                    StarMgrServer.getCurrentState().getCheckpointController());
            clusterSnapshotJobScheduler.start();
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
                setAutomatedSnapshotOn(storageVolumeName, log.getAutomatedSnapshotIntervalSeconds());
                break;
            }
            case AUTOMATED_SNAPSHOT_OFF: {
                setAutomatedSnapshotOff();
                break;
            }
            case AUTOMATED_SNAPSHOT_INTERVAL: {
                setAutomatedSnapshotInterval(log.getAutomatedSnapshotIntervalSeconds());
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
                break;
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
        automatedSnapshotIntervalSeconds = data.getAutomatedSnapshotIntervalSeconds();
        automatedSnapshotJobs = data.getAutomatedSnapshotJobs();
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }
}
