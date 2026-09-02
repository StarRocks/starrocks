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
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.lake.snapshot.ExternalClusterSnapshotJob;
import com.starrocks.metric.MetricRepo;
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
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AdminAlterAutomatedSnapshotIntervalStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.task.ExternalClusterSnapshotTask;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsResponse;
import com.starrocks.thrift.TFinishTaskRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_SNAPSHOT_SCOPE;
import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_WAREHOUSE;

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
    @SerializedName(value = "properties")
    protected Map<String, String> properties;
    @SerializedName(value = "lastSuccFullSnapshotInfo")
    protected ClusterSnapshotInfo lastSuccFullSnapshotInfo;

    protected ClusterSnapshotJobScheduler clusterSnapshotJobScheduler;

    public ClusterSnapshotMgr() {
    }

    // Turn on automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOn(AdminSetAutomatedSnapshotOnStmt stmt) {
        String storageVolumeName = stmt.getStorageVolumeName();
        long intervalSeconds = stmt.getIntervalSeconds();
        Map<String, String> properties = stmt.getProperties();

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotOn(storageVolumeName, intervalSeconds, properties);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            setAutomatedSnapshotOn(storageVolumeName, intervalSeconds, properties);
        });
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName) {
        setAutomatedSnapshotOn(storageVolumeName, 0, null);
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName, long intervalSeconds,
            Map<String, String> properties) {
        this.storageVolumeName = storageVolumeName;
        this.automatedSnapshotIntervalSeconds = intervalSeconds;
        this.properties = properties != null ? new HashMap<>(properties) : null;
    }

    public String getAutomatedSnapshotSvName() {
        return storageVolumeName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isAutomatedSnapshotOn() {
        return RunMode.isSharedDataMode() && storageVolumeName != null;
    }

    public boolean isExternalSnapshot() {
        return properties != null && properties.get(PROPERTIES_SNAPSHOT_SCOPE) != null && 
                properties.get(PROPERTIES_SNAPSHOT_SCOPE).equalsIgnoreCase("external");
    }

    public String getWarehouseName() {
        String warehouseName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (properties != null && properties.get(PROPERTIES_WAREHOUSE) != null) {
            warehouseName = properties.get(PROPERTIES_WAREHOUSE);
        }
        return warehouseName;
    }

    // Turn off automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOff(AdminSetAutomatedSnapshotOffStmt stmt) {
        if (isExternalSnapshot()) {
            clearFinishedAutomatedClusterSnapshotExceptLast();
        } else {
            clearFinishedAutomatedClusterSnapshot(null);
        }

        persistAutomatedSnapshotOff();
    }

    private void persistAutomatedSnapshotOff() {
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setAutomatedSnapshotOff();
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            setAutomatedSnapshotOff();
        });
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        storageVolumeName = null;
        if (properties != null) {
            properties.clear();
        }
        lastSuccFullSnapshotInfo = null;
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

    /**
     * Reset inherited cluster-snapshot state with one journal entry, written as an
     * AUTOMATED_SNAPSHOT_OFF record carrying the reset flag so older FEs can still replay it.
     */
    public void resetSnapshotStateAfterExternalRestore() {
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.resetSnapshotStateAfterExternalRestore();
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            applyExternalSnapshotStateReset();
        });
    }

    protected void applyExternalSnapshotStateReset() {
        int dropped = automatedSnapshotJobs.size();
        automatedSnapshotJobs.clear();
        setAutomatedSnapshotOff();
        if (dropped > 0) {
            LOG.info("Dropped {} snapshot jobs inherited from source cluster image", dropped);
        }
    }

    protected void clearFinishedAutomatedClusterSnapshot(String keepSnapshotName) {
        for (Map.Entry<Long, ClusterSnapshotJob> entry : automatedSnapshotJobs.entrySet()) {
            ClusterSnapshotJob job = entry.getValue();
            if (job instanceof ExternalClusterSnapshotJob) {
                synchronized (this) {
                    clearFinishedAutomatedClusterSnapshot(job, keepSnapshotName);
                }
            } else {
                clearFinishedAutomatedClusterSnapshot(job, keepSnapshotName);
            }
        }
    }

    private void clearFinishedAutomatedClusterSnapshot(ClusterSnapshotJob job, String keepSnapshotName) {
        if (!job.isFinished() && !job.isExpired() && !job.isError()) {
            return;
        }

        if (keepSnapshotName != null && job.getSnapshotName().equals(keepSnapshotName)) {
            return;
        }

        if (job.isFinished()) {
            if (job instanceof ExternalClusterSnapshotJob
                    && !((ExternalClusterSnapshotJob) job).isCleaningCompleted()) {
                return;
            }
            job.persistStateChange(ClusterSnapshotJobState.EXPIRED);
        }

        try {
            ClusterSnapshotUtils.clearClusterSnapshotFromRemote(job);
            if (job.isExpired()) {
                job.persistStateChange(ClusterSnapshotJobState.DELETED);
            }
        } catch (StarRocksException e) {
            LOG.warn("Cluster Snapshot delete failed, ", e);
        }
    }

    public synchronized void finishExternalSnapshotCleanup(ExternalClusterSnapshotJob job) {
        ClusterSnapshotJob current = automatedSnapshotJobs.get(job.getId());
        if (current != job || !job.isFinished() || job.isCleaningCompleted()) {
            return;
        }

        ClusterSnapshotJob latest = getLastFinishedAutomatedClusterSnapshotJob();
        if (latest != null && latest.getId() == job.getId()) {
            // Completion of the latest restore point is an in-memory acknowledgement. A new leader
            // safely repeats the idempotent cleanup from FINISHED(false).
            job.setCleaningCompleted(true);
        } else {
            // Do not set cleaningCompleted first. If the EXPIRED WAL fails, FINISHED(false) remains
            // retryable on the next scheduler cycle.
            job.persistStateChange(ClusterSnapshotJobState.EXPIRED);
        }
    }

    public void retryExpiredExternalSnapshotDeletion() {
        for (ClusterSnapshotJob candidate : automatedSnapshotJobs.values()) {
            if (!(candidate instanceof ExternalClusterSnapshotJob) || !candidate.isExpired()) {
                continue;
            }
            try {
                ClusterSnapshotUtils.clearClusterSnapshotFromRemote(candidate);
            } catch (Exception e) {
                LOG.warn("Failed to delete expired external snapshot {}", candidate.getId(), e);
                continue;
            }

            synchronized (this) {
                ClusterSnapshotJob current = automatedSnapshotJobs.get(candidate.getId());
                if (current == candidate && current.isExpired()) {
                    try {
                        current.persistStateChange(ClusterSnapshotJobState.DELETED);
                    } catch (RuntimeException e) {
                        LOG.warn("Failed to persist deletion of external snapshot {}", candidate.getId(), e);
                    }
                }
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
        ClusterSnapshotJob job;
        if (isExternalSnapshot()) {
            job = new ExternalClusterSnapshotJob(id, snapshotName, storageVolumeName, createTimeMs);
        } else {
            job = new ClusterSnapshotJob(id, snapshotName, storageVolumeName, createTimeMs);
        }
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log, wal -> {
            addSnapshotJob(job);
        });

        if (isExternalSnapshot()) {
            MetricRepo.COUNTER_EXTERNAL_SNAPSHOT_JOB_NUM.increase(1L);
        }
        LOG.info("Create automated cluster snapshot job successfully, job id: {}, snapshot name: {}, scope: {}",
                id, snapshotName, isExternalSnapshot() ? "external" : "local");
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

    // The earliest snapshot consistency point whose data must stay restorable. Recycle-bin objects
    // recycled before this time may be erased. Only two things need protecting: the finished snapshots
    // we keep, and any in-progress job. A job that ended in ERROR protects nothing, so a persistently
    // failing snapshot (e.g. every upload fails) no longer freezes the recycle bin.
    public long getSafeDeletionTimeMs() {
        if (!isAutomatedSnapshotOn()) {
            return Long.MAX_VALUE;
        }
        long boundaryMs = computeProtectionBoundaryMs();
        // Nothing needs protecting (e.g. every job ended in ERROR): do not block the recycle bin.
        return boundaryMs == Long.MAX_VALUE ? System.currentTimeMillis() : boundaryMs;
    }

    // One descending pass finds both things we protect, then stops:
    //  - any in-progress job: these are always the newest jobs (the scheduler runs jobs one at a
    //    time), so they are seen before we reach the completed snapshots and can stop;
    //  - the finished snapshots we keep: the second-newest completed snapshot (matching the historical
    //    "keep the two most-recent" behavior), or the only finished one.
    // A job that ended in ERROR protects nothing.
    private long computeProtectionBoundaryMs() {
        long boundaryMs = Long.MAX_VALUE;
        long newestFinishedCreatedTimeMs = Long.MAX_VALUE;
        for (ClusterSnapshotJob job : automatedSnapshotJobs.descendingMap().values()) {
            if (job.isUnFinishedState()) {
                boundaryMs = Math.min(boundaryMs, job.getCreatedTimeMs());
                continue;
            }
            if (!isCompletedSnapshot(job)) {
                continue; // ERROR
            }
            if (newestFinishedCreatedTimeMs != Long.MAX_VALUE) {
                // second completed snapshot reached: it is the oldest restore point we keep
                return Math.min(boundaryMs, job.getCreatedTimeMs());
            }
            if (job.isFinished()) {
                newestFinishedCreatedTimeMs = job.getCreatedTimeMs();
            }
        }
        // Fewer than two completed snapshots: protect the single finished one (if any) plus in-progress.
        return Math.min(boundaryMs, newestFinishedCreatedTimeMs);
    }

    // A terminal, non-ERROR snapshot state (FINISHED, EXPIRED, or DELETED). These anchor the retention
    // boundary, matching historical behavior; ERROR does not. Note only a FINISHED job can be the first
    // (newest) anchor in computeProtectionBoundaryMs(), so an EXPIRED/DELETED job alone never pins the
    // boundary -- it can only serve as the second, older boundary once a newer FINISHED exists.
    private static boolean isCompletedSnapshot(ClusterSnapshotJob job) {
        return job.isFinished() || job.isExpired() || job.isDeleted();
    }

    // Number of trailing ERROR jobs since the last successful (FINISHED) one.
    // In-progress jobs are not counted; a completed snapshot stops the count.
    public int getConsecutiveFailureCount() {
        int count = 0;
        for (ClusterSnapshotJob job : automatedSnapshotJobs.descendingMap().values()) {
            if (job.isError()) {
                count++;
            } else if (isCompletedSnapshot(job)) {
                break;
            }
            // in-progress: skip without counting or breaking
        }
        return count;
    }

    // finishedTimeMs of the most recent FINISHED automated snapshot, or 0 if none.
    public long getLastSuccessTimeMs() {
        ClusterSnapshotJob job = getLastFinishedAutomatedClusterSnapshotJob();
        return job == null ? 0L : job.getSnapshot().getFinishedTimeMs();
    }

    public boolean isTableSafeToDeleteTablet(long tableId) {
        if (!isAutomatedSnapshotOn()) {
            return true;
        }

        long safeDeletionTimeMs = getSafeDeletionTimeMs();

        Map<Long, AlterJobV2> alterJobs =
                new HashMap<>(GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2());
        alterJobs.putAll(GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2());
        for (AlterJobV2 alterJob : alterJobs.values()) {
            if (alterJob.getTableId() == tableId) {
                if (alterJob.getFinishedTimeMs() >= safeDeletionTimeMs) {
                    return false;
                }
                break;
            }
        }

        // A tablet reshard (split/merge) replaces a table's tablets and leaves the pre-reshard
        // parent/source tablets referenced only by an already-captured snapshot. Keep the table's
        // tablets until every covering automated snapshot has expired, mirroring the ALTER-job
        // handling above. An aborted reshard removes no tablets (an abort can only happen before the
        // old tablets are dropped), so it is not snapshot-relevant and must not pin the table.
        for (TabletReshardJob reshardJob :
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJobs().values()) {
            if (reshardJob.getTableId() == tableId && !reshardJob.isAborted()
                    && (!reshardJob.isDone() || reshardJob.getFinishedTimeMs() >= safeDeletionTimeMs)) {
                return false;
            }
        }

        return true;
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
            job.persistStateChange(ClusterSnapshotJobState.FINISHED);
        }
    }

    public void abortUnfinishedClusterSnapshotJob() {
        ClusterSnapshotJob lastUnfinishedJob = getUnfinishedClusterSnapshotJob();
        if (lastUnfinishedJob != null) {
            // For generic (meta-only) snapshot jobs, we keep the original behavior:
            // unfinished job is marked as ERROR on FE restart, so that scheduler
            // can start a brand-new job later.
            //
            // For ExternalClusterSnapshotJob, we rely on its replay() implementation
            // to reconstruct transient state and continue running after restart,
            // so we do NOT mark it as ERROR here.
            //
            // Only while the automated snapshot is on, though: with it off there is no live
            // configuration behind the job, and a job inherited from another cluster's image by a
            // cross-cluster restore would make this cluster produce a snapshot the operator turned
            // off. Such a job is aborted like any other unfinished one.
            if (lastUnfinishedJob instanceof ExternalClusterSnapshotJob && isAutomatedSnapshotOn()) {
                LOG.info("Keep unfinished ExternalClusterSnapshotJob {} in state {} after FE restart",
                        lastUnfinishedJob.getId(), lastUnfinishedJob.getState());
                clusterSnapshotJobScheduler.setRunningJob(lastUnfinishedJob);
                return;
            }

            lastUnfinishedJob.setErrMsg("Snapshot job has been failed because of FE restart or leader change");
            lastUnfinishedJob.setState(ClusterSnapshotJobState.ERROR);
            lastUnfinishedJob.persistStateChange(ClusterSnapshotJobState.ERROR);
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

    /**
     * Fire-and-forget stop for leader demotion: request stop on the inner scheduler without joining,
     * so the single state-change thread is not blocked. The scheduler's worker self-cleans in
     * onStopped() and deregisters on exit; the re-activation cleanliness gate verifies quiescence. The
     * scheduler reference is nulled so the next {@link #start()} rebuilds it on re-election.
     */
    public void stopBestEffort() {
        ClusterSnapshotJobScheduler scheduler = clusterSnapshotJobScheduler;
        if (scheduler != null) {
            scheduler.stopBestEffort();
            clusterSnapshotJobScheduler = null;
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

    public List<ClusterSnapshotInfo> getRetainExternalClusterSnapshotInfo() {
        List<ClusterSnapshotInfo> clusterSnapshotInfos = Lists.newArrayList();
        if (isExternalSnapshot()) {
            for (ClusterSnapshotJob job : automatedSnapshotJobs.values()) {
                // we should make sure the files in snapshot image are uploaded to remote storage before vacuuming
                if (job.getState() != ClusterSnapshotJobState.UPLOADING) {
                    continue;
                }
                clusterSnapshotInfos.add(job.getSnapshot().getClusterSnapshotInfo());
            }
        }
        return clusterSnapshotInfos;
    }

    public void replayLog(ClusterSnapshotLog log) {
        ClusterSnapshotLog.ClusterSnapshotLogType logType = log.getType();
        if (logType == null) {
            // A record written by a newer FE: gson maps the unknown enum value to null. Skip it instead
            // of letting the switch below throw, which would abort journal replay on this FE.
            LOG.warn("Skip cluster snapshot log with an unknown log type");
            return;
        }
        switch (logType) {
            case AUTOMATED_SNAPSHOT_ON: {
                String storageVolumeName = log.getStorageVolumeName();
                long intervalSeconds = log.getAutomatedSnapshotIntervalSeconds();
                Map<String, String> properties = log.getProperties();
                setAutomatedSnapshotOn(storageVolumeName, intervalSeconds, properties);
                break;
            }
            case AUTOMATED_SNAPSHOT_OFF: {
                if (log.isResetInheritedSnapshotState()) {
                    applyExternalSnapshotStateReset();
                } else {
                    setAutomatedSnapshotOff();
                }
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
                        job.replay();
                        addSnapshotJob(job);
                        break;
                    }
                    case SNAPSHOTING:
                    case UPLOADING:
                    case CLEANING:
                    case FINISHED:
                    case EXPIRED:
                    case DELETED:
                    case ERROR: {
                        job.replay();
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
        properties = data.getProperties();
        lastSuccFullSnapshotInfo = data.getLastSuccFullSnapshotInfo();
    }

    public void setLastSuccFullSnapshotInfo(ClusterSnapshotInfo lastSuccFullSnapshotInfo) {
        this.lastSuccFullSnapshotInfo = lastSuccFullSnapshotInfo;
    }

    public ClusterSnapshotInfo getLastSuccFullSnapshotInfo() {
        return lastSuccFullSnapshotInfo;
    }

    public void finishSnapshotTask(ExternalClusterSnapshotTask task, TFinishTaskRequest request) {
        ClusterSnapshotJob job = automatedSnapshotJobs.get(task.getJobId());
        if (job != null) {
            job.finishSnapshotTask(task, request);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }
}
