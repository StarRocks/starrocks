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

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.leader.CheckpointController;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// ClusterSnapshotJobScheduler daemon is running on master node. Coordinate two checkpoint controller
// together to finish image checkpoint one by one and upload image for backup
public class ClusterSnapshotJobScheduler extends FrontendDaemon implements SnapshotJobContext {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotJobScheduler.class);
    private static int CAPTURE_ID_RETRY_TIME = 10;

    protected final CheckpointController feController;
    protected final CheckpointController starMgrController;
    // cluster snapshot information used for start
    protected final RestoredSnapshotInfo restoredSnapshotInfo;

    protected volatile long lastAutomatedJobStartTimeMs;
    protected volatile ClusterSnapshotJob runningJob;

    public ClusterSnapshotJobScheduler(CheckpointController feController,
            CheckpointController starMgrController) {
        super("cluster-snapshot-job-scheduler", 10L);
        this.feController = feController;
        this.starMgrController = starMgrController;
        this.restoredSnapshotInfo = RestoreClusterSnapshotMgr.getRestoredSnapshotInfo();
        this.lastAutomatedJobStartTimeMs = 0;
    }

    public void setRunningJob(ClusterSnapshotJob runningJob) {
        this.runningJob = runningJob;
    }

    @Override
    public CheckpointController getFeController() {
        return feController;
    }

    @Override
    public CheckpointController getStarMgrController() {
        return starMgrController;
    }

    @Override
    public Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() {
        if (feController == null || starMgrController == null) {
            return null;
        }

        int retryTime = CAPTURE_ID_RETRY_TIME;
        while (retryTime > 0) {
            long feCheckpointIdT1 = feController.getJournal().getMaxJournalId();
            long starMgrCheckpointIdT2 = starMgrController.getJournal().getMaxJournalId();
            long feCheckpointIdT3 = feController.getJournal().getMaxJournalId();
            long starMgrCheckpointIdT4 = starMgrController.getJournal().getMaxJournalId();

            if (feCheckpointIdT1 == feCheckpointIdT3 && starMgrCheckpointIdT2 == starMgrCheckpointIdT4) {
                return Pair.create(feCheckpointIdT3, starMgrCheckpointIdT2);
            }

            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
            --retryTime;
        }
        return null;
    }

    public long getLastAutomatedJobStartTimeMs() {
        return lastAutomatedJobStartTimeMs;
    }

    @Override
    protected void runAfterCatalogReady() {
        // skip first run when the scheduler start
        if (lastAutomatedJobStartTimeMs == 0) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                    .resetSnapshotJobsStateAfterRestarted(restoredSnapshotInfo);
            lastAutomatedJobStartTimeMs = System.currentTimeMillis(); // init last start time
            return;
        }

        retryPendingCleanup();

        setInterval(Config.automated_cluster_snapshot_schedule_interval_millisecond);
        if (runningJob == null &&
                !GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().canScheduleNextJob(lastAutomatedJobStartTimeMs)) {
            return;
        }
        CheckpointController.exclusiveLock();
        try {
            if (runningJob == null) {
                runningJob = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getNextCluterSnapshotJob();
            }

            // set last start time when job has been created and begin to submit
            lastAutomatedJobStartTimeMs = runningJob.getCreatedTimeMs();
            runningJob.run(this);

            if (runningJob.isError()) {
                // Automated snapshots run infrequently (default every 10 min), so a WARN per failed
                // attempt is a low-rate recurring signal rather than spam. The cluster_snapshot_-
                // consecutive_failures metric carries the precise count for alerting.
                int failures = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getConsecutiveFailureCount();
                LOG.warn("Automated cluster snapshot has failed {} times in a row (last error: {}). " +
                        "The recycle bin is NOT blocked, but no new restore point is being produced. " +
                        "Check snapshot storage volume permissions/connectivity.",
                        failures, runningJob.getErrMsg());
            }
        } finally {
            if (runningJob != null && !runningJob.isUnFinishedState()) {
                runningJob = null;
            }
            CheckpointController.exclusiveUnlock();
        }
    }

    /**
     * Periodically check for FINISHED ExternalClusterSnapshotJobs whose cleaning was incomplete,
     * and retry delete tasks. This runs outside the exclusive lock since it's independent of
     * the main snapshot job lifecycle.
     */
    private void retryPendingCleanup() {
        for (ClusterSnapshotJob job : GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                .getAutomatedSnapshotJobs().values()) {
            if (!(job instanceof ExternalClusterSnapshotJob)) {
                continue;
            }
            ExternalClusterSnapshotJob extJob = (ExternalClusterSnapshotJob) job;
            if (!extJob.isFinished() || extJob.isCleaningCompleted()) {
                continue;
            }

            try {
                AgentBatchTask batchTask = extJob.getLakeSnapshotBatchTask();
                if (batchTask.getTaskNum() > 0) {
                    // Check if all tasks have received responses (finished or failed)
                    boolean allResponded = batchTask.getAllTasks().stream()
                            .allMatch(t -> t.isFinished() || t.isFailed());
                    if (!allResponded) {
                        // Some tasks still running, skip
                        continue;
                    }
                    boolean anyFailed = batchTask.getAllTasks().stream().anyMatch(AgentTask::isFailed);
                    if (!anyFailed) {
                        extJob.setCleaningCompleted(true);
                        LOG.info("Cleanup completed for snapshot job: {}", extJob.getId());
                        continue;
                    }
                    LOG.info("Some cleanup tasks failed for job: {}, will retry", extJob.getId());
                }

                // (Re)create and dispatch delete tasks
                if (extJob.getSnapshotDiff() == null) {
                    LOG.warn("snapshotDiff is null for job: {}, marking cleaning as completed", extJob.getId());
                    extJob.setCleaningCompleted(true);
                    continue;
                }
                extJob.createDeleteClusterSnasphotTasks();
                LOG.info("Dispatched cleanup retry tasks for snapshot job: {}", extJob.getId());
            } catch (Exception e) {
                LOG.warn("Failed to retry cleanup for snapshot job: {}", extJob.getId(), e);
            }
        }
    }
}
