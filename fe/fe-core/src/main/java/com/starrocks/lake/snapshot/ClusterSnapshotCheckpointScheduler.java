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

import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.leader.CheckpointController;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// ClusterSnapshotCheckpointScheduler daemon is running on master node. Coordinate two checkpoint controller
// together to finish image checkpoint one by one and upload image for backup
public class ClusterSnapshotCheckpointScheduler extends FrontendDaemon implements SnapshotJobContext {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotCheckpointScheduler.class);
    private static int CAPTURE_ID_RETRY_TIME = 10;

    protected final CheckpointController feController;
    protected final CheckpointController starMgrController;
    // cluster snapshot information used for start
    protected final RestoredSnapshotInfo restoredSnapshotInfo;

    protected long lastAutomatedJobStartTimeMs;
    protected volatile ClusterSnapshotJob runningJob;

    public ClusterSnapshotCheckpointScheduler(CheckpointController feController,
            CheckpointController starMgrController) {
        super("cluster_snapshot_checkpoint_scheduler", 10L);
        this.feController = feController;
        this.starMgrController = starMgrController;
        this.restoredSnapshotInfo = RestoreClusterSnapshotMgr.getRestoredSnapshotInfo();
        this.lastAutomatedJobStartTimeMs = 0;
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

    @Override
    protected void runAfterCatalogReady() {
        // skip first run when the scheduler start
        if (lastAutomatedJobStartTimeMs == 0) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                                            .resetSnapshotJobsStateAfterRestarted(restoredSnapshotInfo);
            lastAutomatedJobStartTimeMs = System.currentTimeMillis(); // init last start time
            return;
        }

        /*
         * Control the interval of automated cluster snapshot manually instead of by Daemon framework
         * for the future purpose.
         */
        if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().canScheduleNextJob(lastAutomatedJobStartTimeMs)
                || (runningJob != null && runningJob.isUnFinishedState())) {
            return;
        }

        CheckpointController.exclusiveLock();
        try {
            if (runningJob == null || !runningJob.isUnFinishedState()) {
                runningJob = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getNextCluterSnapshotJob();
            }

            // set last start time when job has been created and begin to submit
            lastAutomatedJobStartTimeMs = runningJob.getCreatedTimeMs();
            runningJob.run(this);
        } finally {
            if (runningJob != null && !runningJob.isUnFinishedState()) {
                runningJob = null;
            }
            CheckpointController.exclusiveUnlock();
        }
    }
}
