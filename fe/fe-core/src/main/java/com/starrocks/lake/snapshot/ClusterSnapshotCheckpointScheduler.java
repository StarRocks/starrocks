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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.leader.CheckpointController;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// ClusterSnapshotCheckpointScheduler daemon is running on master node. Coordinate two checkpoint controller
// together to finish image checkpoint one by one and upload image for backup
public class ClusterSnapshotCheckpointScheduler extends FrontendDaemon {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotCheckpointScheduler.class);
    private static int CAPTURE_ID_RETRY_TIME = 10;

    private final CheckpointController feController;
    private final CheckpointController starMgrController;

    private boolean firstRun;

    public ClusterSnapshotCheckpointScheduler(CheckpointController feController,
            CheckpointController starMgrController) {
        super("cluster_snapshot_checkpoint_scheduler", Config.automated_cluster_snapshot_interval_seconds * 1000L);
        this.feController = feController;
        this.starMgrController = starMgrController;
        this.firstRun = true;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
            return;
        }

        // skip first run when the scheduler start
        if (firstRun) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().resetAutomatedJobsStateForTheFirstRun();
            firstRun = false;
            return;
        }

        CheckpointController.exclusiveLock();
        try {
            runCheckpointScheduler();
        } finally {
            CheckpointController.exclusiveUnlock();
        }
    }

    protected void runCheckpointScheduler() {
        String errMsg = "";
        ClusterSnapshotJob job = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                .createAutomatedSnapshotJob(); /* INITIALIZING state */

        do {
            // step 1: capture consistent journal id for checkpoint
            Pair<Long, Long> consistentIds = captureConsistentCheckpointIdBetweenFEAndStarMgr();
            if (consistentIds == null) {
                errMsg = "failed to capture consistent journal id for checkpoint";
                break;
            }
            job.setJournalIds(consistentIds.first, consistentIds.second);
            LOG.info(
                    "Successful capture consistent journal id, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                    consistentIds.first, consistentIds.second);

            // step 2: make two controllers accept the requested id to do checkpoint control
            job.setState(ClusterSnapshotJobState.SNAPSHOTING);
            job.logJob();

            long feImageJournalId = feController.getImageJournalId();
            long feCheckpointJournalId = consistentIds.first;
            if (feImageJournalId < feCheckpointJournalId) {
                Pair<Boolean, String> createFEImageRet = feController.runCheckpointControllerWithIds(feImageJournalId,
                        feCheckpointJournalId);
                if (!createFEImageRet.first) {
                    errMsg = "checkpoint failed for FE image: " + createFEImageRet.second;
                    break;
                }
            } else if (feImageJournalId > feCheckpointJournalId) {
                errMsg = "checkpoint journal id for FE is smaller than image version";
                break;
            }
            LOG.info("Finished create image for FE image, version: {}", consistentIds.first);

            long starMgrImageJournalId = starMgrController.getImageJournalId();
            long starMgrCheckpointJournalId = consistentIds.second;
            if (starMgrImageJournalId < starMgrCheckpointJournalId) {
                Pair<Boolean, String> createStarMgrImageRet = starMgrController
                        .runCheckpointControllerWithIds(starMgrImageJournalId, starMgrCheckpointJournalId);
                if (!createStarMgrImageRet.first) {
                    errMsg = "checkpoint failed for starMgr image: " + createStarMgrImageRet.second;
                    break;
                }
            } else if (starMgrImageJournalId > starMgrCheckpointJournalId) {
                errMsg = "checkpoint journal id for starMgr is smaller than image version";
                break;
            }
            LOG.info("Finished create image for starMgr image, version: {}", consistentIds.second);

            // step 3: upload all finished image file
            job.setState(ClusterSnapshotJobState.UPLOADING);
            job.logJob();
            try {
                ClusterSnapshotUtils.uploadAutomatedSnapshotToRemote(job.getSnapshotName());
            } catch (StarRocksException e) {
                errMsg = "upload image failed, err msg: " + e.getMessage();
                break;
            }
            LOG.info(
                    "Finish upload image for Cluster Snapshot, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                    job.getFeJournalId(), job.getStarMgrJournalId());
        } while (false);

        if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn()) {
            errMsg = "Job: " + job.getSnapshotName()
                    + " has been cancelled because automated cluster snapshot has been turn off";
        }

        if (!errMsg.isEmpty()) {
            job.setErrMsg(errMsg);
            job.setState(ClusterSnapshotJobState.ERROR);
            job.logJob();
            LOG.warn("Cluster Snapshot checkpoint failed: " + errMsg);
        } else {
            job.setState(ClusterSnapshotJobState.FINISHED);
            job.logJob();
            LOG.info(
                    "Finish Cluster Snapshot checkpoint, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                    job.getFeJournalId(), job.getStarMgrJournalId());
        }
    }

    /*
     * Definition of consistent: Suppose there are two images generated by FE and
     * StarMgr, call FEImageNew
     * and StarMgrImageNew and satisfy:
     * FEImageNew = FEImageOld + editlog(i) + ... + editlog(j)
     * StarMgrImageNew = StarMgrImageOld + editlog(k) + ... + editlog(m)
     * 
     * Define Tj = generated time of editlog(j), Tmax = max(Tj, Tm)
     * Consistency means all editlogs generated before Tmax (no matter the editlog
     * is belong to FE or starMgr)
     * should be included in the image generated by checkpoint.
     * In other words, there must be no holes before the `maximum` editlog contained
     * in the two images
     * generated by checkpoint.
     * 
     * How to get the consistent id: because editlog is generated and flush in a
     * synchronous way, so we can simply
     * get the `snapshot` of maxJouranlId for both FE side and StarMgr side.
     * We get the `snapshot` in a lock-free way. As shown in the code below:
     * (1) if feCheckpointIdT1 == feCheckpointIdT3 means in [T1, T3], no editlog
     * added for FE side
     * (2) if starMgrCheckpointIdT2 == starMgrCheckpointIdT4 means in [T2, T4], no
     * editlog added for StarMgr side
     * 
     * Because T1 < T2 < T3 < T4, from (1),(2) -> [T2, T3] no editlog added for FE
     * side and StarMgr side
     * So we get the snapshots are feCheckpointIdT3 and starMgrCheckpointIdT2
     */
    private Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() {
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
}
