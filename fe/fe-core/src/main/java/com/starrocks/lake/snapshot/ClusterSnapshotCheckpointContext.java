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
import com.starrocks.backup.Status;
import com.starrocks.common.Pair;
import com.starrocks.journal.Journal;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Helper class to coordinate CheckpointController between FE and starMgr
// for clould native snapshot backup
public class ClusterSnapshotCheckpointContext {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotCheckpointContext.class);
    public static int INVALID_JOURANL_ID = -1;
    private static int CAPTURE_ID_RETRY_TIME = 10;

    private Journal feJournal;
    private Journal starMgrJournal;
    private long feJouranlId;
    private long starMgrJouranlId;
    private String feImageDir;
    private String starMgrImageDir;

    // runtime param to represent a specified round of coordination
    // between FE and starMgr
    // Round Id is used to represent a sepcified round(FE checkpoint + starMgr checkpoint)
    // curRoundId means the lastest round, curFERoundId means lastest finished/error round
    // for FE checkpoint, similar with curStarMgrRoundId.
    // These round id mainly used to coordinate the behavior between two checkpoint thread
    // in a lock-free way.
    private long curRoundId;
    private long curFERoundId;
    private long curStarMgrRoundId;
    private String curErrMsg;

    // runtime param to represent automated snapshot job
    private ClusterSnapshotJob job;

    // Synchronization flag
    Boolean belongToGlobalStateMgrForCancelRound;

    public ClusterSnapshotCheckpointContext() {
        this.feJournal = null;
        this.starMgrJournal = null;
        this.feJouranlId = INVALID_JOURANL_ID;
        this.starMgrJouranlId = INVALID_JOURANL_ID;
        this.curRoundId = 0;
        this.curFERoundId = 0;
        this.curStarMgrRoundId = 0;
        this.curErrMsg = "";
        this.job = null;
        this.belongToGlobalStateMgrForCancelRound = null;
    }

    public void setJournal(Journal journal, boolean belongToGlobalStateMgr) {
        if (!RunMode.isSharedDataMode()) {
            return;
        }

        if (belongToGlobalStateMgr) {
            this.feJournal = journal;
        } else {
            this.starMgrJournal = journal;
        }
    }

    public void setImageDir(String imageDir, boolean belongToGlobalStateMgr) {
        if (!RunMode.isSharedDataMode()) {
            return;
        }

        if (belongToGlobalStateMgr) {
            this.feImageDir = imageDir;
        } else {
            this.starMgrImageDir = imageDir;
        }
    }

    public synchronized long tryToAcquireCheckpointId(boolean belongToGlobalStateMgr, boolean allowToAcquireNewId) {
        // step 1: if the checkpoint id has been allocated by the peer, use it
        if (belongToGlobalStateMgr && this.feJouranlId != INVALID_JOURANL_ID) {
            long id = this.feJouranlId;
            this.feJouranlId = INVALID_JOURANL_ID;
            return id;
        } else if (!belongToGlobalStateMgr && this.starMgrJouranlId != INVALID_JOURANL_ID) {
            long id = this.starMgrJouranlId;
            this.starMgrJouranlId = INVALID_JOURANL_ID;
            return id;
        }

        // step 2: check the last self checkpoint and upload are finished or not

        // last self checkpoint or upload image failed, totally reset and try in a new round
        // because in this case, it is unretryable to continue checkpoint/upload the last image by design.
        // In this case, we should wait until the peer thread arrive in tryToAcquireCheckpointId and then
        // reset all context information by the peer.
        if (belongToGlobalStateMgrForCancelRound != null) {
            if ((belongToGlobalStateMgrForCancelRound.booleanValue() && belongToGlobalStateMgr) ||
                    (!belongToGlobalStateMgrForCancelRound.booleanValue() && !belongToGlobalStateMgr)) {
                resetContext(true);
            } else {
                // wait the peer to reset the context
                return INVALID_JOURANL_ID; 
            }
        } else if (belongToGlobalStateMgr && curFERoundId != curRoundId ||
                   !belongToGlobalStateMgr && curStarMgrRoundId != curRoundId) {
            // reset context by peer
            belongToGlobalStateMgrForCancelRound = !belongToGlobalStateMgr;
            return INVALID_JOURANL_ID;
        }

        // step 3: check the peer finished last round or not
        if (belongToGlobalStateMgr && curStarMgrRoundId != curRoundId) {
            // last checkpoint for starMgr has not been finished, wait
            return INVALID_JOURANL_ID;
        } else if (!belongToGlobalStateMgr && curFERoundId != curRoundId) {
            // last checkpoint for FE has not been finished, wait
            return INVALID_JOURANL_ID;
        }

        if (!allowToAcquireNewId) {
            // not allow to acquire new id for now
            return INVALID_JOURANL_ID;
        }

        // step 4: capture consistent id
        Preconditions.checkState(this.feJouranlId == INVALID_JOURANL_ID);
        Preconditions.checkState(this.starMgrJouranlId == INVALID_JOURANL_ID);
        Preconditions.checkState(this.curFERoundId == this.curStarMgrRoundId);
        Preconditions.checkState(this.curFERoundId == this.curRoundId);

        if (job == null) {
            job = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                                                  .createNewAutomatedSnapshotJob(); /* INITIALIZING state */
        }

        Pair<Long, Long> p = captureConsistentCheckpointIdBetweenFEAndStarMgr();
        if (p == null) {
            // retry if fail to capture consistent id
            return INVALID_JOURANL_ID;
        }

        // step 5: new round begin
        curRoundId++;
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        job.setJournalIds(p.first, p.second);
        if (belongToGlobalStateMgr) {
            this.starMgrJouranlId = p.second;
            return p.first;
        } else {
            this.feJouranlId = p.first;
            return p.second;
        }
    }

    public boolean uploadImageForSnapshot(boolean belongToGlobalStateMgr) {
        Preconditions.checkState(job != null);
        String localMetaDir = belongToGlobalStateMgr ? feImageDir : starMgrImageDir;
        LOG.info("Begin upload snapshot for image for {}", belongToGlobalStateMgr ? "FE image" : "StarMgr image");
        Status st = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                    .actualUploadImageForSnapshot(belongToGlobalStateMgr, job.getSnapshotName(), localMetaDir);
        if (!st.ok()) {
            LOG.warn("upload snapshot for image for {} has failed: {}", belongToGlobalStateMgr ? "FE image" : "StarMgr image",
                     st.getErrMsg());
            setCurErrMsg(st.getErrMsg());
            // The failure should be handled by tryToAcquireCheckpointId
            return false;
        }

        LOG.info("Finished upload snapshot for image for {}", belongToGlobalStateMgr ? "FE image" : "StarMgr image");
        return true;
    }

    // If the caller is the laggard in current round, this function will set the job
    // into finished state to create a successful snapshot. This function will also update the round id.
    public synchronized void updateRoundIdAndMarkJobFinishedFromLaggard(boolean belongToGlobalStateMgr) {
        boolean isLaggard = false;
        String finishLog = "";
        if ((curFERoundId == curRoundId && !belongToGlobalStateMgr) ||
                (curStarMgrRoundId == curRoundId && belongToGlobalStateMgr)) {
            // all images have been finished
            isLaggard = true;
            finishLog += "Finish upload all image file for snapshot, ";
            finishLog += "FE jouranl id: " + String.valueOf(job.getFeJournalId()) + " ";
            finishLog += "starMgr jouranl id: " + String.valueOf(job.getStarMgrJournalId());

            // finish job and create cluster snapshot into Mgr
            job.setState(ClusterSnapshotJobState.FINISHED);
            resetContext(false);
        }

        // update FE/starMgr round id if the all step has been finished
        // order is matter, round id must be updated after set finish
        if (belongToGlobalStateMgr) {
            curFERoundId = curRoundId;
        } else {
            curStarMgrRoundId = curRoundId;
        }

        if (isLaggard) {
            LOG.info(finishLog);
        }
    }

    public synchronized void setCurErrMsg(String curErrMsg) {
        this.curErrMsg = curErrMsg;
    }

    private void resetContext(boolean error) {
        if (error) {
            job.setState(ClusterSnapshotJobState.ERROR);
            job.setErrMsg(curErrMsg);

            // the following vars will be reset if no error happened
            curFERoundId = curRoundId;
            curStarMgrRoundId = curRoundId;
            feJouranlId = INVALID_JOURANL_ID;
            starMgrJouranlId = INVALID_JOURANL_ID;
        } else {
            Preconditions.checkState(curFERoundId == curRoundId || curStarMgrRoundId == curRoundId);
            Preconditions.checkState(feJouranlId == INVALID_JOURANL_ID);
            Preconditions.checkState(starMgrJouranlId == INVALID_JOURANL_ID);
        }

        job = null;
        curErrMsg = "";
        belongToGlobalStateMgrForCancelRound = null;
    }

    /*
     * Definition of consistent: Suppose there are two images generated by FE and StarMgr, call FEImageNew
     * and StarMgrImageNew and satisfy:
     * FEImageNew = FEImageOld + editlog(i) + ... + editlog(j)
     * StarMgrImageNew = StarMgrImageOld + editlog(k) + ... + editlog(m)
     * 
     * Define Tj = generated time of editlog(j), Tmax = max(Tj, Tm)
     * Consistency means all editlogs generated before Tmax (no matter the editlog is belong to FE or starMgr)
     * should be included in the image generated by checkpoint.
     * In other words, there must be no holes before the `maximum` editlog contained in the two images
     * generated by checkpoint.
     * 
     * How to get the consistent id: because editlog is generated and flush in a synchronous way, so we can simply
     * get the `snapshot` of maxJouranlId for both FE side and StarMgr side.
     * We get the `snapshot` in a lock-free way. As shown in the code below:
     * (1) if feCheckpointIdT1 == feCheckpointIdT3 means in [T1, T3], no editlog added for FE side
     * (2) if starMgrCheckpointIdT2 == starMgrCheckpointIdT4 means in [T2, T4], no editlog added for StarMgr side
     * 
     * Because T1 < T2 < T3 < T4, from (1),(2) -> [T2, T3] no editlog added for FE side and StarMgr side
     * So we get the snapshots are feCheckpointIdT3 and starMgrCheckpointIdT2
    */
    private Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr() {
        if (feJournal == null || starMgrJournal == null || feImageDir == null || starMgrImageDir == null) {
            return null;
        }

        int retryTime = CAPTURE_ID_RETRY_TIME;
        while (retryTime > 0) {
            long feCheckpointIdT1 = feJournal.getMaxJournalId();
            long starMgrCheckpointIdT2 = starMgrJournal.getMaxJournalId();
            long feCheckpointIdT3 = feJournal.getMaxJournalId();
            long starMgrCheckpointIdT4 = starMgrJournal.getMaxJournalId();
    
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