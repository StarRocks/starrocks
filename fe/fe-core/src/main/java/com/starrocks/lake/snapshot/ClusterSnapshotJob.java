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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TClusterSnapshotJobsItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterSnapshotJob implements Writable {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotJob.class);

    /*
     * INITIALIZING: INIT state for the snapshot.
     * SNAPSHOTING: Doing checkpoint/image generation by replaying log for image
     * both for FE and StarMgr and
     * then upload the image into remote storage
     * UPLOADING: Uploading image file into remote storage
     * FINISHED: Finish backup snapshot
     * EXPIRED: Not the latest finished backup snapshot
     * DELETED: Not the lastest finished backup snapshot and the cluster snapshot
     * has been deleted from remote
     */
    public enum ClusterSnapshotJobState {
        INITIALIZING, SNAPSHOTING, UPLOADING, FINISHED, EXPIRED, DELETED, ERROR
    }

    @SerializedName(value = "snapshot")
    private ClusterSnapshot snapshot;
    @SerializedName(value = "state")
    private ClusterSnapshotJobState state;
    @SerializedName(value = "errMsg")
    private String errMsg;
    @SerializedName(value = "detailInfo")
    private String detailInfo;

    public ClusterSnapshotJob(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        this.snapshot = createClusterSnapshot(id, snapshotName, storageVolumeName, createdTimeMs);
        this.state = ClusterSnapshotJobState.INITIALIZING;
        this.errMsg = "";
        this.detailInfo = "";
    }

    protected ClusterSnapshot createClusterSnapshot(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        return new ClusterSnapshot(id, snapshotName, ClusterSnapshot.ClusterSnapshotType.AUTOMATED,
                    storageVolumeName, createdTimeMs, -1, 0, 0);
    }

    public void setState(ClusterSnapshotJobState state) {
        this.state = state;
        if (state == ClusterSnapshotJobState.FINISHED) {
            snapshot.setFinishedTimeMs(System.currentTimeMillis());
            if (isAutomated()) {
                GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                        .clearFinishedAutomatedClusterSnapshot(getSnapshotName());
            }
        }
    }

    public void setJournalIds(long feJournalId, long starMgrJournalId) {
        snapshot.setJournalIds(feJournalId, starMgrJournalId);
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getSnapshotName() {
        return snapshot.getSnapshotName();
    }

    public String getStorageVolumeName() {
        return snapshot.getStorageVolumeName();
    }

    public long getCreatedTimeMs() {
        return snapshot.getCreatedTimeMs();
    }

    public long getFinishedTimeMs() {
        return snapshot.getFinishedTimeMs();
    }

    public long getFeJournalId() {
        return snapshot.getFeJournalId();
    }

    public long getStarMgrJournalId() {
        return snapshot.getStarMgrJournalId();
    }

    public long getId() {
        return snapshot.getId();
    }

    public ClusterSnapshot getSnapshot() {
        return snapshot;
    }

    public ClusterSnapshotJobState getState() {
        return state;
    }

    public boolean isUnFinishedState() {
        return state == ClusterSnapshotJobState.INITIALIZING ||
                state == ClusterSnapshotJobState.SNAPSHOTING ||
                state == ClusterSnapshotJobState.UPLOADING;
    }

    public boolean isInitializing() {
        return state == ClusterSnapshotJobState.INITIALIZING;
    }

    public boolean isUploading() {
        return state == ClusterSnapshotJobState.UPLOADING;
    }

    public boolean isError() {
        return state == ClusterSnapshotJobState.ERROR;
    }

    public boolean isFinished() {
        return state == ClusterSnapshotJobState.FINISHED;
    }

    public boolean isExpired() {
        return state == ClusterSnapshotJobState.EXPIRED;
    }

    public boolean isDeleted() {
        return state == ClusterSnapshotJobState.DELETED;
    }

    public boolean isFinalState() {
        return state == ClusterSnapshotJobState.DELETED || state == ClusterSnapshotJobState.ERROR;
    }

    public void setDetailInfo(String detailInfo) {
        this.detailInfo = detailInfo;
    }

    public boolean needClusterSnapshotInfo() {
        return false;
    }

    public boolean isAutomated() {
        return snapshot.isAutomated();
    }

    public void setClusterSnapshotInfo(ClusterSnapshotInfo clusterSnapshotInfo) {
        snapshot.setClusterSnapshotInfo(clusterSnapshotInfo);
    }

    public void logJob() {
        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setSnapshotJob(this);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    public TClusterSnapshotJobsItem getInfo() {
        TClusterSnapshotJobsItem item = new TClusterSnapshotJobsItem();
        item.setSnapshot_name(getSnapshotName());
        item.setJob_id(getId());
        item.setCreated_time(getCreatedTimeMs() / 1000);
        item.setFinished_time(getFinishedTimeMs() / 1000);
        item.setState(state.name());
        item.setDetail_info(detailInfo);
        item.setError_message(errMsg);
        return item;
    }

    /**
     * Default implementation for meta-only snapshot jobs (ClusterSnapshotJob and ManualClusterSnapshotJob).
     * FullClusterSnapshotJob should override this method if it needs different initialization logic.
     */
    protected void runInitializingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.INITIALIZING, state);
        LOG.info("begin to initialize cluster snapshot job. job: {}", getId());

        Pair<Long, Long> consistentIds = context.captureConsistentCheckpointIdBetweenFEAndStarMgr();
        if (consistentIds == null) {
            throw new StarRocksException("failed to capture consistent journal id for checkpoint");
        }
        setJournalIds(consistentIds.first, consistentIds.second);
        LOG.info(
                "Successful capture consistent journal id, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                consistentIds.first, consistentIds.second);

        setState(ClusterSnapshotJobState.SNAPSHOTING);
        logJob();
    }

    /**
     * Default implementation for meta-only snapshot jobs (ClusterSnapshotJob and ManualClusterSnapshotJob).
     * FullClusterSnapshotJob should override this method if it needs different snapshotting logic.
     */
    protected void runSnapshottingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.SNAPSHOTING, state);
        LOG.info("begin to snapshot cluster snapshot job. job: {}", getId());

        CheckpointController feController = context.getFeController();
        CheckpointController starMgrController = context.getStarMgrController();
        long feCheckpointJournalId = getFeJournalId();
        long starMgrCheckpointJournalId = getStarMgrJournalId();

        long feImageJournalId = feController.getImageJournalId();
        if (feImageJournalId < feCheckpointJournalId) {
            Pair<Boolean, String> createFEImageRet = feController.runCheckpointControllerWithIds(feImageJournalId,
                    feCheckpointJournalId, needClusterSnapshotInfo());
            if (!createFEImageRet.first) {
                throw new StarRocksException("checkpoint failed for FE image: " + createFEImageRet.second);
            }
        } else if (feImageJournalId > feCheckpointJournalId) {
            throw new StarRocksException("checkpoint journal id for FE is smaller than image version");
        }
        LOG.info("Finished create image for FE image, version: {}", feCheckpointJournalId);

        long starMgrImageJournalId = starMgrController.getImageJournalId();
        if (starMgrImageJournalId < starMgrCheckpointJournalId) {
            Pair<Boolean, String> createStarMgrImageRet = starMgrController
                    .runCheckpointControllerWithIds(starMgrImageJournalId, starMgrCheckpointJournalId, false);
            if (!createStarMgrImageRet.first) {
                throw new StarRocksException("checkpoint failed for starMgr image: " + createStarMgrImageRet.second);
            }
        } else if (starMgrImageJournalId > starMgrCheckpointJournalId) {
            throw new StarRocksException("checkpoint journal id for starMgr is smaller than image version");
        }
        setClusterSnapshotInfo(feController.getClusterSnapshotInfo());
        setState(ClusterSnapshotJobState.UPLOADING);
        logJob();
        LOG.info("Finished create image for starMgr image, version: {}", starMgrCheckpointJournalId);
    }

    /**
     * Default implementation for meta-only snapshot jobs (ClusterSnapshotJob and ManualClusterSnapshotJob).
     * FullClusterSnapshotJob should override this method if it needs different uploading logic.
     */
    protected void runUploadingJob(SnapshotJobContext context) throws StarRocksException {
        Preconditions.checkState(state == ClusterSnapshotJobState.UPLOADING, state);
        LOG.info("begin to upload cluster snapshot job. job: {}", getId());

        try {
            ClusterSnapshotUtils.uploadClusterSnapshotToRemote(this);
        } catch (StarRocksException e) {
            throw new StarRocksException("upload image failed, err msg: " + e.getMessage());
        }
        setState(ClusterSnapshotJobState.FINISHED);
        logJob();
        LOG.info(
                "Finish upload image for Cluster Snapshot, FE checkpoint journal Id: {}, StarMgr checkpoint journal Id: {}",
                getFeJournalId(), getStarMgrJournalId());
    }

    protected void runFinishedJob() throws StarRocksException{
        // Default implementation: do nothing
    }

    public synchronized void run(SnapshotJobContext context) {
        try {
            while (true) {
                ClusterSnapshotJobState prevState = state;
                switch (prevState) {
                    case INITIALIZING:
                        runInitializingJob(context);
                        break;
                    case SNAPSHOTING:
                        runSnapshottingJob(context);
                        break;
                    case UPLOADING:
                        runUploadingJob(context);
                        break;
                    case FINISHED:
                    case EXPIRED:
                    case DELETED:
                    case ERROR:
                        runFinishedJob();
                        break;
                    default:
                        break;
                }
                if (state == prevState) {
                    break;
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to run cluster snapshot job {}", getId(), e);
            setState(ClusterSnapshotJobState.ERROR);
            setErrMsg(e.getMessage());
            logJob();
            return;
        }
    }

}
