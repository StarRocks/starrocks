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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TClusterSnapshotJobsItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

    public ClusterSnapshotJob(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        this.snapshot = new ClusterSnapshot(id, snapshotName, storageVolumeName, createdTimeMs, -1, 0, 0);
        this.state = ClusterSnapshotJobState.INITIALIZING;
        this.errMsg = "";
    }

    public void setState(ClusterSnapshotJobState state) {
        this.state = state;
        if (state == ClusterSnapshotJobState.FINISHED) {
            snapshot.setFinishedTimeMs(System.currentTimeMillis());
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                    .clearFinishedAutomatedClusterSnapshot(getSnapshotName());
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
        item.setDetail_info("");
        item.setError_message(errMsg);
        return item;
    }

    public static ClusterSnapshotJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ClusterSnapshotJob.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
