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

import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TClusterSnapshotJobsItem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterSnapshotJob implements Writable {
    /*
     * INITIALIZING: INIT state for the snapshot.
     * SNAPSHOTING: Doing checkpoint/image generation by replaying log for image both for FE and StarMgr and
     *              then upload the image into remote storage
     * FINISHED: Finish backup snapshot                 
     */
    public enum ClusterSnapshotJobState { INITIALIZING, SNAPSHOTING, FINISHED, ERROR }

    private long jobId;
    private String snapshotNamePrefix;
    private String snapshotName;
    private String storageVolumeName;
    private long createTime;
    private long successTime;
    private long feJournalId;
    private long starMgrJournalId;
    private ClusterSnapshotJobState state;
    private String errMsg;

    public ClusterSnapshotJob(long jobId, String snapshotNamePrefix, String snapshotName, String storageVolumeName) {
        this.jobId = jobId;
        this.snapshotNamePrefix = snapshotNamePrefix;
        this.snapshotName = snapshotName;
        this.storageVolumeName = storageVolumeName;
        this.createTime = System.currentTimeMillis();
        this.successTime = -1;
        this.feJournalId = 0;
        this.starMgrJournalId = 0;
        this.state = ClusterSnapshotJobState.INITIALIZING;
        this.errMsg = "";
    }

    public void setState(ClusterSnapshotJobState state) {
        if (state == ClusterSnapshotJobState.INITIALIZING) {
            return; /* should not happen */
        }

        this.state = state;
        if (state == ClusterSnapshotJobState.SNAPSHOTING || state == ClusterSnapshotJobState.ERROR) {
            return;
        }

        if (state == ClusterSnapshotJobState.FINISHED) {
            this.successTime = System.currentTimeMillis();
            createSnapshotIfJobIsFinished();
        }
    }

    public void setJournalIds(long feJournalId, long starMgrJournalId) {
        this.feJournalId = feJournalId;
        this.starMgrJournalId = starMgrJournalId;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getSnapshotNamePrefix() {
        return snapshotNamePrefix;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getStorageVolumeName() {
        return storageVolumeName;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getSuccessTime() {
        return successTime;
    }

    public long getFeJournalId() {
        return feJournalId;
    }

    public long getStarMgrJournalId() {
        return starMgrJournalId;
    }

    public long getJobId() {
        return jobId;
    }

    public TClusterSnapshotJobsItem getInfo() {
        TClusterSnapshotJobsItem item = new TClusterSnapshotJobsItem();
        item.setSnapshot_name(snapshotName);
        item.setJob_id(jobId);
        item.setCreated_time(createTime);
        item.setFinished_time(successTime);
        item.setState(state.name());
        item.setDetail_info("");
        item.setError_message(errMsg);
        return item;
    }

    private void createSnapshotIfJobIsFinished() {
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().createAutomatedSnaphot(this, false);
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
