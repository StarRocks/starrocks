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

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.snapshot.ManualClusterSnapshotJob;
import com.starrocks.lake.snapshot.ManualClusterSnapshotRequest;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

public class ManualClusterSnapshotLog implements Writable {
    public enum ManualClusterSnapshotLogType { NONE, ADD_MANUAL_REQUEST, DROP_MANUAL_JOB, UPDATE_SNAPSHOT_JOB }
    @SerializedName(value = "type")
    private ManualClusterSnapshotLogType type = ManualClusterSnapshotLogType.NONE;
    @SerializedName(value = "manualSnapshotJob")
    private ManualClusterSnapshotJob manualSnapshotJob;
    @SerializedName(value = "manualSnapshotRequest")
    private ManualClusterSnapshotRequest manualSnapshotRequest;
    @SerializedName(value = "dropClusterSnapshotName")
    private String dropClusterSnapshotName = "";

    public ManualClusterSnapshotLog() {
        super();
    }

    public void setAddManualRequest(ManualClusterSnapshotRequest request) {
        this.type = ManualClusterSnapshotLogType.ADD_MANUAL_REQUEST;
        this.manualSnapshotRequest = request;
    }

    public void setDropManualJob(String snapshotName) {
        this.type = ManualClusterSnapshotLogType.DROP_MANUAL_JOB;
        this.dropClusterSnapshotName = snapshotName;
    }

    public void setSnapshotJob(ManualClusterSnapshotJob job) {
        this.type = ManualClusterSnapshotLogType.UPDATE_SNAPSHOT_JOB;
        this.manualSnapshotJob = job;
    }

    public ManualClusterSnapshotLogType getType() {
        return type;
    }

    public String getDropClusterSnapshotName() {
        return this.dropClusterSnapshotName;
    }

    public ManualClusterSnapshotJob getManualSnapshotJob() {
        return this.manualSnapshotJob;
    }

    public ManualClusterSnapshotRequest getManualSnapshotRequest() {
        return this.manualSnapshotRequest;
    }

    public static ManualClusterSnapshotLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ManualClusterSnapshotLog.class);
    }
}
