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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

public class ClusterSnapshotLog implements Writable {
    public enum ClusterSnapshotLogType { NONE, AUTOMATED_SNAPSHOT_ON, AUTOMATED_SNAPSHOT_OFF, UPDATE_SNAPSHOT_JOB }
    @SerializedName(value = "type")
    private ClusterSnapshotLogType type = ClusterSnapshotLogType.NONE;
    @SerializedName(value = "storageVolumeName")
    private String storageVolumeName = "";
    // For UPDATE_SNAPSHOT_JOB
    @SerializedName(value = "snapshotJob")
    private ClusterSnapshotJob snapshotJob = null;

    public ClusterSnapshotLog() {}

    public void setAutomatedSnapshotOn(String storageVolumeName) {
        this.type = ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_ON;
        this.storageVolumeName = storageVolumeName;
    }

    public void setAutomatedSnapshotOff() {
        this.type = ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF;
    }

    public void setSnapshotJob(ClusterSnapshotJob job) {
        this.type = ClusterSnapshotLogType.UPDATE_SNAPSHOT_JOB;
        this.snapshotJob = job;
    }

    public ClusterSnapshotLogType getType() {
        return type;
    }

    public String getStorageVolumeName() {
        return this.storageVolumeName;
    }

    public ClusterSnapshotJob getSnapshotJob() {
        return this.snapshotJob;
    }

    public static ClusterSnapshotLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ClusterSnapshotLog.class);
    }

}
