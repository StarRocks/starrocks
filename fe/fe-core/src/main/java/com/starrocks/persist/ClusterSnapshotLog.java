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
import com.starrocks.common.io.Writable;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;

import java.util.Map;

public class ClusterSnapshotLog implements Writable {
    public enum ClusterSnapshotLogType {
        NONE,
        AUTOMATED_SNAPSHOT_ON,
        AUTOMATED_SNAPSHOT_OFF,
        AUTOMATED_SNAPSHOT_INTERVAL,
        UPDATE_SNAPSHOT_JOB
    }
    @SerializedName(value = "type")
    private ClusterSnapshotLogType type = ClusterSnapshotLogType.NONE;
    @SerializedName(value = "storageVolumeName")
    private String storageVolumeName = "";
    @SerializedName(value = "automatedSnapshotIntervalSeconds")
    private long automatedSnapshotIntervalSeconds = 0;
    @SerializedName(value = "properties")
    private Map<String, String> properties = null;
    // For UPDATE_SNAPSHOT_JOB
    @SerializedName(value = "snapshotJob")
    private ClusterSnapshotJob snapshotJob = null;
    // For AUTOMATED_SNAPSHOT_OFF: also drop the snapshot jobs and requests inherited from the source
    // cluster's image. Encoded as a flag on a record type every released FE knows, so a downgraded FE
    // ignores the unknown field and still turns the automated snapshot off instead of failing replay.
    @SerializedName(value = "resetInheritedSnapshotState")
    private boolean resetInheritedSnapshotState = false;

    public ClusterSnapshotLog() {}

    public void setAutomatedSnapshotOn(String storageVolumeName) {
        setAutomatedSnapshotOn(storageVolumeName, 0, null);
    }

    public void setAutomatedSnapshotOn(String storageVolumeName, long intervalSeconds, Map<String, String> properties) {
        this.type = ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_ON;
        this.storageVolumeName = storageVolumeName;
        this.automatedSnapshotIntervalSeconds = intervalSeconds;
        this.properties = properties;
    }

    public void setAutomatedSnapshotOff() {
        this.type = ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF;
    }

    public void setAutomatedSnapshotInterval(long intervalSeconds) {
        this.type = ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_INTERVAL;
        this.automatedSnapshotIntervalSeconds = intervalSeconds;
    }

    public void resetSnapshotStateAfterExternalRestore() {
        this.type = ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF;
        this.resetInheritedSnapshotState = true;
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

    public long getAutomatedSnapshotIntervalSeconds() {
        return automatedSnapshotIntervalSeconds;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public ClusterSnapshotJob getSnapshotJob() {
        return this.snapshotJob;
    }

    public boolean isResetInheritedSnapshotState() {
        return this.resetInheritedSnapshotState;
    }

}
