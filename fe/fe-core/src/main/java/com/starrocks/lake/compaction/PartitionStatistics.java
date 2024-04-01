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


package com.starrocks.lake.compaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Nullable;

public class PartitionStatistics {
    @SerializedName(value = "partition")
    private final PartitionIdentifier partition;
    @SerializedName(value = "compactionVersion")
    private PartitionVersion compactionVersion;
    @SerializedName(value = "currentVersion")
    private PartitionVersion currentVersion;
    @SerializedName(value = "nextCompactionTime")
    private long nextCompactionTime;
    @SerializedName(value = "compactionScore")
    private Quantiles compactionScore;

    // default priority is 0, manual compaction will have priority value 1
    @SerializedName(value = "priority")
    private volatile CompactionPriority priority = CompactionPriority.DEFAULT;

    public enum CompactionPriority {
        DEFAULT(0),
        MANUAL_COMPACT(1);

        private final int value;

        CompactionPriority(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public PartitionStatistics(PartitionIdentifier partition) {
        this.partition = partition;
        this.compactionVersion = null;
        this.nextCompactionTime = 0;
    }

    public PartitionIdentifier getPartition() {
        return partition;
    }

    public PartitionVersion getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(PartitionVersion currentVersion) {
        this.currentVersion = currentVersion;
    }

    public PartitionVersion getCompactionVersion() {
        return compactionVersion;
    }

    public void setCompactionVersion(PartitionVersion value) {
        compactionVersion = value;
    }

    public long getLastCompactionTime() {
        return getCompactionVersion().getCreateTime();
    }

    public void setNextCompactionTime(long nextCompactionTime) {
        this.nextCompactionTime = nextCompactionTime;
    }

    public long getNextCompactionTime() {
        return nextCompactionTime;
    }

    public long getDeltaVersions() {
        return getCurrentVersion().getVersion() - getCompactionVersion().getVersion();
    }

    public void setCompactionScore(@Nullable Quantiles compactionScore) {
        this.compactionScore = compactionScore;
    }

    @Nullable
    public Quantiles getCompactionScore() {
        return compactionScore;
    }

    public CompactionPriority getPriority() {
        // For backward compatibility
        // prevent null value when deserializing JSON that doesn't include the priority field
        return priority == null ? CompactionPriority.DEFAULT : priority;
    }

    public void setPriority(CompactionPriority priority) {
        this.priority = priority;
    }

    public void resetPriority() {
        this.setPriority(CompactionPriority.DEFAULT);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}

