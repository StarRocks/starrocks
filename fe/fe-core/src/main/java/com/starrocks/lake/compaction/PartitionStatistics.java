// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

class PartitionStatistics {
    @SerializedName(value = "partition")
    private final PartitionIdentifier partition;
    @SerializedName(value = "lastCompactionVersion")
    private PartitionVersion lastCompactionVersion;
    @SerializedName(value = "currentVersion")
    private PartitionVersion currentVersion;
    @SerializedName(value = "nextCompactionTime")
    private long nextCompactionTime;

    PartitionStatistics(PartitionIdentifier partition) {
        this.partition = partition;
        this.lastCompactionVersion = null;
        this.nextCompactionTime = 0;
    }

    PartitionIdentifier getPartition() {
        return partition;
    }

    PartitionVersion getCurrentVersion() {
        return currentVersion;
    }

    void setCurrentVersion(PartitionVersion currentVersion) {
        this.currentVersion = currentVersion;
    }

    PartitionVersion getLastCompactionVersion() {
        return lastCompactionVersion;
    }

    void setLastCompactionVersion(PartitionVersion value) {
        lastCompactionVersion = value;
    }

    long getLastCompactionTime() {
        return getLastCompactionVersion().getCreateTime();
    }

    void setNextCompactionTime(long nextCompactionTime) {
        this.nextCompactionTime = nextCompactionTime;
    }

    long getNextCompactionTime() {
        return nextCompactionTime;
    }

    long getDeltaVersions() {
        return getCurrentVersion().getVersion() - getLastCompactionVersion().getVersion();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
