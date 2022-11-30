// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
