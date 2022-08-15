// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.google.gson.Gson;

class PartitionStatistics {
    private final PartitionIdentifier partition;
    private long lastCompactionTime;
    private long lastCompactionVersion;
    private long currentVersion;
    private long nextCompactionTime;
    private boolean doingCompaction;

    PartitionStatistics(PartitionIdentifier partition, long lastCompactionTime, long lastCompactionVersion, long currentVersion) {
        this.partition = partition;
        this.lastCompactionTime = lastCompactionTime;
        this.lastCompactionVersion = lastCompactionVersion;
        this.currentVersion = currentVersion;
        this.nextCompactionTime = 0;
        this.doingCompaction = false;
    }

    PartitionIdentifier getPartitionId() {
        return partition;
    }

    boolean isDoingCompaction() {
        return doingCompaction;
    }

    void setDoingCompaction(boolean doingCompaction) {
        this.doingCompaction = doingCompaction;
    }

    long getCurrentVersion() {
        return currentVersion;
    }

    void setCurrentVersion(long currentVersion) {
        this.currentVersion = currentVersion;
    }

    long getLastCompactionVersion() {
        return lastCompactionVersion;
    }

    void setLastCompactionVersion(long value) {
        lastCompactionVersion = value;
    }

    long getLastCompactionTime() {
        return lastCompactionTime;
    }

    void setLastCompactionTime(long firstUpdateTime) {
        this.lastCompactionTime = firstUpdateTime;
    }

    void setNextCompactionTime(long nextCompactionTime) {
        this.nextCompactionTime = nextCompactionTime;
    }

    long getNextCompactionTime() {
        return nextCompactionTime;
    }

    long getDeltaVersions() {
        return getCurrentVersion() - getLastCompactionVersion();
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
