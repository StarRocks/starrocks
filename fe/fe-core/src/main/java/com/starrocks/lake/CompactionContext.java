// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

class CompactionContext {
    private PartitionIdentifier partition;
    private long firstUpdateTime;
    private long versionUpdateCount;

    CompactionContext(PartitionIdentifier partition) {
        this(partition, System.currentTimeMillis());
    }

    CompactionContext(PartitionIdentifier partition, long firstUpdateTime) {
        this(partition, firstUpdateTime, 0);
    }

    CompactionContext(PartitionIdentifier partition, long firstUpdateTime, long updateCount) {
        this.partition = partition;
        this.firstUpdateTime = firstUpdateTime;
        this.versionUpdateCount = updateCount;
    }

    synchronized long getUpdateCount() {
        return versionUpdateCount;
    }

    synchronized long addUpdateCount(long value) {
        versionUpdateCount += value;
        return versionUpdateCount;
    }

    synchronized long subUpdateCount(long value) {
        versionUpdateCount -= value;
        return versionUpdateCount;
    }

    long getFirstUpdateTime() {
        return firstUpdateTime;
    }

    synchronized void setFirstUpdateTime(long firstUpdateTime) {
        this.firstUpdateTime = firstUpdateTime;
    }

    PartitionIdentifier getPartition() {
        return partition;
    }
}
