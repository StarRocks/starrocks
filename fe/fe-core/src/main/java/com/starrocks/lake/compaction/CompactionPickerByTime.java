// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

public class CompactionPickerByTime implements CompactionPicker {
    private final long maxCompactionInterval;
    private final long minCompactionVersionCount;

    CompactionPickerByTime(long maxCompactionInterval, long minCompactionVersionCount) {
        this.maxCompactionInterval = maxCompactionInterval;
        this.minCompactionVersionCount = minCompactionVersionCount;
    }

    @Override
    public PartitionStatistics pick(Iterable<PartitionStatistics> statistics) {
        long now = System.currentTimeMillis();
        PartitionStatistics target = null;
        long maxTimeDiff = 0;
        for (PartitionStatistics candidate : statistics) {
            if (candidate.isDoingCompaction()
                    || candidate.getDeltaVersions() < minCompactionVersionCount
                    || candidate.getNextCompactionTime() > now
                    || now - candidate.getLastCompactionTime() < maxCompactionInterval) {
                continue;
            }
            long timeDiff = now - candidate.getLastCompactionTime();
            if (target == null || timeDiff > maxTimeDiff) {
                target = candidate;
                maxTimeDiff = timeDiff;
            }
        }
        return target;
    }
}
