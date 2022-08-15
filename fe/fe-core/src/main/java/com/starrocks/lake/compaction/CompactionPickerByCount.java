// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

public class CompactionPickerByCount implements CompactionPicker {
    private final long minCompactionVersionCount;

    CompactionPickerByCount(long minCompactionVersionCount) {
        this.minCompactionVersionCount = minCompactionVersionCount;
    }

    @Override
    public PartitionStatistics pick(Iterable<PartitionStatistics> statistics) {
        long now = System.currentTimeMillis();
        PartitionStatistics target = null;
        for (PartitionStatistics candidate : statistics) {
            if (candidate.isDoingCompaction()
                    || candidate.getDeltaVersions() < minCompactionVersionCount
                    || candidate.getNextCompactionTime() > now) {
                continue;
            }
            if (target == null || candidate.getDeltaVersions() > target.getDeltaVersions()) {
                target = candidate;
            }
        }
        return target;
    }
}
