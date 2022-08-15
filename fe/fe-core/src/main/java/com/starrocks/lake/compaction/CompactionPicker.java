// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

public interface CompactionPicker {
    PartitionStatistics pick(Iterable<PartitionStatistics> statistics);
}
