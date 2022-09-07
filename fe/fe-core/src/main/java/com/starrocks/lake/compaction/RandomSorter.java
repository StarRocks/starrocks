// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;

public class RandomSorter implements Sorter {
    public RandomSorter() {
    }

    @NotNull
    public List<PartitionStatistics> sort(@NotNull List<PartitionStatistics> partitionStatistics) {
        Collections.shuffle(partitionStatistics);
        return partitionStatistics;
    }
}
