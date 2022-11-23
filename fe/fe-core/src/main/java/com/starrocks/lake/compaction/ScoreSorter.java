// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class ScoreSorter implements Sorter {
    @Override
    @NotNull
    public List<PartitionStatistics> sort(@NotNull List<PartitionStatistics> partitionStatistics) {
        return partitionStatistics.stream()
                .filter(p -> p.getCompactionScore() != null)
                .sorted(Comparator.comparing(PartitionStatistics::getCompactionScore).reversed())
                .collect(Collectors.toList());
    }
}
