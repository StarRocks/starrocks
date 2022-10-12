// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.starrocks.common.Config;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class SimpleSelector implements Selector {
    private final long minVersionsToCompact;
    private final long thresholdVersions;
    private final long thresholdMilliseconds;

    public SimpleSelector() {
        minVersionsToCompact = Config.lake_compaction_simple_selector_min_versions;
        thresholdVersions = Config.lake_compaction_simple_selector_threshold_versions;
        thresholdMilliseconds = Config.lake_compaction_simple_selector_threshold_seconds * 1000;
    }

    @Override
    @NotNull
    public List<PartitionStatistics> select(Collection<PartitionStatistics> statistics) {
        long now = System.currentTimeMillis();
        return statistics.stream()
                .filter(p -> p.getNextCompactionTime() <= now)
                .filter(p -> isReadyForCompaction(p, now))
                .collect(Collectors.toList());
    }

    private boolean isReadyForCompaction(PartitionStatistics statistics, long currentTs) {
        if (statistics.getDeltaVersions() < minVersionsToCompact) {
            return false;
        }
        return statistics.getDeltaVersions() >= thresholdVersions ||
                (currentTs - statistics.getLastCompactionTime()) >= thresholdMilliseconds;
    }
}
