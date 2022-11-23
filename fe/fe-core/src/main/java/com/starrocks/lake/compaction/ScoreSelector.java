// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import com.starrocks.common.Config;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class ScoreSelector implements Selector {

    @Override
    @NotNull
    public List<PartitionStatistics> select(@NotNull Collection<PartitionStatistics> statistics) {
        double minScore = Config.lake_compaction_score_selector_min_score;
        long now = System.currentTimeMillis();
        return statistics.stream()
                .filter(p -> p.getNextCompactionTime() <= now)
                .filter(p -> p.getCompactionScore() != null)
                .filter(p -> p.getCompactionScore().getAvg() >= minScore || p.getCompactionScore().getP50() >= minScore)
                .collect(Collectors.toList());
    }
}
