// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.lake.compaction;

import com.starrocks.common.Config;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class ScoreSelector implements Selector {
    @Override
    @NotNull
    public List<PartitionStatisticsSnapshot> select(@NotNull Collection<PartitionStatistics> statistics,
            @NotNull Set<Long> excludeTableOrPartition) {
        double minScore = Config.lake_compaction_score_selector_min_score;
        long now = System.currentTimeMillis();
        return statistics.stream()
                .filter(p -> p.getCompactionScore() != null)
                .filter(p -> !excludeTableOrPartition.contains(p.getPartition().getTableId()))
                .filter(p -> !excludeTableOrPartition.contains(p.getPartition().getPartitionId()))
                // A priority request (manual or UNSHARE) skips the score checks but still honors the retry
                // deadline: the trigger clears it so a new request runs at once, while a request the scheduler had
                // to defer (e.g. the table is under alter) waits it out instead of being retried every cycle.
                // An automatic request needs a positive score of at least the min score.
                .filter(p -> p.getNextCompactionTime() <= now)
                .filter(p -> p.getPriority() != PartitionStatistics.CompactionPriority.DEFAULT
                        || (p.getCompactionScore().getMax() > 0 && p.getCompactionScore().getMax() >= minScore))
                .map(p -> {
                    return p.getSnapshot(); })
                .collect(Collectors.toList());
    }
}
