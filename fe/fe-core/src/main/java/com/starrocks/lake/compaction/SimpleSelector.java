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
