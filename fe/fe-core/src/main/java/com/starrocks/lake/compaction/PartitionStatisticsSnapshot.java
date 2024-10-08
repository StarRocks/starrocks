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

import javax.validation.constraints.NotNull;

// simplified version of `PartitionStatistics`, only contains necessary copied info for compaction sorter
public class PartitionStatisticsSnapshot {
    private final PartitionIdentifier partition;
    // deep copy
    private final PartitionStatistics.CompactionPriority priority;
    // deep copy
    private final Quantiles compactionScore;

    public PartitionStatisticsSnapshot(@NotNull PartitionStatistics ps) {
        this.partition = ps.getPartition();
        this.priority = ps.getPriority();
        this.compactionScore = new Quantiles(ps.getCompactionScore());
    }

    PartitionIdentifier getPartition() {
        return partition;
    }

    PartitionStatistics.CompactionPriority getPriority() {
        return priority;
    }

    Quantiles getCompactionScore() {
        return compactionScore;
    }
}
