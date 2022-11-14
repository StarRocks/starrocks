// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake.compaction;

import java.util.Collection;
import java.util.List;
import javax.validation.constraints.NotNull;

public interface Selector {
    @NotNull
    List<PartitionStatistics> select(@NotNull Collection<PartitionStatistics> statistics);
}
