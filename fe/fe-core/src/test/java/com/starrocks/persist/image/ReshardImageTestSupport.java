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

package com.starrocks.persist.image;

import com.starrocks.alter.reshard.IdenticalTablet;
import com.starrocks.alter.reshard.MergeTabletJob;
import com.starrocks.alter.reshard.MergingTablet;
import com.starrocks.alter.reshard.ReshardingMaterializedIndex;
import com.starrocks.alter.reshard.ReshardingPhysicalPartition;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.SplitTabletJob;
import com.starrocks.alter.reshard.SplittingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.proto.ReshardingTabletPB;
import com.starrocks.persist.proto.TabletReshardJobPB;
import com.starrocks.type.IntegerType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.setField;

final class ReshardImageTestSupport {
    private ReshardImageTestSupport() {
    }

    static TabletRange range(Long lower, Long upper) {
        return new TabletRange(Range.of(lower == null ? null : new Tuple(List.of(new IntVariant(IntegerType.INT, lower))),
                upper == null ? null : new Tuple(List.of(new IntVariant(IntegerType.INT, upper))), lower != null, false));
    }

    static TabletReshardJob job(long id, boolean split, TabletReshardJob.JobState state) {
        long base = id * 1000;
        ReshardingTablet main = split
                ? new SplittingTablet(base + 10, List.of(base + 11, base + 12), List.of(range(null, 50L), range(50L, null)))
                : new MergingTablet(new ArrayList<>(List.of(base + 10, base + 12)), base + 11);
        MaterializedIndex index = new MaterializedIndex(base + 2, base + 3, MaterializedIndex.IndexState.NORMAL, base + 4);
        index.addTablet(new LakeTablet(base + 11, range(null, 50L)), null, false);
        index.addTablet(new LakeTablet(base + 12, range(50L, null)), null, false);
        ReshardingMaterializedIndex reshardingIndex = new ReshardingMaterializedIndex(base + 2, index,
                new ArrayList<>(List.of(main, new IdenticalTablet(base + 20, base + 21))));
        Map<Long, ReshardingMaterializedIndex> indexes = new HashMap<>();
        indexes.put(base + 2, reshardingIndex);
        ReshardingPhysicalPartition partition = new ReshardingPhysicalPartition(base + 1, indexes);
        partition.setCommitVersion(100 + id);
        Map<Long, ReshardingPhysicalPartition> partitions = new HashMap<>();
        partitions.put(base + 1, partition);
        TabletReshardJob job = split ? new SplitTabletJob(id, 100, 200, partitions)
                : new MergeTabletJob(id, 100, 200, partitions);
        setField(job, "jobState", state);
        setField(job, "createdTimeMs", 123456L + id);
        setField(job, "finishedTimeMs", 234567L + id);
        setField(job, "stateStartedTimeMs", 345678L + id);
        setField(job, "errorMessage", "persisted error " + id);
        setField(job, "warehouseId", 99L);
        setField(job, "transactionId", 456789L + id);
        setField(job, "gtid", 567890L + id);
        setField(job, "endTransactionId", 678901L + id);
        return job;
    }

    static Map<Long, ReshardingPhysicalPartition> partitions(TabletReshardJob job) {
        return job instanceof SplitTabletJob split ? split.getReshardingPhysicalPartitions()
                : ((MergeTabletJob) job).getReshardingPhysicalPartitions();
    }

    static ReshardingPhysicalPartition partition(TabletReshardJob job) {
        return partitions(job).values().iterator().next();
    }

    static ReshardingMaterializedIndex index(TabletReshardJob job) {
        return partition(job).getReshardingIndexes().values().iterator().next();
    }

    static TabletReshardJobPB withTablet(TabletReshardJobPB job, int offset, ReshardingTabletPB tablet) {
        var split = job.getSplitTablet().toBuilder();
        var partitions = split.getReshardingPhysicalPartitions().toBuilder();
        long partitionId = partitions.getValuesMap().keySet().iterator().next();
        var partition = partitions.getValuesOrThrow(partitionId).toBuilder();
        var indexes = partition.getReshardingIndexes().toBuilder();
        long indexId = indexes.getValuesMap().keySet().iterator().next();
        var index = indexes.getValuesOrThrow(indexId).toBuilder();
        index.setReshardingTablets(index.getReshardingTablets().toBuilder().setValues(offset, tablet));
        indexes.putValues(indexId, index.build());
        partition.setReshardingIndexes(indexes);
        partitions.putValues(partitionId, partition.build());
        return job.toBuilder().setSplitTablet(split.setReshardingPhysicalPartitions(partitions)).build();
    }
}
