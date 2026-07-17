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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared assertions for the reshard recycle-bin parking behavior (issue #75993), used by both
 * SplitTabletJobTest and MergeTabletJobTest since split and merge park the superseded index through
 * the same TabletReshardJob.recycleSupersededMaterializedIndex helper.
 */
final class ReshardRecycleAssertions {
    private ReshardRecycleAssertions() {
    }

    /**
     * Assert the superseded (old) index is parked in the recycle bin as a partition of {@code table}
     * carrying its old shard group and tablets, rather than deleted immediately.
     */
    static void assertSupersededIndexParked(OlapTable table, long oldShardGroupId, List<Long> oldTabletIds) {
        List<Partition> recycledPartitions =
                GlobalStateMgr.getCurrentState().getRecycleBin().getPartitions(table.getId());
        Assertions.assertFalse(recycledPartitions.isEmpty());

        Set<Long> recycledTabletIds = new HashSet<>();
        Set<Long> recycledShardGroupIds = new HashSet<>();
        for (Partition recycled : recycledPartitions) {
            for (PhysicalPartition sub : recycled.getSubPartitions()) {
                recycledShardGroupIds.addAll(sub.getShardGroupIds());
                for (MaterializedIndex index : sub.getAllMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        recycledTabletIds.add(tablet.getId());
                    }
                }
            }
        }
        Assertions.assertTrue(recycledTabletIds.containsAll(oldTabletIds));
        Assertions.assertTrue(recycledShardGroupIds.contains(oldShardGroupId));
    }
}
