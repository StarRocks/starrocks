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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.vector.VectorIndexBuildScheduler;
import com.starrocks.sql.ast.IndexDef;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TabletReshardVectorIndexTest {

    // A real scheduler (its constructor does not start the daemon thread) that records
    // addPendingTablet calls, so the enqueue helper can be verified without mocking the
    // Thread-derived VectorIndexBuildScheduler class.
    private static class RecordingScheduler extends VectorIndexBuildScheduler {
        final List<String> calls = new ArrayList<>();

        @Override
        public void addPendingTablet(long tabletId, long version, boolean fromCompaction) {
            calls.add(tabletId + ":" + version + ":" + fromCompaction);
        }
    }

    private MaterializedIndex indexWithTablets(long indexId, Map<Long, Long> tabletIdToVibv) {
        MaterializedIndex index = new MaterializedIndex(indexId);
        for (Map.Entry<Long, Long> e : tabletIdToVibv.entrySet()) {
            LakeTablet tablet = new LakeTablet(e.getKey());
            tablet.setVectorIndexBuiltVersion(e.getValue());
            index.addTablet(tablet, null, false);
        }
        return index;
    }

    // minVectorIndexBuiltVersion: split (single parent) inherits the parent; merge (multiple
    // sources) takes the min; an empty source list yields 0 (non-vector tables are all 0).
    @Test
    public void testMinVectorIndexBuiltVersion() {
        Map<Long, Long> ids = new HashMap<>();
        ids.put(101L, 100L);
        ids.put(102L, 50L);
        MaterializedIndex index = indexWithTablets(1L, ids);

        // merge: min across sources
        assertEquals(50L, TabletReshardJob.minVectorIndexBuiltVersion(index, Lists.newArrayList(101L, 102L)));
        // split / identical: single parent
        assertEquals(100L, TabletReshardJob.minVectorIndexBuiltVersion(index, Lists.newArrayList(101L)));
        // empty -> 0
        assertEquals(0L, TabletReshardJob.minVectorIndexBuiltVersion(index, Collections.emptyList()));
    }

    private OlapTable mockTable(boolean asyncVectorIndex, long partitionId, PhysicalPartition physicalPartition) {
        OlapTable table = Mockito.mock(OlapTable.class);
        if (asyncVectorIndex) {
            Index vectorIndex = Mockito.mock(Index.class);
            Mockito.when(vectorIndex.getIndexType()).thenReturn(IndexDef.IndexType.VECTOR);
            Map<String, String> props = new HashMap<>();
            props.put("index_build_mode", "async");
            Mockito.when(vectorIndex.getProperties()).thenReturn(props);
            Mockito.when(table.getIndexes()).thenReturn(Lists.newArrayList(vectorIndex));
        } else {
            Mockito.when(table.getIndexes()).thenReturn(Collections.emptyList());
        }
        Mockito.when(table.getPhysicalPartition(partitionId)).thenReturn(physicalPartition);
        return table;
    }

    private List<ReshardingPhysicalPartition> onePartitionWithTablets(long partitionId, long indexId,
            long... newTabletIds) {
        MaterializedIndex newIndex = new MaterializedIndex(indexId);
        for (long tid : newTabletIds) {
            newIndex.addTablet(new LakeTablet(tid), null, false);
        }
        ReshardingMaterializedIndex reshardingIndex =
                new ReshardingMaterializedIndex(indexId, newIndex, Collections.emptyList());
        Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
        reshardingIndexes.put(indexId, reshardingIndex);
        return Lists.newArrayList(new ReshardingPhysicalPartition(partitionId, reshardingIndexes));
    }

    // Async-vector-index table: every new reshard tablet is enqueued at the partition's visible version.
    @Test
    public void testEnqueueAddsVisibleReshardTablets() {
        RecordingScheduler scheduler = new RecordingScheduler();
        long partitionId = 7L;
        PhysicalPartition physicalPartition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(physicalPartition.getVisibleVersion()).thenReturn(9L);
        OlapTable table = mockTable(/*asyncVectorIndex=*/true, partitionId, physicalPartition);

        TabletReshardJob.enqueueReshardOutputForVectorIndexBuild(table,
                onePartitionWithTablets(partitionId, 100L, 201L, 202L), scheduler);

        assertEquals(2, scheduler.calls.size());
        assertTrue(scheduler.calls.contains("201:9:false"));
        assertTrue(scheduler.calls.contains("202:9:false"));
    }

    // Non-vector-index table: nothing is enqueued (gated by hasAsyncVectorIndex).
    @Test
    public void testEnqueueSkipsNonVectorTable() {
        RecordingScheduler scheduler = new RecordingScheduler();
        long partitionId = 7L;
        PhysicalPartition physicalPartition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(physicalPartition.getVisibleVersion()).thenReturn(9L);
        OlapTable table = mockTable(/*asyncVectorIndex=*/false, partitionId, physicalPartition);

        TabletReshardJob.enqueueReshardOutputForVectorIndexBuild(table,
                onePartitionWithTablets(partitionId, 100L, 201L, 202L), scheduler);

        assertTrue(scheduler.calls.isEmpty());
    }

    // Null scheduler (e.g. scheduler not initialized): no-op, no exception.
    @Test
    public void testEnqueueNullSchedulerIsNoop() {
        long partitionId = 7L;
        PhysicalPartition physicalPartition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(physicalPartition.getVisibleVersion()).thenReturn(9L);
        OlapTable table = mockTable(/*asyncVectorIndex=*/true, partitionId, physicalPartition);

        // Should not throw.
        TabletReshardJob.enqueueReshardOutputForVectorIndexBuild(table,
                onePartitionWithTablets(partitionId, 100L, 201L), null);
    }
}
