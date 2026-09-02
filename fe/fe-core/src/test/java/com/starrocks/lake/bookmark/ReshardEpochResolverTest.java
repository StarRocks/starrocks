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

package com.starrocks.lake.bookmark;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ReshardEpochResolverTest {

    private PhysicalPartition buildPartitionWithGenerations() {
        PhysicalPartition pp = new PhysicalPartition(10, 1); // (id, parentId) external ctor
        MaterializedIndex g0 = new MaterializedIndex(100, 50, MaterializedIndex.IndexState.NORMAL, 7);
        pp.createRollupIndex(g0); // registers metaId 50 with generation g0
        MaterializedIndex g1 = new MaterializedIndex(101, 50, MaterializedIndex.IndexState.NORMAL, 7);
        g1.setTakeoverVersion(20);
        g1.setPredecessorIndexId(100);
        pp.addMaterializedIndex(g1, false);
        MaterializedIndex g2 = new MaterializedIndex(102, 50, MaterializedIndex.IndexState.NORMAL, 7);
        g2.setTakeoverVersion(30);
        g2.setPredecessorIndexId(101);
        pp.addMaterializedIndex(g2, false);
        return pp;
    }

    @Test
    public void testTwoBoundaryResolution() {
        PhysicalPartition pp = buildPartitionWithGenerations();
        // (5, 35] across takeovers 20 and 30: g0 (5,19], g1 (20,29], g2 (30,35]
        List<IndexEpoch> epochs = ReshardEpochResolver.resolveEpochs(pp, 50, 100, 5, 102, 35).orElseThrow();
        Assertions.assertEquals(3, epochs.size());
        Assertions.assertEquals(100, epochs.get(0).index().getId());
        Assertions.assertEquals(5, epochs.get(0).baseVersionExclusive());
        Assertions.assertEquals(19, epochs.get(0).headVersionInclusive());
        Assertions.assertEquals(20, epochs.get(1).baseVersionExclusive());
        Assertions.assertEquals(29, epochs.get(1).headVersionInclusive());
        Assertions.assertEquals(30, epochs.get(2).baseVersionExclusive());
        Assertions.assertEquals(35, epochs.get(2).headVersionInclusive());
    }

    @Test
    public void testEmptySubRangesSkipped() {
        // base version 19 == g1.takeover - 1: g0 epoch (19,19] is empty and must be skipped.
        // head version 30 == g2.takeover: g2 epoch (30,30] is empty and must be skipped.
        PhysicalPartition pp = buildPartitionWithGenerations();
        List<IndexEpoch> epochs = ReshardEpochResolver.resolveEpochs(pp, 50, 100, 19, 102, 30).orElseThrow();
        Assertions.assertEquals(1, epochs.size());
        Assertions.assertEquals(101, epochs.get(0).index().getId()); // g1 (20, 29]
    }

    @Test
    public void testUnresolvable() {
        PhysicalPartition pp = buildPartitionWithGenerations();
        // unknown from-index id: the predecessor walk from 102 never reaches it
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp, 50, 999, 5, 102, 35).isEmpty());
        // reversed direction (walk from 100 hits its chain end before reaching 102)
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp, 50, 102, 5, 100, 35).isEmpty());
        // head generation produced by a non-reshard path: no takeover/predecessor stamped
        MaterializedIndex g3 = new MaterializedIndex(103, 50, MaterializedIndex.IndexState.NORMAL, 7);
        pp.addMaterializedIndex(g3, false); // takeoverVersion / predecessorIndexId stay 0
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp, 50, 100, 5, 103, 40).isEmpty());
        // ERASED middle generation: 102's predecessor pointer (101) dangles after the erase, even
        // though the generation-id list [100, 102] looks contiguous. Must fail closed.
        pp.deleteMaterializedIndexByIndexId(101);
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp, 50, 100, 5, 102, 35).isEmpty());
    }

    @Test
    public void testFailClosedSanity() {
        // Non-increasing takeover along the chain -> unresolvable.
        PhysicalPartition pp = buildPartitionWithGenerations();
        pp.getIndex(102).setTakeoverVersion(15); // < g1's 20
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp, 50, 100, 5, 102, 35).isEmpty());

        // Endpoint versions outside their generations -> unresolvable.
        PhysicalPartition pp2 = buildPartitionWithGenerations();
        // base version 25 >= g1.takeover(20): the base bookmark cannot belong to generation 100
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp2, 50, 100, 25, 102, 35).isEmpty());
        // head version 29 < g2.takeover(30): the head bookmark cannot belong to generation 102
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp2, 50, 100, 5, 102, 29).isEmpty());
        // base version 15 < the base generation g1's OWN takeover(20): g1's tablets have no
        // versions below 20, so a (15, ...] scan on them is impossible -> unresolvable
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp2, 50, 101, 15, 102, 35).isEmpty());
        // reversed version range -> unresolvable (never a present-empty result)
        Assertions.assertTrue(ReshardEpochResolver.resolveEpochs(pp2, 50, 101, 25, 101, 21).isEmpty());
    }

    @Test
    public void testSameGeneration() {
        PhysicalPartition pp = buildPartitionWithGenerations();
        List<IndexEpoch> epochs = ReshardEpochResolver.resolveEpochs(pp, 50, 101, 21, 101, 25).orElseThrow();
        Assertions.assertEquals(1, epochs.size());
        Assertions.assertEquals(21, epochs.get(0).baseVersionExclusive());
        Assertions.assertEquals(25, epochs.get(0).headVersionInclusive());
    }

    @Test
    public void testIsLineageConnected() {
        PhysicalPartition pp = buildPartitionWithGenerations();
        // Same installed generation, and a fully stamped chain across two reshards.
        Assertions.assertTrue(ReshardEpochResolver.isLineageConnected(pp, 50, 101, 101));
        Assertions.assertTrue(ReshardEpochResolver.isLineageConnected(pp, 50, 100, 102));
        // Neither endpoint installed / wrong meta id.
        Assertions.assertFalse(ReshardEpochResolver.isLineageConnected(pp, 50, 999, 102));
        Assertions.assertFalse(ReshardEpochResolver.isLineageConnected(pp, 51, 100, 102));
        // Walk direction matters: the newest generation is not an ancestor of the oldest.
        Assertions.assertFalse(ReshardEpochResolver.isLineageConnected(pp, 50, 102, 100));
    }

    @Test
    public void testIsLineageNotConnectedWithoutStamps() {
        // A generation installed without reshard lineage stamps (a pre-upgrade parking, or any
        // non-reshard index replacement) is not lineage: the recycle bin's bookmark gate does not
        // hold it, so a read must never be scoped to it.
        PhysicalPartition pp = buildPartitionWithGenerations();
        MaterializedIndex unstamped = new MaterializedIndex(103, 50, MaterializedIndex.IndexState.NORMAL, 7);
        pp.addMaterializedIndex(unstamped, false); // takeoverVersion / predecessorIndexId stay 0
        Assertions.assertFalse(ReshardEpochResolver.isLineageConnected(pp, 50, 102, 103));
        Assertions.assertFalse(ReshardEpochResolver.isLineageConnected(pp, 50, 100, 103));

        // Dangling predecessor: 102 points at an erased 101, so the walk cannot reach 100 even
        // though the partition's generation-id list [100, 102] looks contiguous.
        pp.deleteMaterializedIndexByIndexId(101);
        Assertions.assertFalse(ReshardEpochResolver.isLineageConnected(pp, 50, 100, 102));
    }
}
