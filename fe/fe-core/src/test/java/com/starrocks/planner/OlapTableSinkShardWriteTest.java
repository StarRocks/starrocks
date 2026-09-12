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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class OlapTableSinkShardWriteTest {
    private static final List<Long> NODES = Lists.newArrayList(10L, 11L, 12L, 13L);

    @Test
    public void testOwnerComesFirst() {
        // The owner leads the list so the node that will publish the tablet also holds part of its
        // data, and therefore its caches.
        List<Long> nodeIds = OlapTableSink.buildShardWriteNodeIds(12L, NODES, 3, 100L);
        Assertions.assertEquals(3, nodeIds.size());
        Assertions.assertEquals(12L, nodeIds.get(0));
        Assertions.assertEquals(3, nodeIds.stream().distinct().count());
        Assertions.assertTrue(NODES.containsAll(nodeIds));
    }

    @Test
    public void testParallelismOneKeepsSingleNode() {
        Assertions.assertEquals(Lists.newArrayList(12L),
                OlapTableSink.buildShardWriteNodeIds(12L, NODES, OlapTableSink.NO_SHARD_WRITE, 100L));
    }

    @Test
    public void testBoundedBelowAliveNodes() {
        // lake_local_first_write_max_nodes reaches here as the bound. Every node in the list writes its
        // own segments, so a wide warehouse would otherwise cut one load into that many small segments.
        // The nodes left out still run their sink instance -- their rows just travel, as they did before
        // the feature existed -- so the bound may cost locality but must never drop or duplicate a node.
        List<Long> nodeIds = OlapTableSink.buildShardWriteNodeIds(12L, NODES, 2, 100L);
        Assertions.assertEquals(2, nodeIds.size());
        Assertions.assertEquals(2, nodeIds.stream().distinct().count());
        Assertions.assertEquals(12L, nodeIds.get(0));
        Assertions.assertTrue(NODES.containsAll(nodeIds));
    }

    @Test
    public void testClampedToAliveNodes() {
        // "every alive node" reaches createLocation as Integer.MAX_VALUE; the list must clamp rather
        // than repeat a node, which would make one node write the tablet twice.
        List<Long> nodeIds = OlapTableSink.buildShardWriteNodeIds(12L, NODES, Integer.MAX_VALUE, 100L);
        Assertions.assertEquals(NODES.size(), nodeIds.size());
        Assertions.assertEquals(NODES.size(), nodeIds.stream().distinct().count());
        Assertions.assertEquals(12L, nodeIds.get(0));
    }

    @Test
    public void testFollowersVaryWithTabletId() {
        // Two tablets of the same partition must not pile their extra writers onto the same node.
        List<Long> first = OlapTableSink.buildShardWriteNodeIds(10L, NODES, 2, 1L);
        List<Long> second = OlapTableSink.buildShardWriteNodeIds(10L, NODES, 2, 2L);
        Assertions.assertEquals(10L, first.get(0));
        Assertions.assertEquals(10L, second.get(0));
        Assertions.assertNotEquals(first.get(1), second.get(1));
    }

    private static final long GB = 1024L * 1024 * 1024;

    @Test
    public void testNodesFromEstimatedSize() {
        // The worked example: 10 GB at the 2 GB default is five nodes' worth. The caller then takes the
        // minimum of this, lake_local_first_write_max_nodes, and the alive node count.
        Assertions.assertEquals(5, OlapTableSink.nodesForEstimatedSize(10 * GB, 2 * GB));
        Assertions.assertEquals(1, OlapTableSink.nodesForEstimatedSize(2 * GB, 2 * GB));
    }

    @Test
    public void testPartialShareDoesNotBuyANode() {
        // Integer division: a node joins only once there is a whole share for it. Every node in the list
        // writes its own segments and emits its own partial txn log, and open/close reach it whether or
        // not it ends up with rows, so a node given a sliver costs more than it saves.
        Assertions.assertEquals(4, OlapTableSink.nodesForEstimatedSize(9 * GB, 2 * GB));
        // Below one full share the load stays on a single node, which is the feature turned off.
        Assertions.assertEquals(1, OlapTableSink.nodesForEstimatedSize(GB, 2 * GB));
    }

    @Test
    public void testUnknownSizeDefersToTheBound() {
        // An unknown size is not a small size: with no estimate the node count must stay exactly what it
        // was before this knob existed, so the value returned has to lose every min() it takes part in.
        Assertions.assertEquals(Integer.MAX_VALUE, OlapTableSink.nodesForEstimatedSize(-1, 2 * GB));
        Assertions.assertEquals(Integer.MAX_VALUE, OlapTableSink.nodesForEstimatedSize(0, 2 * GB));
        // Same for a session that disables the sizing by zeroing the share.
        Assertions.assertEquals(Integer.MAX_VALUE, OlapTableSink.nodesForEstimatedSize(10 * GB, 0));
        Assertions.assertEquals(Integer.MAX_VALUE, OlapTableSink.nodesForEstimatedSize(10 * GB, -1));
    }

    @Test
    public void testHugeEstimateDoesNotOverflow() {
        // A byte count divided by a one-byte share exceeds int range; it must saturate, not wrap
        // negative, which would make min() pick a nonsense parallelism.
        Assertions.assertEquals(Integer.MAX_VALUE, OlapTableSink.nodesForEstimatedSize(Long.MAX_VALUE, 1));
    }

}
