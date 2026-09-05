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
}
