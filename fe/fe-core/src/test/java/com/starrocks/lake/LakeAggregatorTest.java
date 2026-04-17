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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LakeAggregatorTest {

    private WarehouseManager originalManager;
    private WarehouseManager mockManager;

    private ComputeNode nodeA; // alive + candidate
    private ComputeNode nodeB; // alive + candidate
    private ComputeNode nodeC; // alive, not a candidate
    private ComputeNode nodeDead; // candidate but not in the alive list

    @BeforeAll
    public static void setUpClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
    }

    @BeforeEach
    public void setUp() throws Exception {
        nodeA = new ComputeNode(1001L, "127.0.0.1", 9050);
        nodeA.setAlive(true);
        nodeB = new ComputeNode(1002L, "127.0.0.2", 9050);
        nodeB.setAlive(true);
        nodeC = new ComputeNode(1003L, "127.0.0.3", 9050);
        nodeC.setAlive(true);
        nodeDead = new ComputeNode(1004L, "127.0.0.4", 9050);
        nodeDead.setAlive(false);

        originalManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        mockManager = mock(WarehouseManager.class);
        Field warehouseMgrField = GlobalStateMgr.class.getDeclaredField("warehouseMgr");
        warehouseMgrField.setAccessible(true);
        warehouseMgrField.set(GlobalStateMgr.getCurrentState(), mockManager);
    }

    @AfterEach
    public void tearDown() throws Exception {
        Field warehouseMgrField = GlobalStateMgr.class.getDeclaredField("warehouseMgr");
        warehouseMgrField.setAccessible(true);
        warehouseMgrField.set(GlobalStateMgr.getCurrentState(), originalManager);
    }

    // When at least one candidate is alive in the warehouse, the aggregator must come
    // from the candidate set so that on BE side the batch anchor tablet can be
    // resolved locally via the staros worker cache.
    @Test
    public void testPickFromAliveCandidate() {
        when(mockManager.getAliveComputeNodes(any())).thenReturn(Lists.newArrayList(nodeA, nodeB, nodeC));

        // Only nodeA is both in candidates and alive → it must be chosen.
        ComputeNode picked = LakeAggregator.chooseAggregatorNode(WarehouseComputeResource.of(0L),
                Lists.newArrayList(nodeA, nodeDead));
        Assertions.assertNotNull(picked);
        Assertions.assertEquals(nodeA.getId(), picked.getId());
    }

    // When multiple candidates are alive, we still must return one of the candidates
    // rather than a random alive non-candidate like nodeC.
    @Test
    public void testPickFromAliveCandidateMultiple() {
        when(mockManager.getAliveComputeNodes(any())).thenReturn(Lists.newArrayList(nodeA, nodeB, nodeC));

        for (int i = 0; i < 10; i++) {
            ComputeNode picked = LakeAggregator.chooseAggregatorNode(WarehouseComputeResource.of(0L),
                    Lists.newArrayList(nodeA, nodeB));
            Assertions.assertNotNull(picked);
            Assertions.assertTrue(picked.getId() == nodeA.getId() || picked.getId() == nodeB.getId(),
                    "aggregator must be one of the candidates, got " + picked.getId());
        }
    }

    // When none of the candidates are alive, fall back to any alive node in the
    // warehouse. We should still get a usable aggregator, not null.
    @Test
    public void testFallbackWhenNoLiveCandidate() {
        when(mockManager.getAliveComputeNodes(any())).thenReturn(Lists.newArrayList(nodeA, nodeB));

        // nodeDead is the only candidate but it is not in the alive list.
        ComputeNode picked = LakeAggregator.chooseAggregatorNode(WarehouseComputeResource.of(0L),
                Lists.newArrayList(nodeDead));
        Assertions.assertNotNull(picked);
        Assertions.assertTrue(picked.getId() == nodeA.getId() || picked.getId() == nodeB.getId());
    }

    // Empty / null candidate lists must preserve the legacy behavior of picking a
    // random alive node.
    @Test
    public void testFallbackWhenNoCandidate() {
        when(mockManager.getAliveComputeNodes(any())).thenReturn(Lists.newArrayList(nodeA));

        ComputeNode picked = LakeAggregator.chooseAggregatorNode(WarehouseComputeResource.of(0L),
                Lists.newArrayList());
        Assertions.assertNotNull(picked);
        Assertions.assertEquals(nodeA.getId(), picked.getId());

        ComputeNode pickedNull = LakeAggregator.chooseAggregatorNode(WarehouseComputeResource.of(0L), null);
        Assertions.assertNotNull(pickedNull);
        Assertions.assertEquals(nodeA.getId(), pickedNull.getId());
    }

    // A candidate entry whose id does not match any alive node (e.g. the node has
    // been dropped in the meantime) must not be picked.
    @Test
    public void testCandidateNotAliveIsSkipped() {
        // nodeC is alive but not a candidate; nodeDead is a candidate but not alive.
        when(mockManager.getAliveComputeNodes(any())).thenReturn(Lists.newArrayList(nodeB, nodeC));

        List<ComputeNode> candidates = Lists.newArrayList(nodeDead, nodeB);
        for (int i = 0; i < 10; i++) {
            ComputeNode picked = LakeAggregator.chooseAggregatorNode(WarehouseComputeResource.of(0L), candidates);
            Assertions.assertNotNull(picked);
            // nodeDead must never be chosen since it is not alive.
            Assertions.assertNotEquals(nodeDead.getId(), picked.getId());
            // nodeC must never be chosen since it is not in the candidate list.
            Assertions.assertEquals(nodeB.getId(), picked.getId());
        }
    }
}
