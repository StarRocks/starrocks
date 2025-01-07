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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LoadPlanNodeTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testAvailableNodesForSharedDataWithCn() {
        testAvailableNodesForSharedDataBase(true);
    }

    @Test
    public void testAvailableNodesForSharedDataWithBackend() {
        testAvailableNodesForSharedDataBase(false);
    }

    private void testAvailableNodesForSharedDataBase(boolean useCn) {
        SystemInfoService service = new SystemInfoService();
        List<Long> nodeIds = new ArrayList<>();
        if (useCn) {
            for (int i = 1; i <= 4; i++) {
                ComputeNode computeNode = new ComputeNode(i, "127.0.0." + i, 9050);
                service.addComputeNode(computeNode);
                if (i % 2 == 0) {
                    computeNode.setAlive(true);
                }
                nodeIds.add((long) i);
            }
        } else {
            for (int i = 5; i <= 8; i++) {
                Backend backend = new Backend(i, "127.0.0." + i, 9050);
                service.addBackend(backend);
                if (i % 2 == 0) {
                    backend.setAlive(true);
                }
                nodeIds.add((long) i);
            }
        }

        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(1L);
                result = nodeIds;
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = service;
            }
        };

        List<ComputeNode> nodes = LoadScanNode.getAvailableComputeNodes(1L);
        assertNotNull(nodes);
        assertEquals(2, nodes.size());
        nodes.sort((o1, o2) -> (int) (o1.getId() - o2.getId()));
        if (useCn) {
            assertEquals(2, nodes.get(0).getId());
            assertEquals(4, nodes.get(1).getId());
        } else {
            assertEquals(6, nodes.get(0).getId());
            assertEquals(8, nodes.get(1).getId());
        }
    }

    @Test
    public void testAvailableNodesForSharedNothing() {
        SystemInfoService service = new SystemInfoService();
        for (int i = 1; i <= 4; i++) {
            ComputeNode computeNode = new ComputeNode(i, "127.0.0." + i, 9050);
            service.addComputeNode(computeNode);
            if (i % 2 == 0) {
                computeNode.setAlive(true);
            }
        }

        for (int i = 5; i <= 8; i++) {
            Backend backend = new Backend(i, "127.0.0." + i, 9050);
            service.addBackend(backend);
            if (i % 2 == 0) {
                backend.setAlive(true);
            }
        }

        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return false;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = service;
            }
        };

        List<ComputeNode> nodes = LoadScanNode.getAvailableComputeNodes(0L);
        assertNotNull(nodes);
        assertEquals(2, nodes.size());
        nodes.sort((o1, o2) -> (int) (o1.getId() - o2.getId()));
        assertEquals(6, nodes.get(0).getId());
        assertEquals(8, nodes.get(1).getId());
    }
}
