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

package com.starrocks.sql.plan;

import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.LazyComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * In shared-data mode the planner counts the alive execution nodes of the query's warehouse when it costs broadcast
 * and hash joins (enable_cbo_warehouse_exec_nodes), instead of the alive backends, which a shared-data cluster
 * usually has none of.
 */
public class WarehouseExecNodeNumberTest extends PlanTestBase {
    private static final long OTHER_WAREHOUSE_ID = 999L;
    private static final long GROUP_A = 7L;
    private static final long GROUP_B = 8L;

    private final List<ComputeNode> addedNodes = new ArrayList<>();
    private long nextNodeId = 90000L;

    @AfterEach
    public void tearDown() {
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        addedNodes.forEach(clusterInfo::dropComputeNode);
        addedNodes.clear();
        connectContext.setCurrentComputeResource(null);
        connectContext.getSessionVariable().setEnableCboWarehouseExecNodes(true);
        connectContext.getSessionVariable().setCboDebugAliveBackendNumber(0);
    }

    private void mockSharedData() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
    }

    // Makes the session use the given warehouse, which the test cluster does not need to have.
    private void mockSessionWarehouse(long warehouseId) {
        new MockUp<ConnectContext>() {
            @Mock
            public Long getCurrentWarehouseIdAllowNull() {
                return warehouseId;
            }
        };
    }

    private void addNode(long warehouseId, long workerGroupId, boolean alive) {
        ComputeNode node = new ComputeNode(nextNodeId, "127.0.1." + (nextNodeId - 89999L), 9050);
        nextNodeId++;
        node.setAlive(alive);
        node.setWarehouseId(warehouseId);
        node.setWorkerGroupId(workerGroupId);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNode(node);
        addedNodes.add(node);
    }

    // Alive backends of the default warehouse that the test framework already registered.
    private long aliveBackendsOfDefaultWarehouse() {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends().stream()
                .filter(be -> be.isAlive() && be.getWarehouseId() == WarehouseManager.DEFAULT_WAREHOUSE_ID)
                .count();
    }

    private int count() {
        return connectContext.getQueryExecNodeNumber();
    }

    private static ComputeResource resourceOf(long warehouseId, long workerGroupId) {
        return new ComputeResource() {
            @Override
            public long getWarehouseId() {
                return warehouseId;
            }

            @Override
            public long getWorkerGroupId() {
                return workerGroupId;
            }
        };
    }

    @Test
    public void testUseWarehouseExecNodesOnlyInSharedData() {
        Assertions.assertFalse(connectContext.isUseWarehouseExecNodes());
        mockSharedData();
        Assertions.assertTrue(connectContext.isUseWarehouseExecNodes());
        connectContext.getSessionVariable().setEnableCboWarehouseExecNodes(false);
        Assertions.assertFalse(connectContext.isUseWarehouseExecNodes());
    }

    @Test
    public void testSharedNothingCountsAliveBackends() {
        int aliveBackends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        Assertions.assertEquals(Math.max(1, aliveBackends), count());
    }

    @Test
    public void testCountsAliveNodesOfCurrentWarehouse() {
        mockSharedData();
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        // A dead node and the nodes of another warehouse are not counted.
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, false);
        addNode(OTHER_WAREHOUSE_ID, GROUP_A, true);
        addNode(OTHER_WAREHOUSE_ID, GROUP_A, true);

        Assertions.assertEquals(aliveBackendsOfDefaultWarehouse() + 3, count());

        mockSessionWarehouse(OTHER_WAREHOUSE_ID);
        Assertions.assertEquals(2, count());
    }

    @Test
    public void testCountsWholeWarehouseWhateverNodeGroup() {
        mockSharedData();
        mockSessionWarehouse(OTHER_WAREHOUSE_ID);
        addNode(OTHER_WAREHOUSE_ID, GROUP_A, true);
        addNode(OTHER_WAREHOUSE_ID, GROUP_A, true);
        addNode(OTHER_WAREHOUSE_ID, GROUP_A, true);
        addNode(OTHER_WAREHOUSE_ID, GROUP_B, true);
        Assertions.assertEquals(4, count());

        // The count does not depend on the group this connection has been given, if any, so the first query of a
        // connection and the later ones are costed alike.
        connectContext.setCurrentComputeResource(resourceOf(OTHER_WAREHOUSE_ID, GROUP_B));
        Assertions.assertEquals(4, count());
        connectContext.setCurrentComputeResource(resourceOf(OTHER_WAREHOUSE_ID, GROUP_A));
        Assertions.assertEquals(4, count());
    }

    @Test
    public void testDoesNotAcquireLazyComputeResource() {
        mockSharedData();
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        LazyComputeResource pending = LazyComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID, () -> {
            throw new AssertionError("Counting nodes must not acquire the compute resource");
        });
        connectContext.setCurrentComputeResource(pending);
        Assertions.assertEquals(aliveBackendsOfDefaultWarehouse() + 2, count());
        Assertions.assertFalse(pending.isInitialized());
    }

    @Test
    public void testFallsBackToAllAliveNodesWhenWarehouseHasNone() {
        mockSharedData();
        addNode(OTHER_WAREHOUSE_ID, GROUP_A, true);
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        long allAlive = clusterInfo.getAliveComputeNodeNumber() + clusterInfo.getAliveBackendNumber();
        mockSessionWarehouse(12345L);
        Assertions.assertEquals(allAlive, count());
    }

    @Test
    public void testDebugVariableOverridesCount() {
        mockSharedData();
        addNode(WarehouseManager.DEFAULT_WAREHOUSE_ID, StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
        connectContext.getSessionVariable().setCboDebugAliveBackendNumber(40);
        Assertions.assertEquals(40, count());
    }

    private void mockExecNodes(boolean useWarehouseExecNodes, int nodeNumber) {
        new MockUp<ConnectContext>() {
            @Mock
            public boolean isUseWarehouseExecNodes() {
                return useWarehouseExecNodes;
            }

            @Mock
            public int getQueryExecNodeNumber() {
                return nodeNumber;
            }
        };
    }

    // t0 and t1 are joined on columns neither is bucketed by. The test tables have no data, so the planner prunes
    // their empty partitions and costs one row on each side, and copying t1 to every node costs one row per node: with
    // one node broadcasting t1 is the cheaper plan, with 40 nodes shuffling both sides is.
    private static final String JOIN_SQL = "select count(*) from t0 join t1 on t0.v2 = t1.v5";

    @Test
    public void testBroadcastCostedWithWarehouseExecNodes() throws Exception {
        mockExecNodes(true, 1);
        String plan = getFragmentPlan(JOIN_SQL);
        assertContains(plan, "join op: INNER JOIN (BROADCAST)");

        mockExecNodes(true, 40);
        plan = getFragmentPlan(JOIN_SQL);
        assertContains(plan, "join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testSwitchOffKeepsLegacyCount() throws Exception {
        // With the switch off the planner keeps counting alive backends only, whatever the warehouse has.
        int aliveBackends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();
        Assertions.assertEquals(1, aliveBackends);
        mockExecNodes(false, 40);
        String plan = getFragmentPlan(JOIN_SQL);
        assertContains(plan, "join op: INNER JOIN (BROADCAST)");
    }
}
