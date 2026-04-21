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

package com.starrocks.task;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class TabletTaskExecutorTest {

    private static final long ALIVE_NODE_ID = 10001L;
    private static final long DEAD_NODE_ID = 10002L;
    private static final long DB_ID = 1L;

    private int savedMaxRetries;

    @BeforeEach
    public void setUp() {
        savedMaxRetries = Config.lake_create_tablet_max_retries;
    }

    @AfterEach
    public void tearDown() {
        Config.lake_create_tablet_max_retries = savedMaxRetries;
    }

    // ======================== Helper methods ========================

    private static CreateReplicaTask buildLakeTask(long nodeId, long tabletId) {
        return CreateReplicaTask.newBuilder()
                .setNodeId(nodeId)
                .setDbId(DB_ID)
                .setTableId(2L)
                .setPartitionId(3L)
                .setIndexId(4L)
                .setTabletId(tabletId)
                .setVersion(1L)
                .setStorageMedium(TStorageMedium.SSD)
                .setTabletType(TTabletType.TABLET_TYPE_LAKE)
                .setCompressionType(TCompressionType.LZ4_FRAME)
                .setTabletSchema(new TTabletSchema())
                .build();
    }

    private void mockBuildCreateReplicaTasks(long nodeId, long startTabletId) {
        new MockUp<TabletTaskExecutor>() {
            long nextTabletId = startTabletId;

            @Mock
            List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table,
                                                            PhysicalPartition partition,
                                                            ComputeResource computeResource,
                                                            TabletTaskExecutor.CreateTabletOption option) {
                return new ArrayList<>(Collections.singletonList(buildLakeTask(nodeId, nextTabletId++)));
            }
        };
    }

    /**
     * Mock sendTask: succeed for aliveNodeIds, fail for others. No real RPC.
     * Mock waitForFinished: simulate latch countdown (all non-failed tasks succeed).
     */
    private void mockSendAndWait(Set<Long> aliveNodeIds) {
        new MockUp<TabletTaskExecutor>() {
            @Mock
            CompletableFuture<Boolean> sendTask(Long backendId, List<AgentTask> agentBatchTask) {
                if (aliveNodeIds.contains(backendId)) {
                    return CompletableFuture.completedFuture(true);
                }
                CompletableFuture<Boolean> f = new CompletableFuture<>();
                f.completeExceptionally(new RuntimeException("Can't get backend " + backendId));
                return f;
            }

            @Mock
            void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) {
                while (countDownLatch.getCount() > 0) {
                    List<java.util.Map.Entry<Long, Long>> marks = countDownLatch.getLeftMarks();
                    for (java.util.Map.Entry<Long, Long> mark : marks) {
                        countDownLatch.markedCountDown(mark.getKey(), mark.getValue());
                    }
                }
            }
        };
    }

    private void mockGlobalStateMgr(GlobalStateMgr globalStateMgr, Set<Long> aliveNodeIds) {
        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        List<ComputeNode> aliveNodes = new ArrayList<>();
        for (long id : aliveNodeIds) {
            ComputeNode cn = new ComputeNode(id, "127.0.0." + id, 9050);
            cn.setBePort(9060);
            cn.setAlive(true);
            aliveNodes.add(cn);
        }

        WarehouseManager warehouseManager = new WarehouseManager();
        new MockUp<WarehouseManager>() {
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return aliveNodes;
            }
        };
        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };
    }

    private OlapTable mockLakeTable() {
        return new OlapTable() {
            @Override
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
            @Override
            public String getName() {
                return "test_table";
            }
        };
    }

    private List<PhysicalPartition> mockPartitions(int count) {
        List<PhysicalPartition> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long partId = 100 + i;
            MaterializedIndex index = new MaterializedIndex(200 + i);
            partitions.add(new PhysicalPartition(partId, partId, index));
        }
        return partitions;
    }

    @Test
    public void testBuildPartitionsSequentiallyRetrySuccess(@Mocked GlobalStateMgr globalStateMgr) {
        Config.lake_create_tablet_max_retries = 1;
        mockGlobalStateMgr(globalStateMgr, new HashSet<>(Arrays.asList(ALIVE_NODE_ID)));
        mockBuildCreateReplicaTasks(DEAD_NODE_ID, 20001L);

        final int[] sendCallCount = {0};
        new MockUp<TabletTaskExecutor>() {
            @Mock
            CompletableFuture<Boolean> sendTask(Long backendId, List<AgentTask> agentBatchTask) {
                sendCallCount[0]++;
                if (backendId == DEAD_NODE_ID) {
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    f.completeExceptionally(new RuntimeException("Can't get backend " + backendId));
                    return f;
                }
                return CompletableFuture.completedFuture(true);
            }
            @Mock
            void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) {
                while (countDownLatch.getCount() > 0) {
                    List<java.util.Map.Entry<Long, Long>> marks = countDownLatch.getLeftMarks();
                    for (java.util.Map.Entry<Long, Long> mark : marks) {
                        countDownLatch.markedCountDown(mark.getKey(), mark.getValue());
                    }
                }
            }
        };

        Assertions.assertDoesNotThrow(() ->
                TabletTaskExecutor.buildPartitionsSequentially(DB_ID, mockLakeTable(), mockPartitions(2),
                        2, 2, WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID),
                        new TabletTaskExecutor.CreateTabletOption()));

        // Initial send: all 2 tasks go to DEAD_NODE (1 sendTask call) + retry: 2 tasks to ALIVE_NODE (1 sendTask call)
        Assertions.assertEquals(2, sendCallCount[0]);
    }

    @Test
    public void testBuildPartitionsConcurrentlyRetrySuccess(@Mocked GlobalStateMgr globalStateMgr) {
        Config.lake_create_tablet_max_retries = 1;
        mockGlobalStateMgr(globalStateMgr, new HashSet<>(Arrays.asList(ALIVE_NODE_ID)));
        mockBuildCreateReplicaTasks(DEAD_NODE_ID, 20001L);

        final int[] sendCallCount = {0};
        new MockUp<TabletTaskExecutor>() {
            @Mock
            CompletableFuture<Boolean> sendTask(Long backendId, List<AgentTask> agentBatchTask) {
                sendCallCount[0]++;
                if (backendId == DEAD_NODE_ID) {
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    f.completeExceptionally(new RuntimeException("Can't get backend " + backendId));
                    return f;
                }
                return CompletableFuture.completedFuture(true);
            }
            @Mock
            void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) {
                while (countDownLatch.getCount() > 0) {
                    List<java.util.Map.Entry<Long, Long>> marks = countDownLatch.getLeftMarks();
                    for (java.util.Map.Entry<Long, Long> mark : marks) {
                        countDownLatch.markedCountDown(mark.getKey(), mark.getValue());
                    }
                }
            }
        };

        Assertions.assertDoesNotThrow(() ->
                TabletTaskExecutor.buildPartitionsConcurrently(DB_ID, mockLakeTable(), mockPartitions(3),
                        3, 2, WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID),
                        new TabletTaskExecutor.CreateTabletOption()));

        // Initial send: 3 partitions each with 1 task to DEAD_NODE (3 sendTask calls) + retry: tasks to ALIVE_NODE (1 sendTask call)
        Assertions.assertEquals(4, sendCallCount[0]);
    }

    @Test
    public void testBuildPartitionsConcurrentlyRetryNoAvailableNode(@Mocked GlobalStateMgr globalStateMgr) {
        Config.lake_create_tablet_max_retries = 1;
        // Only DEAD_NODE is alive — will be excluded after failure, no candidate left
        mockGlobalStateMgr(globalStateMgr, new HashSet<>(Arrays.asList(DEAD_NODE_ID)));
        mockBuildCreateReplicaTasks(DEAD_NODE_ID, 20001L);

        new MockUp<TabletTaskExecutor>() {
            @Mock
            CompletableFuture<Boolean> sendTask(Long backendId, List<AgentTask> agentBatchTask) {
                CompletableFuture<Boolean> f = new CompletableFuture<>();
                f.completeExceptionally(new RuntimeException("Can't get backend " + backendId));
                return f;
            }
            @Mock
            void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) {
                while (countDownLatch.getCount() > 0) {
                    List<java.util.Map.Entry<Long, Long>> marks = countDownLatch.getLeftMarks();
                    for (java.util.Map.Entry<Long, Long> mark : marks) {
                        countDownLatch.markedCountDown(mark.getKey(), mark.getValue());
                    }
                }
            }
        };

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                TabletTaskExecutor.buildPartitionsConcurrently(DB_ID, mockLakeTable(), mockPartitions(2),
                        2, 1, WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID),
                        new TabletTaskExecutor.CreateTabletOption()));
        Assertions.assertTrue(exception.getMessage().contains("No available node for retry"));
    }

    @Test
    public void testBuildPartitionsRetryDisabled(@Mocked GlobalStateMgr globalStateMgr) {
        Config.lake_create_tablet_max_retries = 0;
        mockGlobalStateMgr(globalStateMgr, new HashSet<>());
        mockBuildCreateReplicaTasks(DEAD_NODE_ID, 20001L);
        mockSendAndWait(new HashSet<>()); // all nodes fail

        // Retry disabled — RuntimeException from sendTask propagates directly
        Assertions.assertThrows(RuntimeException.class, () ->
                TabletTaskExecutor.buildPartitionsSequentially(DB_ID, mockLakeTable(), mockPartitions(1),
                        1, 1, WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID),
                        new TabletTaskExecutor.CreateTabletOption()));
    }

    @Test
    public void testGetNodeIdForTabletFallback(@Mocked GlobalStateMgr globalStateMgr) {
        Config.lake_create_tablet_max_retries = 1;
        long aliveNodeId = ALIVE_NODE_ID;
        long tabletId = 20001L;

        ComputeNode aliveNode = new ComputeNode(aliveNodeId, "127.0.0.1", 9050);
        aliveNode.setAlive(true);

        WarehouseManager warehouseManager = new WarehouseManager();
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tid) {
                throw new RuntimeException("no assigned node");
            }
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return List.of(aliveNode);
            }
        };

        ComputeResource computeResource = WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        long nodeId = TabletTaskExecutor.getNodeIdForTablet(warehouseManager, computeResource, tabletId);
        Assertions.assertEquals(aliveNodeId, nodeId);
    }

    @Test
    public void testGetNodeIdForTabletNoFallbackWhenRetryDisabled(@Mocked GlobalStateMgr globalStateMgr) {
        Config.lake_create_tablet_max_retries = 0;

        WarehouseManager warehouseManager = new WarehouseManager();
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tid) {
                throw new RuntimeException("no assigned node");
            }
        };

        ComputeResource computeResource = WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assertions.assertThrows(RuntimeException.class, () ->
                TabletTaskExecutor.getNodeIdForTablet(warehouseManager, computeResource, 20001L));
    }

    @Test
    public void testWaitForFinishedTimeoutWithExistingNode(@Mocked GlobalStateMgr globalStateMgr) {
        long backendId = 10001L;
        long tabletId = 20001L;
        String expectedHost = "192.168.1.100";

        // Mock Backend node
        Backend backend = new Backend(backendId, expectedHost, 9050);
        backend.setBePort(9060);
        backend.setAlive(true);

        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == backendId) {
                    return backend;
                }
                return null;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        // Create a timed-out CountDownLatch (count > 0 indicates unfinished tasks)
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(1);
        countDownLatch.addMark(backendId, tabletId);

        // Call waitForFinished method, expecting DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 1L);
        });

        // Verify error message contains node host
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains(expectedHost),
                "Error message should contain node host: " + expectedHost + ", actual error message: " + errorMessage);
        Assertions.assertTrue(errorMessage.contains(String.valueOf(tabletId)),
                "Error message should contain tabletId: " + tabletId);
        Assertions.assertTrue(errorMessage.contains("Table creation timed out"),
                "Error message should contain timeout message");
    }

    @Test
    public void testWaitForFinishedTimeoutWithNonExistingNode(@Mocked GlobalStateMgr globalStateMgr) {
        long backendId = 10002L;
        long tabletId = 20002L;

        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        // Mock SystemInfoService, return null to indicate node does not exist
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return null;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        // Create a timed-out CountDownLatch
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(1);
        countDownLatch.addMark(backendId, tabletId);

        // Call waitForFinished method, expecting DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 1L);
        });

        // Verify error message contains "N/A"
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("N/A"),
                "When node does not exist, error message should contain 'N/A', actual error message: " + errorMessage);
        Assertions.assertTrue(errorMessage.contains(String.valueOf(tabletId)),
                "Error message should contain tabletId: " + tabletId);
        Assertions.assertTrue(errorMessage.contains("Table creation timed out"),
                "Error message should contain timeout message");
    }

    @Test
    public void testWaitForFinishedTimeoutWithMultipleTablets(@Mocked GlobalStateMgr globalStateMgr) {
        long backendId1 = 10001L;
        long backendId2 = 10002L;
        long tabletId1 = 20001L;
        long tabletId2 = 20002L;
        long tabletId3 = 20003L;
        String host1 = "192.168.1.100";

        Backend backend1 = new Backend(backendId1, host1, 9050);
        backend1.setAlive(true);

        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == backendId1) {
                    return backend1;
                }
                return null; // backendId2 does not exist
            }
        };

        // Mock NodeMgr
        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };

        // Mock GlobalStateMgr
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        // Create CountDownLatch with multiple unfinished tasks
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(3);
        countDownLatch.addMark(backendId1, tabletId1);
        countDownLatch.addMark(backendId2, tabletId2);
        countDownLatch.addMark(backendId2, tabletId3);

        // Call waitForFinished method
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 1L);
        });

        // Verify error message
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("(3/3)"),
                "Error message should show the number of unfinished tasks");
        Assertions.assertTrue(errorMessage.contains(host1) && errorMessage.contains("N/A"),
                "Error message should contain node information");
    }

    @Test
    public void testWaitForFinishedSuccess(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(1);
        countDownLatch.addMark(10001L, 20001L);

        // Simulate task completion
        countDownLatch.markedCountDown(10001L, 20001L);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        // Call waitForFinished method, should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 10L);
        });
    }
}
