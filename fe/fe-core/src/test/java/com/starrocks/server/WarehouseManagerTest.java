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

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class WarehouseManagerTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    NodeMgr nodeMgr;

    @Mocked
    SystemInfoService systemInfo;

    @Mocked
    StarOSAgent starOSAgent;

    @BeforeAll
    public static void setUp() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
    }

    @Test
    public void testWarehouseNotExist() {
        WarehouseManager mgr = new WarehouseManager();
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getWarehouse("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getWarehouse(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getAllComputeNodeIds(WarehouseComputeResource.of(1L)));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getComputeNodeId(WarehouseComputeResource.of(1L), 0));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getAliveComputeNodeId(WarehouseComputeResource.of(1L), 0));
    }

    @Test
    public void testGetAliveComputeNodes() throws StarRocksException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfo;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getAllNodeIdsByShard(long shardId, long workerGroupId)
                    throws StarRocksException {
                throw new StarRocksException("get all node ids by shard failure");
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = Lists.newArrayList(10003L, 10004L);
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == 10003L) {
                    ComputeNode node = new ComputeNode();
                    node.setAlive(false);
                    return node;
                }
                ComputeNode node = new ComputeNode();
                node.setAlive(true);
                return node;
            }
        };

        WarehouseManager mgr = new WarehouseManager();
        mgr.initDefaultWarehouse();

        List<Long> nodeIds = mgr.getAllComputeNodeIds(WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(2, nodeIds.size());

        List<ComputeNode> nodes = mgr.getAliveComputeNodes(WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(1, nodes.size());

        LakeTablet tablet = new LakeTablet(1L);
        Long nodeId = mgr.getAliveComputeNodeId(WarehouseManager.DEFAULT_RESOURCE, tablet.getId());
        Assertions.assertNull(nodeId);
    }

    public Optional<Long> getWorkerGroupId(WarehouseManager warehouseManager, long warehouseId) {
        final Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", warehouseId));
        }
        final CRAcquireContext acquireContext = CRAcquireContext.of(warehouseId);
        final Optional<ComputeResource> result = warehouseManager.computeResourceProvider.acquireComputeResource(
                warehouse, acquireContext);
        return result.map(ComputeResource::getWorkerGroupId);
    }

    @Test
    public void testSelectWorkerGroupByWarehouseId_hasAliveNodes() throws StarRocksException {
        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        b1.setBePort(9060);
        b1.setAlive(true);
        b1.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfo;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return b1;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                if (workerGroupId == StarOSAgent.DEFAULT_WORKER_GROUP_ID) {
                    return Lists.newArrayList(b1.getId());
                }
                return Lists.newArrayList();
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                if (computeResource.getWarehouseId() == WarehouseManager.DEFAULT_WAREHOUSE_ID) {
                    return new ArrayList<>(Arrays.asList(b1));
                }
                return Lists.newArrayList();
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                if (warehouseId != WarehouseManager.DEFAULT_WAREHOUSE_ID) {
                    throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("id: %d", warehouseId));
                }
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
        Optional<Long> workerGroupId = getWorkerGroupId(warehouseManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assertions.assertFalse(workerGroupId.isEmpty());
        Assertions.assertEquals(StarOSAgent.DEFAULT_WORKER_GROUP_ID, workerGroupId.get().longValue());

        try {
            workerGroupId = Optional.ofNullable(null);
            workerGroupId = getWorkerGroupId(warehouseManager, 1111L);
            Assertions.assertEquals(1, 2);   // can not be here
        } catch (ErrorReportException e) {
            Assertions.assertTrue(workerGroupId.isEmpty());
            Assertions.assertEquals(workerGroupId.orElse(1000L).longValue(), 1000L);
        }
    }

    @Test
    public void testSelectWorkerGroupByWarehouseId_hasNoAliveNodes() throws StarRocksException {
        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        b1.setBePort(9060);
        b1.setAlive(false);
        b1.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfo;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return b1;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                if (workerGroupId == StarOSAgent.DEFAULT_WORKER_GROUP_ID) {
                    return Lists.newArrayList(b1.getId());
                }
                return Lists.newArrayList();
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return Lists.newArrayList();
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                if (warehouseId != WarehouseManager.DEFAULT_WAREHOUSE_ID) {
                    ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("id: %d", warehouseId));
                }
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        try {
            WarehouseManager warehouseManager = new WarehouseManager();
            warehouseManager.initDefaultWarehouse();
            Optional<Long> workerGroupId = getWorkerGroupId(warehouseManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
            Assertions.assertTrue(workerGroupId.isEmpty());
        } catch (ErrorReportException e) {
            Assertions.assertEquals(1, 2);   // can not be here
        }

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        OlapScanNode scanNode = newOlapScanNode();
        Partition partition = new Partition(123, 456, "aaa", null, null);
        MaterializedIndex index = new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL);
        ErrorReportException ex = Assertions.assertThrows(ErrorReportException.class,
                () -> scanNode.addScanRangeLocations(partition, partition.getDefaultPhysicalPartition(),
                        index, Collections.emptyList(), 1));
        Assertions.assertEquals("No alive backend or compute node in warehouse null.", ex.getMessage());
    }

    @Test
    public void testSelectWorkerGroupByWarehouseId_checkAliveNodesOnce(@Mocked WarehouseManager mockWarehouseMgr)
            throws StarRocksException {
        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        b1.setBePort(9060);
        b1.setAlive(false);
        b1.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfo;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return b1;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                if (workerGroupId == StarOSAgent.DEFAULT_WORKER_GROUP_ID) {
                    return Lists.newArrayList(b1.getId());
                }
                return Lists.newArrayList();
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockWarehouseMgr;
            }

        };

        ComputeNode livingCn = new ComputeNode();
        livingCn.setAlive(true);
        new Expectations() {
            {
                // This is the point of the test -- we only want to call this once even though we're calling
                // addScanRangeLocations multiple times.
                mockWarehouseMgr.getAliveComputeNodes(WarehouseManager.DEFAULT_RESOURCE);
                times = 1;
                result = Lists.newArrayList(livingCn);
            }
        };
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        OlapScanNode scanNode = newOlapScanNode();
        Partition partition = new Partition(123, 456, "aaa", null, null);
        MaterializedIndex index = new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL);
        scanNode.addScanRangeLocations(partition, partition.getDefaultPhysicalPartition(), index, Collections.emptyList(), 1);
        // Since this is the second call to  addScanRangeLocations on the same OlapScanNode, we do not expect another call to
        // getAliveComputeNodes.
        scanNode.addScanRangeLocations(partition, partition.getDefaultPhysicalPartition(), index, Collections.emptyList(), 1);
    }

    private OlapScanNode newOlapScanNode() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        OlapTable table = new OlapTable();
        table.setDefaultDistributionInfo(new HashDistributionInfo(3, Collections.emptyList()));
        desc.setTable(table);
        return new OlapScanNode(new PlanNodeId(1), desc, "OlapScanNode");
    }

    @Test
    public void testBackgroundWarehouse() {
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };
        WarehouseManager mgr = new WarehouseManager();
        mgr.initDefaultWarehouse();
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, mgr.getBackgroundWarehouse(123).getId());
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, mgr.getBackgroundComputeResource(123).getWarehouseId());
    }

    @Test
    public void getWarehouseComputeResourceName_returnsEmptyStringWhenSharedNothingMode() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedNothingMode() {
                return true;
            }
        };
        WarehouseManager warehouseManager = new WarehouseManager();
        ComputeResource computeResource = WarehouseComputeResource.of(0);
        Assertions.assertEquals("", warehouseManager.getWarehouseComputeResourceName(computeResource));
    }

    @Test
    public void getWarehouseComputeResourceName_returnsWarehouseNameWhenValidComputeResource() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        Warehouse warehouse = new DefaultWarehouse(1L, "test_warehouse");
        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.addWarehouse(warehouse);
        ComputeResource computeResource = WarehouseComputeResource.of(1L);
        Assertions.assertEquals("test_warehouse", warehouseManager.getWarehouseComputeResourceName(computeResource));
    }

    @Test
    public void getWarehouseListeners_returnsEmptyListWhenNoListenersRegistered() {
        WarehouseManager warehouseManager = new WarehouseManager();
        Assertions.assertTrue(warehouseManager.getWarehouseListeners().isEmpty());
    }

    class MockWarehouseEventListener implements WarehouseEventListener {
        @Override
        public void onCreateWarehouse(Warehouse wh) {

        }

        @Override
        public void onDropWarehouse(Warehouse wh) {

        }

        @Override
        public void onCreateCNGroup(Warehouse wh, long workerGroupId) {

        }

        @Override
        public void onDropCNGroup(Warehouse wh, long workerGroupId) {

        }
    }

    @Test
    public void getWarehouseListeners_returnsRegisteredListeners() {
        WarehouseEventListener listener1 = new MockWarehouseEventListener() {};
        WarehouseEventListener listener2 = new MockWarehouseEventListener() {};
        WarehouseComputeResourceProvider warehouseComputeResourceProvider = new WarehouseComputeResourceProvider();
        WarehouseManager warehouseManager = new WarehouseManager(warehouseComputeResourceProvider,
                Lists.newArrayList(listener1, listener2));
        Assertions.assertEquals(2, warehouseManager.getWarehouseListeners().size());
        Assertions.assertTrue(warehouseManager.getWarehouseListeners().contains(listener1));
        Assertions.assertTrue(warehouseManager.getWarehouseListeners().contains(listener2));
        Assertions.assertTrue(warehouseManager.getComputeResourceProvider().equals(warehouseComputeResourceProvider));
    }

    @Test
    public void getComputeResourceProvider_returnsDefaultProviderWhenNotSpecified() {
        WarehouseManager warehouseManager = new WarehouseManager();
        Assertions.assertTrue(warehouseManager.getComputeResourceProvider() instanceof WarehouseComputeResourceProvider);
    }

    @Test
    public void getWarehouseComputeResourceName_returnsEmptyStringWhenSharedNothingMode(
            @Mocked ComputeResource computeResource) {
        new Expectations() {
            {
                RunMode.isSharedDataMode();
                result = false;
            }
        };

        WarehouseManager warehouseManager = new WarehouseManager();
        String warehouseName = warehouseManager.getWarehouseComputeResourceName(computeResource);
        Assertions.assertEquals("", warehouseName);
    }

    @Test
    public void getWarehouseComputeResourceName_logsWarningAndReturnsEmptyStringOnException(
            @Mocked ComputeResource computeResource) {
        new Expectations() {
            {
                RunMode.isSharedDataMode();
                result = true;
            }
        };

        WarehouseManager warehouseManager = new WarehouseManager();
        String warehouseName = warehouseManager.getWarehouseComputeResourceName(computeResource);
        Assertions.assertEquals("", warehouseName);
    }

    @Test
    public void getNextComputeNodeIndexFromWarehouse_returnsSameAtomicIntegerInstance(@Mocked ComputeResource computeResource) {
        WarehouseManager warehouseManager = new WarehouseManager();
        AtomicInteger result = warehouseManager.getNextComputeNodeIndexFromWarehouse(computeResource);
        Assertions.assertNotNull(result);
        Assertions.assertSame(warehouseManager.getNextComputeNodeIndexFromWarehouse(computeResource), result);
    }

    @Test
    public void getNextComputeNodeIndexFromWarehouse_returnsNonNullAtomicInteger(@Mocked ComputeResource computeResource) {
        WarehouseManager warehouseManager = new WarehouseManager();
        AtomicInteger result = warehouseManager.getNextComputeNodeIndexFromWarehouse(computeResource);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.get());
    }
}
