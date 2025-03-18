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
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class WarehouseManagerTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    NodeMgr nodeMgr;

    @Mocked
    SystemInfoService systemInfo;

    @Mocked
    StarOSAgent starOSAgent;

    @Test
    public void testWarehouseNotExist() {
        WarehouseManager mgr = new WarehouseManager();
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getWarehouse("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getWarehouse(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse name: a not exist.",
                () -> mgr.getAllComputeNodeIds("a"));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getAllComputeNodeIds(1L));
        ExceptionChecker.expectThrowsWithMsg(ErrorReportException.class, "Warehouse id: 1 not exist.",
                () -> mgr.getComputeNodeId(1L, null));
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

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = Lists.newArrayList(10003L, 10004L);
            }
        };

        WarehouseManager mgr = new WarehouseManager();
        mgr.initDefaultWarehouse();

        List<Long> nodeIds = mgr.getAllComputeNodeIds(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(2, nodeIds.size());

        List<ComputeNode> nodes = mgr.getAliveComputeNodes(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(1, nodes.size());
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
            public List<ComputeNode> getAliveComputeNodes(long warehouseId) {
                if (warehouseId == WarehouseManager.DEFAULT_WAREHOUSE_ID) {
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
        Optional<Long> workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertFalse(workerGroupId.isEmpty());
        Assert.assertEquals(StarOSAgent.DEFAULT_WORKER_GROUP_ID, workerGroupId.get().longValue());

        try {
            workerGroupId = Optional.ofNullable(null);
            workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(1111L);
            Assert.assertEquals(1, 2);   // can not be here
        } catch (ErrorReportException e) {
            Assert.assertTrue(workerGroupId.isEmpty());
            Assert.assertEquals(workerGroupId.orElse(1000L).longValue(), 1000L);
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
            public List<ComputeNode> getAliveComputeNodes(long warehouseId) {
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
            Optional<Long> workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            Assert.assertTrue(workerGroupId.isEmpty());
        } catch (ErrorReportException e) {
            Assert.assertEquals(1, 2);   // can not be here
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
        ErrorReportException ex = Assert.assertThrows(ErrorReportException.class,
                () -> scanNode.addScanRangeLocations(partition, partition.getDefaultPhysicalPartition(),
                        index, Collections.emptyList(), 1));
        Assert.assertEquals("No alive backend or compute node in warehouse null.", ex.getMessage());
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
                mockWarehouseMgr.getAliveComputeNodes(WarehouseManager.DEFAULT_WAREHOUSE_ID);
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
}
