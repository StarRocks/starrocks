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

package com.starrocks.lake.compaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.LakeTable;
import com.starrocks.proto.AggregateCompactRequest;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.ComputeNodePB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionSchedulerTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;
    @Mocked
    private DatabaseTransactionMgr dbTransactionMgr;
    @Mocked
    private LakeService lakeService;
    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    private WarehouseManager warehouseManager;
    @Mocked
    private Warehouse warehouse;
    @Mocked
    private LakeAggregator lakeAggregator;

    @Test
    public void testDisableCompaction() {
        Config.lake_compaction_disable_ids = "23456";
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(),
                        Config.lake_compaction_disable_ids);

        Assertions.assertTrue(compactionScheduler.isTableDisabled(23456L));
        Assertions.assertTrue(compactionScheduler.isPartitionDisabled(23456L));

        compactionScheduler.disableTableOrPartitionId("34567;45678;56789");

        Assertions.assertFalse(compactionScheduler.isPartitionDisabled(23456L));
        Assertions.assertTrue(compactionScheduler.isTableDisabled(34567L));
        Assertions.assertTrue(compactionScheduler.isTableDisabled(45678L));
        Assertions.assertTrue(compactionScheduler.isPartitionDisabled(56789L));

        compactionScheduler.disableTableOrPartitionId("");
        Assertions.assertFalse(compactionScheduler.isTableDisabled(34567L));
        Config.lake_compaction_disable_ids = "";
    }

    @Test
    public void testStartCompaction() {
        OlapTable table = new LakeTable();
        CompactionMgr compactionManager = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics = new PartitionStatistics(partition);
        Quantiles q = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScore(q);
        PartitionStatisticsSnapshot snapshot = new PartitionStatisticsSnapshot(statistics);
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, globalTransactionMgr,
                globalStateMgr, "");
        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return new LocalMetastore(globalStateMgr, null, null);
            }
        };
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return new Database(100, "aaa");
            }
            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return table;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public PhysicalPartition getPhysicalPartition(long physicalPartitionId) {
                return new PhysicalPartition(123, "aaa", 123, new MaterializedIndex());
            }
        };
        CompactionWarehouseInfo info = new CompactionWarehouseInfo("aaa", WarehouseManager.DEFAULT_RESOURCE, 0, 0);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assertions.assertNull(compactionScheduler.startCompaction(snapshot, info));
        table.setState(OlapTable.OlapTableState.NORMAL);
        Assertions.assertNull(compactionScheduler.startCompaction(snapshot, info));
    }

    @Test
    public void testStartCompactionWithFileBundling() throws RpcException {
        LakeTable table = new LakeTable();
        table.setFileBundling(true);
        CompactionMgr compactionManager = new CompactionMgr();
        PartitionIdentifier partition = new PartitionIdentifier(1, 2, 3);
        PartitionStatistics statistics = new PartitionStatistics(partition);
        statistics.setCompactionScore(new Quantiles(1.0, 2.0, 3.0));
        PartitionStatisticsSnapshot snapshot = new PartitionStatisticsSnapshot(statistics);
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, systemInfoService,
                globalTransactionMgr, globalStateMgr, "");

        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return new LocalMetastore(globalStateMgr, null, null);
            }
        };
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                return new Database(100, "aaa");
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return table;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public PhysicalPartition getPhysicalPartition(long physicalPartitionId) {
                return new PhysicalPartition(123, "aaa", 123, new MaterializedIndex());
            }
        };

        new MockUp<CompactionScheduler>() {
            @Mock
            protected long beginTransaction(PartitionIdentifier partition, ComputeResource computeResource) {
                return 100L;
            }

            @Mock
            protected Map<Long, List<Long>> collectPartitionTablets(PhysicalPartition partition,
                                                        ComputeResource computeResource) {
                Map<Long, List<Long>> map = new HashMap<>();
                map.put(1L, Lists.newArrayList(10L));
                return map;
            }
        };

        ComputeNode node = new ComputeNode(1L, "127.0.0.1", 9050);
        node.setBrpcPort(9050);
        ComputeNode aggregatorNode = new ComputeNode(2L, "127.0.0.2", 9050);
        aggregatorNode.setBrpcPort(9050);
        
        new Expectations() {
            {
                systemInfoService.getBackendOrComputeNode(1L);
                result = node;

                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;

                LakeAggregator.chooseAggregatorNode(WarehouseManager.DEFAULT_RESOURCE);
                result = aggregatorNode;
            }
        };

        new Expectations() {
            {
                BrpcProxy.getLakeService("127.0.0.1", 9050);
                result = lakeService;
                
                // 添加为 aggregator node 的 LakeService
                BrpcProxy.getLakeService("127.0.0.2", 9050);
                result = lakeService;
            }
        };

        new MockUp<CompactionTask>() {
            @Mock
            public void sendRequest() {
            }
        };

        new MockUp<AggregateCompactionTask>() {
            @Mock
            public void sendRequest() {
            }
        };

        CompactionWarehouseInfo info = new CompactionWarehouseInfo("aaa", WarehouseManager.DEFAULT_RESOURCE, 0, 0);
        table.setState(OlapTable.OlapTableState.NORMAL);
        compactionScheduler.startCompaction(snapshot, info);
    }

    @Test
    public void testGetHistory() {
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(), "");
        new MockUp<CompactionScheduler>() {
            @Mock
            public ConcurrentHashMap<PartitionIdentifier, CompactionJob> getRunningCompactions() {
                ConcurrentHashMap<PartitionIdentifier, CompactionJob> r = new ConcurrentHashMap<>();
                Database db = new Database();
                Table table = new LakeTable();
                PartitionIdentifier partitionIdentifier1 = new PartitionIdentifier(1, 2, 3);
                PartitionIdentifier partitionIdentifier2 = new PartitionIdentifier(1, 2, 4);
                PhysicalPartition partition1 = new PhysicalPartition(123, "aaa", 123, null);
                PhysicalPartition partition2 = new PhysicalPartition(124, "bbb", 124, null);
                CompactionJob job1 = new CompactionJob(db, table, partition1, 100, false, null, "");
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                CompactionJob job2 = new CompactionJob(db, table, partition2, 101, false, null, "");
                r.put(partitionIdentifier1, job1);
                r.put(partitionIdentifier2, job2);
                return r;
            }
        };

        List<CompactionRecord> list = compactionScheduler.getHistory();
        Assertions.assertEquals(2, list.size());
        Assertions.assertTrue(list.get(0).getStartTs() <= list.get(1).getStartTs());
    }

    @Test
    public void testCompactionTaskLimit() {
        CompactionScheduler compactionScheduler = new CompactionScheduler(new CompactionMgr(), null, null, null, "");

        int defaultValue = Config.lake_compaction_max_tasks;
        // explicitly set config to a value bigger than default -1
        Config.lake_compaction_max_tasks = 10;
        Assertions.assertEquals(10, compactionScheduler.compactionTaskLimit(WarehouseManager.DEFAULT_RESOURCE));

        // reset config to default value
        Config.lake_compaction_max_tasks = defaultValue;

        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10001L, "192.168.0.3", 9050);

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };
        mockedWarehouseManager.setComputeNodesAssignedToTablet(Sets.newHashSet(b1, c1, c2));
        Assertions.assertEquals(3 * 16, compactionScheduler.compactionTaskLimit(WarehouseManager.DEFAULT_RESOURCE));
    }

    @Test
    public void testAbortStaleCompaction() {
        CompactionMgr compactionManager = new CompactionMgr();

        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);

        compactionManager.handleLoadingFinished(partition1, 10, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(10d)));
        compactionManager.handleLoadingFinished(partition2, 10, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(10d)));

        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10002L, "192.168.0.3", 9050);

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        mockedWarehouseManager.initDefaultWarehouse();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
            @Mock
            public boolean isLeader() {
                return true;
            }
            @Mock
            public boolean isReady() {
                return true;
            }
        };
        mockedWarehouseManager.setComputeNodesAssignedToTablet(Sets.newHashSet(c1, c2));

        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, globalTransactionMgr,
                globalStateMgr, "");

        new MockUp<CompactionScheduler>() {
            @Mock
            protected CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot,
                    CompactionWarehouseInfo info) {
                Database db = new Database();
                Table table = new LakeTable();
                long partitionId = partitionStatisticsSnapshot.getPartition().getPartitionId();
                PhysicalPartition partition = new PhysicalPartition(partitionId, "aaa", partitionId, null);
                return new CompactionJob(db, table, partition, 100, false, info.computeResource, info.warehouseName);
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getCompactionComputeResource(long tableId) {
                throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE, "");
            }
        };
        compactionScheduler.runOneCycle();
        Assertions.assertEquals(0, compactionScheduler.getRunningCompactions().size());

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getCompactionComputeResource(long tableId) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        compactionScheduler.runOneCycle();
        Assertions.assertEquals(2, compactionScheduler.getRunningCompactions().size());

        long old = CompactionScheduler.PARTITION_CLEAN_INTERVAL_SECOND;
        CompactionScheduler.PARTITION_CLEAN_INTERVAL_SECOND = 0;
        new MockUp<MetaUtils>() {
            @Mock
            public static boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
                return false;
            }
        };
        new MockUp<CompactionJob>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.NONE_SUCCESS;
            }
            @Mock
            public String getFailMessage() {
                return "abort in test";
            }
        };
        compactionScheduler.runOneCycle();
        Assertions.assertEquals(0, compactionScheduler.getRunningCompactions().size());
        CompactionScheduler.PARTITION_CLEAN_INTERVAL_SECOND = old;
    }

    @Test
    public void testCreateAggregateCompactionTask() throws Exception {
        long currentVersion = 1000L;
        long txnId = 2000L;
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        beToTablets.put(1001L, Lists.newArrayList(101L, 102L));
        beToTablets.put(1002L, Lists.newArrayList(201L, 202L));
        PartitionStatistics.CompactionPriority priority = PartitionStatistics.CompactionPriority.DEFAULT;

        CompactionMgr compactionManager = new CompactionMgr();

        ComputeNode node1 = new ComputeNode(1001L, "192.168.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1002L, "192.168.0.2", 9040);
        node2.setBrpcPort(9050);
        ComputeNode aggregatorNode = new ComputeNode(1003L, "192.168.0.3", 9040);
        aggregatorNode.setBrpcPort(9050);

        new Expectations() {
            {
                systemInfoService.getBackendOrComputeNode(1001L);
                result = node1;
                systemInfoService.getBackendOrComputeNode(1002L);
                result = node2;

                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;
            }
        };

        CompactionScheduler scheduler = new CompactionScheduler(compactionManager, systemInfoService,
                globalTransactionMgr, globalStateMgr, "");

        Method method = CompactionScheduler.class.getDeclaredMethod("createAggregateCompactionTask",
                long.class, Map.class, long.class, PartitionStatistics.CompactionPriority.class, ComputeResource.class,
                long.class);
        method.setAccessible(true);
        CompactionTask task = (CompactionTask) method.invoke(scheduler, currentVersion, beToTablets, txnId, priority,
                WarehouseManager.DEFAULT_RESOURCE, 99L);

        Assertions.assertNotNull(task);
        Assertions.assertTrue(task instanceof AggregateCompactionTask);

        Field serviceField = CompactionTask.class.getDeclaredField("rpcChannel");
        serviceField.setAccessible(true);

        Field requestField = AggregateCompactionTask.class.getDeclaredField("request");
        requestField.setAccessible(true);
        AggregateCompactRequest aggRequest = (AggregateCompactRequest) requestField.get(task);

        Assertions.assertEquals(2, aggRequest.requests.size());
        Assertions.assertEquals(2, aggRequest.computeNodes.size());

        boolean foundTablets1 = false;
        boolean foundTablets2 = false;

        for (CompactRequest req : aggRequest.requests) {
            Assertions.assertEquals(txnId, req.txnId.longValue());
            Assertions.assertEquals(currentVersion, req.version.longValue());
            Assertions.assertEquals(false, req.allowPartialSuccess);
            Assertions.assertEquals(false, req.forceBaseCompaction);

            if (req.tabletIds.equals(Lists.newArrayList(101L, 102L))) {
                foundTablets1 = true;
            } else if (req.tabletIds.equals(Lists.newArrayList(201L, 202L))) {
                foundTablets2 = true;
            }
        }

        Assertions.assertTrue(foundTablets1);
        Assertions.assertTrue(foundTablets2);

        boolean foundNode1 = false;
        boolean foundNode2 = false;

        for (ComputeNodePB nodePB : aggRequest.computeNodes) {
            if (nodePB.getId() == 1001L) {
                Assertions.assertEquals("192.168.0.1", nodePB.getHost());
                Assertions.assertEquals(9050, (int) nodePB.getBrpcPort());
                foundNode1 = true;
            } else if (nodePB.getId() == 1002L) {
                Assertions.assertEquals("192.168.0.2", nodePB.getHost());
                Assertions.assertEquals(9050, (int) nodePB.getBrpcPort());
                foundNode2 = true;
            }
        }

        Assertions.assertTrue(foundNode1);
        Assertions.assertTrue(foundNode2);
    }

    @Test
    public void testCompactionWarehouseLimit() {
        CompactionMgr compactionManager = new CompactionMgr();

        PartitionIdentifier partition1 = new PartitionIdentifier(1, 2, 3);
        PartitionIdentifier partition2 = new PartitionIdentifier(1, 2, 4);
        PartitionIdentifier partition3 = new PartitionIdentifier(1, 2, 5);

        compactionManager.handleLoadingFinished(partition1, 10, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(10d)));
        compactionManager.handleLoadingFinished(partition2, 10, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(10d)));
        compactionManager.handleLoadingFinished(partition3, 10, System.currentTimeMillis(),
                                                Quantiles.compute(Lists.newArrayList(10d)));

        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10002L, "192.168.0.3", 9050);

        int old = Config.lake_compaction_max_tasks;
        Config.lake_compaction_max_tasks = 2;

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        mockedWarehouseManager.initDefaultWarehouse();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
            @Mock
            public boolean isLeader() {
                return true;
            }
            @Mock
            public boolean isReady() {
                return true;
            }
        };
        mockedWarehouseManager.setComputeNodesAssignedToTablet(Sets.newHashSet(c1, c2));

        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, globalTransactionMgr,
                globalStateMgr, "");

        new MockUp<CompactionScheduler>() {
            @Mock
            protected CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot,
                    CompactionWarehouseInfo info) {
                Database db = new Database();
                Table table = new LakeTable();
                long partitionId = partitionStatisticsSnapshot.getPartition().getPartitionId();
                PhysicalPartition partition = new PhysicalPartition(partitionId, "aaa", partitionId, null);
                CompactionJob job = new CompactionJob(db, table, partition, 100, false, info.computeResource, info.warehouseName);
                return job;
            }
        };
        new MockUp<CompactionJob>() {
            @Mock
            public int getNumTabletCompactionTasks() {
                return 1;
            }
        };
        compactionScheduler.runOneCycle();
        Assertions.assertEquals(2, compactionScheduler.getRunningCompactions().size());

        Config.lake_compaction_max_tasks = old;
    }

    @Test
    public void testCreateAggregateCompactionTaskWithNull() throws Exception {
        long currentVersion = 1000L;
        long txnId = 2000L;
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        beToTablets.put(1001L, Lists.newArrayList(101L, 102L));
        beToTablets.put(1002L, Lists.newArrayList(201L, 202L));
        PartitionStatistics.CompactionPriority priority = PartitionStatistics.CompactionPriority.DEFAULT;

        CompactionMgr compactionManager = new CompactionMgr();

        ComputeNode node1 = new ComputeNode(1001L, "192.168.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1002L, "192.168.0.2", 9040);
        node2.setBrpcPort(9050);

        new Expectations() {
            {
                systemInfoService.getBackendOrComputeNode(1001L);
                result = node1;
                systemInfoService.getBackendOrComputeNode(1002L);
                result = node2;

                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;

                lakeAggregator.chooseAggregatorNode(WarehouseManager.DEFAULT_RESOURCE);
                result = null;
            }
        };

        CompactionScheduler scheduler = new CompactionScheduler(compactionManager, systemInfoService,
                globalTransactionMgr, globalStateMgr, "");

        Method method = CompactionScheduler.class.getDeclaredMethod("createAggregateCompactionTask",
                long.class, Map.class, long.class, PartitionStatistics.CompactionPriority.class,
                ComputeResource.class, long.class);
        method.setAccessible(true);
        ExceptionChecker.expectThrows(InvocationTargetException.class,
                () -> {
                    method.invoke(scheduler, currentVersion, beToTablets, txnId, priority,
                            WarehouseManager.DEFAULT_RESOURCE, 99L);
                });
    }

    /**
     * Test createCompactionTasks with parallel compaction config enabled (covers lines 411-416)
     */
    @Test
    public void testCreateCompactionTasksWithParallelConfig() throws Exception {
        long currentVersion = 1000L;
        long txnId = 3000L;
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        beToTablets.put(1001L, Lists.newArrayList(101L, 102L));
        PartitionStatistics.CompactionPriority priority = PartitionStatistics.CompactionPriority.DEFAULT;

        CompactionMgr compactionManager = new CompactionMgr();

        ComputeNode node1 = new ComputeNode(1001L, "192.168.0.1", 9040);
        node1.setBrpcPort(9050);

        new Expectations() {
            {
                systemInfoService.getBackendOrComputeNode(1001L);
                result = node1;
            }
        };

        new Expectations() {
            {
                BrpcProxy.getLakeService("192.168.0.1", 9050);
                result = lakeService;
            }
        };

        CompactionScheduler scheduler = new CompactionScheduler(compactionManager, systemInfoService,
                globalTransactionMgr, globalStateMgr, "");

        // Enable parallel compaction config
        boolean oldEnableParallel = Config.lake_compaction_enable_parallel_per_tablet;
        int oldMaxParallel = Config.lake_compaction_max_parallel_per_tablet;
        long oldMaxBytes = Config.lake_compaction_max_bytes_per_subtask;
        
        try {
            Config.lake_compaction_enable_parallel_per_tablet = true;
            Config.lake_compaction_max_parallel_per_tablet = 5;
            Config.lake_compaction_max_bytes_per_subtask = 1024 * 1024 * 100L; // 100MB

            Method method = CompactionScheduler.class.getDeclaredMethod("createCompactionTasks",
                    long.class, Map.class, long.class, boolean.class, PartitionStatistics.CompactionPriority.class);
            method.setAccessible(true);
            List<CompactionTask> tasks = (List<CompactionTask>) method.invoke(scheduler, currentVersion, beToTablets, 
                    txnId, false, priority);

            Assertions.assertNotNull(tasks);
            Assertions.assertEquals(1, tasks.size());

            // Verify the parallel config was set in the request
            Field requestField = CompactionTask.class.getDeclaredField("request");
            requestField.setAccessible(true);
            CompactRequest request = (CompactRequest) requestField.get(tasks.get(0));

            Assertions.assertNotNull(request.parallelConfig);
            Assertions.assertTrue(request.parallelConfig.enableParallel);
            Assertions.assertEquals(5, (int) request.parallelConfig.maxParallelPerTablet);
            Assertions.assertEquals(1024 * 1024 * 100L, (long) request.parallelConfig.maxBytesPerSubtask);
        } finally {
            // Restore original config values
            Config.lake_compaction_enable_parallel_per_tablet = oldEnableParallel;
            Config.lake_compaction_max_parallel_per_tablet = oldMaxParallel;
            Config.lake_compaction_max_bytes_per_subtask = oldMaxBytes;
        }
    }

    /**
     * Test createAggregateCompactionTask with parallel compaction config enabled (covers lines 457-461)
     */
    @Test
    public void testCreateAggregateCompactionTaskWithParallelConfig() throws Exception {
        long currentVersion = 1000L;
        long txnId = 4000L;
        Map<Long, List<Long>> beToTablets = new HashMap<>();
        beToTablets.put(1001L, Lists.newArrayList(101L, 102L));
        beToTablets.put(1002L, Lists.newArrayList(201L, 202L));
        PartitionStatistics.CompactionPriority priority = PartitionStatistics.CompactionPriority.DEFAULT;

        CompactionMgr compactionManager = new CompactionMgr();

        ComputeNode node1 = new ComputeNode(1001L, "192.168.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1002L, "192.168.0.2", 9040);
        node2.setBrpcPort(9050);
        ComputeNode aggregatorNode = new ComputeNode(1003L, "192.168.0.3", 9040);
        aggregatorNode.setBrpcPort(9050);

        new Expectations() {
            {
                systemInfoService.getBackendOrComputeNode(1001L);
                result = node1;
                systemInfoService.getBackendOrComputeNode(1002L);
                result = node2;

                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;

                LakeAggregator.chooseAggregatorNode(WarehouseManager.DEFAULT_RESOURCE);
                result = aggregatorNode;
            }
        };

        new Expectations() {
            {
                BrpcProxy.getLakeService("192.168.0.3", 9050);
                result = lakeService;
            }
        };

        CompactionScheduler scheduler = new CompactionScheduler(compactionManager, systemInfoService,
                globalTransactionMgr, globalStateMgr, "");

        // Enable parallel compaction config
        boolean oldEnableParallel = Config.lake_compaction_enable_parallel_per_tablet;
        int oldMaxParallel = Config.lake_compaction_max_parallel_per_tablet;
        long oldMaxBytes = Config.lake_compaction_max_bytes_per_subtask;
        
        try {
            Config.lake_compaction_enable_parallel_per_tablet = true;
            Config.lake_compaction_max_parallel_per_tablet = 8;
            Config.lake_compaction_max_bytes_per_subtask = 1024 * 1024 * 200L; // 200MB

            Method method = CompactionScheduler.class.getDeclaredMethod("createAggregateCompactionTask",
                    long.class, Map.class, long.class, PartitionStatistics.CompactionPriority.class, 
                    ComputeResource.class, long.class);
            method.setAccessible(true);
            CompactionTask task = (CompactionTask) method.invoke(scheduler, currentVersion, beToTablets, txnId, 
                    priority, WarehouseManager.DEFAULT_RESOURCE, 99L);

            Assertions.assertNotNull(task);
            Assertions.assertTrue(task instanceof AggregateCompactionTask);

            Field requestField = AggregateCompactionTask.class.getDeclaredField("request");
            requestField.setAccessible(true);
            AggregateCompactRequest aggRequest = (AggregateCompactRequest) requestField.get(task);

            Assertions.assertEquals(2, aggRequest.requests.size());

            // Verify parallel config was set in each request
            for (CompactRequest req : aggRequest.requests) {
                Assertions.assertNotNull(req.parallelConfig, "parallelConfig should be set when enabled");
                Assertions.assertTrue(req.parallelConfig.enableParallel);
                Assertions.assertEquals(8, (int) req.parallelConfig.maxParallelPerTablet);
                Assertions.assertEquals(1024 * 1024 * 200L, (long) req.parallelConfig.maxBytesPerSubtask);
            }
        } finally {
            // Restore original config values
            Config.lake_compaction_enable_parallel_per_tablet = oldEnableParallel;
            Config.lake_compaction_max_parallel_per_tablet = oldMaxParallel;
            Config.lake_compaction_max_bytes_per_subtask = oldMaxBytes;
        }
    }
}
