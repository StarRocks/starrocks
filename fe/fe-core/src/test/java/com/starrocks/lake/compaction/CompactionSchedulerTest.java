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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.LakeTable;
import com.starrocks.proto.AggregateCompactRequest;
import com.starrocks.proto.CompactRequest;
import com.starrocks.proto.ComputeNodePB;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
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
    public void testDisableTableCompaction() {
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(), "12345");

        Assert.assertTrue(compactionScheduler.isTableDisabled(12345L));

        compactionScheduler.disableTables("23456;34567;45678");

        Assert.assertFalse(compactionScheduler.isTableDisabled(12345L));
        Assert.assertTrue(compactionScheduler.isTableDisabled(23456L));
        Assert.assertTrue(compactionScheduler.isTableDisabled(34567L));
        Assert.assertTrue(compactionScheduler.isTableDisabled(45678L));

        compactionScheduler.disableTables("");
        Assert.assertFalse(compactionScheduler.isTableDisabled(23456L));
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
                CompactionJob job1 = new CompactionJob(db, table, partition1, 100, false);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
                CompactionJob job2 = new CompactionJob(db, table, partition2, 101, false);
                r.put(partitionIdentifier1, job1);
                r.put(partitionIdentifier2, job2);
                return r;
            }
        };

        List<CompactionRecord> list = compactionScheduler.getHistory();
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.get(0).getStartTs() >= list.get(1).getStartTs());
    }

    @Test
    public void testCompactionTaskLimit() {
        CompactionScheduler compactionScheduler = new CompactionScheduler(new CompactionMgr(), null, null, null, "");

        int defaultValue = Config.lake_compaction_max_tasks;
        // explicitly set config to a value bigger than default -1
        Config.lake_compaction_max_tasks = 10;
        Assert.assertEquals(10, compactionScheduler.compactionTaskLimit());

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
        Assert.assertEquals(3 * 16, compactionScheduler.compactionTaskLimit());
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
            protected CompactionJob startCompaction(PartitionStatisticsSnapshot partitionStatisticsSnapshot) {
                Database db = new Database();
                Table table = new LakeTable();
                long partitionId = partitionStatisticsSnapshot.getPartition().getPartitionId();
                PhysicalPartition partition = new PhysicalPartition(partitionId, "aaa", partitionId, null);
                return new CompactionJob(db, table, partition, 100, false);
            }
        };
        compactionScheduler.runOneCycle();
        Assert.assertEquals(2, compactionScheduler.getRunningCompactions().size());

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
        Assert.assertEquals(0, compactionScheduler.getRunningCompactions().size());
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

                warehouseManager.getCompactionWarehouse();
                result = warehouse;

                warehouse.getId();
                result = 100L;
            }
        };

        CompactionScheduler scheduler = new CompactionScheduler(compactionManager, systemInfoService,
                globalTransactionMgr, globalStateMgr, "");

        Method method = CompactionScheduler.class.getDeclaredMethod("createAggregateCompactionTask",
                long.class, Map.class, long.class, PartitionStatistics.CompactionPriority.class);
        method.setAccessible(true);
        CompactionTask task = (CompactionTask) method.invoke(scheduler, currentVersion, beToTablets, txnId, priority);

        Assert.assertNotNull(task);
        Assert.assertTrue(task instanceof AggregateCompactionTask);

        Field serviceField = CompactionTask.class.getDeclaredField("rpcChannel");
        serviceField.setAccessible(true);

        Field requestField = AggregateCompactionTask.class.getDeclaredField("request");
        requestField.setAccessible(true);
        AggregateCompactRequest aggRequest = (AggregateCompactRequest) requestField.get(task);

        Assert.assertEquals(2, aggRequest.requests.size());
        Assert.assertEquals(2, aggRequest.computeNodes.size());

        boolean foundTablets1 = false;
        boolean foundTablets2 = false;

        for (CompactRequest req : aggRequest.requests) {
            Assert.assertEquals(txnId, req.txnId.longValue());
            Assert.assertEquals(currentVersion, req.version.longValue());
            Assert.assertEquals(false, req.allowPartialSuccess);
            Assert.assertEquals(false, req.forceBaseCompaction);

            if (req.tabletIds.equals(Lists.newArrayList(101L, 102L))) {
                foundTablets1 = true;
            } else if (req.tabletIds.equals(Lists.newArrayList(201L, 202L))) {
                foundTablets2 = true;
            }
        }

        Assert.assertTrue(foundTablets1);
        Assert.assertTrue(foundTablets2);

        boolean foundNode1 = false;
        boolean foundNode2 = false;

        for (ComputeNodePB nodePB : aggRequest.computeNodes) {
            if (nodePB.getId() == 1001L) {
                Assert.assertEquals("192.168.0.1", nodePB.getHost());
                Assert.assertEquals(9050, (int) nodePB.getBrpcPort());
                foundNode1 = true;
            } else if (nodePB.getId() == 1002L) {
                Assert.assertEquals("192.168.0.2", nodePB.getHost());
                Assert.assertEquals(9050, (int) nodePB.getBrpcPort());
                foundNode2 = true;
            }
        }

        Assert.assertTrue(foundNode1);
        Assert.assertTrue(foundNode2);
    }
}
