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
import com.starrocks.lake.LakeTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.utframe.MockedWarehouseManager;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CompactionSchedulerTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;
    @Mocked
    private DatabaseTransactionMgr dbTransactionMgr;

    @Test
    public void testDisableCompaction() {
        Config.lake_compaction_disable_ids = "23456";
        CompactionMgr compactionManager = new CompactionMgr();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(compactionManager, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState(),
                        Config.lake_compaction_disable_ids);

        Assert.assertTrue(compactionScheduler.isTableDisabled(23456L));
        Assert.assertTrue(compactionScheduler.isPartitionDisabled(23456L));

        compactionScheduler.disableTableOrPartitionId("34567;45678;56789");

        Assert.assertFalse(compactionScheduler.isPartitionDisabled(23456L));
        Assert.assertTrue(compactionScheduler.isTableDisabled(34567L));
        Assert.assertTrue(compactionScheduler.isTableDisabled(45678L));
        Assert.assertTrue(compactionScheduler.isPartitionDisabled(56789L));

        compactionScheduler.disableTableOrPartitionId("");
        Assert.assertFalse(compactionScheduler.isTableDisabled(34567L));
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
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assert.assertNull(compactionScheduler.startCompaction(snapshot));
        table.setState(OlapTable.OlapTableState.NORMAL);
        Assert.assertNull(compactionScheduler.startCompaction(snapshot));
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
        Assert.assertTrue(list.get(0).getStartTs() <= list.get(1).getStartTs());
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
            public boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tableId, long partitionId) {
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

    /**
     * Test that removeFromStartupActiveCompactionTransactionMap is called when compaction completes normally
     */
    @Test
    public void testRemoveFromStartupActiveTxnMapOnCompactionSuccess() throws Exception {
        long txnId = 12345L;
        long tableId = 10002L;
        CompactionMgr compactionManager = new CompactionMgr();
        
        // Build active compaction transaction map with one transaction
        Map<Long, Long> txnIdToTableIdMap = new HashMap<>();
        txnIdToTableIdMap.put(txnId, tableId);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getLakeCompactionActiveTxnStats();
                result = txnIdToTableIdMap;
            }
        };
        compactionManager.buildActiveCompactionTransactionMap();
        
        // Verify the transaction is in the map
        Assertions.assertEquals(1, compactionManager.getRemainedActiveCompactionTxnWhenStart().size());
        Assertions.assertTrue(compactionManager.getRemainedActiveCompactionTxnWhenStart().containsKey(txnId));

        // Set up the compaction scheduler
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, 
                globalTransactionMgr, globalStateMgr, "");
        
        // Create a compaction job
        PartitionIdentifier partitionId = new PartitionIdentifier(1, tableId, 3);
        Database db = new Database(1, "test_db");
        Table table = new LakeTable();
        // Set table ID using reflection to ensure table.getId() returns the correct value
        Field idField = Table.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(table, tableId);
        PhysicalPartition partition = new PhysicalPartition(3, "test_partition", 3, null);
        CompactionJob job = new CompactionJob(db, table, partition, txnId, false, null, "");
        
        // Mock the job to simulate successful completion and transaction visibility
        new MockUp<CompactionJob>() {
            @Mock
            public boolean transactionHasCommitted() {
                return true;
            }
            @Mock
            public boolean waitTransactionVisible(long timeout, TimeUnit unit) {
                return true; // Transaction is visible
            }
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.ALL_SUCCESS; // Ensure job enters success path
            }
            @Mock
            public PhysicalPartition getPartition() {
                return partition;
            }
            @Mock
            public Database getDb() {
                return db;
            }
            @Mock
            public long getFinishTs() {
                return System.currentTimeMillis();
            }
            @Mock
            public long getStartTs() {
                return System.currentTimeMillis() - 1000;
            }
            @Mock
            public String getDebugString() {
                return "test_job";
            }
        };
        
        // Add the job to running compactions
        compactionScheduler.getRunningCompactions().put(partitionId, job);

        // Mock MetaUtils to return partition exists
        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tId, long pId) {
                return true;
            }
        };

        // Trigger compaction cleanup by calling scheduleNewCompaction via reflection
        // This simulates the scheduler's periodic check for completed jobs
        Method scheduleMethod = CompactionScheduler.class.getDeclaredMethod("scheduleNewCompaction");
        scheduleMethod.setAccessible(true);
        scheduleMethod.invoke(compactionScheduler);

        // Verify the job was removed from running compactions
        Assertions.assertEquals(0, compactionScheduler.getRunningCompactions().size(),
                "Job should be removed from running compactions after successful completion");

        // Verify the transaction was removed from the startup active transaction map
        Assertions.assertEquals(0, compactionManager.getRemainedActiveCompactionTxnWhenStart().size(),
                "Active transaction map should be empty after cleanup");
        Assertions.assertFalse(compactionManager.getRemainedActiveCompactionTxnWhenStart().containsKey(txnId),
                "Specific transaction should be removed from active map");
    }

    /**
     * Test that removeFromStartupActiveCompactionTransactionMap is called when compaction fails
     * This covers the case where compaction commits successfully but publish aborts
     */
    @Test
    public void testRemoveFromStartupActiveTxnMapOnCompactionFailure() throws Exception {
        long txnId = 12346L;
        long tableId = 10003L;
        CompactionMgr compactionManager = new CompactionMgr();
        
        // Build active compaction transaction map with one transaction
        Map<Long, Long> txnIdToTableIdMap = new HashMap<>();
        txnIdToTableIdMap.put(txnId, tableId);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getLakeCompactionActiveTxnStats();
                result = txnIdToTableIdMap;
            }
        };
        compactionManager.buildActiveCompactionTransactionMap();
        
        // Verify the transaction is in the map
        Assertions.assertEquals(1, compactionManager.getRemainedActiveCompactionTxnWhenStart().size());
        Assertions.assertTrue(compactionManager.getRemainedActiveCompactionTxnWhenStart().containsKey(txnId));

        // Set up the compaction scheduler
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, 
                globalTransactionMgr, globalStateMgr, "");
        
        // Create a compaction job
        PartitionIdentifier partitionId = new PartitionIdentifier(1, tableId, 4);
        Database db = new Database(1, "test_db");
        Table table = new LakeTable();
        // Set table ID using reflection to ensure table.getId() returns the correct value
        Field idField = Table.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(table, tableId);
        PhysicalPartition partition = new PhysicalPartition(4, "test_partition", 4, null);
        CompactionJob job = new CompactionJob(db, table, partition, txnId, false, null, "");
        
        // Mock the job to simulate failure (e.g., NONE_SUCCESS or PARTIAL_SUCCESS without allow partial)
        new MockUp<CompactionJob>() {
            @Mock
            public boolean transactionHasCommitted() {
                return false; // Transaction hasn't been committed yet
            }
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.NONE_SUCCESS; // Compaction failed
            }
            @Mock
            public String getFailMessage() {
                return "Test compaction failure - publish abort";
            }
            @Mock
            public PhysicalPartition getPartition() {
                return partition;
            }
            @Mock
            public Database getDb() {
                return db;
            }
            @Mock
            public void abort() {
                // Mock abort method
            }
            @Mock
            public long getFinishTs() {
                return System.currentTimeMillis();
            }
            @Mock
            public long getStartTs() {
                return System.currentTimeMillis() - 1000;
            }
            @Mock
            public String getDebugString() {
                return "test_job_failed";
            }
            @Mock
            public List<TabletCommitInfo> buildTabletCommitInfo() {
                return Lists.newArrayList();
            }
        };
        
        // Add the job to running compactions
        compactionScheduler.getRunningCompactions().put(partitionId, job);
        
        // Mock transaction manager to avoid actual transaction abort
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public void abortTransaction(long dbId, long txnId, String reason, 
                    List<TabletCommitInfo> finishedTablets, List<TabletCommitInfo> unfinishedTablets, 
                    Object txnCommitAttachment) {
                // Do nothing, just mock the abort
            }
        };
        
        // Mock MetaUtils to return partition exists
        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tId, long pId) {
                return true;
            }
        };

        // Trigger compaction cleanup by calling scheduleNewCompaction via reflection
        // This simulates the scheduler's periodic check for failed jobs
        Method scheduleMethod = CompactionScheduler.class.getDeclaredMethod("scheduleNewCompaction");
        scheduleMethod.setAccessible(true);
        scheduleMethod.invoke(compactionScheduler);

        // Verify the job was removed from running compactions
        Assertions.assertEquals(0, compactionScheduler.getRunningCompactions().size(),
                "Job should be removed from running compactions after failure");

        // Verify the transaction was removed from the startup active transaction map
        Assertions.assertEquals(0, compactionManager.getRemainedActiveCompactionTxnWhenStart().size(),
                "Active transaction map should be empty after cleanup");
        Assertions.assertFalse(compactionManager.getRemainedActiveCompactionTxnWhenStart().containsKey(txnId),
                "Specific transaction should be removed from active map");
    }

    /**
     * Test that removeFromStartupActiveCompactionTransactionMap is called when compaction 
     * commits but fails during publish (PARTIAL_SUCCESS without allowing partial success)
     */
    @Test
    public void testRemoveFromStartupActiveTxnMapOnPartialSuccessWithoutAllow() throws Exception {
        long txnId = 12347L;
        long tableId = 10004L;
        CompactionMgr compactionManager = new CompactionMgr();
        
        // Build active compaction transaction map with one transaction
        Map<Long, Long> txnIdToTableIdMap = new HashMap<>();
        txnIdToTableIdMap.put(txnId, tableId);
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getLakeCompactionActiveTxnStats();
                result = txnIdToTableIdMap;
            }
        };
        compactionManager.buildActiveCompactionTransactionMap();
        
        // Verify the transaction is in the map
        Assertions.assertEquals(1, compactionManager.getRemainedActiveCompactionTxnWhenStart().size());

        // Set up the compaction scheduler
        CompactionScheduler compactionScheduler = new CompactionScheduler(compactionManager, null, 
                globalTransactionMgr, globalStateMgr, "");
        
        // Create a compaction job
        PartitionIdentifier partitionId = new PartitionIdentifier(1, tableId, 5);
        Database db = new Database(1, "test_db");
        Table table = new LakeTable();
        // Set table ID using reflection to ensure table.getId() returns the correct value
        Field idField = Table.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(table, tableId);
        PhysicalPartition partition = new PhysicalPartition(5, "test_partition", 5, null);
        CompactionJob job = new CompactionJob(db, table, partition, txnId, false, null, "");
        
        // Mock the job to simulate PARTIAL_SUCCESS without allowing partial success
        new MockUp<CompactionJob>() {
            @Mock
            public boolean transactionHasCommitted() {
                return false;
            }
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.PARTIAL_SUCCESS;
            }
            @Mock
            public boolean getAllowPartialSuccess() {
                return false; // Not allowing partial success
            }
            @Mock
            public String getFailMessage() {
                return "Partial success but not allowed";
            }
            @Mock
            public PhysicalPartition getPartition() {
                return partition;
            }
            @Mock
            public Database getDb() {
                return db;
            }
            @Mock
            public void abort() {
            }
            @Mock
            public long getFinishTs() {
                return System.currentTimeMillis();
            }
            @Mock
            public long getStartTs() {
                return System.currentTimeMillis() - 1000;
            }
            @Mock
            public String getDebugString() {
                return "test_job_partial";
            }
            @Mock
            public List<TabletCommitInfo> buildTabletCommitInfo() {
                return Lists.newArrayList();
            }
        };
        
        // Add the job to running compactions
        compactionScheduler.getRunningCompactions().put(partitionId, job);
        
        // Mock transaction manager
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public void abortTransaction(long dbId, long txnId, String reason, 
                    List<TabletCommitInfo> finishedTablets, List<TabletCommitInfo> unfinishedTablets, 
                    Object txnCommitAttachment) {
            }
        };
        
        // Mock MetaUtils
        new MockUp<MetaUtils>() {
            @Mock
            public boolean isPhysicalPartitionExist(GlobalStateMgr stateMgr, long dbId, long tId, long pId) {
                return true;
            }
        };

        // Trigger compaction cleanup by calling scheduleNewCompaction via reflection
        // This simulates the scheduler's periodic check for partial success jobs
        Method scheduleMethod = CompactionScheduler.class.getDeclaredMethod("scheduleNewCompaction");
        scheduleMethod.setAccessible(true);
        scheduleMethod.invoke(compactionScheduler);

        // Verify the job was removed from running compactions
        Assertions.assertEquals(0, compactionScheduler.getRunningCompactions().size(),
                "Job should be removed from running compactions after partial success without allow");

        // Verify the transaction was removed from the startup active transaction map
        Assertions.assertEquals(0, compactionManager.getRemainedActiveCompactionTxnWhenStart().size(),
                "Active transaction map should be empty after cleanup");
    }
}
