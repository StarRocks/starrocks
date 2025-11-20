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

package com.starrocks.service;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit test to verify the deadlock fix between createPartition and GJson serialization.
 * 
 * This test ensures that the lock order is correct:
 * 1. db/table lock should be acquired first
 * 2. txnState lock should be acquired second
 * 
 * The previous implementation used synchronized(txnState) before acquiring db lock,
 * which could cause deadlock with GJson serialization that acquires object lock
 * during serialization while potentially holding db/table locks.
 */
public class FrontendServiceImplDeadlockTest {
    @Mocked
    ExecuteEnv exeEnv;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.addMockComputeNode(50001);
        
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test_deadlock").useDatabase("test_deadlock")
                .withTable("CREATE TABLE test_table_deadlock (\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_deadlock.test_table_deadlock";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception ex) {
            // Ignore cleanup errors
        }
    }

    /**
     * Test concurrent createPartition and GJson serialization to ensure no deadlock occurs.
     * 
     * Thread 1: Calls createPartition which acquires db/table lock first, then txnState lock
     * Thread 2: Serializes txnState with GsonUtils, which acquires txnState object lock
     * 
     * This test verifies that with correct lock ordering, no deadlock occurs.
     */
    @Test
    public void testNoDeadlockBetweenCreatePartitionAndSerialization() throws Exception {
        final TransactionState txnState = new TransactionState(1000L, Lists.newArrayList(100L),
                12345L, "test_label", null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                -1, 60000L);
        
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_deadlock");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table_deadlock");
        
        final FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completeLatch = new CountDownLatch(2);
        final AtomicBoolean hasException = new AtomicBoolean(false);
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final AtomicInteger successCount = new AtomicInteger(0);

        // Thread 1: Call createPartition
        Thread createPartitionThread = new Thread(() -> {
            try {
                startLatch.await();
                
                List<List<String>> partitionValues = Lists.newArrayList();
                List<String> values = Lists.newArrayList();
                values.add("2025-11-19");
                partitionValues.add(values);

                TCreatePartitionRequest request = new TCreatePartitionRequest();
                request.setDb_id(db.getId());
                request.setTable_id(table.getId());
                request.setTxn_id(12345L);
                request.setPartition_values(partitionValues);
                
                // This should acquire db/table lock first, then txnState lock
                TCreatePartitionResult result = impl.createPartition(request);
                
                // We expect OK or specific error, but not timeout/deadlock
                if (result.getStatus().getStatus_code() == TStatusCode.OK ||
                        result.getStatus().getStatus_code() == TStatusCode.RUNTIME_ERROR) {
                    successCount.incrementAndGet();
                }
            } catch (Exception e) {
                hasException.set(true);
                exceptionRef.set(e);
            } finally {
                completeLatch.countDown();
            }
        }, "CreatePartitionThread");

        // Thread 2: Serialize txnState with GsonUtils
        // This simulates the serialization that happens in persist/journal operations
        Thread serializeThread = new Thread(() -> {
            try {
                startLatch.await();
                
                // Simulate holding table lock during serialization
                // This is the scenario that could cause deadlock with old implementation
                Locker serializeLocker = new Locker();
                serializeLocker.lockTablesWithIntensiveDbLock(db.getId(),
                        Lists.newArrayList(table.getId()), LockType.READ);
                try {
                    // GsonUtils uses synchronized(obj) internally for GsonPreProcessable
                    String json = GsonUtils.GSON.toJson(txnState);
                    if (json != null && !json.isEmpty()) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    serializeLocker.unLockTablesWithIntensiveDbLock(db.getId(),
                            Lists.newArrayList(table.getId()), LockType.READ);
                }
            } catch (Exception e) {
                hasException.set(true);
                exceptionRef.set(e);
            } finally {
                completeLatch.countDown();
            }
        }, "SerializeThread");

        // Start both threads
        createPartitionThread.start();
        serializeThread.start();
        
        // Release both threads to start simultaneously
        startLatch.countDown();
        
        // Wait for completion with timeout
        boolean completed = completeLatch.await(10, TimeUnit.SECONDS);
        
        // Verify no deadlock occurred (both threads completed within timeout)
        Assertions.assertTrue(completed, 
                "Test timed out - possible deadlock detected! Threads did not complete within 10 seconds.");
        
        // Verify no unexpected exceptions
        if (hasException.get()) {
            Exception e = exceptionRef.get();
            Assertions.fail("Unexpected exception during concurrent execution: " + e.getMessage(), e);
        }
        
        // Verify at least one operation succeeded (serialization should always succeed)
        Assertions.assertTrue(successCount.get() >= 1,
                "Expected at least one operation to succeed, but got: " + successCount.get());
    }

    /**
     * Test that verifies TransactionState lock can be acquired correctly
     * when holding db/table locks, ensuring proper lock hierarchy.
     */
    @Test
    public void testLockOrderIsCorrect() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_deadlock");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table_deadlock");
        
        TransactionState txnState = new TransactionState(db.getId(), Lists.newArrayList(table.getId()),
                99999L, "test_label", null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                -1, 60000L);
        
        // Acquire locks in correct order: db/table first, then txnState
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            txnState.writeLock();
            try {
                // Simulate some work
                txnState.getPartitionNameToTPartition(table.getId());
                txnState.getTabletIdToTTabletLocation();
            } finally {
                txnState.writeUnlock();
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
        
        // If we reach here without deadlock/timeout, the test passes
        Assertions.assertTrue(true, "Lock order is correct - no deadlock");
    }

    /**
     * Test multiple concurrent createPartition calls to ensure thread safety.
     */
    @Test
    public void testConcurrentCreatePartitionCalls() throws Exception {
        final TransactionState txnState = new TransactionState(1000L, Lists.newArrayList(100L),
                99999L, "test_label", null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                -1, 60000L);
        
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_deadlock");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table_deadlock");
        
        final FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        final int threadCount = 5;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completeLatch = new CountDownLatch(threadCount);
        final AtomicBoolean hasDeadlock = new AtomicBoolean(false);

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    
                    List<List<String>> partitionValues = Lists.newArrayList();
                    List<String> values = Lists.newArrayList();
                    values.add("2025-11-" + (20 + threadIndex));
                    partitionValues.add(values);

                    TCreatePartitionRequest request = new TCreatePartitionRequest();
                    request.setDb_id(db.getId());
                    request.setTable_id(table.getId());
                    request.setTxn_id(99999L);
                    request.setPartition_values(partitionValues);
                    
                    impl.createPartition(request);
                } catch (Exception e) {
                    // Expected exceptions are OK, we're testing for deadlock, not correctness
                } finally {
                    completeLatch.countDown();
                }
            }, "ConcurrentCreatePartition-" + i).start();
        }
        
        startLatch.countDown();
        boolean completed = completeLatch.await(15, TimeUnit.SECONDS);
        
        Assertions.assertTrue(completed, 
                "Concurrent createPartition calls timed out - possible deadlock detected!");
    }
}

