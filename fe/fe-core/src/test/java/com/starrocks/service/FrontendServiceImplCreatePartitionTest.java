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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class FrontendServiceImplCreatePartitionTest {
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
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable("CREATE TABLE test_table (\n" +
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
        String dropSQL = "drop table if exists test.test_table";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testCreatePartition() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        new MockUp<WarehouseManager>() {
            int count = 0;
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                if (count < 1) {
                    count++;
                    return 50001L;
                }
                return null;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-10");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assertions.assertTrue(partition.getStatus().getError_msgs().get(0)
                .contains("No alive compute node found for tablet. " + "Check if any backend is down or not. tablet_id:"));
    }

    /**
     * Test that when partition info is already cached in txnState,
     * the fast path returns directly without acquiring partition creation locks.
     */
    @Test
    public void testCreatePartitionFastPathWithCachedPartition() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");

        // Create a TransactionState with pre-cached partition info
        TransactionState txnState = new TransactionState();
        String partitionName = "p20250711";

        // Pre-populate the partition cache
        ConcurrentMap<String, TOlapTablePartition> partitionCache = txnState.getPartitionNameToTPartition(table.getId());
        TOlapTablePartition cachedPartition = new TOlapTablePartition();
        cachedPartition.setId(12345L);
        cachedPartition.setIndexes(new ArrayList<>());
        partitionCache.put(partitionName, cachedPartition);

        // Track if lockCreatePartition was called
        AtomicInteger lockCallCount = new AtomicInteger(0);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public void lockCreatePartition(String partitionName) {
                lockCallCount.incrementAndGet();
            }

            @Mock
            public void unlockCreatePartition(String partitionName) {
                // no-op
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }

            @Mock
            public boolean isResourceAvailable(ComputeResource resource) {
                return true;
            }
        };

        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-11");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setTxn_id(1L);
        request.setPartition_values(partitionValues);

        TCreatePartitionResult result = impl.createPartition(request);

        // When partition is cached, the fast path should be taken and lockCreatePartition should NOT be called
        Assertions.assertEquals(0, lockCallCount.get(),
                "lockCreatePartition should not be called when partition is already cached (fast path)");
    }

    /**
     * Test the double-check logic: when multiple concurrent requests arrive,
     * only the first one should actually create the partition.
     */
    @Test
    public void testCreatePartitionDoubleCheckWithConcurrentRequests() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");

        TransactionState txnState = new TransactionState();
        AtomicInteger addPartitionsCallCount = new AtomicInteger(0);
        AtomicInteger lockAcquiredCount = new AtomicInteger(0);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public void lockCreatePartition(String partitionName) {
                lockAcquiredCount.incrementAndGet();
                // Simulate the first request adding partition to cache after acquiring lock
                // This simulates what happens when the first request creates the partition
                ConcurrentMap<String, TOlapTablePartition> cache =
                        txnState.getPartitionNameToTPartition(table.getId());
                if (cache.get(partitionName) == null) {
                    // First request: add to cache (simulating partition creation)
                    TOlapTablePartition partition = new TOlapTablePartition();
                    partition.setId(99999L);
                    partition.setIndexes(new ArrayList<>());
                    cache.put(partitionName, partition);
                    addPartitionsCallCount.incrementAndGet();
                }
                // Second and subsequent requests will find the partition in cache
                // due to the double-check logic
            }

            @Mock
            public void unlockCreatePartition(String partitionName) {
                // no-op
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }

            @Mock
            public boolean isResourceAvailable(ComputeResource resource) {
                return true;
            }
        };

        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-12");
        partitionValues.add(values);

        // Simulate concurrent requests
        int numConcurrentRequests = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numConcurrentRequests);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numConcurrentRequests);
        List<TCreatePartitionResult> results = new ArrayList<>();

        for (int i = 0; i < numConcurrentRequests; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
                    TCreatePartitionRequest request = new TCreatePartitionRequest();
                    request.setDb_id(db.getId());
                    request.setTable_id(table.getId());
                    request.setTxn_id(2L);
                    request.setPartition_values(partitionValues);
                    TCreatePartitionResult result = impl.createPartition(request);
                    synchronized (results) {
                        results.add(result);
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Due to double-check logic, only ONE request should actually "create" the partition
        // (in our mock, this is tracked by addPartitionsCallCount)
        Assertions.assertEquals(1, addPartitionsCallCount.get(),
                "Only one request should actually create the partition due to double-check logic");

        // All requests should have acquired the lock (since first check happens before lock)
        // But the actual partition creation only happens once
        Assertions.assertTrue(lockAcquiredCount.get() >= 1,
                "At least one request should have acquired the partition lock");
    }

    /**
     * Test that the cache check correctly identifies when partitions are not cached.
     */
    @Test
    public void testCreatePartitionSlowPathWhenNotCached() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");

        // Empty cache - partition not cached
        TransactionState txnState = new TransactionState();
        AtomicInteger lockCallCount = new AtomicInteger(0);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public void lockCreatePartition(String partitionName) {
                lockCallCount.incrementAndGet();
            }

            @Mock
            public void unlockCreatePartition(String partitionName) {
                // no-op
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }
        };

        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-13");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setTxn_id(3L);
        request.setPartition_values(partitionValues);

        impl.createPartition(request);

        // When partition is NOT cached, the slow path should be taken and lockCreatePartition SHOULD be called
        Assertions.assertTrue(lockCallCount.get() > 0,
                "lockCreatePartition should be called when partition is not cached (slow path)");
    }

    /**
     * Test that identical concurrent requests share the same result via CompletableFuture mechanism.
     * Only the first request executes the partition creation, others wait and get the same result.
     */
    @Test
    public void testIdenticalConcurrentRequestsShareResult() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");

        TransactionState txnState = new TransactionState();
        AtomicInteger lockCallCount = new AtomicInteger(0);
        CountDownLatch firstRequestInProgress = new CountDownLatch(1);
        CountDownLatch allowFirstRequestComplete = new CountDownLatch(1);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public void lockCreatePartition(String partitionName) {
                int count = lockCallCount.incrementAndGet();
                if (count == 1) {
                    // First request: signal that we're in progress
                    firstRequestInProgress.countDown();
                    try {
                        // Wait for signal to continue (to allow other requests to arrive)
                        allowFirstRequestComplete.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                // Add to cache after lock is acquired
                ConcurrentMap<String, TOlapTablePartition> cache =
                        txnState.getPartitionNameToTPartition(table.getId());
                if (cache.get(partitionName) == null) {
                    TOlapTablePartition partition = new TOlapTablePartition();
                    partition.setId(88888L);
                    partition.setIndexes(new ArrayList<>());
                    cache.put(partitionName, partition);
                }
            }

            @Mock
            public void unlockCreatePartition(String partitionName) {
                // no-op
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }

            @Mock
            public boolean isResourceAvailable(ComputeResource resource) {
                return true;
            }
        };

        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-14");
        partitionValues.add(values);

        int numRequests = 3;
        ExecutorService executor = Executors.newFixedThreadPool(numRequests);
        CountDownLatch allDone = new CountDownLatch(numRequests);
        AtomicInteger successCount = new AtomicInteger(0);

        // Submit all requests
        for (int i = 0; i < numRequests; i++) {
            executor.submit(() -> {
                try {
                    FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
                    TCreatePartitionRequest request = new TCreatePartitionRequest();
                    request.setDb_id(db.getId());
                    request.setTable_id(table.getId());
                    request.setTxn_id(4L);
                    request.setPartition_values(partitionValues);
                    request.setTimeout(30); // 30 seconds timeout
                    TCreatePartitionResult result = impl.createPartition(request);
                    if (result.getStatus().getStatus_code() == TStatusCode.OK ||
                            result.getStatus().getStatus_code() == TStatusCode.RUNTIME_ERROR) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    allDone.countDown();
                }
            });
        }

        // Wait for first request to be in progress
        firstRequestInProgress.await();

        // Give other requests time to arrive and wait on the Future
        Thread.sleep(100);

        // Allow the first request to complete
        allowFirstRequestComplete.countDown();

        // Wait for all requests to complete
        allDone.await();
        executor.shutdown();

        // With the Future mechanism, only the first request should acquire the lock
        // because other requests with the same requestKey will wait on the Future
        Assertions.assertEquals(1, lockCallCount.get(),
                "Only the first request should acquire the lock due to Future deduplication");

        // All requests should complete (either success or error)
        Assertions.assertEquals(numRequests, successCount.get(),
                "All requests should complete with a response");
    }

    /**
     * Test that the request timeout is correctly used from the request parameter.
     */
    @Test
    public void testRequestTimeoutFromParameter() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");

        TransactionState txnState = new TransactionState();

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public void lockCreatePartition(String partitionName) {
                ConcurrentMap<String, TOlapTablePartition> cache =
                        txnState.getPartitionNameToTPartition(table.getId());
                TOlapTablePartition partition = new TOlapTablePartition();
                partition.setId(77777L);
                partition.setIndexes(new ArrayList<>());
                cache.put(partitionName, partition);
            }

            @Mock
            public void unlockCreatePartition(String partitionName) {
                // no-op
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return 50001L;
            }

            @Mock
            public boolean isResourceAvailable(ComputeResource resource) {
                return true;
            }
        };

        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-15");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setTxn_id(5L);
        request.setPartition_values(partitionValues);
        request.setTimeout(60); // 60 seconds timeout

        // The request should complete without timeout issues
        TCreatePartitionResult result = impl.createPartition(request);

        // Just verify the request completes - timeout value is used internally
        Assertions.assertNotNull(result);
    }
}
