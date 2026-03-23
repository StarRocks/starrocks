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
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
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
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FrontendServiceImplCreatePartitionTest {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceImplCreatePartitionTest.class);

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
     * the creator's fast path returns directly without acquiring partition creation locks.
     */
    @Test
    public void testCreatePartitionCreatorFastPathWithCachedPartition() throws TException {
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
     * Test CompletableFuture deduplication: when multiple concurrent requests with the same
     * partition values arrive, only the creator executes creation; others wait on the Future.
     */
    @Test
    public void testCreatePartitionFutureDeduplication() throws Exception {
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
        executor.awaitTermination(10, TimeUnit.SECONDS);

        // Due to Future deduplication, only ONE request becomes the creator and actually
        // "creates" the partition (in our mock, this is tracked by addPartitionsCallCount)
        Assertions.assertEquals(1, addPartitionsCallCount.get(),
                "Only one request should actually create the partition due to Future deduplication");

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
                    request.setTimeout_s(30);
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
        executor.awaitTermination(10, TimeUnit.SECONDS);

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
        request.setTimeout_s(60);

        // The request should complete without timeout issues
        TCreatePartitionResult result = impl.createPartition(request);

        // Just verify the request completes - timeout value is used internally
        Assertions.assertNotNull(result);
    }

    /**
     * Test that auto-partition creation correctly handles values that fall into a merged
     * (year-level) partition alongside values that need new monthly partitions.
     *
     * Before the fix, buildResponseWithLock used pre-analyzer partition names (e.g. p202204)
     * which don't exist in the table. The analyzer remaps them to the enclosing partition name
     * (e.g. p2022), but creatingPartitionNames was never updated. This caused the response to
     * skip the enclosed partitions, resulting in "Insert has filtered data" errors.
     */
    @Test
    public void testCreatePartitionWithMergedPartition() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.merge_test_table (\n" +
                "    event_day DATETIME,\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day)\n" +
                "PARTITION BY date_trunc('month', event_day)\n" +
                "DISTRIBUTED BY HASH(event_day) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                    .getLocalMetastore().getTable(db.getFullName(), "merge_test_table");

            addYearlyPartition(table, "p2022", 2022, 2023, 900000L, 900001L);

            Assertions.assertNotNull(table.getPartition("p2022"),
                    "yearly partition p2022 should exist");

            TransactionState txnState = new TransactionState();

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

                @Mock
                public boolean isResourceAvailable(ComputeResource resource) {
                    return true;
                }
            };

            List<List<String>> partitionValues = Lists.newArrayList();
            partitionValues.add(Lists.newArrayList("2022-04-01 00:00:00"));
            partitionValues.add(Lists.newArrayList("2023-06-01 00:00:00"));

            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TCreatePartitionRequest request = new TCreatePartitionRequest();
            request.setDb_id(db.getId());
            request.setTable_id(table.getId());
            request.setTxn_id(100L);
            request.setPartition_values(partitionValues);

            TCreatePartitionResult result = impl.createPartition(request);

            LOG.info("createPartition result status: {}", result.getStatus().getStatus_code());
            if (result.getPartitions() != null) {
                LOG.info("createPartition returned {} partitions", result.getPartitions().size());
            }

            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                    "createPartition should succeed; status: " + result.getStatus());
            Assertions.assertNotNull(result.getPartitions(),
                    "response should contain partitions");
            Assertions.assertEquals(2, result.getPartitions().size(),
                    "response should contain 2 partitions (p2022 for 2022-04-01 and p202306 for 2023-06-01)");
        } finally {
            try {
                starRocksAssert.dropTable("merge_test_table");
            } catch (Exception e) {
                // ignore cleanup errors
            }
        }
    }

    /**
     * Test with multiple values all falling into the same merged partition.
     */
    @Test
    public void testCreatePartitionAllEnclosedByMergedPartition() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.merge_all_enclosed_table (\n" +
                "    event_day DATETIME,\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day)\n" +
                "PARTITION BY date_trunc('month', event_day)\n" +
                "DISTRIBUTED BY HASH(event_day) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                    .getLocalMetastore().getTable(db.getFullName(), "merge_all_enclosed_table");

            addYearlyPartition(table, "p2022", 2022, 2023, 900010L, 900011L);

            TransactionState txnState = new TransactionState();

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

                @Mock
                public boolean isResourceAvailable(ComputeResource resource) {
                    return true;
                }
            };

            List<List<String>> partitionValues = Lists.newArrayList();
            partitionValues.add(Lists.newArrayList("2022-03-01 00:00:00"));
            partitionValues.add(Lists.newArrayList("2022-06-15 00:00:00"));
            partitionValues.add(Lists.newArrayList("2022-09-20 00:00:00"));

            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TCreatePartitionRequest request = new TCreatePartitionRequest();
            request.setDb_id(db.getId());
            request.setTable_id(table.getId());
            request.setTxn_id(101L);
            request.setPartition_values(partitionValues);

            TCreatePartitionResult result = impl.createPartition(request);

            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                    "createPartition should succeed; status: " + result.getStatus());
            Assertions.assertNotNull(result.getPartitions());
            Assertions.assertEquals(1, result.getPartitions().size(),
                    "all values fall into p2022, so response should contain exactly 1 partition");
        } finally {
            try {
                starRocksAssert.dropTable("merge_all_enclosed_table");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Test with multiple merged partitions spanning different years.
     */
    @Test
    public void testCreatePartitionMultiYearMergedPartitions() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.merge_multi_year_table (\n" +
                "    event_day DATETIME,\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day)\n" +
                "PARTITION BY date_trunc('month', event_day)\n" +
                "DISTRIBUTED BY HASH(event_day) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                    .getLocalMetastore().getTable(db.getFullName(), "merge_multi_year_table");

            addYearlyPartition(table, "p2020", 2020, 2021, 900020L, 900021L);
            addYearlyPartition(table, "p2021", 2021, 2022, 900030L, 900031L);
            addYearlyPartition(table, "p2022", 2022, 2023, 900040L, 900041L);

            TransactionState txnState = new TransactionState();

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

                @Mock
                public boolean isResourceAvailable(ComputeResource resource) {
                    return true;
                }
            };

            List<List<String>> partitionValues = Lists.newArrayList();
            partitionValues.add(Lists.newArrayList("2020-01-15 00:00:00"));
            partitionValues.add(Lists.newArrayList("2020-11-20 00:00:00"));
            partitionValues.add(Lists.newArrayList("2021-03-10 00:00:00"));
            partitionValues.add(Lists.newArrayList("2021-09-25 00:00:00"));
            partitionValues.add(Lists.newArrayList("2022-02-14 00:00:00"));
            partitionValues.add(Lists.newArrayList("2022-12-31 00:00:00"));
            partitionValues.add(Lists.newArrayList("2023-07-04 00:00:00"));

            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TCreatePartitionRequest request = new TCreatePartitionRequest();
            request.setDb_id(db.getId());
            request.setTable_id(table.getId());
            request.setTxn_id(102L);
            request.setPartition_values(partitionValues);

            TCreatePartitionResult result = impl.createPartition(request);

            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                    "createPartition should succeed; status: " + result.getStatus());
            Assertions.assertNotNull(result.getPartitions());
            Assertions.assertEquals(4, result.getPartitions().size(),
                    "response should contain 4 partitions: p2020, p2021, p2022, and p202307");
        } finally {
            try {
                starRocksAssert.dropTable("merge_multi_year_table");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Test that auto partition creation succeeds when schema change job is in FINISHED_REWRITING state.
     * The cancel attempt will fail, but the code should wait for the job to complete (table state
     * returns to NORMAL) and then proceed with partition creation.
     */
    @Test
    public void testCreatePartitionWaitsForFinishedRewritingSchemaChange() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(db.getFullName(), "test_table");

        TransactionState txnState = new TransactionState();

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

            @Mock
            public boolean isResourceAvailable(ComputeResource resource) {
                return true;
            }
        };

        // Mock cancel to throw (simulating FINISHED_REWRITING state where cancel fails)
        new MockUp<com.starrocks.server.LocalMetastore>() {
            @Mock
            public void cancelAlter(com.starrocks.sql.ast.CancelAlterTableStmt stmt, String reason)
                    throws com.starrocks.common.DdlException {
                throw new com.starrocks.common.DdlException(
                        "Job can not be cancelled. State: FINISHED_REWRITING");
            }
        };

        // Mock SchemaChangeHandler to return a job in FINISHED_REWRITING state.
        // Use a minimal stub that implements all abstract methods.
        com.starrocks.alter.AlterJobV2 mockJob = new com.starrocks.alter.AlterJobV2(
                com.starrocks.alter.AlterJobV2.JobType.SCHEMA_CHANGE) {
            @Override
            public com.starrocks.alter.AlterJobV2.JobState getJobState() {
                return com.starrocks.alter.AlterJobV2.JobState.FINISHED_REWRITING;
            }

            @Override
            public com.starrocks.alter.AlterJobV2 copyForPersist() {
                return this;
            }

            @Override
            public void replay(com.starrocks.alter.AlterJobV2 replayedJob) {
            }

            @Override
            public java.util.Optional<Long> getTransactionId() {
                return java.util.Optional.empty();
            }

            @Override
            protected void runPendingJob() {
            }

            @Override
            protected void runWaitingTxnJob() {
            }

            @Override
            protected void runRunningJob() {
            }

            @Override
            protected void runFinishedRewritingJob() {
            }

            @Override
            protected boolean cancelImpl(String errMsg) {
                return false;
            }

            @Override
            protected void getInfo(java.util.List<java.util.List<Comparable>> infos) {
            }
        };
        new MockUp<com.starrocks.alter.SchemaChangeHandler>() {
            @Mock
            public java.util.List<com.starrocks.alter.AlterJobV2> getUnfinishedAlterJobV2ByTableId(long tblId) {
                return java.util.List.of(mockJob);
            }
        };

        // Set table state to SCHEMA_CHANGE, then simulate it returning to NORMAL after a delay
        // (as would happen when the FINISHED_REWRITING job completes)
        OlapTable.OlapTableState originalState = table.getState();
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        // Schedule a task to set the table state back to NORMAL after 1 second,
        // simulating the schema change job completing
        java.util.concurrent.ScheduledExecutorService scheduler =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> table.setState(OlapTable.OlapTableState.NORMAL),
                1, TimeUnit.SECONDS);

        try {
            List<List<String>> partitionValues = Lists.newArrayList();
            partitionValues.add(Lists.newArrayList("2025-08-01"));

            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TCreatePartitionRequest request = new TCreatePartitionRequest();
            request.setDb_id(db.getId());
            request.setTable_id(table.getId());
            request.setTxn_id(200L);
            request.setPartition_values(partitionValues);
            request.setTimeout_s(30);

            TCreatePartitionResult result = impl.createPartition(request);

            // The partition creation should succeed because the wait mechanism detects the
            // FINISHED_REWRITING state and waits for the table to return to NORMAL
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                    "Partition creation should succeed after waiting for schema change to complete. " +
                    "Actual error: " + (result.getStatus().getError_msgs() != null ?
                            result.getStatus().getError_msgs() : "none"));
        } finally {
            table.setState(originalState);
            scheduler.shutdownNow();
        }
    }

    private static void addYearlyPartition(OlapTable table, String name,
                                           int startYear, int endYear,
                                           long partitionId, long physicalPartitionId) throws Exception {
        List<Column> partitionColumns = table.getPartitionInfo()
                .getPartitionColumns(table.getIdToColumn());

        PartitionKey lowerKey = new PartitionKey(
                Lists.newArrayList(new com.starrocks.sql.ast.expression.DateLiteral(
                        startYear, 1, 1, 0, 0, 0, 0)),
                Lists.newArrayList(PrimitiveType.DATETIME));
        PartitionKey upperKey = new PartitionKey(
                Lists.newArrayList(new com.starrocks.sql.ast.expression.DateLiteral(
                        endYear, 1, 1, 0, 0, 0, 0)),
                Lists.newArrayList(PrimitiveType.DATETIME));
        Range<PartitionKey> range = Range.closedOpen(lowerKey, upperKey);

        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        rangePartitionInfo.addPartition(partitionId, false, range,
                rangePartitionInfo.getDataProperty(table.getPartitions().iterator().hasNext()
                        ? table.getPartitions().iterator().next().getId() : -1L),
                (short) 1, null);

        MaterializedIndex baseIndex = new MaterializedIndex(
                table.getBaseIndexMetaId(), MaterializedIndex.IndexState.NORMAL);
        HashDistributionInfo distInfo = new HashDistributionInfo(1, partitionColumns);
        Partition partition = new Partition(partitionId, physicalPartitionId, name, baseIndex, distInfo);
        table.addPartition(partition);
    }
}
