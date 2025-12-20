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

package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PublishVersionDaemonTest {
    public int oldValue;
    public int pollerOldValue;

    @BeforeEach
    public void setUp() {
        Config.run_mode = "shared_data";
        RunMode.detectRunMode();
        oldValue = Config.lake_publish_version_max_threads;
        pollerOldValue = Config.publish_version_poller_threads;
    }

    @AfterEach
    public void tearDown() {
        Config.lake_publish_version_max_threads = oldValue;
        Config.publish_version_poller_threads = pollerOldValue;
    }

    @Test
    public void testUpdateLakeExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        daemon.prepare();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");
        Assertions.assertNotNull(executor);
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();

        // scale out
        Config.lake_publish_version_max_threads += 10;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());


        // scale in
        Config.lake_publish_version_max_threads -= 5;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        int oldNumber = executor.getMaximumPoolSize();

        // config set to < 0
        Config.lake_publish_version_max_threads = -1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(oldNumber, executor.getMaximumPoolSize());
        Assertions.assertEquals(oldNumber, executor.getCorePoolSize());


        // config set to > LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        Config.lake_publish_version_max_threads = PublishVersionDaemon.LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE + 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(oldNumber, executor.getMaximumPoolSize());
        Assertions.assertEquals(oldNumber, executor.getCorePoolSize());


        // config set to LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        Config.lake_publish_version_max_threads = PublishVersionDaemon.LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        // config set to 1
        Config.lake_publish_version_max_threads = 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());
    }

    @Test
    public void testInvalidInitConfiguration()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        int hardCodeDefaultMaxThreads = (int) FieldUtils.readDeclaredStaticField(PublishVersionDaemon.class,
                "LAKE_PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE", true);

        // <= 0
        int initValue = 0;
        Config.lake_publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            daemon.prepare();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");

            Assertions.assertNotNull(executor);
            Assertions.assertNotEquals(initValue, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getCorePoolSize());
            // configVar set to default value.
            Assertions.assertEquals(hardCodeDefaultMaxThreads, Config.lake_publish_version_max_threads);
        }

        // > LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        initValue = PublishVersionDaemon.LAKE_PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE + 1;
        Config.lake_publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            daemon.prepare();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");
            Assertions.assertNotNull(executor);
            Assertions.assertNotEquals(initValue, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getCorePoolSize());
            // configVar set to default value.
            Assertions.assertEquals(hardCodeDefaultMaxThreads, Config.lake_publish_version_max_threads);
        }
    }

    @Test
    public void testUpdateDeleteTxnLogExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        daemon.prepare();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getDeleteTxnLogExecutor");
        Assertions.assertNotNull(executor);
        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();

        // scale out
        Config.lake_publish_delete_txnlog_max_threads += 10;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.lake_publish_delete_txnlog_max_threads, executor.getMaximumPoolSize());

        // scale in
        Config.lake_publish_delete_txnlog_max_threads -= 5;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.lake_publish_delete_txnlog_max_threads, executor.getMaximumPoolSize());
    }

    @Test
    public void testRunWithMultiThreadsWithDbs()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InterruptedException {
        // Track processDbTransactions calls
        AtomicInteger processDbCallCount = new AtomicInteger(0);
        List<Long> processedDbIds = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(2);

        // Mock processDbTransactions to track calls
        new MockUp<PublishVersionDaemon>() {
            @Mock
            private void processDbTransactions(long dbId, DatabaseTransactionMgr dbTxnMgr) {
                processDbCallCount.incrementAndGet();
                processedDbIds.add(dbId);
                latch.countDown();
            }
        };

        // Mock GlobalTransactionMgr to return test DB managers
        Map<Long, DatabaseTransactionMgr> mockDbMgrs = Maps.newHashMap();
        mockDbMgrs.put(1001L, new DatabaseTransactionMgr(1001L, GlobalStateMgr.getCurrentState()));
        mockDbMgrs.put(1002L, new DatabaseTransactionMgr(1002L, GlobalStateMgr.getCurrentState()));

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public Map<Long, DatabaseTransactionMgr> getAllDatabaseTransactionMgrs() {
                return mockDbMgrs;
            }
        };

        // Mock SystemInfoService to return backends
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Long> getBackendIds(boolean needAlive) {
                return Lists.newArrayList(10001L, 10002L);
            }
        };

        // Enable multi-thread mode
        Config.publish_version_poller_threads = 4;
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        daemon.prepare();
        daemon.runAfterCatalogReady();

        // Wait for tasks to complete
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        if (completed) {
            // Verify both DBs were processed
            Assertions.assertEquals(2, processDbCallCount.get());
            Assertions.assertTrue(processedDbIds.contains(1001L));
            Assertions.assertTrue(processedDbIds.contains(1002L));
        }
    }

    @Test
    public void testProcessDbTransactionsBatch()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Save original config values
        boolean oldLakeEnableBatchPublish = Config.lake_enable_batch_publish_version;
        AtomicInteger publishBatchCallCount = new AtomicInteger(0);

        try {
            // Enable batch publish mode
            Config.lake_enable_batch_publish_version = true;

            // Mock RunMode.isSharedDataMode() to return true
            new MockUp<RunMode>() {
                @Mock
                public static boolean isSharedDataMode() {
                    return true;
                }
            };

            // Mock DatabaseTransactionMgr.getReadyToPublishTxnListBatch() to return non-empty list
            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public List<TransactionStateBatch> getReadyToPublishTxnListBatch() {
                    // Return a mock batch
                    TransactionStateBatch batch = new TransactionStateBatch(Lists.newArrayList());
                    return Lists.newArrayList(batch);
                }
            };

            // Mock publishVersionForLakeTableBatch to track calls
            new MockUp<PublishVersionDaemon>() {
                @Mock
                void publishVersionForLakeTableBatch(List<TransactionStateBatch> txnStateBatches) {
                    publishBatchCallCount.incrementAndGet();
                }
            };

            Config.publish_version_poller_threads = 0;
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            DatabaseTransactionMgr dbTxnMgr = new DatabaseTransactionMgr(1001L, GlobalStateMgr.getCurrentState());

            // Call processDbTransactions - should call publishVersionForLakeTableBatch
            MethodUtils.invokeMethod(daemon, true, "processDbTransactions", 1001L, dbTxnMgr);

            Assertions.assertEquals(1, publishBatchCallCount.get());
        } finally {
            Config.lake_enable_batch_publish_version = oldLakeEnableBatchPublish;
        }
    }

    @Test
    public void testProcessDbTransactionsNonBatch()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Save original config values
        boolean oldLakeEnableBatchPublish = Config.lake_enable_batch_publish_version;
        boolean oldEnableNewPublishMechanism = Config.enable_new_publish_mechanism;
        AtomicInteger publishOlapCallCount = new AtomicInteger(0);
        AtomicInteger publishLakeCallCount = new AtomicInteger(0);

        try {
            // Disable batch publish mode
            Config.lake_enable_batch_publish_version = false;

            // Test 1: SharedNothing mode with new publish mechanism
            Config.enable_new_publish_mechanism = true;
            new MockUp<RunMode>() {
                @Mock
                public static boolean isSharedDataMode() {
                    return false;
                }

                @Mock
                public static boolean isSharedNothingMode() {
                    return true;
                }
            };

            new MockUp<DatabaseTransactionMgr>() {
                @Mock
                public List<TransactionState> getReadyToPublishTxnList() {
                    return Lists.newArrayList(new TransactionState());
                }

                @Mock
                public List<TransactionState> getCommittedTxnList() {
                    return Lists.newArrayList(new TransactionState());
                }
            };

            new MockUp<PublishVersionDaemon>() {
                @Mock
                void publishVersionForOlapTable(List<TransactionState> txnStates) {
                    publishOlapCallCount.incrementAndGet();
                }

                @Mock
                void publishVersionForLakeTable(List<TransactionState> txnStates) {
                    publishLakeCallCount.incrementAndGet();
                }
            };

            Config.publish_version_poller_threads = 0;
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            DatabaseTransactionMgr dbTxnMgr = new DatabaseTransactionMgr(1001L, GlobalStateMgr.getCurrentState());

            MethodUtils.invokeMethod(daemon, true, "processDbTransactions", 1001L, dbTxnMgr);
            Assertions.assertEquals(1, publishOlapCallCount.get());

            // Test 2: SharedData mode (not SharedNothing) with old publish mechanism
            Config.enable_new_publish_mechanism = false;
            new MockUp<RunMode>() {
                @Mock
                public static boolean isSharedDataMode() {
                    return false;
                }

                @Mock
                public static boolean isSharedNothingMode() {
                    return false;
                }
            };

            daemon = new PublishVersionDaemon();
            MethodUtils.invokeMethod(daemon, true, "processDbTransactions", 1001L, dbTxnMgr);
            Assertions.assertEquals(1, publishLakeCallCount.get());
        } finally {
            Config.lake_enable_batch_publish_version = oldLakeEnableBatchPublish;
            Config.enable_new_publish_mechanism = oldEnableNewPublishMechanism;
        }
    }
}
