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
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.PublishVersionTask;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class PublishVersionDaemonTest {
    public int oldValue;
    private boolean originalSharedNothingPublishUseThreadPool;

    @BeforeEach
    public void setUp() {
        oldValue = Config.publish_version_max_threads;
        originalSharedNothingPublishUseThreadPool = Config.shared_nothing_publish_use_thread_pool;
    }

    @AfterEach
    public void tearDown() {
        Config.publish_version_max_threads = oldValue;
        Config.shared_nothing_publish_use_thread_pool = originalSharedNothingPublishUseThreadPool;
    }

    @Test
    public void testUpdateLakeExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getTaskExecutor");
        Assertions.assertNotNull(executor);
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getCorePoolSize());

        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();

        // scale out
        Config.publish_version_max_threads += 10;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getCorePoolSize());


        // scale in
        Config.publish_version_max_threads -= 5;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getCorePoolSize());

        int oldNumber = executor.getMaximumPoolSize();

        // config set to < 0
        Config.publish_version_max_threads = -1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(oldNumber, executor.getMaximumPoolSize());
        Assertions.assertEquals(oldNumber, executor.getCorePoolSize());


        // config set to > PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        Config.publish_version_max_threads = PublishVersionDaemon.PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE + 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(oldNumber, executor.getMaximumPoolSize());
        Assertions.assertEquals(oldNumber, executor.getCorePoolSize());


        // config set to PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        Config.publish_version_max_threads = PublishVersionDaemon.PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getCorePoolSize());

        // config set to 1
        Config.publish_version_max_threads = 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getMaximumPoolSize());
        Assertions.assertEquals(Config.publish_version_max_threads, executor.getCorePoolSize());
    }

    @Test
    public void testInvalidInitConfiguration()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        int hardCodeDefaultMaxThreads = (int) FieldUtils.readDeclaredStaticField(PublishVersionDaemon.class,
                "PUBLISH_THREAD_POOL_DEFAULT_MAX_SIZE", true);

        // <= 0
        int initValue = 0;
        Config.publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getTaskExecutor");

            Assertions.assertNotNull(executor);
            Assertions.assertNotEquals(initValue, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getCorePoolSize());
            // configVar set to default value.
            Assertions.assertEquals(hardCodeDefaultMaxThreads, Config.publish_version_max_threads);
        }

        // > PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE
        initValue = PublishVersionDaemon.PUBLISH_THREAD_POOL_HARD_LIMIT_SIZE + 1;
        Config.publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getTaskExecutor");
            Assertions.assertNotNull(executor);
            Assertions.assertNotEquals(initValue, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getMaximumPoolSize());
            Assertions.assertEquals(hardCodeDefaultMaxThreads, executor.getCorePoolSize());
            // configVar set to default value.
            Assertions.assertEquals(hardCodeDefaultMaxThreads, Config.publish_version_max_threads);
        }
    }

    @Test
    public void testUpdateDeleteTxnLogExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();

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

    /**
     * Helper method to create a mock TransactionState for shared_nothing_publish_use_thread_pool tests
     */
    private TransactionState createMockTransactionState(long txnId, long dbId, boolean allTasksFinished) {
        TransactionState txnState = new TransactionState(
                dbId,
                Lists.newArrayList(1L),
                txnId,
                "test_label_" + txnId,
                null,
                TransactionState.LoadJobSourceType.FRONTEND,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe"),
                -1,
                Config.stream_load_default_timeout_second
        );
        txnState.setTransactionStatus(TransactionStatus.COMMITTED);

        PublishVersionTask task1 = new PublishVersionTask(1L, txnId, 0L, dbId, System.currentTimeMillis(),
                null, null, null, System.currentTimeMillis(), null, false, TransactionType.TXN_NORMAL);
        task1.setIsFinished(allTasksFinished);
        txnState.addPublishVersionTask(1L, task1);

        return txnState;
    }

    private static class SynchronousExecutor extends ThreadPoolExecutor {
        public SynchronousExecutor() {
            super(1, 1, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                    new java.util.concurrent.LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    /**
     * Test shared_nothing_publish_use_thread_pool=true scenarios:
     * 1. Transactions are submitted to thread pool, finishTransaction is called, and publishingTransactionIds is cleaned up
     * 2. Duplicate transactions are skipped
     * 3. canTxnFinished returns false - transaction should not finish
     * 4. When thread pool submission fails, publishingTransactionIds is still cleaned up
     */
    @Test
    public void testSharedNothingPublishWithThreadPool(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr,
            @Mocked NodeMgr nodeMgr,
            @Mocked SystemInfoService systemInfoService) throws Exception {
        Config.shared_nothing_publish_use_thread_pool = true;

        long txnId1 = 1001L;
        long txnId2 = 1002L;
        long txnId3 = 1003L;
        long dbId = 100L;

        TransactionState txnState1 = createMockTransactionState(txnId1, dbId, true);
        TransactionState txnState2 = createMockTransactionState(txnId2, dbId, true);
        TransactionState txnState3 = createMockTransactionState(txnId3, dbId, false);

        List<Long> finishedTxnIds = new ArrayList<>();
        AtomicInteger canTxnFinishedCallCount = new AtomicInteger(0);

        new MockUp<PublishVersionDaemon>() {
            @Mock
            public ThreadPoolExecutor getTaskExecutor() {
                return new SynchronousExecutor();
            }
        };

        // Test 1: Multiple transactions processed, finishTransaction called
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getClusterInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackendIds(false);
                result = Lists.newArrayList(1L, 2L, 3L);
                minTimes = 0;

                globalTransactionMgr.getReadyToPublishTransactions(anyBoolean);
                result = Lists.newArrayList(txnState1, txnState2);

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any);
                result = true;
                minTimes = 0;

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<Void>() {
                    void finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                    }
                };
            }
        };

        PublishVersionDaemon daemon = new PublishVersionDaemon();
        daemon.runAfterCatalogReady();

        Assertions.assertEquals(2, finishedTxnIds.size());
        Assertions.assertTrue(finishedTxnIds.contains(txnId1));
        Assertions.assertTrue(finishedTxnIds.contains(txnId2));
        // Verify publishingTransactionIds is properly cleaned up after completion
        Assertions.assertFalse(daemon.publishingTransactionIds.contains(txnId1));
        Assertions.assertFalse(daemon.publishingTransactionIds.contains(txnId2));

        // Test 2: Duplicate transaction skipped
        finishedTxnIds.clear();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getClusterInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackendIds(false);
                result = Lists.newArrayList(1L, 2L, 3L);
                minTimes = 0;

                globalTransactionMgr.getReadyToPublishTransactions(anyBoolean);
                result = Lists.newArrayList(txnState1);

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any);
                result = true;
                minTimes = 0;

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<Void>() {
                    void finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                    }
                };
                minTimes = 0;
            }
        };

        daemon.publishingTransactionIds = Sets.newConcurrentHashSet();
        daemon.publishingTransactionIds.add(txnId1);
        daemon.runAfterCatalogReady();

        Assertions.assertEquals(0, finishedTxnIds.size());

        // Test 3: canTxnFinished returns false - transaction should not finish
        finishedTxnIds.clear();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getClusterInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackendIds(false);
                result = Lists.newArrayList(1L, 2L, 3L);
                minTimes = 0;

                globalTransactionMgr.getReadyToPublishTransactions(anyBoolean);
                result = Lists.newArrayList(txnState3);

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any);
                result = new mockit.Delegate<Boolean>() {
                    boolean canTxnFinished(TransactionState state, Set<Long> err, Set<Long> unfinished) {
                        canTxnFinishedCallCount.incrementAndGet();
                        return false;
                    }
                };

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<Void>() {
                    void finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                    }
                };
                minTimes = 0;
            }
        };

        PublishVersionDaemon daemon2 = new PublishVersionDaemon();
        daemon2.runAfterCatalogReady();

        Assertions.assertTrue(canTxnFinishedCallCount.get() > 0);
        Assertions.assertEquals(0, finishedTxnIds.size());

        // Test 4: Thread pool submission fails, publishingTransactionIds is still cleaned up
        finishedTxnIds.clear();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                minTimes = 0;
                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;
                nodeMgr.getClusterInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackendIds(false);
                result = Lists.newArrayList(1L, 2L, 3L);
                minTimes = 0;

                globalTransactionMgr.getReadyToPublishTransactions(anyBoolean);
                result = Lists.newArrayList(txnState1);
            }
        };

        new MockUp<PublishVersionDaemon>() {
            @Mock
            public ThreadPoolExecutor getTaskExecutor() {
                return new ThreadPoolExecutor(1, 1, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                        new java.util.concurrent.LinkedBlockingQueue<>()) {
                    @Override
                    public void execute(Runnable command) {
                        throw new java.util.concurrent.RejectedExecutionException("Simulated rejection");
                    }
                };
            }
        };

        PublishVersionDaemon daemon3 = new PublishVersionDaemon();
        daemon3.runAfterCatalogReady();

        // Even if submission fails, publishingTransactionIds should be cleaned up
        Assertions.assertFalse(daemon3.publishingTransactionIds.contains(txnId1));
    }
}
