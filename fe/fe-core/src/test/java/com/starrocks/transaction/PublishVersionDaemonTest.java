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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.PublishVersionTask;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.warehouse.cngroup.ComputeResource;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        return createMockTransactionState(txnId, dbId, TransactionStatus.COMMITTED, allTasksFinished);
    }

    private TransactionState createMockTransactionState(long txnId, long dbId, TransactionStatus status,
                                                        boolean allTasksFinished) {
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
        txnState.setTransactionStatus(status);

        PublishVersionTask task1 = new PublishVersionTask(1L, txnId, 0L, dbId, System.currentTimeMillis(),
                null, null, null, System.currentTimeMillis(), null, false, TransactionType.TXN_NORMAL);
        task1.setErrorTablets(Lists.newArrayList());
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

        // Prevent AgentBatchTask from being dispatched to the shared background agent-task pool.
        // Otherwise AgentBatchTask.run() would invoke methods on the @Mocked SystemInfoService
        // (e.g. getBackend) from an uncontrolled daemon thread, racing with JMockit's mock
        // lifecycle and causing flaky "Missing invocation" failures. This test only verifies
        // publish bookkeeping, not agent task dispatch.
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
                // no-op
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

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any,
                        anyLong);
                result = true;
                minTimes = 0;

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<TransactionState>() {
                    TransactionState finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                        return txn == txnId1 ? txnState1 : txnState2;
                    }
                };
            }
        };

        PublishVersionDaemon daemon = new PublishVersionDaemon();
        daemon.runAfterLeaseValid();

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

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any,
                        anyLong);
                result = true;
                minTimes = 0;

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<TransactionState>() {
                    TransactionState finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                        return txnState1;
                    }
                };
                minTimes = 0;
            }
        };

        daemon.publishingTransactionIds = Sets.newConcurrentHashSet();
        daemon.publishingTransactionIds.add(txnId1);
        daemon.runAfterLeaseValid();

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

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any,
                        anyLong);
                result = new mockit.Delegate<Boolean>() {
                    boolean canTxnFinished(TransactionState state, Set<Long> err, Set<Long> unfinished, long timeout) {
                        canTxnFinishedCallCount.incrementAndGet();
                        return false;
                    }
                };

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<TransactionState>() {
                    TransactionState finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                        return txnState3;
                    }
                };
                minTimes = 0;
            }
        };

        PublishVersionDaemon daemon2 = new PublishVersionDaemon();
        daemon2.runAfterLeaseValid();

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
        daemon3.runAfterLeaseValid();

        // Even if submission fails, publishingTransactionIds should be cleaned up
        Assertions.assertFalse(daemon3.publishingTransactionIds.contains(txnId1));

        // Test 5: canTxnFinished throws LockTimeoutException - should return early, finishTransaction never called
        finishedTxnIds.clear();
        new MockUp<PublishVersionDaemon>() {
            @Mock
            public ThreadPoolExecutor getTaskExecutor() {
                return new SynchronousExecutor();
            }
        };
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

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any,
                        anyLong);
                result = new LockTimeoutException("lock timeout");

                globalTransactionMgr.finishTransaction(anyLong, anyLong, (Set<Long>) any, anyLong);
                result = new mockit.Delegate<TransactionState>() {
                    TransactionState finishTransaction(long db, long txn, Set<Long> err, long timeout) {
                        finishedTxnIds.add(txn);
                        return txnState3;
                    }
                };
                minTimes = 0;
            }
        };

        PublishVersionDaemon daemon4 = new PublishVersionDaemon();
        // ERR_LOCK_ERROR is swallowed with an info log and retried next cycle — must not throw
        Assertions.assertDoesNotThrow(daemon4::runAfterLeaseValid);
        // finishTransaction must NOT have been invoked because tryFinishTransaction returned early
        Assertions.assertEquals(0, finishedTxnIds.size());

        // Test 6: canTxnFinished throws a non-lock StarRocksException - should propagate out of tryFinishTransaction
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

                globalTransactionMgr.canTxnFinished((TransactionState) any, (Set<Long>) any, (Set<Long>) any,
                        anyLong);
                result = new StarRocksException("internal non-lock error");
            }
        };

        // The non-lock exception propagates from tryFinishTransaction, is caught as Throwable
        // by the executor's catch block (LOG.error), so runAfterLeaseValid itself does not throw
        PublishVersionDaemon daemon5 = new PublishVersionDaemon();
        Assertions.assertDoesNotThrow(daemon5::runAfterLeaseValid);
        // finishTransaction was never reached because exception was thrown before it
        Assertions.assertEquals(0, finishedTxnIds.size());
    }

    @Test
    public void testTryFinishTransactionUsesReturnedTxnStateForCleanup(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked GlobalTransactionMgr globalTransactionMgr) throws Exception {
        long txnId = 2001L;
        long dbId = 200L;
        TransactionState originalState = createMockTransactionState(txnId, dbId, TransactionStatus.COMMITTED, true);
        TransactionState latestState = createMockTransactionState(txnId, dbId, TransactionStatus.VISIBLE, true);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.finishTransaction(dbId, txnId, (Set<Long>) any, anyLong);
                result = latestState;
            }
        };

        PublishVersionDaemon daemon = new PublishVersionDaemon();
        MethodUtils.invokeMethod(daemon, true, "tryFinishTransaction", originalState);

        Assertions.assertEquals(1, originalState.getPublishVersionTasks().size());
        Assertions.assertTrue(latestState.getPublishVersionTasks().isEmpty());
    }

    @Test
    public void testPublishVersionNewUsesReturnedTxnStateForCleanup(@Mocked GlobalTransactionMgr globalTransactionMgr)
            throws Exception {
        long txnId = 2002L;
        long dbId = 201L;
        TransactionState originalState = createMockTransactionState(txnId, dbId, TransactionStatus.COMMITTED, true);
        TransactionState latestState = createMockTransactionState(txnId, dbId, TransactionStatus.VISIBLE, true);

        new MockUp<TransactionState>() {
            @Mock
            public boolean allPublishTasksFinishedOrQuorumWaitTimeout(Set<Long> publishErrorReplicas) {
                return true;
            }

            @Mock
            public boolean checkCanFinish() {
                return true;
            }
        };

        new Expectations() {
            {
                globalTransactionMgr.finishTransactionNew(originalState, (Set<Long>) any);
                result = latestState;
            }
        };

        PublishVersionDaemon daemon = new PublishVersionDaemon();
        MethodUtils.invokeMethod(daemon, true, "publishVersionNew", globalTransactionMgr,
                Lists.newArrayList(originalState));

        Assertions.assertEquals(1, originalState.getPublishVersionTasks().size());
        Assertions.assertTrue(latestState.getPublishVersionTasks().isEmpty());
    }

    @Test
    public void testOnStoppedReleasesExecutorsAndDedupSets() {
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        // Force lazy init of both executors through their getters.
        ThreadPoolExecutor taskExec = daemon.getTaskExecutor();
        ThreadPoolExecutor deleteExec = daemon.getDeleteTxnLogExecutor();
        Assertions.assertNotNull(taskExec);
        Assertions.assertNotNull(deleteExec);

        // Populate both dedup sets so we can verify onStopped() clears them.
        Set<Long> publishing = Sets.newConcurrentHashSet();
        publishing.add(42L);
        daemon.publishingTransactionIds = publishing;
        Set<Long> batchTable = Sets.newConcurrentHashSet();
        batchTable.add(7L);
        daemon.publishingLakeTransactionsBatchTableId = batchTable;

        daemon.onStopped();

        Assertions.assertTrue(taskExec.isShutdown(), "taskExecutor must be shut down");
        Assertions.assertTrue(deleteExec.isShutdown(), "deleteTxnLogExecutor must be shut down");
        Assertions.assertTrue(taskExec.isTerminated(),
                "taskExecutor must be terminated after onStopped() awaits drain");
        Assertions.assertTrue(deleteExec.isTerminated(),
                "deleteTxnLogExecutor must be terminated after onStopped() awaits drain");
        Assertions.assertNull(daemon.taskExecutor,
                "taskExecutor reference must be nulled after successful drain so getter rebuilds fresh");
        Assertions.assertNull(daemon.deleteTxnLogExecutor,
                "deleteTxnLogExecutor reference must be nulled after successful drain");
        Assertions.assertTrue(publishing.isEmpty(), "publishingTransactionIds must be cleared");
        Assertions.assertTrue(batchTable.isEmpty(), "publishingLakeTransactionsBatchTableId must be cleared");
    }

    @Test
    public void testOnStoppedTolerantOfNullExecutorsAndNullSets() {
        // Fresh instance: executors and dedup sets may both be null. onStopped must be no-op-safe.
        PublishVersionDaemon daemon = new PublishVersionDaemon();
        daemon.taskExecutor = null;
        daemon.deleteTxnLogExecutor = null;
        daemon.publishingTransactionIds = null;
        daemon.publishingLakeTransactionsBatchTableId = null;
        Assertions.assertDoesNotThrow(daemon::onStopped);
    }

    @Test
    public void testConfigRefreshListenersDoNotAccumulateAcrossStopCycles() throws Exception {
        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();
        @SuppressWarnings("unchecked")
        List<?> listeners = (List<?>) FieldUtils.readField(configDaemon, "listeners", true);

        PublishVersionDaemon daemon = new PublishVersionDaemon();
        int before = listeners.size();
        // First activation: both listeners get registered.
        daemon.getTaskExecutor();
        MethodUtils.invokeMethod(daemon, true, "getDeleteTxnLogExecutor");
        Assertions.assertEquals(before + 2, listeners.size(),
                "first getTaskExecutor + getDeleteTxnLogExecutor should register exactly two listeners");

        // Simulate demote: executors are nulled so the next activation will recreate them.
        daemon.onStopped();

        // Re-activation: executors recreated, but listeners must not be registered again.
        daemon.getTaskExecutor();
        MethodUtils.invokeMethod(daemon, true, "getDeleteTxnLogExecutor");
        Assertions.assertEquals(before + 2, listeners.size(),
                "re-creating executors after onStopped must not leak additional listeners");

        // And a third cycle stays stable.
        daemon.onStopped();
        daemon.getTaskExecutor();
        MethodUtils.invokeMethod(daemon, true, "getDeleteTxnLogExecutor");
        Assertions.assertEquals(before + 2, listeners.size(),
                "listener count must stay stable across repeated demote/re-elect cycles");
    }

    @Test
    public void testMaybeLogSlowPublishPartition() {
        // Fast path: total < 3000ms, helper returns without logging. No exception expected.
        long now = System.currentTimeMillis();
        Assertions.assertDoesNotThrow(() -> PublishVersionDaemon.maybeLogSlowPublishPartition(
                1L, 2L, 3L,
                now, now + 10, now + 20, now + 30, now + 100));

        // Slow path: total >= 3000ms, helper formats and emits the warn log.
        long submitTimeMs = now;
        long lambdaEntryMs = submitTimeMs + 200;
        long lockAcquiredMs = lambdaEntryMs + 400;
        long rpcStartMs = lockAcquiredMs + 100;
        long rpcEndMs = submitTimeMs + 5000;
        Assertions.assertDoesNotThrow(() -> PublishVersionDaemon.maybeLogSlowPublishPartition(
                10L, 20L, 30L,
                submitTimeMs, lambdaEntryMs, lockAcquiredMs, rpcStartMs, rpcEndMs));

        // Boundary: total == 3000ms triggers the slow path (>=).
        Assertions.assertDoesNotThrow(() -> PublishVersionDaemon.maybeLogSlowPublishPartition(
                11L, 21L, 31L,
                submitTimeMs, lambdaEntryMs, lockAcquiredMs, rpcStartMs, submitTimeMs + 3000));
    }

    // Regression for the file-bundling compaction-vs-rollup bug: a transaction whose touched-index set
    // (publishedNormalIndexIds) misses a currently-visible NORMAL index (e.g. a rollup/MV index that a
    // lake compaction did not touch) must carry that index's tablets forward, so the new version's bundle
    // stays a complete whole-partition snapshot. SHADOW indexes are never carried forward.
    @Test
    public void testCollectFileBundlingCarryForwardTablets() {
        TStorageMedium medium = TStorageMedium.HDD;
        // base index (id=1): tablets 101,102 ; rollup index (id=2): tablets 201,202 ; shadow index (id=3): 301
        MaterializedIndex baseIndex = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        baseIndex.addTablet(new LakeTablet(101L), new TabletMeta(1L, 2L, 3L, 1L, medium, true), false);
        baseIndex.addTablet(new LakeTablet(102L), new TabletMeta(1L, 2L, 3L, 1L, medium, true), false);

        MaterializedIndex rollupIndex = new MaterializedIndex(2L, MaterializedIndex.IndexState.NORMAL);
        rollupIndex.addTablet(new LakeTablet(201L), new TabletMeta(1L, 2L, 3L, 2L, medium, true), false);
        rollupIndex.addTablet(new LakeTablet(202L), new TabletMeta(1L, 2L, 3L, 2L, medium, true), false);

        MaterializedIndex shadowIndex = new MaterializedIndex(3L, MaterializedIndex.IndexState.SHADOW);
        shadowIndex.addTablet(new LakeTablet(301L), new TabletMeta(1L, 2L, 3L, 3L, medium, true), false);

        List<MaterializedIndex> visibleIndexes = Lists.newArrayList(baseIndex, rollupIndex, shadowIndex);

        // Compaction touched only the base index (id=1): the rollup index (id=2) must be carried forward,
        // and the SHADOW index (id=3) must be excluded.
        List<Tablet> carry = PublishVersionDaemon.collectFileBundlingCarryForwardTablets(
                visibleIndexes, Sets.newHashSet(1L));
        Assertions.assertNotNull(carry);
        Assertions.assertEquals(Lists.newArrayList(201L, 202L),
                carry.stream().map(Tablet::getId).sorted().collect(Collectors.toList()));

        // Every visible NORMAL index already touched (normal load): nothing to carry forward.
        Assertions.assertNull(PublishVersionDaemon.collectFileBundlingCarryForwardTablets(
                visibleIndexes, Sets.newHashSet(1L, 2L)));

        // Nothing touched: both NORMAL indexes carried forward, SHADOW still excluded.
        List<Tablet> carryAll = PublishVersionDaemon.collectFileBundlingCarryForwardTablets(
                visibleIndexes, Sets.newHashSet());
        Assertions.assertEquals(Lists.newArrayList(101L, 102L, 201L, 202L),
                carryAll.stream().map(Tablet::getId).sorted().collect(Collectors.toList()));
    }

    // The touched tablets (this publish's real transactions) and the carry-forward tablets (untouched but
    // visible indexes) must go into ONE aggregate request; two separate aggregate publishes would each
    // truncate-write the same meta/0_<version>.meta and drop one set. Because a batch can span several
    // versions, the carry-forward emits one no-op empty transaction per real transaction so the untouched
    // tablets advance across every version (the BE requires new_version == base_version + txns.size()).
    @Test
    public void testAggregatePublishWithCarryForwardBuildsSingleRequest() throws Exception {
        List<List<Tablet>> capturedTablets = new ArrayList<>();
        List<List<TxnInfoPB>> capturedTxnInfos = new ArrayList<>();
        List<AggregatePublishVersionRequest> capturedRequests = new ArrayList<>();
        AtomicInteger sendCount = new AtomicInteger(0);
        List<AggregatePublishVersionRequest> sentRequests = new ArrayList<>();

        new MockUp<Utils>() {
            @Mock
            public void createSubRequestForAggregatePublish(List<Tablet> tablets, List<TxnInfoPB> txnInfos,
                    long baseVersion, long newVersion, Map<ComputeNode, List<Long>> nodeToTablets,
                    ComputeResource computeResource, AggregatePublishVersionRequest request) {
                capturedTablets.add(tablets);
                capturedTxnInfos.add(txnInfos);
                capturedRequests.add(request);
            }

            @Mock
            public void sendAggregatePublishVersionRequest(AggregatePublishVersionRequest request,
                    long baseVersion, ComputeResource computeResource, Map<Long, Double> compactionScores,
                    Map<Long, TabletRange> tabletRanges, Map<Long, TabletStatPB> tabletStats,
                    List<VectorIndexBuildInfoPB> vectorIndexBuildInfos) {
                sendCount.incrementAndGet();
                sentRequests.add(request);
            }
        };

        List<Tablet> touched = Lists.newArrayList(new LakeTablet(101L), new LakeTablet(102L));
        List<Tablet> carryForward = Lists.newArrayList(new LakeTablet(201L), new LakeTablet(202L));
        // A two-transaction batch (versions 5 and 6): base is version 4, new version is 6.
        TxnInfoPB t1 = new TxnInfoPB();
        t1.txnId = 1001L;
        t1.commitTime = 111L;
        t1.gtid = 9001L;
        TxnInfoPB t2 = new TxnInfoPB();
        t2.txnId = 1002L;
        t2.commitTime = 222L;
        t2.gtid = 9002L;
        List<TxnInfoPB> txnInfos = Lists.newArrayList(t1, t2);

        PublishVersionDaemon.aggregatePublishWithCarryForward(touched, txnInfos, carryForward,
                4L, 6L, null, WarehouseManager.DEFAULT_RESOURCE, new java.util.HashMap<>(),
                new java.util.HashMap<>(), new ArrayList<>());

        // Exactly two sub-requests, both attached to the SAME request, sent exactly once.
        Assertions.assertEquals(2, capturedRequests.size());
        Assertions.assertSame(capturedRequests.get(0), capturedRequests.get(1));
        Assertions.assertEquals(1, sendCount.get());
        Assertions.assertSame(capturedRequests.get(0), sentRequests.get(0));

        // First sub-request: the touched tablets with the real transactions unchanged.
        Assertions.assertEquals(Lists.newArrayList(101L, 102L),
                capturedTablets.get(0).stream().map(Tablet::getId).sorted().collect(Collectors.toList()));
        Assertions.assertSame(txnInfos, capturedTxnInfos.get(0));

        // Second sub-request: the carry-forward tablets with one no-op empty transaction per real transaction.
        Assertions.assertEquals(Lists.newArrayList(201L, 202L),
                capturedTablets.get(1).stream().map(Tablet::getId).sorted().collect(Collectors.toList()));
        List<TxnInfoPB> carryTxnInfos = capturedTxnInfos.get(1);
        Assertions.assertEquals(txnInfos.size(), carryTxnInfos.size());
        for (int i = 0; i < carryTxnInfos.size(); i++) {
            TxnInfoPB empty = carryTxnInfos.get(i);
            Assertions.assertTrue(empty.noOpPublish, "carry-forward txn must be a no-op publish");
            Assertions.assertEquals(-1L, empty.txnId, "carry-forward txn must carry the empty txn id");
            Assertions.assertEquals(TxnTypePB.TXN_EMPTY, empty.txnType);
            Assertions.assertFalse(empty.combinedTxnLog);
            // commitTime / gtid are copied from the corresponding real transaction.
            Assertions.assertEquals(txnInfos.get(i).commitTime, empty.commitTime);
            Assertions.assertEquals(txnInfos.get(i).gtid, empty.gtid);
        }
    }
}
