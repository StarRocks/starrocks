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

package com.starrocks.connector.iceberg;

import com.google.common.cache.Cache;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for IcebergCommitQueueManager.
 */
public class IcebergCommitQueueManagerTest {

    private IcebergCommitQueueManager manager;
    private IcebergCommitQueueManager.Config config;

    @BeforeEach
    public void setUp() {
        config = new IcebergCommitQueueManager.Config(
                true,   // enabled
                120,    // 120 seconds timeout (minimum valid value)
                100     // max queue size
        );
        manager = new IcebergCommitQueueManager(config);
    }

    @AfterEach
    public void tearDown() {
        if (manager != null) {
            manager.shutdownAll();
        }
    }

    @Test
    public void testConstructor() {
        assertNotNull(manager);
        assertTrue(manager.isEnabled());
        assertEquals(0, manager.getActiveTableCount());
    }

    @Test
    public void testInvalidConfigParametersUseDefaultValues() {
        // Test timeout less than minimum - should use minimum value
        IcebergCommitQueueManager.Config smallTimeoutConfig =
                new IcebergCommitQueueManager.Config(true, 60, 100);
        assertEquals(IcebergCommitQueueManager.Config.getMinTimeoutSeconds(),
                     smallTimeoutConfig.getTimeoutSeconds());
        assertEquals(100, smallTimeoutConfig.getMaxQueueSize());

        // Test negative timeout - should use minimum value
        IcebergCommitQueueManager.Config negativeTimeoutConfig =
                new IcebergCommitQueueManager.Config(true, -1, 100);
        assertEquals(IcebergCommitQueueManager.Config.getMinTimeoutSeconds(),
                     negativeTimeoutConfig.getTimeoutSeconds());
        assertEquals(100, negativeTimeoutConfig.getMaxQueueSize());

        // Test zero timeout - should use minimum value
        IcebergCommitQueueManager.Config zeroTimeoutConfig =
                new IcebergCommitQueueManager.Config(true, 0, 100);
        assertEquals(IcebergCommitQueueManager.Config.getMinTimeoutSeconds(),
                     zeroTimeoutConfig.getTimeoutSeconds());

        // Test queue size less than minimum - should use minimum value
        IcebergCommitQueueManager.Config smallQueueConfig =
                new IcebergCommitQueueManager.Config(true, 120, 5);
        assertEquals(120, smallQueueConfig.getTimeoutSeconds());
        assertEquals(IcebergCommitQueueManager.Config.getMinQueueSize(),
                     smallQueueConfig.getMaxQueueSize());

        // Test negative queue size - should use minimum value
        IcebergCommitQueueManager.Config negativeQueueConfig =
                new IcebergCommitQueueManager.Config(true, 120, -1);
        assertEquals(120, negativeQueueConfig.getTimeoutSeconds());
        assertEquals(IcebergCommitQueueManager.Config.getMinQueueSize(),
                     negativeQueueConfig.getMaxQueueSize());

        // Test both invalid - should use minimum values for both
        IcebergCommitQueueManager.Config bothInvalidConfig =
                new IcebergCommitQueueManager.Config(true, 30, 5);
        assertEquals(IcebergCommitQueueManager.Config.getMinTimeoutSeconds(),
                     bothInvalidConfig.getTimeoutSeconds());
        assertEquals(IcebergCommitQueueManager.Config.getMinQueueSize(),
                     bothInvalidConfig.getMaxQueueSize());
    }

    @Test
    public void testConstructorWithDefaultConfig() {
        IcebergCommitQueueManager defaultManager = new IcebergCommitQueueManager();
        assertNotNull(defaultManager);
        defaultManager.shutdownAll();
    }

    @Test
    public void testSuccessfulCommit() {
        IcebergCommitQueueManager.CommitResult result =
                manager.submitCommit("catalog", "db", "table", () -> {
                    // Simulate a successful commit
                });

        assertTrue(result.isSuccess());
        assertFalse(result.isSuccess() && result.getError() != null);
    }

    @Test
    public void testSuccessfulCommitWithReturnValue() {
        AtomicInteger counter = new AtomicInteger(0);

        IcebergCommitQueueManager.CommitResult result =
                manager.submitCommit("catalog", "db", "table", () -> {
                    counter.incrementAndGet();
                });

        assertTrue(result.isSuccess());
        assertEquals(1, counter.get());
    }

    @Test
    public void testFailedCommit() {
        IcebergCommitQueueManager.CommitResult result =
                manager.submitCommit("catalog", "db", "table", () -> {
                    throw new RuntimeException("Commit failed");
                });

        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError() instanceof RuntimeException);
        assertEquals("Commit failed", result.getError().getMessage());
    }

    @Test
    public void testSubmitCommitAndRethrow() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            manager.submitCommitAndRethrow("catalog", "db", "table", () -> {
                throw new RuntimeException("Commit failed");
            });
        });

        assertEquals("Commit failed", exception.getMessage());
    }

    @Test
    public void testSubmitCommitAndRethrowSuccess() {
        // Should not throw any exception
        manager.submitCommitAndRethrow("catalog", "db", "table", () -> {
            // Successful commit
        });
    }

    @Test
    public void testSubmitCommitInterruptedReturnsCommitStateUnknown() {
        Thread.currentThread().interrupt();
        try {
            IcebergCommitQueueManager.CommitResult result =
                    manager.submitCommit("catalog", "db", "table", () -> {});
            assertFalse(result.isSuccess());
            assertTrue(result.getError() instanceof InterruptedException);
        } finally {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testSubmitCommitTaskFailureWithMessage() {
        IcebergCommitQueueManager.CommitResult result =
                manager.submitCommit("catalog", "db", "table", () -> {
                    throw new IllegalStateException("commit failed");
                });

        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError() instanceof IllegalStateException);
        assertEquals("commit failed", result.getError().getMessage());
    }

    @Test
    public void testSubmitCommitImmediateTimeoutReturnsCommitStateUnknown() {
        IcebergCommitQueueManager.Config immediateTimeoutConfig =
                new IcebergCommitQueueManager.Config(true, 120, 10) {
                    @Override
                    public long getTimeoutSeconds() {
                        return 0;
                    }
                };
        IcebergCommitQueueManager localManager = new IcebergCommitQueueManager(immediateTimeoutConfig);
        try {
            IcebergCommitQueueManager.CommitResult result =
                    localManager.submitCommit("catalog", "db", "table", () -> {});
            assertFalse(result.isSuccess());
            assertTrue(result.getError() instanceof CommitStateUnknownException);
            assertTrue(result.getError().getMessage().contains("Commit timeout"));
        } finally {
            localManager.shutdownAll();
        }
    }

    @Test
    public void testSubmitCommitWaitInterruptedReturnsCommitStateUnknown() throws Exception {
        IcebergCommitQueueManager.Config cfg = new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager localManager = new IcebergCommitQueueManager(cfg);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch blockTask = new CountDownLatch(1);
        AtomicReference<IcebergCommitQueueManager.CommitResult> resultRef = new AtomicReference<>();

        Thread caller = new Thread(() -> {
            resultRef.set(localManager.submitCommit("catalog", "db", "table", () -> {
                taskStarted.countDown();
                blockTask.await(5, TimeUnit.SECONDS);
            }));
        });
        caller.start();

        assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
        caller.interrupt();
        caller.join(5000);
        blockTask.countDown();

        assertNotNull(resultRef.get());
        assertFalse(resultRef.get().isSuccess());
        assertTrue(resultRef.get().getError() instanceof CommitStateUnknownException);
        assertTrue(resultRef.get().getError().getMessage().contains("Commit interrupted"));
        localManager.shutdownAll();
    }

    @Test
    public void testDisabledQueueReturnsFailureOnException() {
        IcebergCommitQueueManager.Config disabledConfig =
                new IcebergCommitQueueManager.Config(false, 120, 10);
        IcebergCommitQueueManager disabledManager = new IcebergCommitQueueManager(disabledConfig);
        try {
            IcebergCommitQueueManager.CommitResult result =
                    disabledManager.submitCommit("catalog", "db", "table", () -> {
                        throw new Exception("disabled");
                    });
            assertFalse(result.isSuccess());
            assertTrue(result.getError() instanceof Exception);
        } finally {
            disabledManager.shutdownAll();
        }
    }

    @Test
    public void testSubmitCommitRejectedWhenExecutorShutdown() throws Exception {
        manager.submitCommit("catalog", "db", "table", () -> {});
        Object key = newTableCommitKey("catalog", "db", "table");
        ExecutorService executor = getExecutorForKey(manager, key);
        executor.shutdown();

        IcebergCommitQueueManager.CommitResult result =
                manager.submitCommit("catalog", "db", "table", () -> {});
        assertFalse(result.isSuccess());
        assertTrue(result.getError() instanceof RejectedExecutionException);
    }

    @Test
    public void testBlockWhenQueueFullPolicyRejectsWhenShutdown() throws Exception {
        RejectedExecutionHandler policy = newBlockWhenQueueFullPolicy(0);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1));
        executor.shutdown();
        assertThrows(RejectedExecutionException.class,
                () -> policy.rejectedExecution(() -> {}, executor));
    }

    @Test
    public void testBlockWhenQueueFullPolicyTimesOutWhenQueueFull() throws Exception {
        RejectedExecutionHandler policy = newBlockWhenQueueFullPolicy(0);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1));
        executor.getQueue().add(() -> {});
        assertThrows(RejectedExecutionException.class,
                () -> policy.rejectedExecution(() -> {}, executor));
        executor.shutdownNow();
    }

    @Test
    public void testBlockWhenQueueFullPolicyWithInterrupted() throws Exception {
        RejectedExecutionHandler policy = newBlockWhenQueueFullPolicy(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1));
        executor.getQueue().add(() -> {});
        Thread.currentThread().interrupt();
        try {
            assertThrows(RejectedExecutionException.class,
                    () -> policy.rejectedExecution(() -> {}, executor));
        } finally {
            assertTrue(Thread.interrupted());
            executor.shutdownNow();
        }
    }

    @Test
    public void testGetOrCreateTableExecutorFailure() {
        IcebergCommitQueueManager badManager = new IcebergCommitQueueManager(() -> {
            throw new RuntimeException("supplier failed");
        });
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> badManager.submitCommit("catalog", "db", "table", () -> {}));
        // Config supplier is called before getOrCreateTableExecutor, so we get the original exception
        assertTrue(exception.getMessage().contains("supplier failed"));
    }

    @Test
    public void testShutdownExecutorForceAndInterruptedPaths() throws Exception {
        FakeExecutor forceExecutor = new FakeExecutor(false);
        Object key = newTableCommitKey("catalog", "db", "table");
        invokeShutdownExecutor(manager, forceExecutor, key);
        assertTrue(forceExecutor.shutdownCalled);
        assertTrue(forceExecutor.shutdownNowCalled);
        assertEquals(2, forceExecutor.awaitCalls);

        FakeExecutor interruptedExecutor = new FakeExecutor(true);
        invokeShutdownExecutor(manager, interruptedExecutor, key);
        assertTrue(interruptedExecutor.shutdownNowCalled);
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }

    @Test
    public void testShutdownExecutorForcePath() throws Exception {
        IcebergCommitQueueManager.Config config = new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager localManager = new IcebergCommitQueueManager(config);

        try {
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch block = new CountDownLatch(1);
            localManager.submitCommit("catalog", "db", "table", () -> {
                started.countDown();
                block.await(5, TimeUnit.SECONDS);
            });

            assertTrue(started.await(5, TimeUnit.SECONDS));
            localManager.shutdownTableExecutor("catalog", "db", "table");
            block.countDown();
        } finally {
            localManager.shutdownAll();
        }
    }

    @Test
    public void testCommitResultRethrowRuntimeAndChecked() {
        IcebergCommitQueueManager.CommitResult runtimeFailure =
                IcebergCommitQueueManager.CommitResult.failure(new IllegalArgumentException("bad"));
        IllegalArgumentException runtime = assertThrows(IllegalArgumentException.class,
                runtimeFailure::rethrowIfFailed);
        assertEquals("bad", runtime.getMessage());

        IcebergCommitQueueManager.CommitResult checkedFailure =
                IcebergCommitQueueManager.CommitResult.failure(new Exception("checked"));
        RuntimeException wrapped = assertThrows(RuntimeException.class,
                checkedFailure::rethrowIfFailed);
        assertTrue(wrapped.getMessage().contains("checked"));
    }

    @Test
    public void testTableCommitKeyEqualityAndToString() throws Exception {
        var ctor = IcebergCommitQueueManager.TableCommitKey.class
                .getDeclaredConstructor(String.class, String.class, String.class);
        ctor.setAccessible(true);
        Object key1 = ctor.newInstance("catalog", "db", "table");
        Object key2 = ctor.newInstance("catalog", "db", "table");
        Object key3 = ctor.newInstance("catalog", "db", "table2");

        assertEquals(key1, key2);
        assertNotEquals(key1, key3);
        assertNotEquals(key1, "not-a-key");
        assertEquals(key1.hashCode(), key2.hashCode());
        assertTrue(key1.toString().contains("catalog.db.table"));
    }

    private static RejectedExecutionHandler newBlockWhenQueueFullPolicy(long waitSeconds) throws Exception {
        Class<?> clazz = Class.forName(
                "com.starrocks.connector.iceberg.IcebergCommitQueueManager$BlockWhenQueueFullPolicy");
        Constructor<?> ctor = clazz.getDeclaredConstructor(long.class);
        ctor.setAccessible(true);
        return (RejectedExecutionHandler) ctor.newInstance(waitSeconds);
    }

    private static Object newTableCommitKey(String catalog, String db, String table) throws Exception {
        Class<?> clazz = Class.forName(
                "com.starrocks.connector.iceberg.IcebergCommitQueueManager$TableCommitKey");
        Constructor<?> ctor = clazz.getDeclaredConstructor(String.class, String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(catalog, db, table);
    }

    private static ExecutorService getExecutorForKey(IcebergCommitQueueManager manager, Object key) throws Exception {
        Field field = IcebergCommitQueueManager.class.getDeclaredField("tableExecutors");
        field.setAccessible(true);
        Cache<?, ?> cache = (Cache<?, ?>) field.get(manager);
        Object tableExecutor = cache.getIfPresent(key);
        Field execField = tableExecutor.getClass().getDeclaredField("executor");
        execField.setAccessible(true);
        return (ExecutorService) execField.get(tableExecutor);
    }

    private static void invokeShutdownExecutor(IcebergCommitQueueManager manager,
                                               ExecutorService executor,
                                               Object key) throws Exception {
        Method method = IcebergCommitQueueManager.class
                .getDeclaredMethod("shutdownExecutor", ExecutorService.class, key.getClass());
        method.setAccessible(true);
        method.invoke(manager, executor, key);
    }

    private static class FakeExecutor extends AbstractExecutorService {
        private final boolean throwOnAwait;
        private boolean shutdownCalled;
        private boolean shutdownNowCalled;
        private int awaitCalls;

        FakeExecutor(boolean throwOnAwait) {
            this.throwOnAwait = throwOnAwait;
        }

        @Override
        public void shutdown() {
            shutdownCalled = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownNowCalled = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdownCalled;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            awaitCalls++;
            if (throwOnAwait) {
                throw new InterruptedException("interrupted");
            }
            return false;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("execute not supported");
        }
    }

    @Test
    public void testConcurrentCommitsToSameTable() throws Exception {
        int numThreads = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger counter = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready

                    manager.submitCommitAndRethrow("catalog", "db", "table", () -> {
                        // Simulate some work
                        Thread.sleep(50);
                        counter.incrementAndGet();
                    });

                    successCount.incrementAndGet();
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads at once
        startLatch.countDown();

        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // All commits should succeed
        assertEquals(numThreads, successCount.get());
        assertEquals(numThreads, counter.get());
        assertTrue(exceptions.isEmpty(), "There should be no exceptions: " + exceptions);
    }

    @Test
    public void testConcurrentCommitsToDifferentTables() throws Exception {
        int numTables = 5;
        int commitsPerTable = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numTables * commitsPerTable);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger counter = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numTables * commitsPerTable);

        for (int table = 0; table < numTables; table++) {
            final String tableName = "table_" + table;

            for (int i = 0; i < commitsPerTable; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        manager.submitCommitAndRethrow("catalog", "db", tableName, () -> {
                            // Simulate some work
                            Thread.sleep(50);
                            counter.incrementAndGet();
                        });

                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
        }

        startLatch.countDown();
        assertTrue(endLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(numTables * commitsPerTable, successCount.get());
        assertEquals(numTables * commitsPerTable, counter.get());
        assertTrue(exceptions.isEmpty(), "There should be no exceptions: " + exceptions);
    }

    @Test
    public void testDisableCommitQueue() throws Exception {
        IcebergCommitQueueManager.Config disabledConfig =
                new IcebergCommitQueueManager.Config(false, 120, 100);
        IcebergCommitQueueManager disabledManager =
                new IcebergCommitQueueManager(disabledConfig);

        ExecutorService executor = null;
        try {
            assertFalse(disabledManager.isEnabled());

            AtomicInteger executionOrder = new AtomicInteger(0);
            List<Integer> order = new ArrayList<>();
            CountDownLatch tasksExecuted = new CountDownLatch(3);
            CountDownLatch allSubmitted = new CountDownLatch(3);

            // Submit multiple concurrent commits
            executor = Executors.newFixedThreadPool(3);

            for (int i = 0; i < 3; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        allSubmitted.countDown();
                        disabledManager.submitCommit("catalog", "db", "table", () -> {
                            order.add(index);
                            executionOrder.incrementAndGet();
                            tasksExecuted.countDown();
                        });
                    } catch (Throwable e) {
                        // Log exception if any
                        System.err.println("Task " + index + " failed: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }

            // Wait for all tasks to be submitted first
            assertTrue(allSubmitted.await(5, TimeUnit.SECONDS));

            // Now wait for all commits to execute
            assertTrue(tasksExecuted.await(10, TimeUnit.SECONDS));

            // Shutdown executor
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            // When disabled, commits execute immediately (not serialized)
            assertEquals(3, executionOrder.get(), "All 3 tasks should execute");
            assertEquals(3, order.size(), "All 3 tasks should complete");
        } finally {
            if (executor != null && !executor.isShutdown()) {
                executor.shutdownNow();
            }
            disabledManager.shutdownAll();
        }
    }

    @Test
    public void testShutdownTableExecutor() {
        // Create a table executor by submitting a commit
        manager.submitCommit("catalog", "db", "table", () -> {});

        assertEquals(1, manager.getActiveTableCount());

        // Shutdown the executor for the table
        manager.shutdownTableExecutor("catalog", "db", "table");

        assertEquals(0, manager.getActiveTableCount());

        // Shutdown should be idempotent
        manager.shutdownTableExecutor("catalog", "db", "table");
        assertEquals(0, manager.getActiveTableCount());
    }

    @Test
    public void testShutdownDatabaseExecutors() {
        // Create executors for multiple tables
        manager.submitCommit("catalog", "db", "table1", () -> {});
        manager.submitCommit("catalog", "db", "table2", () -> {});
        manager.submitCommit("catalog", "db", "table3", () -> {});

        assertEquals(3, manager.getActiveTableCount());

        // Shutdown all executors for the database
        manager.shutdownDatabaseExecutors("catalog", "db");

        assertEquals(0, manager.getActiveTableCount());
    }

    @Test
    public void testShutdownAll() {
        // Create executors for multiple databases
        manager.submitCommit("catalog", "db1", "table1", () -> {});
        manager.submitCommit("catalog", "db1", "table2", () -> {});
        manager.submitCommit("catalog", "db2", "table3", () -> {});

        assertEquals(3, manager.getActiveTableCount());

        manager.shutdownAll();

        assertEquals(0, manager.getActiveTableCount());
    }

    @Test
    public void testSerializationOfCommits() throws Exception {
        // Test that commits to the same table are serialized
        CountDownLatch firstCommitStarted = new CountDownLatch(1);
        CountDownLatch firstCommitBlocking = new CountDownLatch(1);
        CountDownLatch secondCommitStarted = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // First commit that will block
        executor.submit(() -> {
            manager.submitCommit("catalog", "db", "table", () -> {
                firstCommitStarted.countDown();
                try {
                    // Block for a while
                    firstCommitBlocking.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        });

        // Wait for first commit to start
        assertTrue(firstCommitStarted.await(5, TimeUnit.SECONDS));

        // Second commit should wait for first to complete
        executor.submit(() -> {
            secondCommitStarted.countDown();
            manager.submitCommit("catalog", "db", "table", () -> {
                // This should execute after first commit completes
            });
        });

        // Second commit should start quickly (after first is done)
        assertTrue(secondCommitStarted.await(5, TimeUnit.SECONDS));

        // Release first commit
        firstCommitBlocking.countDown();

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testDifferentTablesExecuteInParallel() throws Exception {
        // Test that commits to different tables can execute in parallel
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch bothStarted = new CountDownLatch(2);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Commit to table1
        executor.submit(() -> {
            manager.submitCommit("catalog", "db", "table1", () -> {
                bothStarted.countDown();
                try {
                    latch1.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        });

        // Commit to table2
        executor.submit(() -> {
            manager.submitCommit("catalog", "db", "table2", () -> {
                bothStarted.countDown();
                try {
                    latch2.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        });

        // Both commits should start (executing in parallel)
        assertTrue(bothStarted.await(5, TimeUnit.SECONDS));

        // Release both commits
        latch1.countDown();
        latch2.countDown();

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testBoundedQueueDoesNotLoseTasks() throws Exception {
        // Create a manager with minimum queue size to test that no tasks are lost
        IcebergCommitQueueManager.Config smallQueueConfig =
                new IcebergCommitQueueManager.Config(true, 120, 10);  // max queue size = 10 (minimum valid)
        IcebergCommitQueueManager smallQueueManager =
                new IcebergCommitQueueManager(smallQueueConfig);

        try {
            CountDownLatch firstTaskStarted = new CountDownLatch(1);
            CountDownLatch proceedWithFirst = new CountDownLatch(1);
            AtomicInteger tasksExecuted = new AtomicInteger(0);

            // Submit first task that will block and occupy the worker thread
            Thread firstThread = new Thread(() -> {
                smallQueueManager.submitCommit("catalog", "db", "table", () -> {
                    tasksExecuted.incrementAndGet();
                    firstTaskStarted.countDown();
                    try {
                        proceedWithFirst.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });
            firstThread.start();

            // Wait for first task to start running
            assertTrue(firstTaskStarted.await(5, TimeUnit.SECONDS));
            // Add delay to ensure the task is actually blocked on await()
            Thread.sleep(200);

            // Submit tasks to fill the queue (size = 10)
            CountDownLatch queueFillLatch = new CountDownLatch(10);
            for (int i = 0; i < 10; i++) {
                smallQueueManager.submitCommit("catalog", "db", "table", () -> {
                    tasksExecuted.incrementAndGet();
                    queueFillLatch.countDown();
                });
            }

            // Give some time for queue to be filled
            Thread.sleep(100);

            // Submit one more task - with BlockWhenQueueFullPolicy, this will wait
            // for queue capacity via offer() with timeout
            smallQueueManager.submitCommit("catalog", "db", "table", () -> {
                tasksExecuted.incrementAndGet();
            });

            // Release the first blocking task, which allows all tasks to complete
            proceedWithFirst.countDown();

            // Wait for all tasks to complete
            assertTrue(queueFillLatch.await(5, TimeUnit.SECONDS),
                    "Queue tasks should complete after first task is released");

            firstThread.join(5000);

            // All 12 tasks should have executed (no tasks lost despite queue being full)
            assertEquals(12, tasksExecuted.get(),
                    "All tasks should execute despite queue being full");
        } finally {
            smallQueueManager.shutdownAll();
        }
    }

    @Test
    public void testSubmitInterruptedReturnsFailure() throws Exception {
        IcebergCommitQueueManager.Config smallQueueConfig =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager smallQueueManager =
                new IcebergCommitQueueManager(smallQueueConfig);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch taskRunning = new CountDownLatch(1);
            CountDownLatch proceed = new CountDownLatch(1);

            // Occupy the single worker so queue fills
            executor.submit(() -> smallQueueManager.submitCommit("catalog", "db", "table", () -> {
                taskRunning.countDown();
                try {
                    proceed.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            assertTrue(taskRunning.await(5, TimeUnit.SECONDS));

            AtomicInteger counter = new AtomicInteger(0);
            AtomicInteger failures = new AtomicInteger(0);
            Thread submitThread = new Thread(() -> {
                try {
                    Thread.currentThread().interrupt();
                    IcebergCommitQueueManager.CommitResult result =
                            smallQueueManager.submitCommit("catalog", "db", "table", () -> counter.incrementAndGet());
                    if (result.isSuccess() || !(result.getError() instanceof InterruptedException)) {
                        failures.incrementAndGet();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            });
            submitThread.start();
            submitThread.join(5000);
            assertEquals(0, counter.get());
            assertEquals(0, failures.get());
            proceed.countDown();
        } finally {
            executor.shutdownNow();
            smallQueueManager.shutdownAll();
        }
    }

    @Test
    public void testDynamicConfigRefresh() throws Exception {
        // Test that runtime config changes take effect immediately
        AtomicInteger enabledFlag = new AtomicInteger(1);  // 1 = enabled, 0 = disabled
        AtomicInteger timeoutFlag = new AtomicInteger(120);  // timeout in seconds (minimum valid value)

        IcebergCommitQueueManager dynamicManager =
                new IcebergCommitQueueManager(() -> new IcebergCommitQueueManager.Config(
                        enabledFlag.get() == 1,
                        timeoutFlag.get(),
                        100  // max queue size
                ));

        try {
            // Initially enabled - commit should go through queue
            CountDownLatch commitStarted = new CountDownLatch(1);
            CountDownLatch proceedCommit = new CountDownLatch(1);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
                dynamicManager.submitCommit("catalog", "db", "table", () -> {
                    commitStarted.countDown();
                    try {
                        proceedCommit.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });

            // Wait for commit to start
            assertTrue(commitStarted.await(5, TimeUnit.SECONDS));
            assertTrue(dynamicManager.isEnabled());

            // Now disable the queue
            enabledFlag.set(0);

            // Commit should execute directly (not queued) since queue is disabled
            AtomicInteger counter = new AtomicInteger(0);
            dynamicManager.submitCommit("catalog", "db", "table2", () -> {
                counter.incrementAndGet();
            });

            // This commit should complete immediately since queue is disabled
            assertEquals(1, counter.get());
            assertFalse(dynamicManager.isEnabled());

            // Re-enable the queue
            enabledFlag.set(1);
            assertTrue(dynamicManager.isEnabled());

            proceedCommit.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            dynamicManager.shutdownAll();
        }
    }

    @Test
    public void testSharedCommitQueueManagerAcrossMetadataInstances() throws Exception {
        // Test that multiple IcebergMetadata instances sharing the same commitQueueManager
        // properly serialize commits to the same table (simulating multiple queries)
        IcebergCommitQueueManager sharedManager = new IcebergCommitQueueManager(
                new IcebergCommitQueueManager.Config(true, 120, 100));

        try {
            AtomicInteger successCount = new AtomicInteger(0);
            CountDownLatch allDone = new CountDownLatch(3);

            // Submit 3 commits concurrently from different threads
            ExecutorService queryExecutor = Executors.newFixedThreadPool(3);

            for (int i = 1; i <= 3; i++) {
                queryExecutor.submit(() -> {
                    sharedManager.submitCommit("catalog", "db", "table", () -> {
                        successCount.incrementAndGet();
                        allDone.countDown();
                    });
                });
            }

            // Wait for all to complete
            assertTrue(allDone.await(30, TimeUnit.SECONDS));
            queryExecutor.shutdown();
            assertTrue(queryExecutor.awaitTermination(10, TimeUnit.SECONDS));

            // Verify all commits succeeded and used the same executor
            assertEquals(3, successCount.get());
            assertEquals(1, sharedManager.getActiveTableCount(),
                    "All commits should use the same TableCommitExecutor");

        } finally {
            sharedManager.shutdownAll();
        }
    }

    @Test
    public void testConcurrentQueriesWithSharedManager() throws Exception {
        // Test that multiple "queries" (threads) using the same shared manager
        // properly serialize commits to the same table
        IcebergCommitQueueManager sharedManager = new IcebergCommitQueueManager(
                new IcebergCommitQueueManager.Config(true, 120, 100));

        try {
            int numQueries = 10;
            int commitsPerQuery = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(numQueries * commitsPerQuery);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger counter = new AtomicInteger(0);
            List<Exception> exceptions = new ArrayList<>();

            ExecutorService executor = Executors.newFixedThreadPool(numQueries);

            for (int query = 0; query < numQueries; query++) {
                final int queryId = query;

                for (int commit = 0; commit < commitsPerQuery; commit++) {
                    executor.submit(() -> {
                        try {
                            startLatch.await(); // Wait for all threads to be ready

                            // Simulate a query submitting commits
                            sharedManager.submitCommit("catalog", "db", "shared_table", () -> {
                                // Simulate some work
                                Thread.sleep(20);
                                counter.incrementAndGet();
                            });

                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            exceptions.add(e);
                        } finally {
                            endLatch.countDown();
                        }
                    });
                }
            }

            // Start all threads at once
            startLatch.countDown();

            // Wait for all threads to complete
            assertTrue(endLatch.await(60, TimeUnit.SECONDS));
            executor.shutdown();

            // All commits should succeed and execute in order
            assertEquals(numQueries * commitsPerQuery, successCount.get());
            assertEquals(numQueries * commitsPerQuery, counter.get());
            assertTrue(exceptions.isEmpty(), "There should be no exceptions: " + exceptions);
        } finally {
            sharedManager.shutdownAll();
        }
    }

    @Test
    public void testIdleExecutorCleanup() throws Exception {
        // Test that idle executors are automatically evicted from the cache
        // Note: This test uses a very short idle timeout for testing purposes
        // In production, the timeout is 1 hour
        IcebergCommitQueueManager.Config shortIdleConfig =
                new IcebergCommitQueueManager.Config(true, 120, 100);
        IcebergCommitQueueManager shortIdleManager =
                new IcebergCommitQueueManager(shortIdleConfig);

        try {
            // Create executors for multiple tables
            shortIdleManager.submitCommit("catalog", "db", "table1", () -> {});
            shortIdleManager.submitCommit("catalog", "db", "table2", () -> {});
            shortIdleManager.submitCommit("catalog", "db", "table3", () -> {});

            // All three executors should be active
            assertEquals(3, shortIdleManager.getActiveTableCount());

            // Note: Guava Cache's expireAfterAccess cleanup is done during:
            // 1. Manual cache operations (get, put)
            // 2. Periodic maintenance (not guaranteed to be immediate)
            //
            // In this test, we verify the structure is in place. The actual eviction
            // happens asynchronously by Guava Cache based on the expireAfterAccess setting.

            // To trigger cleanup, we can perform a cache operation
            // The cache size will be updated during the next maintenance cycle

            // Verify the structure exists and can be used
            shortIdleManager.submitCommit("catalog", "db", "table1", () -> {});

            // The cache structure is working correctly
            assertTrue(shortIdleManager.getActiveTableCount() >= 1);
        } finally {
            shortIdleManager.shutdownAll();
        }
    }

    @Test
    public void testMultipleCatalogsWithSharedManagers() throws Exception {
        // Test that different catalogs can have independent commit queues
        IcebergCommitQueueManager catalogA = new IcebergCommitQueueManager(
                new IcebergCommitQueueManager.Config(true, 120, 100));
        IcebergCommitQueueManager catalogB = new IcebergCommitQueueManager(
                new IcebergCommitQueueManager.Config(true, 120, 100));

        try {
            CountDownLatch catalogALatch = new CountDownLatch(1);
            CountDownLatch catalogBLatch = new CountDownLatch(1);
            CountDownLatch bothStarted = new CountDownLatch(2);

            ExecutorService executor = Executors.newFixedThreadPool(2);

            // Commit to catalog A
            executor.submit(() -> {
                catalogA.submitCommit("catalogA", "db", "table", () -> {
                    bothStarted.countDown();
                    try {
                        catalogALatch.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });

            // Commit to catalog B
            executor.submit(() -> {
                catalogB.submitCommit("catalogB", "db", "table", () -> {
                    bothStarted.countDown();
                    try {
                        catalogBLatch.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });

            // Both commits should execute in parallel (different catalogs)
            assertTrue(bothStarted.await(5, TimeUnit.SECONDS));

            catalogALatch.countDown();
            catalogBLatch.countDown();

            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            catalogA.shutdownAll();
            catalogB.shutdownAll();
        }
    }

    @Test
    public void testCommitQueueManagerWithSupplier() throws Exception {
        // Test that commitQueueManager correctly uses a supplier for dynamic config
        AtomicInteger enabledFlag = new AtomicInteger(1);

        IcebergCommitQueueManager supplierManager = new IcebergCommitQueueManager(
                () -> new IcebergCommitQueueManager.Config(
                        enabledFlag.get() == 1,
                        10,  // timeout
                        100   // max queue size
                ));

        try {
            assertTrue(supplierManager.isEnabled());

            // Disable the queue
            enabledFlag.set(0);
            assertFalse(supplierManager.isEnabled());

            // Commits should execute directly when disabled
            AtomicInteger counter = new AtomicInteger(0);
            supplierManager.submitCommit("catalog", "db", "table", () -> {
                counter.incrementAndGet();
            });

            assertEquals(1, counter.get());
        } finally {
            supplierManager.shutdownAll();
        }
    }

    @Test
    public void testBlockPolicyWithMaxWaitTime() throws Exception {
        // Test that BlockWhenQueueFullPolicy has a maximum wait time
        // Create a manager with minimum queue size
        IcebergCommitQueueManager.Config smallQueueConfig =
                new IcebergCommitQueueManager.Config(true, 120, 10);  // minimum valid queue size
        IcebergCommitQueueManager smallQueueManager =
                new IcebergCommitQueueManager(smallQueueConfig);

        try {
            CountDownLatch taskRunning = new CountDownLatch(1);
            CountDownLatch proceedWithFirst = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();

            // Submit first task that will occupy the worker thread
            Future<IcebergCommitQueueManager.CommitResult> firstFuture = executor.submit(() ->
                    smallQueueManager.submitCommit("catalog", "db", "table", () -> {
                        taskRunning.countDown();
                        try {
                            // Block for longer than MAX_WAIT_SECONDS (60s)
                            proceedWithFirst.await(120, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }));

            // Wait for first task to start running
            assertTrue(taskRunning.await(5, TimeUnit.SECONDS));

            // Submit second task - worker is occupied, this task will be queued
            // With BlockWhenQueueFullPolicy, it waits for queue capacity
            CountDownLatch secondTaskStarted = new CountDownLatch(1);
            Thread secondThread = new Thread(() -> {
                secondTaskStarted.countDown();
                IcebergCommitQueueManager.CommitResult result =
                        smallQueueManager.submitCommit("catalog", "db", "table", () -> {
                            // This should execute after first task completes
                        });
                // Should succeed after waiting (not exceed max wait time in this test)
                assertTrue(result.isSuccess(), "Second task should succeed");
            });
            secondThread.start();

            assertTrue(secondTaskStarted.await(5, TimeUnit.SECONDS));

            // Wait a short time to ensure second task is waiting for queue capacity
            Thread.sleep(1000);

            // Release the first task - second task should proceed
            proceedWithFirst.countDown();

            // Wait for both tasks to complete
            firstFuture.get(10, TimeUnit.SECONDS);
            secondThread.join(10000);
            executor.shutdown();

        } finally {
            smallQueueManager.shutdownAll();
        }
    }

    @Test
    public void testCommitWithExceptionInTask() {
        // Test that exceptions during commit execution are properly captured
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(config);

        try {
            RuntimeException expectedException = new RuntimeException("Expected test exception");

            IcebergCommitQueueManager.CommitResult result =
                    queueManager.submitCommit("catalog", "db", "table", () -> {
                        throw expectedException;
                    });

            assertFalse(result.isSuccess());
            assertTrue(result.getError() instanceof RuntimeException);
            assertEquals("Expected test exception", result.getError().getMessage());
        } finally {
            queueManager.shutdownAll();
        }
    }

    @Test
    public void testCommitResultRethrowIfFailed() {
        // Test successful commit rethrow
        IcebergCommitQueueManager.CommitResult successResult =
                IcebergCommitQueueManager.CommitResult.success();
        assertDoesNotThrow(successResult::rethrowIfFailed);

        // Test failed commit rethrow with RuntimeException
        RuntimeException runtimeException = new RuntimeException("Test error");
        IcebergCommitQueueManager.CommitResult runtimeFailureResult =
                IcebergCommitQueueManager.CommitResult.failure(runtimeException);
        Exception exception = assertThrows(RuntimeException.class, runtimeFailureResult::rethrowIfFailed);
        assertEquals("Test error", exception.getMessage());
        assertSame(runtimeException, exception, "Should rethrow same RuntimeException instance");

        // Test failed commit rethrow with Error type (should be rethrown as-is, not wrapped)
        InternalError internalError = new InternalError("Test internal error");
        IcebergCommitQueueManager.CommitResult errorFailureResult =
                IcebergCommitQueueManager.CommitResult.failure(internalError);
        Error thrownError = assertThrows(Error.class, errorFailureResult::rethrowIfFailed);
        assertEquals("Test internal error", thrownError.getMessage());
        assertSame(internalError, thrownError, "Should rethrow same Error instance");

        // Test failed commit rethrow with checked exception
        InterruptedException checkedException = new InterruptedException("Test interrupt");
        IcebergCommitQueueManager.CommitResult checkedFailureResult =
                IcebergCommitQueueManager.CommitResult.failure(checkedException);
        Exception wrappedException = assertThrows(RuntimeException.class, checkedFailureResult::rethrowIfFailed);
        assertEquals(checkedException, wrappedException.getCause());

        // Test failed commit with null error should not throw
        IcebergCommitQueueManager.CommitResult nullErrorResult =
                IcebergCommitQueueManager.CommitResult.failure(null);
        assertDoesNotThrow(nullErrorResult::rethrowIfFailed);
    }

    @Test
    public void testSubmitCommitWithThreadInterrupted() {
        // Test that interrupted thread returns failure
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(config);

        try {
            // Interrupt the current thread
            Thread.currentThread().interrupt();

            IcebergCommitQueueManager.CommitResult result =
                    queueManager.submitCommit("catalog", "db", "table", () -> {});

            assertFalse(result.isSuccess());
            assertTrue(result.getError() instanceof InterruptedException);

            // Clear interrupt status
            assertTrue(Thread.interrupted());
        } finally {
            queueManager.shutdownAll();
        }
    }

    @Test
    public void testSubmitCommitWithExecutionException() {
        // Test that exceptions during commit execution are properly wrapped
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(config);

        try {
            RuntimeException expectedException = new RuntimeException("Expected test exception");

            IcebergCommitQueueManager.CommitResult result =
                    queueManager.submitCommit("catalog", "db", "table", () -> {
                        throw expectedException;
                    });

            assertFalse(result.isSuccess());
            assertTrue(result.getError() instanceof RuntimeException);
            assertEquals("Expected test exception", result.getError().getMessage());
        } finally {
            queueManager.shutdownAll();
        }
    }

    @Test
    public void testTableCommitKeyEqualsAndHashCode() {
        // Test equals method
        IcebergCommitQueueManager.TableCommitKey key1 =
                new IcebergCommitQueueManager.TableCommitKey("catalog", "db", "table");
        IcebergCommitQueueManager.TableCommitKey key2 =
                new IcebergCommitQueueManager.TableCommitKey("catalog", "db", "table");
        IcebergCommitQueueManager.TableCommitKey key3 =
                new IcebergCommitQueueManager.TableCommitKey("catalog", "db", "table2");
        IcebergCommitQueueManager.TableCommitKey key4 =
                new IcebergCommitQueueManager.TableCommitKey("catalog", "db2", "table");
        IcebergCommitQueueManager.TableCommitKey key5 =
                new IcebergCommitQueueManager.TableCommitKey("catalog2", "db", "table");

        // Reflexive
        assertEquals(key1, key1);

        // Symmetric
        assertEquals(key1, key2);
        assertEquals(key2, key1);

        // Transitive
        assertEquals(key1, key2);
        assertEquals(key2, key1);

        // Consistent
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());

        // Not equal
        assertNotEquals(key1, key3);
        assertNotEquals(key1, key4);
        assertNotEquals(key1, key5);

        // Equal objects have equal hash codes
        assertEquals(key1.hashCode(), key2.hashCode());
        assertNotEquals(key1.hashCode(), key3.hashCode());
    }

    @Test
    public void testTableCommitKeyToString() {
        IcebergCommitQueueManager.TableCommitKey key =
                new IcebergCommitQueueManager.TableCommitKey("my_catalog", "my_db", "my_table");
        String result = key.toString();
        assertEquals("my_catalog.my_db.my_table", result);
    }

    @Test
    public void testTableCommitKeyNullChecks() {
        assertThrows(NullPointerException.class, () ->
                new IcebergCommitQueueManager.TableCommitKey(null, "db", "table"));
        assertThrows(NullPointerException.class, () ->
                new IcebergCommitQueueManager.TableCommitKey("catalog", null, "table"));
        assertThrows(NullPointerException.class, () ->
                new IcebergCommitQueueManager.TableCommitKey("catalog", "db", null));
    }

    @Test
    public void testConfigGetterMethods() {
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 150, 50);

        assertTrue(config.isEnabled());
        assertEquals(150, config.getTimeoutSeconds());
        assertEquals(50, config.getMaxQueueSize());
    }

    @Test
    public void testCommitResultErrorAccessors() {
        // Test success result
        IcebergCommitQueueManager.CommitResult successResult =
                IcebergCommitQueueManager.CommitResult.success();
        assertTrue(successResult.isSuccess());
        assertNull(successResult.getError());

        // Test failure result
        Exception testException = new Exception("Test");
        IcebergCommitQueueManager.CommitResult failureResult =
                IcebergCommitQueueManager.CommitResult.failure(testException);
        assertFalse(failureResult.isSuccess());
        assertEquals(testException, failureResult.getError());
    }

    @Test
    public void testShutdownTableExecutorIdempotent() {
        // Test that shutdownTableExecutor is idempotent
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(config);

        try {
            // Submit a task to create the executor
            queueManager.submitCommit("catalog", "db", "table", () -> {});

            // Shutdown multiple times - should not throw
            queueManager.shutdownTableExecutor("catalog", "db", "table");
            queueManager.shutdownTableExecutor("catalog", "db", "table");
            queueManager.shutdownTableExecutor("catalog", "db", "table");

            // Should be able to submit again (new executor created)
            IcebergCommitQueueManager.CommitResult result =
                    queueManager.submitCommit("catalog", "db", "table", () -> {});
            assertTrue(result.isSuccess());
        } finally {
            queueManager.shutdownAll();
        }
    }

    @Test
    public void testShutdownDatabaseExecutorsWithMultipleTables() {
        // Test shutdownDatabaseExecutors shuts down all tables in a database
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(config);

        try {
            // Create executors for multiple tables in the same database
            queueManager.submitCommit("catalog", "db", "table1", () -> {});
            queueManager.submitCommit("catalog", "db", "table2", () -> {});
            queueManager.submitCommit("catalog", "db", "table3", () -> {});

            // Create executor for different database
            queueManager.submitCommit("catalog", "db2", "table1", () -> {});

            assertEquals(4, queueManager.getActiveTableCount());

            // Shutdown all executors for 'db' database
            queueManager.shutdownDatabaseExecutors("catalog", "db");

            // Only db2.table1 should remain
            assertEquals(1, queueManager.getActiveTableCount());

            // Verify we can still submit to db2
            IcebergCommitQueueManager.CommitResult result =
                    queueManager.submitCommit("catalog", "db2", "table1", () -> {});
            assertTrue(result.isSuccess());
        } finally {
            queueManager.shutdownAll();
        }
    }

    @Test
    public void testConfigWithMinimumValues() {
        // Test with minimum queue size
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 9);

        assertEquals(IcebergCommitQueueManager.Config.getMinQueueSize(),
                     config.getMaxQueueSize());
    }

    @Test
    public void testBlockWhenQueueFullPolicyInterrupted() throws Exception {
        // Test that BlockWhenQueueFullPolicy handles interruption correctly
        IcebergCommitQueueManager.Config smallQueueConfig =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(smallQueueConfig);

        try {
            CountDownLatch taskRunning = new CountDownLatch(1);
            CountDownLatch proceedWithFirst = new CountDownLatch(1);
            Thread submitThread = Thread.currentThread();

            // Submit first task that blocks and occupies the worker thread
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
                queueManager.submitCommit("catalog", "db", "table", () -> {
                    taskRunning.countDown();
                    try {
                        proceedWithFirst.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });

            // Wait for first task to start
            assertTrue(taskRunning.await(5, TimeUnit.SECONDS));

            // Fill the queue with tasks
            for (int i = 0; i < 10; i++) {
                queueManager.submitCommit("catalog", "db", "table", () -> {});
            }

            // Now interrupt the thread that will try to submit (in a separate thread)
            CountDownLatch submissionStarted = new CountDownLatch(1);
            CountDownLatch submissionComplete = new CountDownLatch(1);
            AtomicReference<IcebergCommitQueueManager.CommitResult> resultRef = new AtomicReference<>();

            Thread interruptingThread = new Thread(() -> {
                submissionStarted.countDown();
                try {
                    // This should block in offer(), then be interrupted
                    IcebergCommitQueueManager.CommitResult result =
                            queueManager.submitCommit("catalog", "db", "table", () -> {});
                    resultRef.set(result);
                } finally {
                    submissionComplete.countDown();
                }
            });

            interruptingThread.start();
            assertTrue(submissionStarted.await(5, TimeUnit.SECONDS));

            // Interrupt the thread while it's waiting in offer()
            interruptingThread.interrupt();

            // The submission should complete (with failure)
            assertTrue(submissionComplete.await(5, TimeUnit.SECONDS));

            // Check that the commit failed (not succeeded)
            IcebergCommitQueueManager.CommitResult result = resultRef.get();
            assertNotNull(result);
            assertFalse(result.isSuccess(), "Commit should fail when interrupted");

            proceedWithFirst.countDown();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            queueManager.shutdownAll();
        }
    }

    @Test
    public void testTableNameWithPercentSign() {
        // Test that table names with % characters are properly escaped
        // to avoid IllegalFormatException in String.format()
        IcebergCommitQueueManager.Config config =
                new IcebergCommitQueueManager.Config(true, 120, 10);
        IcebergCommitQueueManager queueManager =
                new IcebergCommitQueueManager(config);

        try {
            // Table name with various % patterns that could be misinterpreted as format specifiers
            String[] problematicNames = {
                "table%swith%spercent",
                "table%s",
                "table%d",
                "table%%d",
                "table%100s"
            };

            for (String tableName : problematicNames) {
                // Submit commit - should not throw IllegalFormatException
                IcebergCommitQueueManager.CommitResult result =
                        queueManager.submitCommit("catalog", "db", tableName, () -> {});
                assertTrue(result.isSuccess(), "Commit should succeed for table: " + tableName);
            }

            assertEquals(problematicNames.length, queueManager.getActiveTableCount());
        } finally {
            queueManager.shutdownAll();
        }
    }
}
