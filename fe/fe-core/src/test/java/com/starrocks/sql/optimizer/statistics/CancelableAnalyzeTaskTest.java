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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.CancelableAnalyzeTask;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class CancelableAnalyzeTaskTest {

    private CancelableAnalyzeTask task;
    private AtomicBoolean taskExecuted;
    private AtomicInteger executionCount;
    private AnalyzeStatus analyzeStatus;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert;
        starRocksAssert = new StarRocksAssert();
        String createTblStmtStr = "create table db.tbl(kk1 int, kk2 varchar(32), kk3 int, kk4 int) "
                + "AGGREGATE KEY(kk1, kk2,kk3,kk4) distributed by hash(kk1) buckets 3 properties('replication_num' = "
                + "'1');";
        starRocksAssert.withDatabase("db").useDatabase("db");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @BeforeEach
    void setUp() {
        taskExecuted = new AtomicBoolean(false);
        executionCount = new AtomicInteger(0);

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "tbl");

        analyzeStatus = new NativeAnalyzeStatus(
                -1, // id
                testDb.getId(), // dbId
                table.getId(), // tableId
                Lists.newArrayList(), // columns
                StatsConstants.AnalyzeType.FULL, // type
                StatsConstants.ScheduleType.ONCE, // scheduleType
                Maps.newHashMap(), // properties
                LocalDateTime.now() // startTime
        );
    }

    @Test
    void shouldCreateTaskWithCorrectInitialState() {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);
        assertAll("Initial state",
                () -> assertFalse(task.isDone(), "Task should not be done initially"),
                () -> assertFalse(task.isCancelled(), "Task should not be cancelled initially")
        );
    }

    @Test
    void shouldThrowExceptionWhenCreatedWithNullParameters() {
        assertAll("Null parameter validation",
                () -> assertThrows(NullPointerException.class,
                        () -> new CancelableAnalyzeTask(null, analyzeStatus)),
                () -> assertThrows(NullPointerException.class,
                        () -> new CancelableAnalyzeTask(createSimpleTask(), null))
        );
    }


    @Test
    void shouldExecuteTaskSuccessfully() throws Exception {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);

        executeTaskInThread();

        assertAll("Successful execution",
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertFalse(task.isCancelled(), "Task should not be cancelled"),
                () -> assertTrue(taskExecuted.get(), "Original task should have executed"),
                () -> assertNull(task.get(), "get() should return null for successful execution"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FINISH, analyzeStatus.getStatus(),
                        "Status should be FINISH")
        );
    }

    @Test
    void shouldHandleTaskFailureCorrectly() throws Exception {
        task = new CancelableAnalyzeTask(createFailingTask(), analyzeStatus);

        executeTaskInThread();

        assertAll("Failed execution",
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertFalse(task.isCancelled(), "Task should not be cancelled"),
                () -> assertTrue(taskExecuted.get(), "Original task should have executed"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should be FAILED")
        );

        ExecutionException exception = assertThrows(ExecutionException.class,
                () -> task.get(), "get() should throw ExecutionException");

        assertAll("Exception details",
                () -> assertInstanceOf(RuntimeException.class, exception.getCause()),
                () -> assertEquals("Task failed intentionally", exception.getCause().getMessage())
        );
    }

    @Test
    void shouldHandleThreadInterruptionDuringExecution() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        task = new CancelableAnalyzeTask(
                createInterruptibleTask(taskStarted), analyzeStatus);

        Thread taskThread = new Thread(task);
        taskThread.start();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

        taskThread.interrupt();
        taskThread.join(1000);

        assertAll("Interrupted execution",
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertTrue(taskExecuted.get(), "Original task should have started"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should be FAILED")
        );

        ExecutionException exception = assertThrows(ExecutionException.class, () -> task.get());
        assertInstanceOf(RuntimeException.class, exception.getCause());
    }

    @Test
    void shouldCancelTaskBeforeExecution() {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);

        boolean cancelled = task.cancel(false);

        assertAll("Cancel before execution",
                () -> assertTrue(cancelled, "cancel() should return true"),
                () -> assertTrue(task.isCancelled(), "Task should be cancelled"),
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertFalse(taskExecuted.get(), "Original task should not execute"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should be FAILED")
        );

        task.run();
        assertFalse(taskExecuted.get(), "Original task should still not execute");
    }

    @Test
    void shouldCancelTaskDuringExecutionWithInterrupt() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch allowTaskToContinue = new CountDownLatch(1);
        task = new CancelableAnalyzeTask(
                createLongRunningTask(taskStarted, allowTaskToContinue), analyzeStatus);

        Thread taskThread = new Thread(task);
        taskThread.start();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

        boolean cancelled = task.cancel(true);
        allowTaskToContinue.countDown();
        taskThread.join(1000);

        assertAll("Cancel during execution with interrupt",
                () -> assertTrue(cancelled, "cancel() should return true"),
                () -> assertTrue(task.isCancelled(), "Task should be cancelled"),
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertTrue(taskExecuted.get(), "Original task should have started"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should be FAILED")
        );
    }

    @Test
    void shouldCancelTaskDuringExecutionWithoutInterrupt() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch allowTaskToContinue = new CountDownLatch(1);
        task = new CancelableAnalyzeTask(
                createLongRunningTask(taskStarted, allowTaskToContinue), analyzeStatus);

        Thread taskThread = new Thread(task);
        taskThread.start();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

        boolean cancelled = task.cancel(false);
        allowTaskToContinue.countDown();
        taskThread.join(6000);

        assertAll("Cancel during execution without interrupt",
                () -> assertTrue(cancelled, "cancel() should return true"),
                () -> assertTrue(task.isCancelled(), "Task should be cancelled"),
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should be FAILED")
        );
    }

    @Test
    void shouldNotCancelCompletedTask() throws Exception {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);
        executeTaskInThread();

        boolean cancelled = task.cancel(true);

        assertAll("Cancel after completion",
                () -> assertFalse(cancelled, "cancel() should return false for completed task"),
                () -> assertFalse(task.isCancelled(), "Task should not be cancelled"),
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertTrue(taskExecuted.get(), "Original task should have executed"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FINISH, analyzeStatus.getStatus(),
                        "Status should be FINISH")
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleConcurrentCancellationAttempts(boolean mayInterrupt) throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        task = new CancelableAnalyzeTask(
                createLongRunningTask(taskStarted, new CountDownLatch(1)), analyzeStatus);

        Thread taskThread = new Thread(task);
        taskThread.start();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

        AtomicInteger successfulCancellations = new AtomicInteger(0);
        Thread[] cancelThreads = new Thread[5];

        for (int i = 0; i < cancelThreads.length; i++) {
            cancelThreads[i] = new Thread(() -> {
                if (task.cancel(mayInterrupt)) {
                    successfulCancellations.incrementAndGet();
                }
            });
        }

        for (Thread thread : cancelThreads) {
            thread.start();
        }

        for (Thread thread : cancelThreads) {
            thread.join();
        }

        taskThread.join(1000);

        assertAll("Concurrent cancellation",
                () -> assertTrue(task.isCancelled(), "Task should be cancelled"),
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertEquals(1, successfulCancellations.get(),
                        "Only one cancellation should succeed")
        );
    }

    @Test
    void shouldUseConvenienceCancelMethod() {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);

        task.cancel();

        assertAll("Convenience cancel method",
                () -> assertTrue(task.isCancelled(), "Task should be cancelled"),
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should be FAILED")
        );
    }

    @Test
    void shouldThrowCancellationExceptionWhenGettingCancelledTaskResult() {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);
        task.cancel(false);

        assertAll("Get cancelled task result",
                () -> assertThrows(CancellationException.class, () -> task.get(),
                        "get() should throw CancellationException"),
                () -> assertThrows(CancellationException.class,
                        () -> task.get(100, TimeUnit.MILLISECONDS),
                        "get(timeout) should throw CancellationException")
        );
    }

    @Test
    @Timeout(5)
    void shouldTimeoutWhenGettingLongRunningTaskResult() throws Exception {
        CountDownLatch neverComplete = new CountDownLatch(1);
        task = new CancelableAnalyzeTask(
                createLongRunningTask(null, neverComplete), analyzeStatus);

        Thread taskThread = new Thread(task);
        taskThread.start();

        try {
            assertThrows(TimeoutException.class,
                    () -> task.get(100, TimeUnit.MILLISECONDS),
                    "get() should timeout");
        } finally {
            task.cancel(true);
            taskThread.interrupt();
            taskThread.join(1000);
        }
    }

    @Test
    void shouldReturnImmediatelyWhenGettingCompletedTaskResultWithTimeout() throws Exception {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);
        executeTaskInThread();

        assertDoesNotThrow(() -> {
            Void result = task.get(100, TimeUnit.MILLISECONDS);
            assertNull(result, "Result should be null");
        }, "get(timeout) should not throw for completed task");
    }

    @Test
    void shouldHandleMultipleGetCalls() throws Exception {
        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);
        executeTaskInThread();

        assertAll("Multiple get calls",
                () -> assertNull(task.get(), "First get() should return null"),
                () -> assertNull(task.get(), "Second get() should return null"),
                () -> assertNull(task.get(1, TimeUnit.SECONDS), "get(timeout) should return null")
        );
    }

    @Test
    void shouldUpdateAnalyzeStatusProgressDuringExecution() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch allowTaskToContinue = new CountDownLatch(1);

        Runnable progressTask = () -> {
            taskExecuted.set(true);
            taskStarted.countDown();
            try {
                analyzeStatus.setProgress(25);
                Thread.sleep(100);
                analyzeStatus.setProgress(50);
                Thread.sleep(100);
                analyzeStatus.setProgress(75);
                allowTaskToContinue.await();
                analyzeStatus.setProgress(100);
                // Simulate StatisticsExecutor setting status to FINISH
                analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task interrupted", e);
            }
        };

        task = new CancelableAnalyzeTask(progressTask, analyzeStatus);

        Thread taskThread = new Thread(task);
        taskThread.start();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

        Thread.sleep(50);
        assertTrue(analyzeStatus.getProgress() >= 25, "Progress should be updated");

        allowTaskToContinue.countDown();
        taskThread.join(1000);

        assertAll("Progress tracking",
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertEquals(100, analyzeStatus.getProgress(), "Progress should be 100%"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FINISH, analyzeStatus.getStatus(),
                        "Status should be FINISH")
        );
    }

    @Test
    void shouldHandleAnalyzeStatusWithDifferentScheduleTypes() throws MetaNotFoundException, InterruptedException {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "t0");

        AnalyzeStatus scheduleStatus = new NativeAnalyzeStatus(
                -1, testDb.getId(), table.getId(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE,
                Maps.newHashMap(), LocalDateTime.now()
        );

        // Create a task that sets FINISH status to simulate StatisticsExecutor behavior
        Runnable taskWithStatusUpdate = () -> {
            taskExecuted.set(true);
            executionCount.incrementAndGet();
            scheduleStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        };
        
        task = new CancelableAnalyzeTask(taskWithStatusUpdate, scheduleStatus);
        executeTaskInThread();

        assertEquals(StatsConstants.ScheduleStatus.FINISH, scheduleStatus.getStatus());
        assertEquals(StatsConstants.ScheduleType.SCHEDULE, scheduleStatus.getScheduleType());
    }

    @Test
    void shouldPreserveAnalyzeStatusProperties() throws Exception {
        analyzeStatus.getProperties().put("test_property", "test_value");
        analyzeStatus.getProperties().put("sample_ratio", "0.1");

        task = new CancelableAnalyzeTask(createSimpleTask(), analyzeStatus);

        executeTaskInThread();

        assertAll("Properties preservation",
                () -> assertEquals("test_value", analyzeStatus.getProperties().get("test_property")),
                () -> assertEquals("0.1", analyzeStatus.getProperties().get("sample_ratio")),
                () -> assertEquals(StatsConstants.ScheduleStatus.FINISH, analyzeStatus.getStatus())
        );
    }

    @Test
    void shouldNotOverrideFailedStatusSetInternallyByTask() throws Exception {
        task = new CancelableAnalyzeTask(createTaskThatSetsFailedStatus(), analyzeStatus);

        executeTaskInThread();

        assertAll("Internal FAILED status preservation",
                () -> assertTrue(task.isDone(), "Task should be done"),
                () -> assertFalse(task.isCancelled(), "Task should not be cancelled"),
                () -> assertTrue(taskExecuted.get(), "Original task should have executed"),
                () -> assertEquals(StatsConstants.ScheduleStatus.FAILED, analyzeStatus.getStatus(),
                        "Status should remain FAILED set by StatisticsExecutor")
        );
    }

    private Runnable createSimpleTask() {
        return () -> {
            taskExecuted.set(true);
            executionCount.incrementAndGet();
            try {
                Thread.sleep(50);
                // Simulate StatisticsExecutor setting status to FINISH
                analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task interrupted", e);
            }
        };
    }

    private Runnable createLongRunningTask(CountDownLatch startLatch, CountDownLatch endLatch) {
        return () -> {
            taskExecuted.set(true);
            executionCount.incrementAndGet();
            if (startLatch != null) {
                startLatch.countDown();
            }
            try {
                if (endLatch != null) {
                    endLatch.await();
                }
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task interrupted", e);
            }
        };
    }

    private Runnable createInterruptibleTask(CountDownLatch startLatch) {
        return () -> {
            taskExecuted.set(true);
            executionCount.incrementAndGet();
            if (startLatch != null) {
                startLatch.countDown();
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task was interrupted", e);
            }
        };
    }

    private Runnable createFailingTask() {
        return () -> {
            taskExecuted.set(true);
            executionCount.incrementAndGet();
            throw new RuntimeException("Task failed intentionally");
        };
    }

    private Runnable createTaskThatSetsFailedStatus() {
        return () -> {
            taskExecuted.set(true);
            executionCount.incrementAndGet();
            // Simulate StatisticsExecutor setting FAILED status internally
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
        };
    }

    private void executeTaskInThread() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(task);
            try {
                future.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                throw new InterruptedException("Task execution timed out");
            } catch (ExecutionException e) {
            }
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }
}