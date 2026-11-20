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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.AlterTaskInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A refresh that breaks an incremental MV inactivates it and stops scheduling its task, as every other inactivation
 * does, but lets the failing run report its own error.
 */
public class IvmBreakingInactivationTaskTest extends MVIVMIcebergTestBase {
    // Without START the first scheduled run fires within minutes of CREATE and races the case's own runs.
    private static final String SCHEDULED = "ASYNC START('2122-12-31') EVERY(INTERVAL 1 DAY)";
    private static final AtomicInteger VIEWS = new AtomicInteger();

    enum Breakage {
        NON_APPEND_ONLY_DELTA("non-append-only change on base table",
                MaterializedViewExceptions.INACTIVE_REASON_FOR_INCREMENTAL_BREAKING),
        SNAPSHOT_ANCESTRY_BROKEN("snapshot ancestry broken for base table",
                MaterializedViewExceptions.INACTIVE_REASON_FOR_INCREMENTAL_BREAKING),
        MV_SCHEMA_MISMATCH(MaterializedViewExceptions.MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER,
                MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH);

        final String failure;
        final String inactiveReasonPrefix;

        Breakage(String failure, String inactiveReasonPrefix) {
            this.failure = failure;
            this.inactiveReasonPrefix = inactiveReasonPrefix;
        }
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
    }

    static Stream<Arguments> breakages() {
        return Stream.of(
                Arguments.of(SCHEDULED, Breakage.NON_APPEND_ONLY_DELTA),
                Arguments.of(SCHEDULED, Breakage.SNAPSHOT_ANCESTRY_BROKEN),
                Arguments.of(SCHEDULED, Breakage.MV_SCHEMA_MISMATCH),
                Arguments.of("MANUAL", Breakage.NON_APPEND_ONLY_DELTA));
    }

    @ParameterizedTest
    @MethodSource("breakages")
    public void testABreakingRefreshSuspendsTheTaskWithoutKillingTheRun(String refresh, Breakage breakage)
            throws Exception {
        MaterializedView mv = incrementalView(refresh);
        Task task = taskOf(mv);
        List<AlterTaskInfo> taskJournal = captureTaskJournal();
        breakIncrementalRefresh(breakage);

        TaskRun run = TaskRunBuilder.newBuilder(task).properties(Map.of()).build();
        // registered as the scheduler does, so that a kill of the task's runs would reach this one
        taskManager().getTaskRunScheduler().addRunningTaskRun(run);
        Exception e;
        try {
            e = Assertions.assertThrows(Exception.class, () -> executeBatch(run));
        } finally {
            taskManager().getTaskRunScheduler().removeRunningTask(task.getId());
        }
        Assertions.assertTrue(MaterializedViewExceptions.isIncrementalBreakingFailure(e), e.getMessage());
        Assertions.assertTrue(ExceptionUtils.getThrowableList(e).stream().anyMatch(cause ->
                StringUtils.contains(cause.getMessage(), breakage.failure)), ExceptionUtils.getStackTrace(e));
        Assertions.assertFalse(run.isKilled(), "the failing run must report its own error");
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().startsWith(breakage.inactiveReasonPrefix),
                mv.getInactiveReason());

        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertNull(scheduleOf(task), "an inactive view must not keep its schedule");
        Assertions.assertTrue(taskJournal.stream().anyMatch(info -> info.getName().equals(task.getName())
                && info.getState() == Constants.TaskState.PAUSE), "followers have to see the task paused");

        Exception plain = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(plain.getMessage().contains("is not active"), plain.getMessage());
    }

    @Test
    public void testResumingABrokenViewSchedulesItOnce() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        Task task = taskOf(mv);
        ScheduledFuture<?> beforeBreak = scheduleOf(task);
        Assertions.assertNotNull(beforeBreak);
        breakIncrementalRefresh(Breakage.NON_APPEND_ONLY_DELTA);
        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));

        // what ALTER ... ACTIVE does to the task of the view it activates
        taskManager().resumeTask(task, false);

        Assertions.assertEquals(Constants.TaskState.ACTIVE, task.getState());
        Assertions.assertTrue(beforeBreak.isCancelled(), "the schedule from before the break would keep firing");
        Assertions.assertFalse(scheduleOf(task).isCancelled());
    }

    @Test
    public void testABreakingRefreshDropsTheRunsQueuedBehindIt() throws Exception {
        MaterializedView mv = incrementalView("MANUAL");
        Task task = taskOf(mv);
        breakIncrementalRefresh(Breakage.NON_APPEND_ONLY_DELTA);
        TaskRunScheduler scheduler = taskManager().getTaskRunScheduler();
        TaskRun run = TaskRunBuilder.newBuilder(task).properties(Map.of()).build();
        // running first, so that the dispatcher cannot start a queued one meanwhile
        scheduler.addRunningTaskRun(run);
        long stillQueued;
        try {
            for (int i = 0; i < 3; i++) {
                TaskRun queued = TaskRunBuilder.newBuilder(task).properties(Map.of()).build();
                queued.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis() + i);
                Assertions.assertTrue(scheduler.addPendingTaskRun(queued));
            }
            Assertions.assertThrows(Exception.class, () -> executeBatch(run));
            stillQueued = scheduler.getTaskIdPendingTaskRunCount(task.getId());
        } finally {
            scheduler.removeRunningTask(task.getId());
            scheduler.removePendingTask(task);
        }
        Assertions.assertFalse(run.isKilled());
        Assertions.assertEquals(0, stillQueued, "each would fail as not active and count as a consecutive failure");
    }

    @Test
    public void testAnActivationCannotLandBetweenInactivatingAndSuspending() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        breakIncrementalRefresh(Breakage.NON_APPEND_ONLY_DELTA);
        AtomicBoolean suspended = new AtomicBoolean();
        AtomicBoolean activationGotTheLock = new AtomicBoolean();
        new MockUp<TaskManager>() {
            @Mock
            public void suspendTaskWithoutKilling(Invocation invocation, Task task) throws InterruptedException {
                suspended.set(true);
                // the lock ALTER ... ACTIVE takes
                Thread activation = new Thread(() -> {
                    Locker locker = new Locker();
                    if (locker.tryLockTableWithIntensiveDbLock(mv.getDbId(), mv.getId(), LockType.WRITE,
                            100, TimeUnit.MILLISECONDS)) {
                        activationGotTheLock.set(true);
                        locker.unLockTableWithIntensiveDbLock(mv.getDbId(), mv.getId(), LockType.WRITE);
                    }
                });
                activation.start();
                activation.join();
                invocation.proceed();
            }
        };

        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(suspended.get());
        Assertions.assertFalse(activationGotTheLock.get(), "the suspension would pause the view it activated");
    }

    /** An incremental view over an Iceberg table whose next refresh runs the incremental processor. */
    private static MaterializedView incrementalView(String refresh) throws Exception {
        String name = "mv_breaking_task_" + VIEWS.incrementAndGet();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " REFRESH DEFERRED " + refresh +
                " PROPERTIES ('refresh_mode' = 'INCREMENTAL') " +
                "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", name);
        seedTvrBaselineAtVersionZero(mv);
        return mv;
    }

    private void breakIncrementalRefresh(Breakage breakage) {
        if (breakage == Breakage.MV_SCHEMA_MISMATCH) {
            new MockUp<MVIVMRefreshProcessor>() {
                @Mock
                public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                    throw new SemanticException(MaterializedViewExceptions.inactiveReasonForColumnNotCompatible(
                            "`id` int", "`id` bigint"));
                }
            };
            return;
        }
        advanceTableVersionTo(2);
        if (breakage == Breakage.NON_APPEND_ONLY_DELTA) {
            mockListTableDeltaTraits();
        } else {
            mockListTableDeltaTraitsThrowsConnector(
                    "Starting snapshot (exclusive) 0 is not a parent ancestor of end snapshot 2");
        }
    }

    private static TaskRun runFirstBatch(MaterializedView mv) throws Exception {
        Task task = taskOf(mv);
        task.getProperties().put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(Map.of()).build();
        executeBatch(taskRun);
        return taskRun;
    }

    private static Constants.TaskRunState executeBatch(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        return taskRun.executeTaskRun();
    }

    private static List<AlterTaskInfo> captureTaskJournal() {
        List<AlterTaskInfo> journal = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logAlterTask(Invocation invocation, AlterTaskInfo info, WALApplier applier) {
                journal.add(info);
                invocation.proceed();
            }
        };
        return journal;
    }

    private static TaskManager taskManager() {
        return GlobalStateMgr.getCurrentState().getTaskManager();
    }

    private static Task taskOf(MaterializedView mv) {
        return taskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
    }

    private static ScheduledFuture<?> scheduleOf(Task task) {
        return taskManager().getPeriodFutureMap().get(task.getId());
    }
}
