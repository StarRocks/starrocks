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

import com.starrocks.alter.AlterJobV2;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterTaskInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.MVRefreshSchemaChecker;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.DmlException;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
    private static final String DB = "test";
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
        starRocksAssert.useDatabase(DB);
    }

    @BeforeEach
    public void restoreConnectContext() {
        // an activation attempt on this thread, such as a run's prepare makes, removes the thread's context
        connectContext.setThreadLocalInfo();
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
        Task ownTask = taskOf(mv);
        breakIncrementalRefresh(Breakage.NON_APPEND_ONLY_DELTA);
        AtomicBoolean suspended = new AtomicBoolean();
        AtomicBoolean activationGotTheLock = new AtomicBoolean();
        new MockUp<TaskManager>() {
            @Mock
            public void suspendTaskWithoutKilling(Invocation invocation, Task task) throws InterruptedException {
                // a scheduled run of another test's view breaks on the same mock, under that view's lock
                if (task != ownTask) {
                    invocation.proceed();
                    return;
                }
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

    @Test
    public void testAViewActivatedAfterPartitionSyncFoundItInactiveIsNotBroken() throws Exception {
        MaterializedView mv = pctView();
        Task task = taskOf(mv);
        Constants.TaskState taskState = task.getState();
        // a base column change lands after the run's prepare, before its partition sync
        new MockUp<MVRefreshSchemaChecker>() {
            @Mock
            public void checkExternalBaseSchemaCompat(MaterializedView view) {
                view.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForColumnChanged(Set.of("v")));
            }
        };
        activateWhenTheFailureIsHandled(mv);

        Exception e = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active due to base table schema changed for columns: v"),
                e.getMessage());
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(taskState, task.getState());
    }

    @Test
    public void testAViewActivatedAfterPrepareFoundItInactiveIsNotBroken() throws Exception {
        MaterializedView mv = pctView();
        Task task = taskOf(mv);
        Constants.TaskState taskState = task.getState();
        mv.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForColumnChanged(Set.of("v")));
        // the run's own activation fails, as it does while the column is still missing
        new MockUp<MVActiveChecker>() {
            @Mock
            public void tryToActivate(MaterializedView view) {
            }
        };
        activateWhenTheFailureIsHandled(mv);

        Exception e = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active due to base table schema changed for columns: v"),
                e.getMessage());
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(taskState, task.getState());
    }

    @Test
    public void testAReloadChangesTheStatusEpochBeforeItEnds() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        long epochBefore = mv.getStatusSnapshot().epoch();
        CountDownLatch inWindow = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread reload = new Thread(mv::onReload);
        pauseReloadInItsWindow(reload, inWindow, release);
        reload.start();
        MaterializedView.StatusSnapshot inTheWindow;
        try {
            Assertions.assertTrue(inWindow.await(30, TimeUnit.SECONDS));
            inTheWindow = mv.getStatusSnapshot();
        } finally {
            release.countDown();
            reload.join();
        }
        Assertions.assertFalse(inTheWindow.active());
        Assertions.assertNotEquals(epochBefore, inTheWindow.epoch());
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
    }

    @Test
    public void testABreakAfterTheViewWasInactivatedAndActivatedAgainIsDropped() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        Task task = taskOf(mv);
        Constants.TaskState taskState = task.getState();
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                mv.setInactiveAndReason("inactivated while the refresh ran");
                mv.setActive();
                throw new SemanticException("Cannot incrementally refresh materialized view: "
                        + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
            }
        };

        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(taskState, task.getState());
    }

    @Test
    public void testAFailureHandledWhileTheViewReloadsLeavesItAndItsTaskAlone() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        Task task = taskOf(mv);
        Constants.TaskState taskState = task.getState();
        presetConsecutiveFailures(task, Config.max_task_consecutive_fail_count - 1);
        CountDownLatch inWindow = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread reload = new Thread(mv::onReload);
        pauseReloadInItsWindow(reload, inWindow, release);
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext)
                    throws InterruptedException {
                reload.start();
                Assertions.assertTrue(inWindow.await(30, TimeUnit.SECONDS));
                throw new SemanticException("Cannot incrementally refresh materialized view: "
                        + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
            }
        };

        try {
            Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        } finally {
            release.countDown();
            reload.join();
        }
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(taskState, task.getState());
        Assertions.assertEquals(Config.max_task_consecutive_fail_count, task.getConsecutiveFailCount());
    }

    @Test
    public void testAReloadBetweenTheTwoChecksOfPrepareLeavesTheViewAndItsTaskAlone() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        Task task = taskOf(mv);
        Constants.TaskState taskState = task.getState();
        presetConsecutiveFailures(task, Config.max_task_consecutive_fail_count - 1);
        CountDownLatch inWindow = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread reload = new Thread(mv::onReload);
        pauseReloadInItsWindow(reload, inWindow, release);
        AtomicBoolean started = new AtomicBoolean();
        // prepare asks for the metrics entity between its two activity checks
        new MockUp<MaterializedViewMetricsRegistry>() {
            @Mock
            public IMaterializedViewMetricsEntity getMetricsEntity(Invocation invocation, MvId mvId)
                    throws InterruptedException {
                if (started.compareAndSet(false, true)) {
                    reload.start();
                    Assertions.assertTrue(inWindow.await(30, TimeUnit.SECONDS));
                }
                return invocation.proceed();
            }
        };

        try {
            Exception e = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
            Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        } finally {
            release.countDown();
            reload.join();
        }
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(taskState, task.getState());
    }

    @Test
    public void testConsecutiveFailuresKeepTheReasonAnInactiveViewHas() throws Exception {
        OlapTable fact = createFact();
        MaterializedView mv = pctView(fact);
        Task task = taskOf(mv);
        // v comes back as a string: every activation attempt fails on the incompatible column and records that,
        // which changes the view's status epoch -- unlike a column that is simply missing
        alterTable(fact, "ALTER TABLE " + fact.getName() + " DROP COLUMN v");
        alterTable(fact, "ALTER TABLE " + fact.getName() + " ADD COLUMN v VARCHAR(16)");
        Assertions.assertFalse(mv.isActive());
        // a view inactivated in memory keeps its task scheduled; resuming it stands for that
        taskManager().resumeTask(task, false);

        for (int i = 0; i < Config.max_task_consecutive_fail_count; i++) {
            Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        }

        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertFalse(mv.isActive());
        String reason = mv.getInactiveReason();
        Assertions.assertFalse(reason.startsWith(MaterializedViewExceptions.INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES),
                reason);
        Assertions.assertFalse(MVActiveChecker.isNonAutoActivatableReason(reason), reason);

        alterTable(fact, "ALTER TABLE " + fact.getName() + " DROP COLUMN v");
        alterTable(fact, "ALTER TABLE " + fact.getName() + " ADD COLUMN v BIGINT");
        new MVActiveChecker().runForTest(true);
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, task.getState());
    }

    @Test
    public void testConsecutiveFailuresInactivateAnActiveView() throws Exception {
        MaterializedView mv = pctView();
        Task task = taskOf(mv);
        presetConsecutiveFailures(task, Config.max_task_consecutive_fail_count - 1);
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                throw new RuntimeException("transient failure (simulated)");
            }
        };

        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertEquals(Constants.TaskState.PAUSE, task.getState());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().startsWith(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES), mv.getInactiveReason());
    }

    @Test
    public void testARunThatInactivatesItsOwnViewAtTheThresholdLeavesTheTaskRunning() throws Exception {
        MaterializedView mv = pctView();
        Task task = taskOf(mv);
        Constants.TaskState taskState = task.getState();
        presetConsecutiveFailures(task, Config.max_task_consecutive_fail_count - 1);
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                // what MVRefreshProcessor does when a base table disappeared during the refresh
                mv.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForBaseTableNotExists("t"));
                throw new DmlException("base table t does not exist (simulated)");
            }
        };

        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertEquals(taskState, task.getState());
        Assertions.assertEquals(MaterializedViewExceptions.inactiveReasonForBaseTableNotExists("t"),
                mv.getInactiveReason());
    }

    @Test
    public void testConsecutiveFailuresBeforeTheRunResolvedItsView() throws Exception {
        MaterializedView active = pctView();
        MaterializedView inactive = pctView();
        String reason = MaterializedViewExceptions.inactiveReasonForBaseTableNotExists("t");
        inactive.setInactiveAndReason(reason);
        new MockUp<MVTaskRunProcessor>() {
            @Mock
            public TaskRunContext prepare(TaskRunContext context) {
                throw new DmlException("database of the view does not exist (simulated)");
            }
        };

        for (MaterializedView mv : List.of(active, inactive)) {
            presetConsecutiveFailures(taskOf(mv), Config.max_task_consecutive_fail_count - 1);
            Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
            Assertions.assertEquals(Constants.TaskState.PAUSE, taskOf(mv).getState(), mv.getName());
            Assertions.assertFalse(mv.isActive(), mv.getName());
        }
        Assertions.assertTrue(active.getInactiveReason().startsWith(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES), active.getInactiveReason());
        Assertions.assertEquals(reason, inactive.getInactiveReason());
    }

    @Test
    public void testABreakOfAViewDroppedMeanwhileJournalsNothingForIt() throws Exception {
        assertAFailureAfterTheViewWasDroppedJournalsNothing(0, new SemanticException(
                "Cannot incrementally refresh materialized view: " + MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER));
    }

    @Test
    public void testConsecutiveFailuresOfAViewDroppedMeanwhileJournalNothingForIt() throws Exception {
        assertAFailureAfterTheViewWasDroppedJournalsNothing(Config.max_task_consecutive_fail_count - 1,
                new DmlException("refresh failed (simulated)"));
    }

    @Test
    public void testConsecutiveFailuresOfARunThatFoundItsViewDroppedJournalNothing() throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        Task task = taskOf(mv);
        presetConsecutiveFailures(task, Config.max_task_consecutive_fail_count - 1);
        List<AlterTaskInfo> taskJournal = captureTaskJournal();
        DmlException failure = new DmlException("materialized view does not exist (simulated)");
        new MockUp<MVTaskRunProcessor>() {
            @Mock
            public TaskRunContext prepare(TaskRunContext context) throws Exception {
                starRocksAssert.dropMaterializedView(DB + "." + mv.getName());
                throw failure;
            }
        };

        Exception thrown = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertSame(failure, ExceptionUtils.getRootCause(thrown), String.valueOf(thrown));
        Assertions.assertTrue(taskJournal.stream().noneMatch(info -> info.getName().equals(task.getName())
                && info.getState() == Constants.TaskState.PAUSE), "a pause of a dropped task cannot be replayed");
    }

    private static void assertAFailureAfterTheViewWasDroppedJournalsNothing(int failuresBefore, RuntimeException failure)
            throws Exception {
        MaterializedView mv = incrementalView(SCHEDULED);
        presetConsecutiveFailures(taskOf(mv), failuresBefore);
        List<AlterMaterializedViewStatusLog> statusJournal = captureStatusJournal();
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) throws Exception {
                starRocksAssert.dropMaterializedView(DB + "." + mv.getName());
                throw failure;
            }
        };

        Exception thrown = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertSame(failure, ExceptionUtils.getRootCause(thrown), String.valueOf(thrown));
        Assertions.assertTrue(statusJournal.isEmpty(), statusJournal.toString());
    }

    private static List<AlterMaterializedViewStatusLog> captureStatusJournal() {
        List<AlterMaterializedViewStatusLog> journal = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logAlterMvStatus(Invocation invocation, AlterMaterializedViewStatusLog log, WALApplier applier) {
                journal.add(log);
                invocation.proceed();
            }
        };
        return journal;
    }

    private static OlapTable createFact() throws Exception {
        String name = "fact_breaking_task_" + VIEWS.incrementAndGet();
        starRocksAssert.withTable("CREATE TABLE " + name + " (k INT NOT NULL, v BIGINT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
    }

    private static MaterializedView pctView() throws Exception {
        return pctView(createFact());
    }

    private static MaterializedView pctView(OlapTable fact) throws Exception {
        String name = "mv_" + fact.getName();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " DISTRIBUTED BY HASH(k) BUCKETS 1 " +
                "REFRESH DEFERRED MANUAL AS SELECT k, v FROM " + fact.getName());
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT, mv.getCurrentRefreshMode());
        return mv;
    }

    /** An ALTER ... ACTIVE that lands between the run seeing the view inactive and its failure being handled. */
    private static void activateWhenTheFailureIsHandled(MaterializedView mv) {
        new MockUp<Task>() {
            @Mock
            public int incConsecutiveFailCount(Invocation invocation) {
                mv.setActive();
                return invocation.proceed();
            }
        };
    }

    /** Runs a schema change on {@code table} and waits for its jobs; jobs other tests left behind are not ours. */
    private static void alterTable(OlapTable table, String sql) throws Exception {
        starRocksAssert.alterTable(sql);
        for (AlterJobV2 job : GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values()) {
            if (job.getTableId() != table.getId()) {
                continue;
            }
            while (!job.getJobState().isFinalState()) {
                Thread.sleep(200);
            }
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        }
    }

    private static void presetConsecutiveFailures(Task task, int count) {
        task.resetConsecutiveFailCount();
        for (int i = 0; i < count; i++) {
            task.incConsecutiveFailCount();
        }
    }

    /** Holds {@code reloadThread} inside onReload, after the view went inactive and before its activation is derived. */
    private static void pauseReloadInItsWindow(Thread reloadThread, CountDownLatch inWindow, CountDownLatch release) {
        new MockUp<MaterializedView>() {
            @Mock
            public void changeReloadState(Invocation invocation, int state) throws InterruptedException {
                // 0 is RELOAD_STATE_ING, set right after onReload marks the view inactive
                if (state == 0 && Thread.currentThread() == reloadThread) {
                    inWindow.countDown();
                    release.await();
                }
                invocation.proceed();
            }
        };
    }

    /** An incremental view over an Iceberg table whose next refresh runs the incremental processor. */
    private static MaterializedView incrementalView(String refresh) throws Exception {
        String name = "mv_breaking_task_" + VIEWS.incrementAndGet();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " REFRESH DEFERRED " + refresh +
                " PROPERTIES ('refresh_mode' = 'INCREMENTAL') " +
                "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB, name);
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
