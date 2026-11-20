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

import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.AlterMVJobExecutor;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * REFRESH ... FORCE activates an incremental MV whose incremental chain broke, since the rebuild never reads the
 * baseline it broke on, and puts the inactive reason back when the rebuild fails before it writes.
 */
public class IvmForceActivateTest extends IvmIcebergRefreshJobTestBase {
    private static final AtomicInteger VIEWS = new AtomicInteger();

    @Test
    public void testForceActivatesABrokenViewAndRebuildsIt() throws Exception {
        MaterializedView mv = brokenView(MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);

        TaskRun batch = runForceFirstBatch(mv);
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, taskOf(mv).getState());
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        runRemainingBatches(batch);
        Assertions.assertEquals(currentSnapshotId(FACT), baselineSnapshotId(mv, FACT));

        loadFact();
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(runFirstBatch(mv)));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
    }

    @Test
    public void testPlainRefreshOfABrokenViewFailsAndNamesForce() throws Exception {
        MaterializedView mv = brokenView(MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        String reason = mv.getInactiveReason();

        Exception e = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("REFRESH MATERIALIZED VIEW `test`.`" + mv.getName() + "` FORCE"),
                e.getMessage());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
    }

    @Test
    public void testExplainOfAForceLeavesABrokenViewInactive() throws Exception {
        MaterializedView mv = brokenView(MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        String reason = mv.getInactiveReason();
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        StatementBase explain = MVTestBase.getAnalyzedPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName() + " FORCE", starRocksAssert.getCtx());
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGH.value(), false,
                new HashMap<>(Map.of(TaskRun.FORCE, "true")));

        Assertions.assertThrows(Exception.class, () -> taskManager.getMVRefreshExplain(taskOf(mv), option, explain));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
    }

    @ParameterizedTest
    @ValueSource(strings = {MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH,
            AlterJobMgr.MANUAL_INACTIVE_MV_REASON,
            MVActiveChecker.MV_BACKUP_INACTIVE_REASON,
            MaterializedViewExceptions.INACTIVE_REASON_FOR_BASE_TABLE_OPTIMIZED,
            MaterializedViewExceptions.INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES})
    public void testForceLeavesInactiveAViewAPlainRefreshCannotActivate(String reason) throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_force_activate_" + VIEWS.incrementAndGet(), "incremental");
        runJob(mv);
        AlterMVJobExecutor.inactiveMvAndLog(mv, reason);

        Exception e = Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
    }

    @Test
    public void testForceThatFailsBeforeItWritesPutsTheViewBackInactiveAndPaused() throws Exception {
        MaterializedView mv = brokenView(MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        String reason = mv.getInactiveReason();
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline(mv));
        failPctPlanning("pct planning failed (simulated)");

        Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
        Assertions.assertEquals(baseline, tvrBaseline(mv));
        Assertions.assertEquals(Constants.TaskState.PAUSE, taskOf(mv).getState(),
                "the activation resumed the task, and a scheduled view would fail every period");
    }

    @Test
    public void testForceThatFailsAfterDroppingTheBaselineLeavesTheViewToBeRebuilt() throws Exception {
        MaterializedView mv = brokenView(MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        onWrite = () -> {
            throw new RuntimeException("write failed (simulated)");
        };

        Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, taskOf(mv).getState());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), "the rebuild drops the baseline before it writes");

        onWrite = () -> {
        };
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        runRemainingBatches(batch);
        Assertions.assertEquals(currentSnapshotId(FACT), baselineSnapshotId(mv, FACT));
    }

    @Test
    public void testForceThatBreaksOnTheSchemaKeepsTheReasonItBrokeOn() throws Exception {
        MaterializedView mv = brokenView(MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        failPctPlanning(MaterializedViewExceptions.MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER + "`id` bigint vs `id` int");

        Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().startsWith(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH), mv.getInactiveReason());
    }

    /** Refreshed once, inactivated the way a refresh failing on {@code breakage} inactivates it, then loaded. */
    private MaterializedView brokenView(String breakage) throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_force_activate_" + VIEWS.incrementAndGet(), "incremental");
        runJob(mv);
        AlterMVJobExecutor.inactiveMvAndSuspendTask(mv, MaterializedViewExceptions.inactiveReasonForBreakingFailure(
                new SemanticException(breakage), mv.getName()));
        loadFact();
        return mv;
    }

    private static Task taskOf(MaterializedView mv) {
        return GlobalStateMgr.getCurrentState().getTaskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
    }

    private static void failPctPlanning(String message) {
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                throw new SemanticException(message);
            }
        };
    }
}
