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
import com.starrocks.catalog.OlapTable;
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
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * REFRESH ... FORCE activates an incremental MV whose incremental chain broke, since the rebuild never reads the
 * baseline it broke on, and puts the inactive reason back when the rebuild fails before it writes.
 */
public class IvmForceActivateTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_force_activate";
    private static final AtomicInteger VIEWS = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @ParameterizedTest
    @ValueSource(strings = {MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER,
            MaterializedViewExceptions.SNAPSHOT_ANCESTRY_BROKEN_MARKER})
    public void testForceActivatesABrokenViewAndRebuildsIt(String breakage) throws Exception {
        OlapTable fact = createFact("fact_force_activate_" + VIEWS.incrementAndGet());
        MaterializedView mv = brokenView(fact, breakage);

        TaskRun batch = runForceFirstBatch(mv);
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, taskOf(mv).getState());
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);

        loadPartition(fact, "p1");
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(runFirstBatch(mv)));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
    }

    @Test
    public void testPlainRefreshOfABrokenViewFailsAndNamesForce() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_force_activate_plain"),
                MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        String reason = mv.getInactiveReason();

        Exception e = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("REFRESH MATERIALIZED VIEW " + mv.getName() + " FORCE"),
                e.getMessage());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
    }

    @Test
    public void testExplainOfAForceLeavesABrokenViewInactive() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_force_activate_explain"),
                MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        String reason = mv.getInactiveReason();
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        StatementBase explain = MVTestBase.getAnalyzedPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName() + " FORCE", starRocksAssert.getCtx());
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGH.value(), false,
                new HashMap<>(Map.of(TaskRun.FORCE, "true")));

        Assertions.assertThrows(Exception.class, () -> taskManager.getMVRefreshExplain(task, option, explain));
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
        OlapTable fact = createFact("fact_force_activate_" + VIEWS.incrementAndGet());
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_" + fact.getName(), fact, "INCREMENTAL");
        mockLakeDml();
        runJob(mv, fact);
        AlterMVJobExecutor.inactiveMvAndLog(mv, reason);

        Exception e = Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
    }

    @Test
    public void testForceThatFailsBeforeItWritesPutsTheViewBackInactiveAndPaused() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_force_activate_unplanned"),
                MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
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
        OlapTable fact = createFact("fact_force_activate_unwritten");
        boolean[] failWrites = {false};
        MaterializedView mv = brokenView(fact, MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER, insert -> {
            if (failWrites[0]) {
                throw new RuntimeException("write failed (simulated)");
            }
        });

        failWrites[0] = true;
        Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, taskOf(mv).getState());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), "the rebuild drops the baseline before it writes");

        failWrites[0] = false;
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testForceThatBreaksOnTheSchemaKeepsTheReasonItBrokeOn() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_force_activate_schema"),
                MaterializedViewExceptions.FE_NON_APPEND_ONLY_MARKER);
        failPctPlanning(MaterializedViewExceptions.MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER + "`v` bigint vs `v` int");

        Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().startsWith(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH), mv.getInactiveReason());
    }

    private static MaterializedView brokenView(OlapTable fact, String breakage) throws Exception {
        return brokenView(fact, breakage, insert -> {
        });
    }

    /** Refreshed once, inactivated the way a refresh failing on {@code breakage} inactivates it, then loaded. */
    private static MaterializedView brokenView(OlapTable fact, String breakage, Consumer<InsertStmt> onWrite)
            throws Exception {
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_" + fact.getName(), fact, "INCREMENTAL");
        mockLakeDml(onWrite);
        runJob(mv, fact);
        AlterMVJobExecutor.inactiveMvAndSuspendTask(mv, MaterializedViewExceptions.inactiveReasonForBreakingFailure(
                new SemanticException(breakage), mv.getName()));
        loadEveryPartition(fact);
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
