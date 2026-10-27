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
import com.starrocks.common.RefreshFailureKind;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
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
import com.starrocks.sql.analyzer.mv.IvmBrokenException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class IvmAlterActiveRecoveryTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_alter_active_recovery";
    private static final AtomicInteger VIEWS = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @ParameterizedTest
    @EnumSource(value = RefreshFailureKind.class, names = {"INCREMENTAL_BROKEN", "BASELINE_UNREACHABLE"})
    public void testAlterActiveRecoversABrokenView(RefreshFailureKind kind) throws Exception {
        OlapTable fact = createFact("fact_alter_active_" + VIEWS.incrementAndGet());
        MaterializedView mv = brokenView(fact, kind);
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();

        alterActive(mv);
        assertRecoveredFromChangedPartitions(mv, fact, journal);
    }

    @ParameterizedTest
    @CsvSource({"base-table optimized:, INCREMENTAL", "base-table reordered columns:, INCREMENTAL",
            "base-table optimized:, AUTO", "base-table reordered columns:, AUTO"})
    public void testAlterActiveRecoversAViewOverARewrittenBase(String reasonPrefix, String refreshMode)
            throws Exception {
        Assertions.assertTrue(MaterializedViewExceptions.invalidatesBaseline(reasonPrefix), reasonPrefix);
        OlapTable fact = createFact("fact_alter_active_" + VIEWS.incrementAndGet());
        MaterializedView mv = refreshedView(fact, refreshMode, insert -> {
        });
        AlterMVJobExecutor.inactiveMvAndLog(mv, reasonPrefix + fact.getName());
        loadEveryPartition(fact);
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();

        alterActive(mv);
        assertRecoveredFromChangedPartitions(mv, fact, journal);
    }

    @Test
    public void testAlterActiveThatFailsStillDropsAnUnusableBaseline() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_alter_active_unresolved"),
                RefreshFailureKind.INCREMENTAL_BROKEN);
        String reason = mv.getInactiveReason();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        new MockUp<AlterJobMgr>() {
            @Mock
            public void rebuildRelationshipForActivate(MaterializedView view,
                                                       AlterJobMgr.AlterMaterializedViewStatusContext context) {
            }
        };

        Assertions.assertThrows(Exception.class, () -> alterActive(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), tvrBaseline(mv).toString());
        Assertions.assertEquals(1, journal.size());
    }

    @Test
    public void testActivationWhoseStatusLogFailsStaysInactiveWithoutTheBaseline() throws Exception {
        OlapTable fact = createFact("fact_alter_active_unjournaled");
        MaterializedView mv = brokenView(fact, RefreshFailureKind.INCREMENTAL_BROKEN);
        String reason = mv.getInactiveReason();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        boolean[] failStatusLog = {true};
        new MockUp<EditLog>() {
            @Mock
            public void logAlterMvStatus(Invocation invocation, AlterMaterializedViewStatusLog log,
                                         WALApplier walApplier) {
                if (failStatusLog[0]) {
                    failStatusLog[0] = false;
                    throw new RuntimeException("status log write failed (simulated)");
                }
                invocation.proceed();
            }
        };

        Assertions.assertThrows(Exception.class, () -> alterActive(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.PAUSE, taskOf(mv).getState());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), tvrBaseline(mv).toString());

        alterActive(mv);
        assertRecoveredFromChangedPartitions(mv, fact, journal);
    }

    @Test
    public void testFollowerReplayingTheActivationDropsTheBaseline() throws Exception {
        OlapTable fact = createFact("fact_alter_active_follower");
        MaterializedView mv = brokenView(fact, RefreshFailureKind.INCREMENTAL_BROKEN);
        String reason = mv.getInactiveReason();
        MaterializedView.MvRefreshScheme followerScheme = mv.getRefreshScheme().copy();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        List<AlterMaterializedViewStatusLog> statusJournal = captureStatusJournal();

        alterActive(mv);
        Assertions.assertEquals(1, journal.size());
        Assertions.assertEquals(1, statusJournal.size());

        // A follower still has the view inactive, with the baseline it broke on, when it replays the two entries.
        mv.setInactiveAndReason(reason);
        mv.setRefreshScheme(followerScheme);
        Assertions.assertFalse(tvrBaseline(mv).isEmpty());
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        alterJobMgr.replayChangeMaterializedViewRefreshScheme(journal.get(0));
        alterJobMgr.replayAlterMaterializedViewStatus(statusJournal.get(0));
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(mv::isActive);
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), tvrBaseline(mv).toString());

        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testAlterActiveRejectedOnTheSchemaStillDropsAnUnusableBaseline() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_alter_active_incompatible"),
                RefreshFailureKind.INCREMENTAL_BROKEN);
        String schemaReason = RefreshFailureKind.SCHEMA_MISMATCH.inactiveReason(mv.getName());
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        new MockUp<AlterJobMgr>() {
            @Mock
            public AlterJobMgr.AlterMaterializedViewStatusContext resolveActivate(MaterializedView view, String reason,
                                                                                  boolean isReplay) {
                throw new AlterJobMgr.IncompatibleSchemaException(schemaReason);
            }
        };

        Assertions.assertThrows(Exception.class, () -> alterActive(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(schemaReason, mv.getInactiveReason());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), tvrBaseline(mv).toString());
        Assertions.assertEquals(1, journal.size());
    }

    @ParameterizedTest
    @ValueSource(strings = {AlterJobMgr.MANUAL_INACTIVE_MV_REASON,
            MaterializedViewExceptions.INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES,
            MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH})
    public void testAlterActiveKeepsTheBaselineForOtherReasons(String reason) throws Exception {
        OlapTable fact = createFact("fact_alter_active_" + VIEWS.incrementAndGet());
        MaterializedView mv = refreshedView(fact);
        AlterMVJobExecutor.inactiveMvAndLog(mv, reason);
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline(mv));
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();

        alterActive(mv);
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertFalse(baseline.isEmpty());
        Assertions.assertEquals(baseline, tvrBaseline(mv));
        Assertions.assertTrue(journal.isEmpty());
    }

    @Test
    public void testRecoveryThatFailsToWriteIsRetriedByTheNextRefresh() throws Exception {
        OlapTable fact = createFact("fact_alter_active_unwritten");
        boolean[] failWrites = {false};
        MaterializedView mv = brokenView(fact, RefreshFailureKind.INCREMENTAL_BROKEN, insert -> {
            if (failWrites[0]) {
                throw new RuntimeException("write failed (simulated)");
            }
        });
        alterActive(mv);

        failWrites[0] = true;
        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), tvrBaseline(mv).toString());

        failWrites[0] = false;
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testRecoveryThatBreaksOnTheSchemaKeepsTheReasonItBrokeOn() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_alter_active_schema"),
                RefreshFailureKind.INCREMENTAL_BROKEN);
        alterActive(mv);
        failPctPlanning(new IvmBrokenException(RefreshFailureKind.SCHEMA_MISMATCH,
                MaterializedViewExceptions.MV_SCHEMA_COLUMN_NOT_COMPATIBLE_MARKER + "`v` bigint vs `v` int"));

        Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertFalse(mv.isActive());
        Assertions.assertTrue(mv.getInactiveReason().startsWith(
                MaterializedViewExceptions.INACTIVE_REASON_FOR_MV_SCHEMA_MISMATCH), mv.getInactiveReason());
    }

    @Test
    public void testRefreshActivatesAViewInactiveForASortKeyChange() throws Exception {
        OlapTable fact = createFact("fact_alter_active_sort_key");
        MaterializedView mv = refreshedView(fact);
        AlterMVJobExecutor.inactiveMvAndLog(mv,
                MaterializedViewExceptions.inactiveReasonForBaseTableReorderColumns(fact.getName()));
        loadEveryPartition(fact);

        TaskRun batch = runFirstBatch(mv);
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testPlainRefreshOfABrokenViewNamesAlterActive() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_alter_active_plain"),
                RefreshFailureKind.INCREMENTAL_BROKEN);
        String reason = mv.getInactiveReason();

        Exception e = Assertions.assertThrows(Exception.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains(
                "ALTER MATERIALIZED VIEW `" + DB + "`.`" + mv.getName() + "` ACTIVE"), e.getMessage());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
    }

    @Test
    public void testExplainOfAForceLeavesABrokenViewInactive() throws Exception {
        MaterializedView mv = brokenView(createFact("fact_alter_active_explain"),
                RefreshFailureKind.INCREMENTAL_BROKEN);
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

    private static Stream<String> reasonsAPlainRefreshCannotActivate() {
        return Stream.of(
                RefreshFailureKind.INCREMENTAL_BROKEN.inactiveReason("mv"),
                RefreshFailureKind.BASELINE_UNREACHABLE.inactiveReason("mv"),
                RefreshFailureKind.SCHEMA_MISMATCH.inactiveReason("mv"),
                MaterializedViewExceptions.inactiveReasonForBaseTableOptimized("fact"),
                AlterJobMgr.MANUAL_INACTIVE_MV_REASON,
                MVActiveChecker.MV_BACKUP_INACTIVE_REASON,
                MaterializedViewExceptions.INACTIVE_REASON_FOR_CONSECUTIVE_FAILURES);
    }

    @ParameterizedTest
    @MethodSource("reasonsAPlainRefreshCannotActivate")
    public void testForceDoesNotActivate(String reason) throws Exception {
        OlapTable fact = createFact("fact_alter_active_" + VIEWS.incrementAndGet());
        MaterializedView mv = refreshedView(fact);
        AlterMVJobExecutor.inactiveMvAndSuspendTask(mv, reason);
        loadEveryPartition(fact);
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline(mv));

        Exception e = Assertions.assertThrows(Exception.class, () -> runForceFirstBatch(mv));
        Assertions.assertTrue(e.getMessage().contains("is not active"), e.getMessage());
        Assertions.assertFalse(mv.isActive());
        Assertions.assertEquals(reason, mv.getInactiveReason());
        Assertions.assertEquals(baseline, tvrBaseline(mv));
    }

    private static void assertRecoveredFromChangedPartitions(MaterializedView mv, OlapTable fact,
                                                             List<ChangeMaterializedViewRefreshSchemeLog> journal)
            throws Exception {
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
        Assertions.assertEquals(Constants.TaskState.ACTIVE, taskOf(mv).getState());
        Assertions.assertTrue(tvrBaseline(mv).isEmpty(), tvrBaseline(mv).toString());
        Assertions.assertEquals(1, journal.size());
        Assertions.assertTrue(tvrBaselineIn(journal.get(0)).isEmpty(), "a follower replays the cleared baseline");

        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);

        loadPartition(fact, "p1");
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(runFirstBatch(mv)));
        Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
    }

    private static void alterActive(MaterializedView mv) throws Exception {
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW " + mv.getName() + " ACTIVE");
    }

    private static List<AlterMaterializedViewStatusLog> captureStatusJournal() {
        List<AlterMaterializedViewStatusLog> journal = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logAlterMvStatus(Invocation invocation, AlterMaterializedViewStatusLog log,
                                         WALApplier walApplier) {
                journal.add(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log), AlterMaterializedViewStatusLog.class));
                invocation.proceed();
            }
        };
        return journal;
    }

    private static MaterializedView refreshedView(OlapTable fact) throws Exception {
        return refreshedView(fact, "INCREMENTAL", insert -> {
        });
    }

    private static MaterializedView refreshedView(OlapTable fact, String refreshMode, Consumer<InsertStmt> onWrite)
            throws Exception {
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_" + fact.getName(), fact, refreshMode);
        mockLakeDml(onWrite);
        runJob(mv, fact);
        return mv;
    }

    private static MaterializedView brokenView(OlapTable fact, RefreshFailureKind kind) throws Exception {
        return brokenView(fact, kind, insert -> {
        });
    }

    /** Refreshed once, inactivated the way a refresh failing with {@code kind} inactivates it, then loaded. */
    private static MaterializedView brokenView(OlapTable fact, RefreshFailureKind kind, Consumer<InsertStmt> onWrite)
            throws Exception {
        MaterializedView mv = refreshedView(fact, "INCREMENTAL", onWrite);
        AlterMVJobExecutor.inactiveMvAndSuspendTask(mv, kind.inactiveReason(mv.getName()));
        loadEveryPartition(fact);
        return mv;
    }

    private static Task taskOf(MaterializedView mv) {
        return GlobalStateMgr.getCurrentState().getTaskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
    }

    private static void failPctPlanning(SemanticException failure) {
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                throw failure;
            }
        };
    }
}
