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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshPlanBuilder;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.plan.ExecPlan;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A PCT refresh of an incrementally maintained MV must drop the IVM baseline before it writes: an incremental run
 * from a baseline the data has moved past applies the same changes twice.
 */
public class IvmFallbackBaselineClearTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_fallback_baseline_clear";

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @Test
    public void testFallbackClearsTheBaselineBeforeItWrites() throws Exception {
        OlapTable fact = createFact("fact_clear_before_write");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_clear_before_write", fact, "AUTO");
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        List<Boolean> baselineEmptyAtWrite = new ArrayList<>();
        List<Boolean> clearJournaledAtWrite = new ArrayList<>();
        mockLakeDml(insert -> {
            baselineEmptyAtWrite.add(tvrBaseline(mv).isEmpty());
            clearJournaledAtWrite.add(journal.stream().anyMatch(log -> tvrBaselineIn(log).isEmpty()));
        });
        runJob(mv, fact);
        Assertions.assertFalse(tvrBaseline(mv).isEmpty(), "the bootstrap must leave a baseline");

        loadEveryPartition(fact);
        failIncrementalPlanning();
        journal.clear();
        baselineEmptyAtWrite.clear();
        clearJournaledAtWrite.clear();
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(batch), "the job must run as pct");
        long frozenBookmarkId = frozenBookmarkId(mv);
        Assertions.assertEquals(List.of(true), clearJournaledAtWrite,
                "the dropped baseline must be in the journal before the first write, or a restart brings it back");

        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        Assertions.assertFalse(baselineEmptyAtWrite.contains(false), "no batch may write while a baseline is set");
        Assertions.assertEquals(frozenBookmarkId, bookmarkIdOf(tvrBaseline(mv), fact),
                "the last batch must promote the frozen snapshot");
    }

    @Test
    public void testFallbackKeepsTheBaselineOfATableItDoesNotMove() throws Exception {
        OlapTable fact = createFact("fact_unmoved_dim");
        OlapTable dim = createDim("dim_unmoved_dim");
        loadEveryPartition(fact);
        loadPartition(dim, dim.getName());
        MaterializedView mv = createJoinMv("mv_unmoved_dim", fact, dim, "AUTO");
        mockLakeDml();
        runJob(mv, fact);
        long dimBaselineId = bookmarkIdOf(tvrBaseline(mv), dim);

        loadEveryPartition(fact);
        failIncrementalPlanningOnce();
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(batch));
        Assertions.assertTrue(tvrBaseline(mv).keySet().stream().noneMatch(table -> table.getTableId() == fact.getId()),
                "the write reads the fact table past its baseline");
        Assertions.assertEquals(dimBaselineId, bookmarkIdOf(tvrBaseline(mv), dim),
                "the write reads the dim table at its baseline, and nothing would set it again");

        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        Assertions.assertFalse(runsAsPct(runFirstBatch(mv)), "with every baseline in place the next refresh is incremental");
    }

    @Test
    public void testInterruptedFallbackIsTakenOverByPct() throws Exception {
        OlapTable fact = createFact("fact_interrupted_fallback");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_interrupted_fallback", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);

        loadEveryPartition(fact);
        failIncrementalPlanningOnce();
        TaskRun leftover = nextOf(runFirstBatch(mv));
        Assertions.assertNotNull(leftover, "partition_refresh_number=1 must split the job into batches");

        TaskRun takeover = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(takeover), "a job that stopped after writing must be rebuilt, not resumed");
        for (TaskRun batch = nextOf(takeover); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(leftover),
                "a batch of the interrupted job must leave the takeover's state alone");
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testLeftoverBatchSkipsOnceTheTakeoverReleasedThePinningState() throws Exception {
        OlapTable fact = createFact("fact_released_takeover");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_released_takeover", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);

        loadEveryPartition(fact);
        failIncrementalPlanningOnce();
        TaskRun leftover = nextOf(runFirstBatch(mv));
        Assertions.assertNotNull(leftover);
        TaskRun takeover = runFirstBatch(mv);
        for (TaskRun batch = nextOf(takeover); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        Assertions.assertNull(mv.getRefreshScheme().getAsyncRefreshContext().getTempTvrOwnerStartTaskRunId());
        Map<BaseTableInfo, TvrVersionRange> takeoverBaseline = new HashMap<>(tvrBaseline(mv));

        loadEveryPartition(fact);
        int writesBefore = SCANNED_VERSIONS.size();
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(leftover),
                "a batch of the job the takeover replaced must not run as an unpinned one");
        Assertions.assertEquals(writesBefore, SCANNED_VERSIONS.size());
        Assertions.assertEquals(takeoverBaseline, tvrBaseline(mv));
        Assertions.assertFalse(runsAsPct(runFirstBatch(mv)), "the takeover's baseline serves the next refresh");
    }

    @Test
    public void testFailedWriteBackIsTakenOverByPct() throws Exception {
        OlapTable fact = createFact("fact_failed_write_back");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_failed_write_back", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);

        loadEveryPartition(fact);
        failIncrementalPlanningOnce();
        failPctMetaUpdateOnce();
        int writesBefore = SCANNED_VERSIONS.size();
        Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));
        Assertions.assertEquals(writesBefore + 1, SCANNED_VERSIONS.size(), "the batch must have written its data");

        TaskRun takeover = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(takeover), "data written without its metadata must be rebuilt, not resumed");
        for (TaskRun batch = nextOf(takeover); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testExecutionRejectionClearsTheBaselineBeforeThePctWrite() throws Exception {
        OlapTable fact = createFact("fact_execution_rejection");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_execution_rejection", fact, "AUTO");
        List<Boolean> baselineEmptyAtWrite = new ArrayList<>();
        mockLakeDml(insert -> baselineEmptyAtWrite.add(tvrBaseline(mv).isEmpty()));
        runJob(mv, fact);

        loadEveryPartition(fact);
        rejectIncrementalExecutionOnce();
        baselineEmptyAtWrite.clear();
        TaskRun batch = runFirstBatch(mv);

        Assertions.assertTrue(runsAsPct(batch), "the rejected run must fall back to pct");
        Assertions.assertEquals(List.of(true), baselineEmptyAtWrite, "the pct write must find the baseline gone");
    }

    @Test
    public void testPlanningFailureKeepsTheBaseline() throws Exception {
        OlapTable fact = createFact("fact_planning_failure");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_planning_failure", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);
        long baselineId = bookmarkIdOf(tvrBaseline(mv), fact);

        loadEveryPartition(fact);
        failIncrementalPlanning();
        new MockUp<MVPCTRefreshPlanBuilder>() {
            @Mock
            public InsertStmt analyzeAndBuildInsertPlan(InsertStmt insertStmt, PCellSortedSet mvToRefreshedPartitions,
                                                        Map<Table, PCellSortedSet> refTableRefreshPartitions,
                                                        ConnectContext ctx) {
                throw new SemanticException("pct planning failed (simulated)");
            }
        };
        DmlException failure = Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(failure.getMessage().contains("pct planning failed (simulated)"), failure.getMessage());

        Assertions.assertEquals(baselineId, bookmarkIdOf(tvrBaseline(mv), fact),
                "a fallback that wrote nothing must leave the next run its incremental start");
    }

    @Test
    public void testSkippedFallbackPromotesTheFrozenSnapshotWithoutClearing() throws Exception {
        OlapTable fact = createFact("fact_skipped_fallback");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_skipped_fallback", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);

        failIncrementalPlanning();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        int writesBefore = SCANNED_VERSIONS.size();
        runFirstBatch(mv);

        Assertions.assertEquals(writesBefore, SCANNED_VERSIONS.size(), "nothing changed, so nothing is written");
        Assertions.assertTrue(journal.stream().noneMatch(log -> tvrBaselineIn(log).isEmpty()),
                "a fallback that writes nothing must not clear the baseline");
        MaterializedView.AsyncRefreshContext context = mv.getRefreshScheme().getAsyncRefreshContext();
        Assertions.assertFalse(context.getBaseTableInfoTvrVersionRangeMap().isEmpty());
        Assertions.assertEquals(Map.of(), context.getTempBaseTableInfoTvrDeltaMap(),
                "the skipped fallback must promote the snapshot it froze");
    }

    @Test
    public void testRefreshWithoutABaselineJournalsNothingBeforeItWrites() throws Exception {
        OlapTable fact = createFact("fact_no_baseline");
        loadEveryPartition(fact);
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        int[] journalSizeAtBatchStart = {0};
        List<Integer> journaledInBatchBeforeWrite = new ArrayList<>();
        mockLakeDml(insert -> journaledInBatchBeforeWrite.add(journal.size() - journalSizeAtBatchStart[0]));

        for (String refreshMode : List.of("INCREMENTAL", "PCT")) {
            MaterializedView mv = createMv("mv_no_baseline_" + refreshMode.toLowerCase(), fact, refreshMode);
            journalSizeAtBatchStart[0] = journal.size();
            TaskRun batch = runFirstBatch(mv);
            for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
                journalSizeAtBatchStart[0] = journal.size();
                executeBatch(batch);
            }
        }
        Assertions.assertEquals(2 * PARTITIONS.size(), journaledInBatchBeforeWrite.size());
        Assertions.assertTrue(journaledInBatchBeforeWrite.stream().allMatch(count -> count == 0),
                "with no baseline to drop, a batch must not journal before it writes: " + journaledInBatchBeforeWrite);
    }

    @Test
    public void testClearThatCannotLockTheMvWritesNothing() throws Exception {
        OlapTable fact = createFact("fact_clear_lock_timeout");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_clear_lock_timeout", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);
        long baselineId = bookmarkIdOf(tvrBaseline(mv), fact);

        loadEveryPartition(fact);
        failIncrementalPlanning();
        withholdTheMvWriteLockOnceThePctWriteStarts(mv);
        int writesBefore = SCANNED_VERSIONS.size();
        Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));

        Assertions.assertEquals(writesBefore, SCANNED_VERSIONS.size(), "the batch must stop before it writes");
        Assertions.assertEquals(baselineId, bookmarkIdOf(tvrBaseline(mv), fact));
    }

    @Test
    public void testClearThatCannotBeJournaledWritesNothing() throws Exception {
        OlapTable fact = createFact("fact_clear_journal_failure");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_clear_journal_failure", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);
        long baselineId = bookmarkIdOf(tvrBaseline(mv), fact);

        loadEveryPartition(fact);
        failIncrementalPlanning();
        boolean[] armed = {true};
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(Invocation invocation, ChangeMaterializedViewRefreshSchemeLog log,
                                                 WALApplier walApplier) {
                if (armed[0] && tvrBaselineIn(log).isEmpty() && !tvrBaseline(mv).isEmpty()) {
                    armed[0] = false;
                    throw new IllegalStateException("journal write failed (simulated)");
                }
                invocation.proceed();
            }
        };
        int writesBefore = SCANNED_VERSIONS.size();
        DmlException failure = Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(failure.getMessage().contains("journal write failed (simulated)"), failure.getMessage());

        Assertions.assertEquals(writesBefore, SCANNED_VERSIONS.size(), "the batch must stop before it writes");
        Assertions.assertEquals(baselineId, bookmarkIdOf(tvrBaseline(mv), fact));
    }

    @Test
    public void testReplayingTheClearDropsTheBaseline() throws Exception {
        OlapTable fact = createFact("fact_replay_clear");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_replay_clear", fact, "AUTO");
        mockLakeDml();
        runJob(mv, fact);
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline(mv));

        loadEveryPartition(fact);
        failIncrementalPlanning();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        runFirstBatch(mv);
        ChangeMaterializedViewRefreshSchemeLog clear = journal.stream()
                .filter(log -> tvrBaselineIn(log).isEmpty())
                .findFirst().orElseThrow(() -> new AssertionError("no journal entry drops the baseline"));

        tvrBaseline(mv).putAll(baseline);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().replayChangeMaterializedViewRefreshScheme(clear);
        Assertions.assertEquals(Map.of(), tvrBaseline(mv), "a follower must drop the baseline too");
    }

    @Test
    public void testPromotionJournalCarriesNoPinningRecord() throws Exception {
        OlapTable fact = createFact("fact_promotion_journal");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_promotion_journal", fact, "AUTO");
        mockLakeDml();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        runJob(mv, fact);

        ChangeMaterializedViewRefreshSchemeLog promotion = journal.stream()
                .filter(log -> !tvrBaselineIn(log).isEmpty())
                .findFirst().orElseThrow(() -> new AssertionError("no journal entry promotes the baseline"));
        MaterializedView.AsyncRefreshContext journaled = promotion.getAsyncRefreshContext();
        Assertions.assertNull(journaled.getTempTvrOwnerStartTaskRunId(),
                "a replayed promotion must not revive the finished job's pinning owner");
        Assertions.assertEquals(Map.of(), journaled.getTempBaseTableInfoTvrDeltaMap());
    }

    private static boolean runsAsPct(TaskRun taskRun) {
        return ((MVHybridRefreshProcessor) refreshProcessorOf(taskRun)).getCurrentProcessor()
                instanceof MVPCTRefreshProcessor;
    }

    private static Map<BaseTableInfo, TvrVersionRange> tvrBaseline(MaterializedView mv) {
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
    }

    private static Map<BaseTableInfo, TvrVersionRange> tvrBaselineIn(ChangeMaterializedViewRefreshSchemeLog log) {
        return log.getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
    }

    /** Every refresh scheme journal entry, as it was written: the in-memory scheme keeps changing afterwards. */
    private static List<ChangeMaterializedViewRefreshSchemeLog> captureRefreshSchemeJournal() {
        List<ChangeMaterializedViewRefreshSchemeLog> journal = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(Invocation invocation, ChangeMaterializedViewRefreshSchemeLog log,
                                                 WALApplier walApplier) {
                journal.add(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log),
                        ChangeMaterializedViewRefreshSchemeLog.class));
                invocation.proceed();
            }
        };
        return journal;
    }

    private static void failIncrementalPlanningOnce() {
        boolean[] armed = {true};
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(Invocation invocation,
                                                                          TaskRunContext taskRunContext) {
                if (armed[0]) {
                    armed[0] = false;
                    throw new SemanticException("incremental planning failed (simulated)");
                }
                return invocation.proceed();
            }
        };
    }

    private static void rejectIncrementalExecutionOnce() {
        boolean[] armed = {true};
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public Constants.TaskRunState execProcessExecPlan(Invocation invocation, TaskRunContext taskRunContext,
                                                              MVRefreshProcessor.ProcessExecPlan processExecPlan,
                                                              MVRefreshExecutor executor) {
                if (armed[0]) {
                    armed[0] = false;
                    throw new RuntimeException(
                            "CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete");
                }
                return invocation.proceed();
            }
        };
    }

    /** The next PCT metadata update fails after its batch has written, as when its lock times out. */
    private static void failPctMetaUpdateOnce() {
        boolean[] armed = {true};
        new MockUp<MVRefreshProcessor>() {
            @Mock
            public void updatePCTMeta(Invocation invocation, ExecPlan execPlan, PCellSortedSet mvRefreshedPartitions,
                                      Map<BaseTableSnapshotInfo, PCellSortedSet> refTableAndPartitionNames,
                                      Map<BaseTableInfo, TvrVersionRange> tvrDeltaToPromote) {
                if (armed[0]) {
                    armed[0] = false;
                    throw new DmlException("update meta failed: lock timeout (simulated)");
                }
                invocation.proceed();
            }
        };
    }

    private static void withholdTheMvWriteLockOnceThePctWriteStarts(MaterializedView mv) {
        boolean[] armed = {false};
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public Constants.TaskRunState execProcessExecPlan(Invocation invocation, TaskRunContext taskRunContext,
                                                              MVRefreshProcessor.ProcessExecPlan processExecPlan,
                                                              MVRefreshExecutor executor) {
                armed[0] = true;
                return invocation.proceed();
            }
        };
        new MockUp<Locker>() {
            @Mock
            public boolean tryLockTableWithIntensiveDbLock(Invocation invocation, Long dbId, Long tableId,
                                                           LockType lockType, long timeout, TimeUnit unit) {
                if (armed[0] && tableId == mv.getId() && lockType == LockType.WRITE) {
                    armed[0] = false;
                    return false;
                }
                return invocation.proceed();
            }
        };
    }
}
