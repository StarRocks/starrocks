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
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.MVRefreshExecutor;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
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
public class IvmFallbackBaselineClearTest extends IvmIcebergRefreshJobTestBase {

    @Test
    public void testFallbackClearsTheBaselineBeforeItWrites() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_clear_before_write", "auto");
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        List<Boolean> baselineEmptyAtWrite = new ArrayList<>();
        List<Boolean> clearJournaledAtWrite = new ArrayList<>();
        onWrite = () -> {
            baselineEmptyAtWrite.add(tvrBaseline(mv).isEmpty());
            clearJournaledAtWrite.add(journal.stream().anyMatch(log -> tvrBaselineIn(log).isEmpty()));
        };
        runJob(mv);
        Assertions.assertFalse(tvrBaseline(mv).isEmpty(), "the bootstrap must leave a baseline");

        loadFact();
        failIncrementalPlanning();
        journal.clear();
        baselineEmptyAtWrite.clear();
        clearJournaledAtWrite.clear();
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(batch), "the job must run as pct");
        long frozenSnapshotId = frozenSnapshotId(mv, FACT);
        Assertions.assertEquals(List.of(true), clearJournaledAtWrite,
                "the dropped baseline must be in the journal before the first write, or a restart brings it back");

        runRemainingBatches(batch);
        Assertions.assertEquals(FACT_PARTITIONS.size(), baselineEmptyAtWrite.size());
        Assertions.assertFalse(baselineEmptyAtWrite.contains(false), "no batch may write while a baseline is set");
        Assertions.assertEquals(frozenSnapshotId, baselineSnapshotId(mv, FACT),
                "the last batch must promote the frozen snapshot");
    }

    @Test
    public void testFallbackKeepsTheBaselineOfATableItDoesNotMove() throws Exception {
        loadFact();
        loadDim();
        MaterializedView mv = createJoinMv("mv_unmoved_dim", "auto");
        runJob(mv);
        long dimBaseline = baselineSnapshotId(mv, DIM);

        loadFact();
        failIncrementalPlanningOnce();
        TaskRun batch = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(batch));
        Assertions.assertFalse(hasBaseline(mv, FACT), "the write reads the fact table past its baseline");
        Assertions.assertEquals(dimBaseline, baselineSnapshotId(mv, DIM),
                "the write reads the dim table at its baseline, and nothing would set it again");

        runRemainingBatches(batch);
        Assertions.assertFalse(runsAsPct(runFirstBatch(mv)), "with every baseline in place the next refresh is incremental");
    }

    @Test
    public void testInterruptedFallbackIsTakenOverByPct() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_interrupted_fallback", "auto");
        runJob(mv);

        loadFact();
        failIncrementalPlanningOnce();
        TaskRun leftover = nextOf(runFirstBatch(mv));
        Assertions.assertNotNull(leftover, "partition_refresh_number=1 must split the job into batches");

        TaskRun takeover = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(takeover), "a job that stopped after writing must be rebuilt, not resumed");
        runRemainingBatches(takeover);
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(leftover),
                "a batch of the interrupted job must leave the takeover's state alone");
        Assertions.assertEquals(currentSnapshotId(FACT), baselineSnapshotId(mv, FACT));
    }

    @Test
    public void testLeftoverBatchSkipsOnceTheTakeoverReleasedThePinningState() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_released_takeover", "auto");
        runJob(mv);

        loadFact();
        failIncrementalPlanningOnce();
        TaskRun leftover = nextOf(runFirstBatch(mv));
        Assertions.assertNotNull(leftover);
        runRemainingBatches(runFirstBatch(mv));
        Assertions.assertNull(mv.getRefreshScheme().getAsyncRefreshContext().getTempTvrOwnerStartTaskRunId());
        Map<BaseTableInfo, TvrVersionRange> takeoverBaseline = new HashMap<>(tvrBaseline(mv));

        loadFact();
        int writesBefore = writes;
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(leftover),
                "a batch of the job the takeover replaced must not run as an unpinned one");
        Assertions.assertEquals(writesBefore, writes);
        Assertions.assertEquals(takeoverBaseline, tvrBaseline(mv));
        Assertions.assertFalse(runsAsPct(runFirstBatch(mv)), "the takeover's baseline serves the next refresh");
    }

    @Test
    public void testFailedWriteBackIsTakenOverByPct() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_failed_write_back", "auto");
        runJob(mv);

        loadFact();
        failIncrementalPlanningOnce();
        failPctMetaUpdateOnce();
        int writesBefore = writes;
        Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));
        Assertions.assertEquals(writesBefore + 1, writes, "the batch must have written its data");

        TaskRun takeover = runFirstBatch(mv);
        Assertions.assertTrue(runsAsPct(takeover), "data written without its metadata must be rebuilt, not resumed");
        runRemainingBatches(takeover);
        Assertions.assertEquals(currentSnapshotId(FACT), baselineSnapshotId(mv, FACT));
    }

    @Test
    public void testPlanningFailureKeepsTheBaseline() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_planning_failure", "auto");
        runJob(mv);
        long baseline = baselineSnapshotId(mv, FACT);

        loadFact();
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

        Assertions.assertEquals(baseline, baselineSnapshotId(mv, FACT),
                "a fallback that wrote nothing must leave the next run its incremental start");
    }

    @Test
    public void testSkippedFallbackKeepsTheBaseline() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_skipped_fallback", "auto");
        runJob(mv);
        long baseline = baselineSnapshotId(mv, FACT);

        failIncrementalPlanning();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        int writesBefore = writes;
        Assertions.assertTrue(runsAsPct(runFirstBatch(mv)));

        Assertions.assertEquals(writesBefore, writes, "nothing changed, so nothing is written");
        Assertions.assertTrue(journal.stream().noneMatch(log -> tvrBaselineIn(log).isEmpty()),
                "a fallback that writes nothing must not clear the baseline");
        Assertions.assertEquals(baseline, baselineSnapshotId(mv, FACT));
    }

    @Test
    public void testRefreshWithoutABaselineJournalsNothingBeforeItWrites() throws Exception {
        loadFact();
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        int[] journalSizeAtBatchStart = {0};
        List<Integer> journaledInBatchBeforeWrite = new ArrayList<>();
        onWrite = () -> journaledInBatchBeforeWrite.add(journal.size() - journalSizeAtBatchStart[0]);

        for (String refreshMode : List.of("incremental", "pct")) {
            MaterializedView mv = createFactMv("mv_no_baseline_" + refreshMode, refreshMode);
            journalSizeAtBatchStart[0] = journal.size();
            TaskRun batch = runFirstBatch(mv);
            for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
                journalSizeAtBatchStart[0] = journal.size();
                executeBatch(batch);
            }
        }
        Assertions.assertEquals(2 * FACT_PARTITIONS.size(), journaledInBatchBeforeWrite.size());
        Assertions.assertTrue(journaledInBatchBeforeWrite.stream().allMatch(count -> count == 0),
                "with no baseline to drop, a batch must not journal before it writes: " + journaledInBatchBeforeWrite);
    }

    @Test
    public void testClearThatCannotLockTheMvWritesNothing() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_clear_lock_timeout", "auto");
        runJob(mv);
        long baseline = baselineSnapshotId(mv, FACT);

        loadFact();
        failIncrementalPlanning();
        withholdTheMvWriteLockOnceThePctWriteStarts(mv);
        int writesBefore = writes;
        Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));

        Assertions.assertEquals(writesBefore, writes, "the batch must stop before it writes");
        Assertions.assertEquals(baseline, baselineSnapshotId(mv, FACT));
    }

    @Test
    public void testClearThatCannotBeJournaledWritesNothing() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_clear_journal_failure", "auto");
        runJob(mv);
        long baseline = baselineSnapshotId(mv, FACT);

        loadFact();
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
        int writesBefore = writes;
        DmlException failure = Assertions.assertThrows(DmlException.class, () -> runFirstBatch(mv));
        Assertions.assertTrue(failure.getMessage().contains("journal write failed (simulated)"), failure.getMessage());

        Assertions.assertEquals(writesBefore, writes, "the batch must stop before it writes");
        Assertions.assertEquals(baseline, baselineSnapshotId(mv, FACT));
    }

    @Test
    public void testReplayingTheClearDropsTheBaseline() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_replay_clear", "auto");
        runJob(mv);
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline(mv));

        loadFact();
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
        loadFact();
        MaterializedView mv = createFactMv("mv_promotion_journal", "auto");
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        runJob(mv);

        ChangeMaterializedViewRefreshSchemeLog promotion = journal.stream()
                .filter(log -> !tvrBaselineIn(log).isEmpty())
                .findFirst().orElseThrow(() -> new AssertionError("no journal entry promotes the baseline"));
        MaterializedView.AsyncRefreshContext journaled = promotion.getAsyncRefreshContext();
        Assertions.assertNull(journaled.getTempTvrOwnerStartTaskRunId(),
                "a replayed promotion must not revive the finished job's pinning owner");
        Assertions.assertEquals(Map.of(), journaled.getTempBaseTableInfoTvrDeltaMap());
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
