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
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.HolderId;
import com.starrocks.lake.bookmark.Reference;
import com.starrocks.lake.ivm.MvBookmarkOps;
import com.starrocks.load.loadv2.IVMInsertLoadTxnCallback;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * EXPLAIN / TRACE REFRESH plans a refresh but must not touch what a real refresh of the same view relies on: the
 * bookmark references of the view's holder, and the pinning state a refresh in flight commits from.
 */
public class IvmExplainRefreshIsolationTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_explain_refresh_isolation";
    private static final AtomicInteger VIEWS = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testExplainKeepsTheBookmarkOfARefreshInFlight(String refreshMode) throws Exception {
        int n = VIEWS.incrementAndGet();
        OlapTable fact = createFact("fact_explain_" + n);
        MaterializedView mv = refreshedView("mv_explain_" + n, fact, refreshMode);
        long committed = bookmarkIdOf(tvrBaseline(mv), fact);
        loadPartition(fact, "p1");
        long inFlight = MvBookmarkOps.acquire(db.getId(), fact, mv.getMvId()).getSnapshotId();
        loadPartition(fact, "p2");

        explain(mv, false);

        Assertions.assertEquals(Set.of(committed, inFlight), mvReferences(mv, fact));
        Assertions.assertEquals(Set.of(), explainReferences(fact));
    }

    @ParameterizedTest
    @CsvSource({"incremental, true", "auto, true", "auto, false"})
    public void testExplainOnThePctPathLeavesTheStagedWindowAlone(String refreshMode, boolean force)
            throws Exception {
        int n = VIEWS.incrementAndGet();
        OlapTable fact = createFact("fact_explain_" + n);
        MaterializedView mv = refreshedView("mv_explain_" + n, fact, refreshMode);
        MaterializedView.AsyncRefreshContext context = mv.getRefreshScheme().getAsyncRefreshContext();
        loadPartition(fact, "p1");
        // What an incremental refresh stages right before its insert.
        long committed = bookmarkIdOf(tvrBaseline(mv), fact);
        long inFlight = MvBookmarkOps.acquire(db.getId(), fact, mv.getMvId()).getSnapshotId();
        context.replaceTempBaseTableInfoTvrDeltaMap("in-flight", Map.of(baseTableInfo(mv), TvrTableDelta.of(committed,
                inFlight)));
        Map<BaseTableInfo, TvrVersionRange> staged = new HashMap<>(context.getTempBaseTableInfoTvrDeltaMap());
        loadPartition(fact, "p2");
        if (!force) {
            failIncrementalPlanning();
        }

        TaskRun explainRun = explain(mv, force);

        Assertions.assertEquals("PCT", executedRefreshMode(explainRun));
        Assertions.assertEquals("in-flight", context.getTempTvrOwnerStartTaskRunId());
        Assertions.assertEquals(staged, context.getTempBaseTableInfoTvrDeltaMap());
        // The explain still plans against a snapshot it froze, just not one the view keeps.
        Assertions.assertEquals(Set.of(fact.getName()), explainRun.getStatus().getMvTaskRunExtraMessage()
                .getPinnedSnapshotIdMap().keySet().stream()
                .map(table -> table.substring(table.lastIndexOf('.') + 1))
                .collect(Collectors.toSet()));
        Assertions.assertEquals(Set.of(committed, inFlight), mvReferences(mv, fact));
        context.clearTempBaseTableInfoTvrDeltaState();
    }

    @Test
    public void testBatchedFallbackKeepsItsPinThroughAnExplain() throws Exception {
        int n = VIEWS.incrementAndGet();
        OlapTable fact = createFact("fact_explain_" + n);
        MaterializedView mv = refreshedView("mv_explain_" + n, fact, "auto");
        loadEveryPartition(fact);
        failIncrementalPlanning();
        TaskRun batch = runFirstBatch(mv);
        String job = mv.getRefreshScheme().getAsyncRefreshContext().getTempTvrOwnerStartTaskRunId();
        long frozen = frozenBookmarkId(mv);
        Assertions.assertNotNull(job);

        explain(mv, false);

        Assertions.assertEquals(job, mv.getRefreshScheme().getAsyncRefreshContext().getTempTvrOwnerStartTaskRunId());
        int batches = 1;
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(batch));
            batches++;
        }
        Assertions.assertEquals(PARTITIONS.size(), batches);
        Assertions.assertEquals(frozen, bookmarkIdOf(tvrBaseline(mv), fact));
    }

    @Test
    public void testCommitRefusesAWindowAnotherRunStaged() throws Exception {
        int n = VIEWS.incrementAndGet();
        OlapTable fact = createFact("fact_explain_" + n);
        MaterializedView mv = refreshedView("mv_explain_" + n, fact, "incremental");
        MaterializedView.AsyncRefreshContext context = mv.getRefreshScheme().getAsyncRefreshContext();
        long committed = bookmarkIdOf(tvrBaseline(mv), fact);
        Map<BaseTableInfo, TvrVersionRange> window = Map.of(baseTableInfo(mv),
                TvrTableDelta.of(committed, committed + 1));

        context.replaceTempBaseTableInfoTvrDeltaMap("other-run", window);
        TransactionState replaced = insertTxnState();
        Assertions.assertThrows(TransactionException.class,
                () -> new IVMInsertLoadTxnCallback(db.getId(), mv.getId(), "this-run").beforeCommitted(replaced));
        Assertions.assertNull(commitInfoOf(replaced));

        context.replaceTempBaseTableInfoTvrDeltaMap("this-run", window);
        TransactionState own = insertTxnState();
        new IVMInsertLoadTxnCallback(db.getId(), mv.getId(), "this-run").beforeCommitted(own);
        Assertions.assertNotNull(commitInfoOf(own));

        // A window cleared under the run (ALTER ... ACTIVE / INACTIVE) still commits with nothing to install.
        context.clearTempBaseTableInfoTvrDeltaState();
        TransactionState cleared = insertTxnState();
        new IVMInsertLoadTxnCallback(db.getId(), mv.getId(), "this-run").beforeCommitted(cleared);
        Assertions.assertNull(commitInfoOf(cleared));
    }

    @Test
    public void testIncrementalInsertCarriesItsRunAsTheStagingOwner() throws Exception {
        int n = VIEWS.incrementAndGet();
        OlapTable fact = createFact("fact_explain_" + n);
        MaterializedView mv = refreshedView("mv_explain_" + n, fact, "incremental");
        loadPartition(fact, "p1");
        List<String> owners = new ArrayList<>();
        mockLakeDml(insert -> owners.add(ConnectContext.get().getTvrStagingOwner()));

        TaskRun run = runFirstBatch(mv);

        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(run));
        Assertions.assertEquals(List.of(run.getStatus().getStartTaskRunId()), owners);
        Assertions.assertNull(ConnectContext.get().getTvrStagingOwner());
    }

    private static MaterializedView refreshedView(String name, OlapTable fact, String refreshMode) throws Exception {
        MaterializedView mv = createMv(name, fact, refreshMode);
        mockLakeDml();
        runJob(mv, fact);
        Assertions.assertFalse(tvrBaseline(mv).isEmpty(), "the first refresh records no baseline");
        return mv;
    }

    private static TaskRun explain(MaterializedView mv, boolean force) throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Map<String, String> properties = new HashMap<>(task.getProperties());
        properties.put(TaskRun.FORCE, Boolean.toString(force));
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGH.value(), false, properties);
        StatementBase statement = MVTestBase.getAnalyzedPlan("EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName() +
                (force ? " FORCE" : ""), starRocksAssert.getCtx());
        TaskRun taskRun = taskManager.buildTaskRun(task, option);
        taskManager.getMVRefreshProcessExecPlan(taskRun, task, option, statement);
        return taskRun;
    }

    private static BaseTableInfo baseTableInfo(MaterializedView mv) {
        List<BaseTableInfo> infos = mv.getBaseTableInfos();
        Assertions.assertEquals(1, infos.size());
        return infos.get(0);
    }

    private static Set<Long> mvReferences(MaterializedView mv, OlapTable fact) {
        return new TreeSet<>(GlobalStateMgr.getCurrentState().getBookmarkManager()
                .listBookmarkIdsByHolder(db.getId(), fact.getId(), HolderId.forMv(mv.getMvId())));
    }

    private static Set<String> explainReferences(OlapTable fact) {
        return GlobalStateMgr.getCurrentState().getBookmarkManager()
                .listAllBookmarks(Optional.of(db.getId()), Optional.of(fact.getId()), Optional.empty()).stream()
                .map(Bookmark.View::getReferences)
                .flatMap(List::stream)
                .map(Reference.View::getHolderId)
                .filter(holder -> holder.startsWith("mv-explain:"))
                .collect(Collectors.toSet());
    }

    private static TransactionState insertTxnState() {
        TransactionState txnState = new TransactionState();
        txnState.setTxnCommitAttachment(new InsertTxnCommitAttachment(0));
        return txnState;
    }

    private static Object commitInfoOf(TransactionState txnState) {
        return ((InsertTxnCommitAttachment) txnState.getTxnCommitAttachment()).getIvmRefreshCommitInfo();
    }
}
