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
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.load.loadv2.IVMInsertLoadTxnCallback;
import com.starrocks.load.loadv2.InsertLoadTxnCallbackFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionException;
import com.starrocks.transaction.TransactionState;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * EXPLAIN / TRACE REFRESH plans the pct path against a snapshot it freezes for itself; the change window a refresh in
 * flight staged on the view, and the run that owns it, must come out of it untouched.
 */
public class IvmExplainPinStateIsolationTest extends MVIVMIcebergTestBase {
    private static final AtomicInteger VIEWS = new AtomicInteger();
    private static final String IN_FLIGHT = "in-flight-refresh";

    @BeforeAll
    public static void beforeClass() throws Exception {
        // As in PlanTestNoneDBBase: the async-profiler sampler can SIGSEGV the forked test JVM.
        Config.proc_profile_cpu_enable = false;
        Config.proc_profile_mem_enable = false;
        MVIVMIcebergTestBase.beforeClass();
    }

    @ParameterizedTest
    @CsvSource({"incremental, true, EXPLAIN", "auto, true, EXPLAIN", "auto, false, EXPLAIN",
            "incremental, true, TRACE TIMES", "auto, true, TRACE TIMES", "auto, false, TRACE TIMES"})
    public void testExplainOnThePctPathLeavesTheStagedWindowAlone(String mode, boolean force, String prefix)
            throws Exception {
        AtomicLong live = mockLiveVersion(1L);
        MaterializedView mv = createView(mode, false);
        seedTvrBaselineAtVersionZero(mv);
        BaseTableInfo baseTable = onlyBaseTable(mv);
        // What an incremental refresh stages right before its insert.
        refreshContext(mv).replaceTempBaseTableInfoTvrDeltaMap(IN_FLIGHT, Map.of(baseTable, TvrTableDelta.of(0L, 1L)));
        Map<BaseTableInfo, TvrVersionRange> staged = Map.copyOf(refreshContext(mv).getTempBaseTableInfoTvrDeltaMap());
        live.set(2L);
        if (!force) {
            failIncrementalPlanning();
        }
        commitRightAfterTheFreeze(live, 3L);

        Explained explained = explain(mv, prefix, force);

        Assertions.assertEquals(3L, live.get(), "the explain never reached its freeze");
        Assertions.assertEquals("PCT", refreshModeOf(explained.run()));
        Assertions.assertEquals(IN_FLIGHT, refreshContext(mv).getTempTvrOwnerStartTaskRunId());
        Assertions.assertEquals(staged, refreshContext(mv).getTempBaseTableInfoTvrDeltaMap());
        Assertions.assertEquals(Map.of(baseTable.getReadableString(), 2L),
                explained.run().getStatus().getMvTaskRunExtraMessage().getPinnedSnapshotIdMap());
        assertScansSnapshot(explained.plan(), 2L, 3L);
    }

    @Test
    public void testBatchedFallbackKeepsItsPinThroughAnExplain() throws Exception {
        AtomicLong live = mockLiveVersion(2L);
        MaterializedView mv = createView("auto", true);
        BaseTableInfo baseTable = onlyBaseTable(mv);
        failIncrementalPlanning();
        TaskRun first = firstBatch(mv);
        String job = first.getStatus().getStartTaskRunId();
        Assertions.assertEquals(job, refreshContext(mv).getTempTvrOwnerStartTaskRunId());
        Map<BaseTableInfo, TvrVersionRange> frozen = Map.copyOf(refreshContext(mv).getTempBaseTableInfoTvrDeltaMap());
        Assertions.assertEquals(2L, frozen.get(baseTable).end().orElseThrow());
        TaskRun batch = nextOf(first);
        Assertions.assertNotNull(batch, "the fallback was not batched");
        live.set(3L);

        for (String prefix : List.of("EXPLAIN", "TRACE TIMES")) {
            for (boolean force : List.of(false, true)) {
                explain(mv, prefix, force);
                String step = prefix + ", force=" + force;
                Assertions.assertEquals(job, refreshContext(mv).getTempTvrOwnerStartTaskRunId(), step);
                Assertions.assertEquals(frozen, refreshContext(mv).getTempBaseTableInfoTvrDeltaMap(), step);
            }
        }

        int batches = 1;
        for (; batch != null; batch = nextOf(batch)) {
            Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(batch));
            assertScansSnapshot(getMVTaskRunProcessor(batch).getMvTaskRunContext().getExecPlan(), 2L, 3L);
            batches++;
        }
        Assertions.assertEquals(mv.getPartitions().size(), batches);
        Assertions.assertEquals(2L, tvrBaseline(mv).get(baseTable).to().getVersion());
        Assertions.assertNull(refreshContext(mv).getTempTvrOwnerStartTaskRunId());
    }

    @Test
    public void testCommitRefusesAWindowAnotherRunStaged() throws Exception {
        MaterializedView mv = createView("incremental", false);
        seedTvrBaselineAtVersionZero(mv);
        Map<BaseTableInfo, TvrVersionRange> window = Map.of(onlyBaseTable(mv), TvrTableDelta.of(0L, 1L));

        refreshContext(mv).replaceTempBaseTableInfoTvrDeltaMap("other-run", window);
        TransactionState replaced = insertTxnState();
        TransactionException error = Assertions.assertThrows(TransactionException.class,
                () -> commitCallback(mv, "this-run").beforeCommitted(replaced));
        Assertions.assertTrue(error.getMessage().contains("other-run"), error.getMessage());
        Assertions.assertNull(commitInfoOf(replaced));

        refreshContext(mv).replaceTempBaseTableInfoTvrDeltaMap("this-run", window);
        TransactionState own = insertTxnState();
        commitCallback(mv, "this-run").beforeCommitted(own);
        Assertions.assertNotNull(commitInfoOf(own));

        // A window cleared under the run (ALTER ... ACTIVE / INACTIVE) still commits, with nothing to record.
        refreshContext(mv).clearTempBaseTableInfoTvrDeltaState();
        TransactionState cleared = insertTxnState();
        commitCallback(mv, "this-run").beforeCommitted(cleared);
        Assertions.assertNull(commitInfoOf(cleared));
    }

    @Test
    public void testIncrementalRefreshCommitsTheWindowItStaged() throws Exception {
        MaterializedView mv = createView("incremental", false);
        seedTvrBaselineAtVersionZero(mv);
        advanceTableVersionTo(1L);
        mockListTableDeltaTraitsPerVersion(1L);
        List<String> stagingOwners = new ArrayList<>();
        List<TransactionState> commits = new ArrayList<>();
        new MockUp<MVTaskRunProcessor>() {
            @Mock
            public void executePlan(Invocation invocation, ExecPlan execPlan, InsertStmt insertStmt) {
                MVTaskRunProcessor processor = invocation.getInvokedInstance();
                ConnectContext ctx = processor.getMvTaskRunContext().getCtx();
                stagingOwners.add(ctx.getTvrStagingOwner());
                TransactionState txnState = insertTxnState();
                try {
                    InsertLoadTxnCallbackFactory.of(ctx, mv.getDbId(), mv).beforeCommitted(txnState);
                } catch (TransactionException e) {
                    throw new AssertionError(e);
                }
                commits.add(txnState);
                invocation.proceed();
            }
        };

        TaskRun run = firstBatch(mv);

        Assertions.assertEquals("INCREMENTAL", refreshModeOf(run));
        Assertions.assertEquals(List.of(run.getStatus().getStartTaskRunId()), stagingOwners);
        Assertions.assertNotNull(commitInfoOf(commits.get(0)));
        Assertions.assertNull(getMVTaskRunProcessor(run).getMvTaskRunContext().getCtx().getTvrStagingOwner());
    }

    private record Explained(TaskRun run, ExecPlan plan) {
    }

    private MaterializedView createView(String mode, boolean partitioned) throws Exception {
        String name = "mv_explain_pin_" + VIEWS.incrementAndGet();
        String source = partitioned ? "`partitioned_db`.`t1`" : "`unpartitioned_db`.`t0`";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name +
                (partitioned ? " PARTITION BY `date`" : "") +
                " REFRESH DEFERRED MANUAL PROPERTIES (\"refresh_mode\" = \"incremental\"" +
                (partitioned ? ", \"partition_refresh_number\" = \"1\"" : "") +
                ") AS SELECT id, data, date FROM `iceberg0`." + source);
        MaterializedView mv = getMv(name);
        // AUTO is not accepted in new DDL; promote the analyzed incremental view as the other AUTO fixtures do.
        if (mode.equals("auto")) {
            mv.setCurrentRefreshMode(MaterializedView.RefreshMode.AUTO);
        }
        return mv;
    }

    private static AtomicLong mockLiveVersion(long initial) {
        AtomicLong live = new AtomicLong(initial);
        new MockUp<MockIcebergMetadata>() {
            @Mock
            public TvrTableSnapshot getCurrentTvrSnapshot(String dbName, Table table) {
                return TvrTableSnapshot.of(live.get());
            }
        };
        return live;
    }

    private static void commitRightAfterTheFreeze(AtomicLong live, long version) {
        new MockUp<MVPCTRefreshProcessor>() {
            @Mock
            public void freezeSnapshotBeforePartitionAlign(Invocation invocation) {
                invocation.proceed();
                live.set(version);
            }
        };
    }

    private static void failIncrementalPlanning() {
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                throw new SemanticException("incremental planning failed (simulated)");
            }
        };
    }

    private static Explained explain(MaterializedView mv, String prefix, boolean force) {
        TaskManager manager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskOf(mv);
        Map<String, String> properties = new HashMap<>(task.getProperties());
        properties.put(TaskRun.IS_TEST, "true");
        properties.put(TaskRun.FORCE, Boolean.toString(force));
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGH.value(), false, properties);
        StatementBase statement = getAnalyzedPlan(prefix + " REFRESH MATERIALIZED VIEW " + mv.getName() +
                (force ? " FORCE" : ""), connectContext);
        TaskRun run = manager.buildTaskRun(task, option);
        MVRefreshProcessor.ProcessExecPlan plan = manager.getMVRefreshProcessExecPlan(run, task, option, statement);
        Assertions.assertNotNull(plan.execPlan(), "the explain produced no refresh plan");
        return new Explained(run, plan.execPlan());
    }

    private static TaskRun firstBatch(MaterializedView mv) throws Exception {
        Task task = taskOf(mv);
        task.getProperties().put(TaskRun.IS_TEST, "true");
        TaskRun run = TaskRunBuilder.newBuilder(task).build();
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(run));
        return run;
    }

    private static Constants.TaskRunState executeBatch(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        return taskRun.executeTaskRun();
    }

    private static TaskRun nextOf(TaskRun executed) {
        MVRefreshProcessor processor = getMVTaskRunProcessor(executed).getMVRefreshProcessor();
        if (processor instanceof MVHybridRefreshProcessor hybrid) {
            return hybrid.getCurrentProcessor().getNextTaskRun();
        }
        return processor.getNextTaskRun();
    }

    private static Task taskOf(MaterializedView mv) {
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Assertions.assertNotNull(task);
        return task;
    }

    private static void assertScansSnapshot(ExecPlan plan, long pinned, long live) {
        String explain = plan.getExplainString(TExplainLevel.NORMAL);
        Assertions.assertTrue(explain.contains("Snapshot@(" + pinned + ")"), explain);
        Assertions.assertFalse(explain.contains("Snapshot@(" + live + ")"), explain);
    }

    private static MaterializedView.AsyncRefreshContext refreshContext(MaterializedView mv) {
        return mv.getRefreshScheme().getAsyncRefreshContext();
    }

    private static Map<BaseTableInfo, TvrVersionRange> tvrBaseline(MaterializedView mv) {
        return refreshContext(mv).getBaseTableInfoTvrVersionRangeMap();
    }

    private static BaseTableInfo onlyBaseTable(MaterializedView mv) {
        List<BaseTableInfo> infos = mv.getBaseTableInfos();
        Assertions.assertEquals(1, infos.size());
        return infos.get(0);
    }

    private static String refreshModeOf(TaskRun run) {
        return run.getStatus().getMvTaskRunExtraMessage().getRefreshMode();
    }

    private static IVMInsertLoadTxnCallback commitCallback(MaterializedView mv, String stagingOwner) {
        return new IVMInsertLoadTxnCallback(mv.getDbId(), mv.getId(), stagingOwner);
    }

    private static Object commitInfoOf(TransactionState txnState) {
        return ((InsertTxnCommitAttachment) txnState.getTxnCommitAttachment()).getIvmRefreshCommitInfo();
    }
}
