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
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Exercises refresh planning and the real transaction manager with the upstream Iceberg fixture. */
public class IvmExplainRefreshIsolationTest extends MVIVMIcebergTestBase {
    private static final AtomicInteger VIEWS = new AtomicInteger();
    private static final String MV_QUERY =
            "SELECT id, count(*) AS cnt FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id";

    @ParameterizedTest
    @CsvSource({"incremental, EXPLAIN, NORMAL", "auto, EXPLAIN, NORMAL",
            "incremental, TRACE TIMES, NORMAL", "auto, TRACE TIMES, NORMAL",
            "incremental, EXPLAIN ANALYZE, NORMAL", "auto, EXPLAIN ANALYZE, NORMAL",
            "incremental, EXPLAIN, ANALYZE", "auto, EXPLAIN, ANALYZE",
            "incremental, TRACE TIMES, ANALYZE", "auto, TRACE TIMES, ANALYZE",
            "incremental, EXPLAIN ANALYZE, ANALYZE", "auto, EXPLAIN ANALYZE, ANALYZE"})
    public void testRepeatedExplainDoesNotStartTransactions(String mode, String prefix, String defaultLevel)
            throws Exception {
        MaterializedView mv = viewWithANewBaseVersion(mode);
        Map<BaseTableInfo, TvrVersionRange> baseline = baseline(mv);
        Set<String> transactions = transactionIds(mv);
        String previousLevel = Config.query_explain_level;
        try {
            Config.query_explain_level = defaultLevel;
            for (int i = 0; i < 3; i++) {
                TaskRun run = explain(mv, prefix);
                Assertions.assertEquals("INCREMENTAL", run.getStatus().getMvTaskRunExtraMessage().getRefreshMode());
                Assertions.assertEquals(transactions, transactionIds(mv),
                        "EXPLAIN must not create running or finished transactions");
                Assertions.assertEquals(baseline, baseline(mv));
                Assertions.assertTrue(mv.getRefreshScheme().getAsyncRefreshContext()
                        .getTempBaseTableInfoTvrDeltaMap().isEmpty());
            }
        } finally {
            Config.query_explain_level = previousLevel;
        }
    }

    @ParameterizedTest
    @CsvSource({"incremental, NORMAL", "auto, NORMAL", "incremental, ANALYZE", "auto, ANALYZE"})
    public void testExplainPlanningFailureDoesNotStartTransactions(String mode, String defaultLevel) throws Exception {
        MaterializedView mv = viewWithANewBaseVersion(mode);
        Map<BaseTableInfo, TvrVersionRange> baseline = baseline(mv);
        Set<String> transactions = transactionIds(mv);
        AtomicInteger attempts = new AtomicInteger();
        failInsertPlanning(attempts, new AtomicLong());
        String previousLevel = Config.query_explain_level;
        try {
            Config.query_explain_level = defaultLevel;
            for (int i = 0; i < 3; i++) {
                if (mode.equals("auto")) {
                    TaskRun run = explain(mv, "EXPLAIN");
                    Assertions.assertEquals("PCT", run.getStatus().getMvTaskRunExtraMessage().getRefreshMode());
                } else {
                    DmlException error = Assertions.assertThrows(DmlException.class, () -> explain(mv, "EXPLAIN"));
                    Assertions.assertTrue(error.getMessage().contains("IVM insert planning failed"), error.getMessage());
                }
                Assertions.assertEquals(i + 1, attempts.get());
                Assertions.assertEquals(transactions, transactionIds(mv),
                        "failed EXPLAIN must not even create an aborted transaction");
                Assertions.assertEquals(baseline, baseline(mv));
                // Upstream AUTO fallback still freezes a shared PCT window; its isolation is a separate issue.
                if (mode.equals("incremental")) {
                    Assertions.assertTrue(mv.getRefreshScheme().getAsyncRefreshContext()
                            .getTempBaseTableInfoTvrDeltaMap().isEmpty());
                }
            }
        } finally {
            Config.query_explain_level = previousLevel;
        }
    }

    @Test
    public void testOrdinaryRefreshStillAbortsTransactionOnPlanningFailure() throws Exception {
        MaterializedView mv = viewWithANewBaseVersion("incremental");
        Map<BaseTableInfo, TvrVersionRange> baseline = baseline(mv);
        AtomicInteger attempts = new AtomicInteger();
        AtomicLong txnId = new AtomicLong();
        failInsertPlanning(attempts, txnId);
        TaskRun run = buildMVTaskRun(mv, "test");
        run.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        MVTaskRunProcessor processor = (MVTaskRunProcessor) run.getProcessor();
        processor.prepare(run.buildTaskRunContext());
        SemanticException error = Assertions.assertThrows(SemanticException.class, () -> processor.getMVRefreshProcessor()
                .getProcessExecPlan(processor.getMvTaskRunContext()));
        Assertions.assertTrue(error.getMessage().contains("IVM insert planning failed"), error.getMessage());
        Assertions.assertEquals(1, attempts.get());
        Assertions.assertTrue(txnId.get() > 0, "ordinary refresh must start its INSERT transaction");
        TransactionState txn = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionState(mv.getDbId(), txnId.get());
        Assertions.assertNotNull(txn);
        Assertions.assertEquals(TransactionStatus.ABORTED, txn.getTransactionStatus());
        Assertions.assertEquals(baseline, baseline(mv));
    }

    private MaterializedView viewWithANewBaseVersion(String mode) throws Exception {
        String name = "test_ivm_explain_" + VIEWS.incrementAndGet();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name +
                " REFRESH DEFERRED MANUAL PROPERTIES (\"refresh_mode\" = \"incremental\") AS " + MV_QUERY);
        MaterializedView mv = getMv(name);
        // AUTO is not accepted in new DDL; promote the analyzed incremental view as existing AUTO fixtures do.
        if (mode.equals("auto")) {
            mv.setCurrentRefreshMode(MaterializedView.RefreshMode.AUTO);
        }
        seedTvrBaselineAtVersionZero(mv);
        advanceTableVersionTo(1L);
        mockListTableDeltaTraitsPerVersion(1L);
        return mv;
    }

    private static Map<BaseTableInfo, TvrVersionRange> baseline(MaterializedView mv) {
        return Map.copyOf(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap());
    }

    private static Set<String> transactionIds(MaterializedView mv) throws Exception {
        var transactions = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getDatabaseTransactionMgr(mv.getDbId());
        Set<String> ids = new TreeSet<>();
        for (boolean running : List.of(true, false)) {
            transactions.getTxnStateInfoList(running, Integer.MAX_VALUE).forEach(row -> ids.add(row.get(0)));
        }
        return ids;
    }

    private static TaskRun explain(MaterializedView mv, String prefix) throws Exception {
        TaskManager manager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = manager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Map<String, String> properties = new HashMap<>(task.getProperties());
        properties.put(TaskRun.IS_TEST, "true");
        ExecuteOption option = new ExecuteOption(Constants.TaskRunPriority.HIGH.value(), false, properties);
        StatementBase statement = getAnalyzedPlan(prefix + " REFRESH MATERIALIZED VIEW " + mv.getName(), connectContext);
        TaskRun run = manager.buildTaskRun(task, option);
        MVRefreshProcessor.ProcessExecPlan plan = manager.getMVRefreshProcessExecPlan(run, task, option, statement);
        Assertions.assertNotNull(plan.execPlan(), "the delta must produce a refresh plan");
        return run;
    }

    private static void failInsertPlanning(AtomicInteger attempts, AtomicLong txnId) {
        new MockUp<InsertPlanner>() {
            @Mock
            public ExecPlan plan(Invocation invocation, InsertStmt insert, ConnectContext ctx) {
                ExecPlan plan = invocation.proceed(insert, ctx);
                if (ctx.getSessionVariable().isEnableIVMRefresh()) {
                    attempts.incrementAndGet();
                    txnId.set(insert.getTxnId());
                    throw new SemanticException("IVM insert planning failed (simulated)");
                }
                return plan;
            }
        };
    }
}
