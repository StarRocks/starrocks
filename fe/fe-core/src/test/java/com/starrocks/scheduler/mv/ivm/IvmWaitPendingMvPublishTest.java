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
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/**
 * An incremental run must not plan on the view while one of the view's transactions is committed but not yet
 * visible: the merge reads the view as it was before that transaction, whose window is already recorded.
 */
public class IvmWaitPendingMvPublishTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_wait_pending_mv_publish";
    private static final AtomicInteger VIEWS = new AtomicInteger();
    private static final long DEFAULT_WAIT_MS = Config.mv_incremental_refresh_wait_visible_timeout_ms;

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @AfterEach
    public void restoreWaitTimeout() {
        Config.mv_incremental_refresh_wait_visible_timeout_ms = DEFAULT_WAIT_MS;
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testPlansOnlyAfterThePendingTransactionIsVisible(String refreshMode) throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = refreshedMv(fact, refreshMode);
        loadPartition(fact, "p1");
        AtomicInteger pendingChecks = pendingFor(mv, 3, check -> { });
        AtomicInteger checksBeforeWrite = new AtomicInteger(-1);
        mockLakeDml(insertStmt -> checksBeforeWrite.compareAndSet(-1, pendingChecks.get()));

        TaskRun run = newRun(mv);

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(run));
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(run));
        Assertions.assertTrue(checksBeforeWrite.get() > 3,
                "the run wrote after " + checksBeforeWrite.get() + " checks, while the transaction was pending");
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testFailsWithoutWritingWhenTheTransactionStaysPending(String refreshMode) throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = refreshedMv(fact, refreshMode);
        loadPartition(fact, "p1");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        pendingFor(mv, Integer.MAX_VALUE, check -> { });
        Map<BaseTableInfo, TvrVersionRange> baselineBefore = tvrBaseline(mv);
        int scansBefore = SCANNED_VERSIONS.size();

        TaskRun run = newRun(mv);
        Exception failure = Assertions.assertThrows(Exception.class, () -> executeBatch(run));

        Assertions.assertTrue(ExceptionUtils.indexOfType(failure, MvPublishPendingException.class) >= 0,
                ExceptionUtils.getStackTrace(failure));
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(run));
        Assertions.assertEquals(scansBefore, SCANNED_VERSIONS.size(), "the run wrote the view");
        Assertions.assertEquals(baselineBefore, tvrBaseline(mv));
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testStopsWaitingWhenTheRunIsKilled(String refreshMode) throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = refreshedMv(fact, refreshMode);
        loadPartition(fact, "p1");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 60_000;
        TaskRun run = newRun(mv);
        AtomicInteger checks = pendingFor(mv, Integer.MAX_VALUE, check -> {
            if (check == 3) {
                run.kill();
            }
        });
        int scansBefore = SCANNED_VERSIONS.size();

        Exception failure = Assertions.assertThrows(Exception.class, () -> executeBatch(run));

        assertStoppedWaiting(failure, checks, run, scansBefore);
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testStopsWaitingWhenTheThreadIsInterrupted(String refreshMode) throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = refreshedMv(fact, refreshMode);
        loadPartition(fact, "p1");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 60_000;
        TaskRun run = newRun(mv);
        AtomicInteger checks = pendingFor(mv, Integer.MAX_VALUE, check -> {
            if (check == 3) {
                Thread.currentThread().interrupt();
            }
        });
        int scansBefore = SCANNED_VERSIONS.size();

        try {
            Exception failure = Assertions.assertThrows(Exception.class, () -> executeBatch(run));

            assertStoppedWaiting(failure, checks, run, scansBefore);
        } finally {
            Thread.interrupted();
        }
    }

    private static void assertStoppedWaiting(Exception failure, AtomicInteger checks, TaskRun run, int scansBefore) {
        Assertions.assertTrue(ExceptionUtils.indexOfType(failure, MvPublishPendingException.class) >= 0,
                ExceptionUtils.getStackTrace(failure));
        Assertions.assertTrue(checks.get() <= 4, "kept waiting for " + checks.get() + " checks");
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(run));
        Assertions.assertEquals(scansBefore, SCANNED_VERSIONS.size(), "the run wrote the view");
    }

    /** Reports a committed transaction on {@code mv} for its first {@code times} checks, handing each check to onCheck. */
    private static AtomicInteger pendingFor(MaterializedView mv, int times, IntConsumer onCheck) {
        AtomicInteger checks = new AtomicInteger();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public boolean existCommittedTxns(Invocation invocation, Long dbId, Long tableId, Long partitionId) {
                if (dbId == db.getId() && tableId != null && tableId == mv.getId() && partitionId == null) {
                    int check = checks.incrementAndGet();
                    onCheck.accept(check);
                    return check <= times;
                }
                return invocation.proceed();
            }
        };
        return checks;
    }

    private static OlapTable newFact() throws Exception {
        OlapTable fact = createFact("fact_wait_publish_" + VIEWS.incrementAndGet());
        loadEveryPartition(fact);
        return fact;
    }

    private static MaterializedView refreshedMv(OlapTable fact, String refreshMode) throws Exception {
        MaterializedView mv = createMv("mv_wait_publish_" + VIEWS.get(), fact, refreshMode);
        mockLakeDml();
        runJob(mv, fact);
        return mv;
    }

    private static TaskRun newRun(MaterializedView mv) {
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
        task.getProperties().put(TaskRun.IS_TEST, "true");
        return TaskRunBuilder.newBuilder(task).properties(Map.of()).build();
    }
}
