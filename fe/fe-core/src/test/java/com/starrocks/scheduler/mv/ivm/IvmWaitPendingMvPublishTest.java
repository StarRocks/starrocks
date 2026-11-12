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
import com.starrocks.scheduler.TaskRun;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/**
 * An incremental run must not plan on the view while one of the view's transactions is committed but not yet
 * visible: the merge reads the view as it was before that transaction, whose window is already recorded.
 */
public class IvmWaitPendingMvPublishTest extends MVIVMIcebergTestBase {
    private static final String MV_QUERY =
            "SELECT id, count(*) AS cnt FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id";
    private static final long DEFAULT_WAIT_MS = Config.mv_incremental_refresh_wait_visible_timeout_ms;

    @AfterEach
    public void restoreWaitTimeout() {
        Config.mv_incremental_refresh_wait_visible_timeout_ms = DEFAULT_WAIT_MS;
    }

    @Test
    public void testPlansOnlyAfterThePendingTransactionIsVisible() throws Exception {
        MaterializedView mv = incrementalMvWithANewBaseVersion();
        AtomicInteger checks = pendingFor(mv, 3, check -> { });
        TaskRun run = buildMVTaskRun(mv, "test");

        initAndExecuteTaskRun(run);

        Assertions.assertEquals(4, checks.get());
        Assertions.assertNotNull(getMVTaskRunProcessor(run).getMvTaskRunContext().getExecPlan());
    }

    @Test
    public void testFailsWithoutPlanningWhenTheTransactionStaysPending() throws Exception {
        MaterializedView mv = incrementalMvWithANewBaseVersion();
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        pendingFor(mv, Integer.MAX_VALUE, check -> { });
        Map<BaseTableInfo, TvrVersionRange> baselineBefore = tvrBaseline(mv);
        TaskRun run = buildMVTaskRun(mv, "test");

        Exception failure = Assertions.assertThrows(Exception.class, () -> initAndExecuteTaskRun(run));

        Assertions.assertTrue(ExceptionUtils.indexOfType(failure, MvPublishPendingException.class) >= 0,
                ExceptionUtils.getStackTrace(failure));
        Assertions.assertNull(getMVTaskRunProcessor(run).getMvTaskRunContext().getExecPlan());
        Assertions.assertEquals(baselineBefore, tvrBaseline(mv));
    }

    @Test
    public void testStopsWaitingWhenTheRunIsKilled() throws Exception {
        MaterializedView mv = incrementalMvWithANewBaseVersion();
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 60_000;
        TaskRun run = buildMVTaskRun(mv, "test");
        AtomicInteger checks = pendingFor(mv, Integer.MAX_VALUE, check -> {
            if (check == 3) {
                run.kill();
            }
        });

        Exception failure = Assertions.assertThrows(Exception.class, () -> initAndExecuteTaskRun(run));

        assertStoppedWaiting(failure, checks, run);
    }

    @Test
    public void testStopsWaitingWhenTheThreadIsInterrupted() throws Exception {
        MaterializedView mv = incrementalMvWithANewBaseVersion();
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 60_000;
        TaskRun run = buildMVTaskRun(mv, "test");
        AtomicInteger checks = pendingFor(mv, Integer.MAX_VALUE, check -> {
            if (check == 3) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            Exception failure = Assertions.assertThrows(Exception.class, () -> initAndExecuteTaskRun(run));

            assertStoppedWaiting(failure, checks, run);
        } finally {
            Thread.interrupted();
        }
    }

    private static void assertStoppedWaiting(Exception failure, AtomicInteger checks, TaskRun run) {
        Assertions.assertTrue(ExceptionUtils.indexOfType(failure, MvPublishPendingException.class) >= 0,
                ExceptionUtils.getStackTrace(failure));
        Assertions.assertTrue(checks.get() <= 4, "kept waiting for " + checks.get() + " checks");
        Assertions.assertNull(getMVTaskRunProcessor(run).getMvTaskRunContext().getExecPlan());
    }

    private MaterializedView incrementalMvWithANewBaseVersion() throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(MV_QUERY, "incremental");
        seedTvrBaselineAtVersionZero(mv);
        advanceTableVersionTo(1L);
        mockListTableDeltaTraitsPerVersion(0L, 0L);
        return mv;
    }

    /** Reports a committed transaction on {@code mv} for its first {@code times} checks, handing each check to onCheck. */
    private static AtomicInteger pendingFor(MaterializedView mv, int times, IntConsumer onCheck) {
        AtomicInteger checks = new AtomicInteger();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public boolean existCommittedTxns(Invocation invocation, Long dbId, Long tableId, Long partitionId) {
                if (dbId == mv.getDbId() && tableId != null && tableId == mv.getId() && partitionId == null) {
                    int check = checks.incrementAndGet();
                    onCheck.accept(check);
                    return check <= times;
                }
                return invocation.proceed();
            }
        };
        return checks;
    }

    private static Map<BaseTableInfo, TvrVersionRange> tvrBaseline(MaterializedView mv) {
        return Map.copyOf(mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap());
    }
}
