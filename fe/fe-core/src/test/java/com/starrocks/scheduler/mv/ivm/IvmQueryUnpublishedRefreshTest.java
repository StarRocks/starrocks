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
import com.starrocks.load.loadv2.IVMInsertLoadTxnCallback;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Queries take an MV partition as fresh when the version map records the base versions it was refreshed to. An
 * incremental refresh whose insert returns before its write is visible must not record them until it is.
 */
public class IvmQueryUnpublishedRefreshTest extends IvmIcebergRefreshJobTestBase {
    private static final long DEFAULT_WAIT_MS = Config.mv_incremental_refresh_wait_visible_timeout_ms;
    private static int views = 0;

    @AfterEach
    public void restoreWaitTimeout() {
        Config.mv_incremental_refresh_wait_visible_timeout_ms = DEFAULT_WAIT_MS;
    }

    @Test
    public void testLeavesTheVersionsOfAWriteThatStaysUnpublishedToALaterRefresh() throws Exception {
        MaterializedView mv = refreshedMv();
        loadFact();
        Map<String, String> recordedBefore = recordedVersions(mv);
        long freshnessBefore = mv.getRefreshScheme().getLastFreshnessConfirmedAt();
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        writesStayCommitted(mv, new AtomicBoolean(true));

        TaskRun run = newRun(mv);

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(run));
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(run));
        Assertions.assertEquals(recordedBefore, recordedVersions(mv));
        Assertions.assertEquals(freshnessBefore, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
    }

    @Test
    public void testRecordsTheVersionsOnceTheWriteBecomesVisible() throws Exception {
        MaterializedView mv = refreshedMv();
        loadFact();
        Map<String, String> recordedBefore = recordedVersions(mv);
        long freshnessBefore = mv.getRefreshScheme().getLastFreshnessConfirmedAt();
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 60_000;
        writesBecomeVisibleAfter(mv, 300);

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));

        Assertions.assertNotEquals(recordedBefore, recordedVersions(mv));
        Assertions.assertTrue(mv.getRefreshScheme().getLastFreshnessConfirmedAt() > freshnessBefore);
    }

    @Test
    public void testARefreshThatReadsNoChangeRecordsTheVersionsAnUnpublishedWriteLeft() throws Exception {
        MaterializedView mv = refreshedMv();
        loadFact();
        Map<String, String> recordedBefore = recordedVersions(mv);
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        AtomicBoolean unpublished = new AtomicBoolean(true);
        writesStayCommitted(mv, unpublished);
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));
        Assertions.assertEquals(recordedBefore, recordedVersions(mv));
        commitStagedWindow(mv);
        unpublished.set(false);

        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(newRun(mv)));

        Assertions.assertNotEquals(recordedBefore, recordedVersions(mv));
    }

    private MaterializedView refreshedMv() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_query_unpublished_" + (++views), "incremental");
        runJob(mv);
        return mv;
    }

    // The faked insert commits no transaction, so its commit callback is run here to move the view's baseline.
    private static void commitStagedWindow(MaterializedView mv) throws Exception {
        IVMInsertLoadTxnCallback callback = new IVMInsertLoadTxnCallback(mv.getDbId(), mv.getId());
        TransactionState txn = insertTxnState();
        callback.beforeCommitted(txn);
        callback.afterCommitted(txn);
    }

    /** A run that confirms the view fresh as of now once it succeeds. */
    private static TaskRun newRun(MaterializedView mv) {
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
        task.getProperties().put(TaskRun.IS_TEST, "true");
        return TaskRunBuilder.newBuilder(task)
                .properties(Map.of(TaskRun.MV_FRESHNESS_BASELINE_TIME, String.valueOf(System.currentTimeMillis())))
                .build();
    }

    private static Map<String, String> recordedVersions(MaterializedView mv) {
        Map<String, String> versions = new TreeMap<>();
        for (Map.Entry<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> entry :
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().entrySet()) {
            if (entry.getKey().getTableName().equals(FACT)) {
                entry.getValue().forEach((partition, info) -> versions.put(partition, info.toString()));
            }
        }
        return versions;
    }

    /** Reports the view's writes as COMMITTED, never published, while {@code unpublished} holds. */
    private static void writesStayCommitted(MaterializedView mv, AtomicBoolean unpublished) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long txnId) {
                if (dbId != mv.getDbId() || !unpublished.get()) {
                    return invocation.proceed();
                }
                TransactionState committed = new TransactionState();
                committed.setTransactionStatus(TransactionStatus.COMMITTED);
                return committed;
            }
        };
    }

    /** Reports the view's writes as COMMITTED, then publishes them {@code delayMs} later. */
    private static void writesBecomeVisibleAfter(MaterializedView mv, long delayMs) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long txnId) {
                if (dbId != mv.getDbId()) {
                    return invocation.proceed();
                }
                TransactionState committed = new TransactionState();
                committed.setTransactionStatus(TransactionStatus.COMMITTED);
                Thread publisher = new Thread(() -> {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    committed.setTransactionStatus(TransactionStatus.VISIBLE);
                    committed.notifyVisible();
                });
                publisher.setDaemon(true);
                publisher.start();
                return committed;
            }
        };
    }
}
