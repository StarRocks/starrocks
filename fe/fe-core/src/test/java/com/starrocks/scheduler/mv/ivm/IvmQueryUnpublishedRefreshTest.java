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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Queries take an MV partition as fresh when the version map records the base versions it was refreshed to. An
 * incremental refresh whose insert returns before its write is visible must not record them until it is.
 */
public class IvmQueryUnpublishedRefreshTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_query_unpublished_refresh";
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
    public void testLeavesTheVersionsOfAWriteThatStaysUnpublishedToALaterRefresh(String refreshMode)
            throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = freshTransparentMv(fact, refreshMode);
        long recordedBefore = recordedVersion(mv, fact, "p2");
        long freshnessBefore = mv.getRefreshScheme().getLastFreshnessConfirmedAt();
        loadPartition(fact, "p2");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        writesStayCommitted(mv, new AtomicBoolean(true));

        TaskRun run = newRun(mv);

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(run));
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(run));
        Assertions.assertEquals(recordedBefore, recordedVersion(mv, fact, "p2"));
        Assertions.assertEquals(freshnessBefore, mv.getRefreshScheme().getLastFreshnessConfirmedAt());
        ExecPlan plan = planOf(mv);
        Assertions.assertEquals(Set.of("p1", "p3", "p4"), scannedPartitions(plan, mv));
        Assertions.assertEquals(Set.of("p2"), scannedPartitions(plan, fact));
    }

    /** A publish turns the transaction VISIBLE before it updates the partitions, and releases the waiters after. */
    @Test
    public void testAVisibleStatusIsNotEnoughBeforeThePublishUpdatesThePartitions() throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = freshTransparentMv(fact, "incremental");
        long recordedBefore = recordedVersion(mv, fact, "p2");
        loadPartition(fact, "p2");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long txnId) {
                TransactionState txn = invocation.proceed();
                if (txn == null || !txn.getTableIdList().contains(mv.getId())) {
                    return txn;
                }
                TransactionState visible = new TransactionState(txn);
                visible.setTransactionStatus(TransactionStatus.VISIBLE);
                return visible;
            }
        };

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));

        Assertions.assertEquals(recordedBefore, recordedVersion(mv, fact, "p2"));
    }

    @Test
    public void testRecordsTheVersionsOnceTheWriteBecomesVisible() throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = freshTransparentMv(fact, "incremental");
        loadPartition(fact, "p2");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 60_000;
        long freshnessBefore = mv.getRefreshScheme().getLastFreshnessConfirmedAt();
        writesBecomeVisibleAfter(mv, 300);

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));

        Assertions.assertEquals(livePartitionVersion(fact, "p2"), recordedVersion(mv, fact, "p2"));
        Assertions.assertTrue(mv.getRefreshScheme().getLastFreshnessConfirmedAt() > freshnessBefore);
        ExecPlan plan = planOf(mv);
        Assertions.assertEquals(new TreeSet<>(PARTITIONS), scannedPartitions(plan, mv));
        Assertions.assertNull(scannedPartitions(plan, fact));
    }

    @Test
    public void testTheNextRefreshRecordsTheVersionsAnUnpublishedWriteLeft() throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = freshTransparentMv(fact, "incremental");
        loadPartition(fact, "p2");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        AtomicBoolean unpublished = new AtomicBoolean(true);
        writesStayCommitted(mv, unpublished);
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));
        promoteStagedDeltas(mv);
        unpublished.set(false);
        loadPartition(fact, "p3");

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));

        Assertions.assertEquals(livePartitionVersion(fact, "p2"), recordedVersion(mv, fact, "p2"));
        Assertions.assertEquals(livePartitionVersion(fact, "p3"), recordedVersion(mv, fact, "p3"));
        ExecPlan plan = planOf(mv);
        Assertions.assertEquals(new TreeSet<>(PARTITIONS), scannedPartitions(plan, mv));
        Assertions.assertNull(scannedPartitions(plan, fact));
    }

    @Test
    public void testARefreshThatReadsNoChangeRecordsTheVersionsAnUnpublishedWriteLeft() throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = freshTransparentMv(fact, "incremental");
        loadPartition(fact, "p2");
        Config.mv_incremental_refresh_wait_visible_timeout_ms = 200;
        AtomicBoolean unpublished = new AtomicBoolean(true);
        writesStayCommitted(mv, unpublished);
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));
        promoteStagedDeltas(mv);
        unpublished.set(false);

        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(newRun(mv)));

        Assertions.assertEquals(livePartitionVersion(fact, "p2"), recordedVersion(mv, fact, "p2"));
        ExecPlan plan = planOf(mv);
        Assertions.assertEquals(new TreeSet<>(PARTITIONS), scannedPartitions(plan, mv));
        Assertions.assertNull(scannedPartitions(plan, fact));
    }

    /**
     * A write whose transaction is VISIBLE already passes the next run's wait for committed writes, while its publish
     * still holds the MV and updates its partitions: the run must read the MV only after that.
     */
    @Test
    public void testTheNextRunReadsTheMvAfterAPublishThatHoldsIt() throws Exception {
        OlapTable fact = newFact();
        MaterializedView mv = freshTransparentMv(fact, "incremental");
        loadPartition(fact, "p2");
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));
        promoteStagedDeltas(mv);
        loadPartition(fact, "p2");
        PhysicalPartition mvPartition = mv.getPartition("p2").getDefaultPhysicalPartition();
        long publishedVersion = mvPartition.getVisibleVersion() + 1;
        AtomicReference<Thread> publisher = new AtomicReference<>();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public boolean existCommittedTxns(Invocation invocation, Long dbId, Long tableId, Long partitionId)
                    throws InterruptedException {
                if (tableId == null || tableId != mv.getId() || partitionId != null || publisher.get() != null) {
                    return invocation.proceed();
                }
                CountDownLatch locked = new CountDownLatch(1);
                publisher.set(new Thread(() -> publishHoldingTheMv(mv, mvPartition, publishedVersion, locked)));
                publisher.get().start();
                locked.await();
                return false;
            }
        };
        int scansBefore = SCANNED_VERSIONS.size();

        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, executeBatch(newRun(mv)));
        publisher.get().join();

        Assertions.assertEquals(publishedVersion,
                SCANNED_VERSIONS.get(scansBefore).get(mv.getName()).get(mvPartition.getId()));
    }

    private static void publishHoldingTheMv(MaterializedView mv, PhysicalPartition partition, long version,
                                            CountDownLatch locked) {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(mv.getDbId(), mv.getId(), LockType.WRITE);
        try {
            locked.countDown();
            Thread.sleep(500);
            partition.setVisibleVersion(version, System.currentTimeMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            locker.unLockTableWithIntensiveDbLock(mv.getDbId(), mv.getId(), LockType.WRITE);
        }
    }

    private static OlapTable newFact() throws Exception {
        OlapTable fact = createFact("fact_query_unpublished_" + VIEWS.incrementAndGet());
        loadEveryPartition(fact);
        return fact;
    }

    private static MaterializedView freshTransparentMv(OlapTable fact, String refreshMode) throws Exception {
        MaterializedView mv = createMv("mv_query_unpublished_" + VIEWS.get(), fact, refreshMode);
        mockLakeDml();
        runJob(mv, fact);
        starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW " + mv.getName() +
                " SET ('transparent_mv_rewrite_mode' = 'true')");
        return mv;
    }

    /** A run that confirms the view fresh as of now once it succeeds. */
    private static TaskRun newRun(MaterializedView mv) {
        Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(TaskBuilder.getMvTaskName(mv.getId()));
        task.getProperties().put(TaskRun.IS_TEST, "true");
        return TaskRunBuilder.newBuilder(task)
                .properties(Map.of(TaskRun.MV_FRESHNESS_BASELINE_TIME, String.valueOf(System.currentTimeMillis())))
                .build();
    }

    /** Reports the transactions writing {@code mv} as COMMITTED, never visible, while {@code unpublished} holds. */
    private static void writesStayCommitted(MaterializedView mv, AtomicBoolean unpublished) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long txnId) {
                TransactionState txn = invocation.proceed();
                if (txn == null || !unpublished.get() || !txn.getTableIdList().contains(mv.getId())) {
                    return txn;
                }
                return committedCopyOf(txn);
            }
        };
    }

    /** Reports the transactions writing {@code mv} as COMMITTED, then publishes them {@code delayMs} later. */
    private static void writesBecomeVisibleAfter(MaterializedView mv, long delayMs) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(Invocation invocation, long dbId, long txnId) {
                TransactionState txn = invocation.proceed();
                if (txn == null || !txn.getTableIdList().contains(mv.getId())) {
                    return txn;
                }
                TransactionState committed = committedCopyOf(txn);
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

    // The copy shares the latch of the transaction, which the faked DML never publishes.
    private static TransactionState committedCopyOf(TransactionState txn) {
        TransactionState committed = new TransactionState(txn);
        committed.setTransactionStatus(TransactionStatus.COMMITTED);
        return committed;
    }

    private static ExecPlan planOf(MaterializedView mv) throws Exception {
        return UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), "SELECT k, total FROM " + mv.getName()).second;
    }

    /** The partitions of {@code table} the plan scans, null if it does not scan the table. */
    private static Set<String> scannedPartitions(ExecPlan plan, OlapTable table) {
        Set<String> partitions = null;
        for (ScanNode node : plan.getScanNodes()) {
            if (node instanceof OlapScanNode olapScanNode && olapScanNode.getOlapTable().getId() == table.getId()) {
                if (partitions == null) {
                    partitions = new TreeSet<>();
                }
                for (Long partitionId : olapScanNode.getSelectedPartitionIds()) {
                    partitions.add(table.getPartition(partitionId).getName());
                }
            }
        }
        return partitions;
    }
}
