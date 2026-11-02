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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.history.TaskRunHistory;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MergePartitionJobTest extends DDLTestBase {

    // createTwoMonthDailyPartitionedTable's daily partitions, and the monthly ones a merge turns them into
    private static final List<String> TWO_MONTH_DAILY_PARTITIONS = List.of("p20240130", "p20240131", "p20240201");
    private static final List<String> TWO_MONTH_MERGED_PARTITIONS = List.of("p202401", "p202402");

    // transactions a test started, so that they can be driven to a final state afterwards
    private final Map<Long, Long> testTxnIdToDbId = Maps.newLinkedHashMap();

    @BeforeAll
    public static void beforeAll() {
        DDLTestBase.beforeAll();
        UtFrameUtils.setUpForPersistTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @AfterEach
    public void tearDown() throws Exception {
        // transactions started by a test must reach a final state, a lingering COMMITTED one would keep the next
        // test from dropping the database in setUp
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        for (Map.Entry<Long, Long> entry : testTxnIdToDbId.entrySet()) {
            TransactionState txnState = transactionMgr.getTransactionState(entry.getValue(), entry.getKey());
            if (txnState == null) {
                continue;
            }
            if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                transactionMgr.finishTransaction(entry.getValue(), entry.getKey(), Sets.newHashSet());
            } else if (txnState.isRunning()) {
                transactionMgr.abortTransaction(entry.getValue(), entry.getKey(), "test cleanup");
            }
        }
        testTxnIdToDbId.clear();
    }

    // ---- waiting for the rewrites to be published ----------------------------------------------

    /**
     * A rewrite that is committed but not published yet must not be swapped in, and must not be thrown away
     * either: the job waits for the publish and then merges every group. Driven with real transactions on the
     * temp partitions, so that the whole lookup path is exercised rather than stubbed away.
     */
    @Test
    public void testUnpublishedRewriteWaitsThenFinishes() throws Exception {
        OlapTable table = createTwoMonthDailyPartitionedTable("t_merge_wait_unpublished");
        MergePartitionJob job = submitMergeWithRealVisibility(table);
        runToRunning(job);
        removeTasksFromScheduler(job);

        // the rewrites of both groups are committed but not published yet
        List<Long> rewriteTxnIds = new ArrayList<>();
        for (String tempPartitionName : TWO_MONTH_MERGED_PARTITIONS) {
            rewriteTxnIds.add(commitTxnWithoutPublish(table, table.getPartition(tempPartitionName, true)));
        }
        List<String> taskRunQueryIds = markAllRewriteTasksSucceeded(job);

        // the job keeps waiting instead of dropping rewrites that only miss their publish
        job.runRunningJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
        assertNotMergedYet(table);

        // the task run history can be archived while the job waits, which must not hide the finished rewrites
        TaskRunHistory taskRunHistory = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                .getTaskRunHistory();
        taskRunQueryIds.forEach(taskRunHistory::removeTaskByQueryId);

        // once published, the same job goes through
        finishTransactions(rewriteTxnIds);
        job.runRunningJob();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState(), job.errMsg);
        assertMerged(table);
    }

    /**
     * Only one group's rewrite is still unpublished. The job must not finish with that group silently left
     * behind while the other one is merged: it waits, then merges both groups.
     */
    @Test
    public void testOneUnpublishedRewriteDoesNotSkipItsGroup() throws Exception {
        OlapTable table = createTwoMonthDailyPartitionedTable("t_merge_wait_one_unpublished");
        MergePartitionJob job = submitMergeWithRealVisibility(table);
        runToRunning(job);
        removeTasksFromScheduler(job);

        long januaryTxnId = commitTxnWithoutPublish(table, table.getPartition("p202401", true));
        markAllRewriteTasksSucceeded(job);

        job.runRunningJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
        assertNotMergedYet(table);

        finishTransactions(List.of(januaryTxnId));
        job.runRunningJob();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState(), job.errMsg);
        assertMerged(table);
    }

    /**
     * A load into several source partitions of one merge group blocks that group once. The job error lists each
     * blocking transaction once, so that a duplicate neither crowds another blocker out of the reported sample nor
     * inflates the total.
     */
    @Test
    public void testLoadIntoSeveralSourcePartitionsOfAGroupIsReportedOnce() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_blocker_reported_once");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);

        // one load into two of the daily partitions that merge into the same monthly one
        long txnId = 4242L;
        Map<Long, List<Long>> conflictingTxnIds = Maps.newHashMap();
        table.getPartitions().stream().limit(2)
                .forEach(partition -> conflictingTxnIds.put(partition.getId(), List.of(txnId)));
        Mockito.doReturn(conflictingTxnIds).when(job).getConflictingTxnIds(Mockito.any());
        for (OptimizeTask task : job.getOptimizeTasks()) {
            task.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        job.run();

        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        Assertions.assertTrue(job.errMsg.contains("has ingestion during merge partitions (txn: " + txnId + ")"),
                job.errMsg);
    }

    /**
     * Creates a table with three daily partitions, which a merge into monthly partitions turns into one.
     */
    private static OlapTable createDailyPartitionedTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (k DATE, v INT) DUPLICATE KEY(k) " +
                "PARTITION BY date_trunc('day', k) (START ('2020-02-01') END ('2020-02-04') EVERY (INTERVAL 1 DAY)) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
    }

    /**
     * Submits the merge through a handler whose daemon is never started, so only the test runs the job. The returned
     * job sees every earlier load as finished and every load into its temp partition as visible.
     */
    private static MergePartitionJob submitMerge(OlapTable table) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " PARTITION BY date_trunc('month', k)", starRocksAssert.getCtx());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        SchemaChangeHandler handler = new SchemaChangeHandler();
        handler.process(stmt.getAlterClauseList(), db, table);
        Assertions.assertEquals(1, handler.getAlterJobsV2().size());
        MergePartitionJob job = Mockito.spy((MergePartitionJob) handler.getAlterJobsV2().values().iterator().next());
        Mockito.doReturn(true).when(job).isPreviousLoadFinished();
        Mockito.doReturn(Collections.emptySet()).when(job).getNotVisibleTempPartitionIds(Mockito.any());
        return job;
    }

    private static void runToRunning(MergePartitionJob job) throws Exception {
        job.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
    }

    // ---- helpers for the publish wait -----------------------------------------------------------

    /**
     * Creates a table with daily partitions across two months, which a merge into monthly partitions turns into
     * TWO_MONTH_MERGED_PARTITIONS.
     */
    private static OlapTable createTwoMonthDailyPartitionedTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (k DATE, v INT) DUPLICATE KEY(k) " +
                "PARTITION BY date_trunc('day', k) (START ('2024-01-30') END ('2024-02-02') EVERY (INTERVAL 1 DAY)) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        OlapTable table = (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
        for (String partitionName : TWO_MONTH_DAILY_PARTITIONS) {
            Assertions.assertNotNull(table.getPartition(partitionName), partitionName);
        }
        return table;
    }

    /**
     * Same as submitMerge, but the job sees the real visibility of the loads into its temp partitions.
     */
    private static MergePartitionJob submitMergeWithRealVisibility(OlapTable table) throws Exception {
        MergePartitionJob job = submitMerge(table);
        Mockito.doCallRealMethod().when(job).getNotVisibleTempPartitionIds(Mockito.any());
        return job;
    }

    /**
     * Write into the given partition and commit, without letting the transaction publish, which is the state a
     * rewrite is in when its data is durable but not visible yet.
     */
    private long commitTxnWithoutPublish(OlapTable table, Partition partition) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long txnId = transactionMgr.beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                "label_" + UUIDUtil.genUUID(), TransactionState.TxnCoordinator.fromThisFE(),
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        testTxnIdToDbId.put(txnId, db.getId());
        // register the write target the way the tablet sink does, then commit every replica
        TransactionState txnState = transactionMgr.getTransactionState(db.getId(), txnId);
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            txnState.addPartitionLoadedIndexes(table.getId(), physicalPartition.getId(),
                    Lists.newArrayList(table.getBaseIndexMetaId()));
            for (MaterializedIndex index : physicalPartition.getAllMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                        tabletCommitInfos.add(new TabletCommitInfo(tablet.getId(), replica.getBackendId()));
                    }
                }
            }
        }
        transactionMgr.commitTransaction(db.getId(), txnId, tabletCommitInfos, Lists.newArrayList(), null);
        return txnId;
    }

    private void finishTransactions(List<Long> txnIds) throws Exception {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        for (Long txnId : txnIds) {
            transactionMgr.finishTransaction(testTxnIdToDbId.get(txnId), txnId, Sets.newHashSet());
        }
    }

    /**
     * Record a successful run of every rewrite task, returning the query ids of the recorded runs.
     */
    private static List<String> markAllRewriteTasksSucceeded(MergePartitionJob job) {
        String dbName = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1)
                .getFullName();
        TaskRunHistory taskRunHistory = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                .getTaskRunHistory();
        List<String> queryIds = new ArrayList<>();
        // removeTaskFromScheduler gives up when the task run lock is busy, so take a second chance here
        removeTasksFromScheduler(job);
        for (OptimizeTask rewriteTask : job.getOptimizeTasks()) {
            TaskRunStatus status = new TaskRunStatus();
            // the history is keyed by query id, so every run needs its own
            status.setQueryId(UUIDUtil.genUUID().toString());
            status.setTaskName(rewriteTask.getName());
            status.setDbName(dbName);
            status.setState(Constants.TaskRunState.SUCCESS);
            status.setExpireTime(System.currentTimeMillis() + 3600L * 1000L);
            taskRunHistory.addHistory(status);
            queryIds.add(status.getQueryId());
        }
        return queryIds;
    }

    /**
     * The tests stand in for the rewrite inserts, keep the real ones the job submitted from running.
     */
    private static void removeTasksFromScheduler(MergePartitionJob job) {
        TaskRunManager taskRunManager = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager();
        TaskRunScheduler taskRunScheduler = taskRunManager.getTaskRunScheduler();
        for (OptimizeTask rewriteTask : job.getOptimizeTasks()) {
            if (taskRunManager.tryTaskRunLock()) {
                try {
                    taskRunScheduler.removePendingTask(rewriteTask);
                    taskRunScheduler.removeRunningTask(rewriteTask.getId());
                } finally {
                    taskRunManager.taskRunUnlock();
                }
            }
        }
    }

    private static void assertNotMergedYet(OlapTable table) {
        for (String partitionName : TWO_MONTH_DAILY_PARTITIONS) {
            Assertions.assertNotNull(table.getPartition(partitionName), partitionName);
        }
        for (String partitionName : TWO_MONTH_MERGED_PARTITIONS) {
            Assertions.assertNull(table.getPartition(partitionName), partitionName);
            Assertions.assertNotNull(table.getPartition(partitionName, true), partitionName);
        }
    }

    private static void assertMerged(OlapTable table) {
        for (String partitionName : TWO_MONTH_DAILY_PARTITIONS) {
            Assertions.assertNull(table.getPartition(partitionName), partitionName);
        }
        for (String partitionName : TWO_MONTH_MERGED_PARTITIONS) {
            Assertions.assertNotNull(table.getPartition(partitionName), partitionName);
        }
        Assertions.assertFalse(table.existTempPartitions());
    }
}
