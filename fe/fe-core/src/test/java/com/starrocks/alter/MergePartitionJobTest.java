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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.history.TaskRunHistory;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TStatusCode;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    // ---- merging a table some of whose partitions are merged already ------------------------------

    /**
     * A merge leaves the partition expression of the table alone, so loads keep creating hourly partitions after a
     * merge into days, and merging into days again without a range takes the day partition of the first merge as a
     * source partition too. That partition already is the day partition the merge would build from it, so the merge
     * leaves it alone and merges only the new hourly partitions.
     */
    @Test
    public void testMergeAgainLeavesMergedPartitionAlone() throws Exception {
        OlapTable table = createHourlyPartitionedTable("t_merge_again");
        addHourlyPartitions(table, "2020-01-01 00:00:00", "2020-01-01 01:00:00", "2020-01-01 02:00:00");
        mergeToFinished(table, "date_trunc('day', k)");
        Partition mergedDay = table.getPartition("p20200101");
        Assertions.assertNotNull(mergedDay, partitionNames(table).toString());

        addHourlyPartitions(table, "2020-01-03 02:00:00", "2020-01-03 03:00:00");
        MergePartitionJob job = submitMerge(table, "date_trunc('day', k)");
        runToRunning(job);
        Assertions.assertEquals(List.of("p20200103"), tempPartitionNames(table));
        runToFinished(job);

        Assertions.assertEquals(Set.of("p20200101", "p20200103"), partitionNames(table));
        Assertions.assertSame(mergedDay, table.getPartition("p20200101"));
        Assertions.assertTrue(table.getTempPartitions().isEmpty());
    }

    /**
     * Merging a table whose partitions are all merged into the target granularity already has nothing to merge, so the
     * merge finishes without touching them.
     */
    @Test
    public void testMergeOfMergedPartitionsChangesNothing() throws Exception {
        OlapTable table = createHourlyPartitionedTable("t_merge_merged");
        addHourlyPartitions(table, "2020-01-01 00:00:00", "2020-01-01 01:00:00");
        mergeToFinished(table, "date_trunc('day', k)");
        Partition mergedDay = table.getPartition("p20200101");
        Assertions.assertNotNull(mergedDay, partitionNames(table).toString());

        MergePartitionJob job = submitMerge(table, "date_trunc('day', k)");
        runToRunning(job);
        Assertions.assertTrue(table.getTempPartitions().isEmpty());
        Assertions.assertTrue(job.getOptimizeTasks().isEmpty());
        runToFinished(job);

        Assertions.assertEquals(Set.of("p20200101"), partitionNames(table));
        Assertions.assertSame(mergedDay, table.getPartition("p20200101"));
        Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
    }

    /**
     * A partition coarser than the target granularity, such as the month partition of an earlier merge into months,
     * has nothing to merge into a day either. The merge leaves it alone rather than rewriting it into its first day.
     */
    @Test
    public void testMergeLeavesCoarserPartitionAlone() throws Exception {
        OlapTable table = createHourlyPartitionedTable("t_merge_coarser");
        addHourlyPartitions(table, "2020-01-01 00:00:00", "2020-01-02 05:00:00");
        mergeToFinished(table, "date_trunc('month', k)");
        Partition mergedMonth = table.getPartition("p202001");
        Assertions.assertNotNull(mergedMonth, partitionNames(table).toString());

        addHourlyPartitions(table, "2020-02-03 02:00:00", "2020-02-03 03:00:00");
        MergePartitionJob job = submitMerge(table, "date_trunc('day', k)");
        runToRunning(job);
        Assertions.assertEquals(List.of("p20200203"), tempPartitionNames(table));
        runToFinished(job);

        Assertions.assertEquals(Set.of("p202001", "p20200203"), partitionNames(table));
        Assertions.assertSame(mergedMonth, table.getPartition("p202001"));
    }

    /**
     * A merged partition takes the name of its range. When a partition the merge does not leave alone already has that
     * name, the job is cancelled with an error that names the conflict, instead of reporting that the merged partition
     * does not exist. DDL keeps every partition of an automatic partitioned table under the name of its range, so the
     * test renames an hourly partition to the name of its day in the catalog.
     */
    @Test
    public void testMergeIntoTakenPartitionNameReportsConflict() throws Exception {
        OlapTable table = createHourlyPartitionedTable("t_merge_name_taken");
        addHourlyPartitions(table, "2020-01-03 02:00:00", "2020-01-03 03:00:00");
        table.renamePartition("p2020010302", "p20200103");
        Assertions.assertEquals(Set.of("p20200103", "p2020010303"), partitionNames(table));

        MergePartitionJob job = submitMerge(table, "date_trunc('day', k)");
        job.run();

        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        Assertions.assertTrue(job.errMsg.contains("already has a partition named p20200103"), job.errMsg);
        Assertions.assertEquals(Set.of("p20200103", "p2020010303"), partitionNames(table));
        Assertions.assertTrue(table.getTempPartitions().isEmpty());
        Assertions.assertEquals(OlapTableState.NORMAL, table.getState());
    }

    /**
     * Loads may write into a partition the merge leaves alone while the job runs. That does not concern the merge,
     * which still swaps its own partition in and keeps the new data of the other one.
     */
    @Test
    public void testLoadIntoPartitionLeftAloneDuringMerge() throws Exception {
        OlapTable table = createHourlyPartitionedTable("t_merge_load_left_alone");
        addHourlyPartitions(table, "2020-01-01 00:00:00", "2020-01-01 01:00:00");
        mergeToFinished(table, "date_trunc('day', k)");
        addHourlyPartitions(table, "2020-01-03 02:00:00", "2020-01-03 03:00:00");

        MergePartitionJob job = submitMerge(table, "date_trunc('day', k)");
        runToRunning(job);
        // A load publishes a new version of the day partition of the first merge.
        PhysicalPartition mergedDay = table.getPartition("p20200101").getDefaultPhysicalPartition();
        long loadedVersion = mergedDay.getVisibleVersion() + 1;
        mergedDay.setVisibleVersion(loadedVersion, System.currentTimeMillis());
        runToFinished(job);

        Assertions.assertEquals(Set.of("p20200101", "p20200103"), partitionNames(table));
        Assertions.assertEquals(loadedVersion,
                table.getPartition("p20200101").getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertTrue(table.getTempPartitions().isEmpty());
    }

    /**
     * A table partitioned on a DATE column builds its partition keys from DATE values: merging its days into months again
     * leaves the month partition of the first merge alone as well.
     */
    @Test
    public void testMergeAgainOnDateColumnLeavesMergedPartitionAlone() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_again_date");
        mergeToFinished(table, "date_trunc('month', k)");
        Partition mergedMonth = table.getPartition("p202002");
        Assertions.assertNotNull(mergedMonth, partitionNames(table).toString());

        MergePartitionJob job = submitMerge(table, "date_trunc('month', k)");
        runToRunning(job);
        Assertions.assertTrue(table.getTempPartitions().isEmpty());
        Assertions.assertTrue(job.getOptimizeTasks().isEmpty());
        runToFinished(job);

        Assertions.assertEquals(Set.of("p202002"), partitionNames(table));
        Assertions.assertSame(mergedMonth, table.getPartition("p202002"));
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
     * Creates a table partitioned by hour that has no partition yet; addHourlyPartitions adds them.
     */
    private static OlapTable createHourlyPartitionedTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (k DATETIME NOT NULL, v INT) DUPLICATE KEY(k) " +
                "PARTITION BY date_trunc('hour', k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
    }

    /**
     * Creates the partitions of the given times the way a load creates the partitions of the rows it writes.
     */
    private static void addHourlyPartitions(OlapTable table, String... times) throws Exception {
        List<List<String>> partitionValues = Arrays.stream(times).map(List::of).collect(Collectors.toList());
        AddPartitionClause clause =
                AnalyzerUtils.getAddPartitionClauseFromPartitionValues(table, partitionValues, false, null);
        new AlterTableClauseAnalyzer(table).analyze(starRocksAssert.getCtx(), clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addPartitions(starRocksAssert.getCtx(), db, table.getName(), clause);
    }

    private static void mergeToFinished(OlapTable table, String partitionBy) throws Exception {
        MergePartitionJob job = submitMerge(table, partitionBy);
        runToRunning(job);
        runToFinished(job);
    }

    /**
     * The names of the partitions of the table, without the shadow partition of an automatic partitioned table.
     */
    private static Set<String> partitionNames(OlapTable table) {
        return table.getVisiblePartitions().stream().map(Partition::getName).collect(Collectors.toSet());
    }

    private static List<String> tempPartitionNames(OlapTable table) {
        return table.getTempPartitions().stream().map(Partition::getName).sorted().collect(Collectors.toList());
    }

    private static MergePartitionJob submitMerge(OlapTable table) throws Exception {
        return submitMerge(table, "date_trunc('month', k)");
    }

    /**
     * Submits the merge through a handler whose daemon is never started, so only the test runs the job. The returned
     * job sees every earlier load as finished and every load into its temp partition as visible.
     */
    private static MergePartitionJob submitMerge(OlapTable table, String partitionBy) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " PARTITION BY " + partitionBy, starRocksAssert.getCtx());
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

    /**
     * Marks every rewrite task as succeeded and drives the job to FINISHED through run(), the scheduler's entry point.
     */
    private static void runToFinished(MergePartitionJob job) {
        for (OptimizeTask task : job.getOptimizeTasks()) {
            task.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        job.run();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    // ---- dynamic overwrite into a merged table --------------------------------------------------

    /**
     * A merge leaves the partition expression at day, and the dynamic overwrite RPC used to build its temp partitions
     * from it: txn_p20240130 and so on, which the commit then made formal partitions inside the merged p202401. The
     * temp partitions take the merged partitions' names and ranges now, so the swap replaces them as a whole.
     */
    @Test
    public void testDynamicOverwriteAfterMergeReplacesMergedPartitions() throws Exception {
        OlapTable table = createTwoMonthDailyPartitionedTable("t_merge_dynamic_overwrite");
        mergeIntoMonths(table);
        assertDynamicOverwriteReplacesMergedPartitions(table, "2024-01-30", "2024-01-31", "2024-02-01");
    }

    @Test
    public void testDynamicOverwriteAfterMergeReplacesMergedDatetimePartitions() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t_merge_dynamic_overwrite_datetime (k DATETIME, v INT) " +
                "DUPLICATE KEY(k) PARTITION BY date_trunc('day', k) " +
                "(START ('2024-01-30') END ('2024-02-02') EVERY (INTERVAL 1 DAY)) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        OlapTable table = (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1,
                "t_merge_dynamic_overwrite_datetime");
        mergeIntoMonths(table);
        assertDynamicOverwriteReplacesMergedPartitions(table,
                "2024-01-30 08:00:00", "2024-01-31 23:59:59", "2024-02-01 00:00:00");
    }

    /**
     * Only temp partitions built from the table's own expression adopt the partition covering their value. An
     * ordinary load keeps the expression's partitions (the sink never asks for a value a partition covers, and the
     * analyzer maps such a partition to the merged one), and a merge gives the granularity of its target itself.
     */
    @Test
    public void testOnlyDynamicOverwriteTempPartitionsAdoptTheCoveringPartition() throws Exception {
        OlapTable table = createTwoMonthDailyPartitionedTable("t_merge_clause_adopts");
        mergeIntoMonths(table);
        List<List<String>> values = List.of(List.of("2024-01-30"), List.of("2024-01-31"), List.of("2024-03-05"));

        // the two January days collapse into the merged p202401, March has no partition and keeps the expression's
        AddPartitionClause temp = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(table, values, true, "txn7");
        Assertions.assertEquals(List.of("txn7_p202401", "txn7_p20240305"), partitionNames(temp));
        assertPartitionBounds(temp, "txn7_p202401", "2024-01-01", "2024-02-01");
        assertPartitionBounds(temp, "txn7_p20240305", "2024-03-05", "2024-03-06");

        AddPartitionClause formal = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(table, values, false, null);
        Assertions.assertEquals(List.of("p20240130", "p20240131", "p20240305"), partitionNames(formal));

        AddPartitionClause merge = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(table,
                new ExpressionPartitionDesc(partitionExpr(table)), null, values, true, "txn7");
        Assertions.assertEquals(List.of("txn7_p20240130", "txn7_p20240131", "txn7_p20240305"), partitionNames(merge));
    }

    /**
     * A temp partition that does not match the merged partition (built from the day expression, as before the fix)
     * would become a formal partition inside p202401. The commit refuses that swap and the table stays as it was.
     */
    @Test
    public void testDynamicOverwriteCommitRefusesOverlappingSwap() throws Exception {
        OlapTable table = createTwoMonthDailyPartitionedTable("t_merge_dynamic_overwrite_overlap");
        mergeIntoMonths(table);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        AddPartitionClause dayClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(table,
                new ExpressionPartitionDesc(partitionExpr(table)), null, List.of(List.of("2024-01-30")), true, "txn42");
        new AlterTableClauseAnalyzer(table).analyze(starRocksAssert.getCtx(), dayClause);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addPartitions(starRocksAssert.getCtx(), db, table.getName(), dayClause);
        Assertions.assertNotNull(table.getPartition("txn42_p20240130", true));
        // and one that matches the merged p202402, as the dynamic overwrite builds it now
        AddPartitionClause monthClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(table,
                List.of(List.of("2024-02-01")), true, "txn42");
        new AlterTableClauseAnalyzer(table).analyze(starRocksAssert.getCtx(), monthClause);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addPartitions(starRocksAssert.getCtx(), db, table.getName(), monthClause);
        Assertions.assertNotNull(table.getPartition("txn42_p202402", true));

        table.checkReplaceMatchPartitions(List.of("txn42_p202402"));
        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> table.checkReplaceMatchPartitions(List.of("txn42_p202402", "txn42_p20240130")));
        Assertions.assertTrue(e.getMessage().contains("temporary partition txn42_p20240130"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("overlaps partition p202401"), e.getMessage());

        // the check changes nothing
        Assertions.assertEquals(TWO_MONTH_MERGED_PARTITIONS, formalPartitionNames(table));
        assertFormalRangesDoNotOverlap(table);
    }

    private static void mergeIntoMonths(OlapTable table) throws Exception {
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        runToFinished(job);
        assertMerged(table);
    }

    /**
     * Runs the dynamic overwrite partition RPC for the values, which fall into the merged partitions, and then the
     * swap its commit does.
     */
    private void assertDynamicOverwriteReplacesMergedPartitions(OlapTable table, String... values) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(db.getId(),
                Lists.newArrayList(table.getId()), "label_" + UUIDUtil.genUUID(),
                TransactionState.TxnCoordinator.fromThisFE(), TransactionState.LoadJobSourceType.INSERT_STREAMING,
                Config.stream_load_default_timeout_second);
        testTxnIdToDbId.put(txnId, db.getId());

        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setTxn_id(txnId);
        request.setIs_temp(true);
        request.setPartition_values(Arrays.stream(values).map(Collections::singletonList).collect(Collectors.toList()));
        TCreatePartitionResult result = new FrontendServiceImpl(ExecuteEnv.getInstance()).createPartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(), result.getStatus().toString());

        String prefix = "txn" + txnId + AnalyzerUtils.PARTITION_NAME_PREFIX_SPLIT;
        List<String> tempPartitionNames = TWO_MONTH_MERGED_PARTITIONS.stream()
                .map(name -> prefix + name).collect(Collectors.toList());
        Assertions.assertEquals(tempPartitionNames, table.getTempPartitions().stream()
                .map(Partition::getName).sorted().collect(Collectors.toList()));
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        for (String mergedPartitionName : TWO_MONTH_MERGED_PARTITIONS) {
            Range<PartitionKey> merged = rangeInfo.getRange(table.getPartition(mergedPartitionName).getId());
            Range<PartitionKey> temp = rangeInfo.getRange(table.getPartition(prefix + mergedPartitionName, true).getId());
            Assertions.assertTrue(RangeUtils.isRangeEqual(merged, temp), merged + " vs " + temp);
        }

        table.checkReplaceMatchPartitions(tempPartitionNames);
        table.replaceMatchPartitions(db.getId(), tempPartitionNames);
        assertMerged(table);
        Assertions.assertEquals(TWO_MONTH_MERGED_PARTITIONS, formalPartitionNames(table));
        assertFormalRangesDoNotOverlap(table);
    }

    private static Expr partitionExpr(OlapTable table) {
        return ((ExpressionRangePartitionInfo) table.getPartitionInfo()).getPartitionExprs(table.getIdToColumn()).get(0);
    }

    private static List<String> partitionNames(AddPartitionClause clause) {
        return ((RangePartitionDesc) clause.getPartitionDesc()).getSingleRangePartitionDescs().stream()
                .map(SingleRangePartitionDesc::getPartitionName).collect(Collectors.toList());
    }

    private static void assertPartitionBounds(AddPartitionClause clause, String partitionName, String lower,
                                              String upper) {
        SingleRangePartitionDesc desc = ((RangePartitionDesc) clause.getPartitionDesc()).getSingleRangePartitionDescs()
                .stream().filter(d -> d.getPartitionName().equals(partitionName)).findFirst().orElseThrow();
        PartitionKeyDesc keyDesc = desc.getPartitionKeyDesc();
        Assertions.assertEquals(lower, keyDesc.getLowerValues().get(0).getStringValue(), partitionName);
        Assertions.assertEquals(upper, keyDesc.getUpperValues().get(0).getStringValue(), partitionName);
    }

    // the formal partitions of the table, without the shadow partition of automatic partitioning
    private static List<String> formalPartitionNames(OlapTable table) {
        return table.getPartitions().stream().map(Partition::getName)
                .filter(name -> !name.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX))
                .sorted().collect(Collectors.toList());
    }

    /**
     * No two formal partitions overlap, and the sorted range map that SHOW PARTITIONS reads lists every one of them.
     */
    private static void assertFormalRangesDoNotOverlap(OlapTable table) {
        RangePartitionInfo rangeInfo = (RangePartitionInfo) table.getPartitionInfo();
        List<Map.Entry<Long, Range<PartitionKey>>> sorted = rangeInfo.getSortedRangeMap(false);
        Assertions.assertEquals(rangeInfo.getIdToRange(false).keySet(),
                sorted.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
        for (int i = 1; i < sorted.size(); i++) {
            Range<PartitionKey> previous = sorted.get(i - 1).getValue();
            Range<PartitionKey> current = sorted.get(i).getValue();
            Assertions.assertFalse(previous.isConnected(current) && !previous.intersection(current).isEmpty(),
                    previous + " overlaps " + current);
        }
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
