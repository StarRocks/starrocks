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
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.alter.AlterCancelException;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.OperationType;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
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
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MergePartitionJobTest extends DDLTestBase {

    private static final List<String> TWO_MONTH_DAILY_PARTITIONS = List.of("p20240130", "p20240131", "p20240201");
    private static final List<String> TWO_MONTH_MERGED_PARTITIONS = List.of("p202401", "p202402");
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

    @Test
    public void testTempPartitionNotVisibleRaisesFailure() throws Exception {
        String tempPartitionName = "tmp_p1";
        MergePartitionJob job = new MergePartitionJob(1L, 2L, 3L, "tbl", 1000L) {
            @Override
            protected boolean hasCommittedNotVisible(long partitionId) {
                return partitionId == 100L;
            }
        };

        Field nameMappingField = MergePartitionJob.class.getDeclaredField("tempPartitionNameToSourcePartitionNames");
        nameMappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Multimap<String, String> nameMapping = (Multimap<String, String>) nameMappingField.get(job);
        nameMapping.put(tempPartitionName, "p1");

        OptimizeTask task = new OptimizeTask("task-1");
        task.setTempPartitionName(tempPartitionName);
        task.setPartitionName("p1");
        task.setLastVersion(0L);
        task.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        job.getOptimizeTasks().add(task);

        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(2L);

        OlapTable table = Mockito.mock(OlapTable.class);
        Partition tempPartition = Mockito.mock(Partition.class);
        Mockito.when(tempPartition.getId()).thenReturn(100L);
        Mockito.when(tempPartition.getName()).thenReturn(tempPartitionName);
        Mockito.when(table.getPartition(tempPartitionName, true)).thenReturn(tempPartition);
        Mockito.doNothing().when(table).dropTempPartition(tempPartitionName, true);
        Mockito.when(table.getPartition("p1")).thenReturn(null);

        Method onFinished = MergePartitionJob.class.getDeclaredMethod("onFinished", Database.class, OlapTable.class);
        onFinished.setAccessible(true);

        AlterCancelException ex = Assertions.assertThrows(AlterCancelException.class, () -> {
            try {
                onFinished.invoke(job, db, table);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof AlterCancelException) {
                    throw (AlterCancelException) cause;
                }
                throw new RuntimeException(cause);
            }
        });
        Assertions.assertTrue(ex.getMessage().contains("rewrite failed"));

        Mockito.verify(table).dropTempPartition(tempPartitionName, true);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, task.getOptimizeTaskState());
        Assertions.assertTrue(nameMapping.isEmpty());
    }

    // ---- expiry of the rewrite tasks registered in TaskManager ------------------------------------

    @Test
    public void testRewriteTasksOutliveJobDeadline() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_task_expiry");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        List<String> taskNames = rewriteTaskNames(job);
        Assertions.assertEquals(1, taskNames.size());

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.removeExpiredTasks();
        long deadline = job.createTimeMs + job.getTimeoutMs();
        Map<String, Long> expireTimes = new HashMap<>();
        for (String taskName : taskNames) {
            Task task = taskManager.getTask(taskName);
            Assertions.assertNotNull(task, taskName);
            Assertions.assertTrue(task.getExpireTime() >= deadline + Config.task_ttl_second * 1000L,
                    taskName + " expires at " + task.getExpireTime() + ", less than a task TTL after the job deadline "
                            + deadline);
            expireTimes.put(taskName, task.getExpireTime());
        }
        assertExpireTimesPersisted(expireTimes);
    }

    @Test
    public void testExpiredRewriteTasksAreRemoved() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_task_expired");
        MergePartitionJob job = submitMerge(table);
        try (MockedStatic<TaskBuilder> ignored = backdateRewriteTasks(job.getTimeoutMs())) {
            runToRunning(job);
        }
        List<String> taskNames = rewriteTaskNames(job);
        Assertions.assertEquals(1, taskNames.size());
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        for (String taskName : taskNames) {
            Assertions.assertNotNull(taskManager.getTask(taskName), taskName);
        }

        taskManager.removeExpiredTasks();

        for (String taskName : taskNames) {
            Assertions.assertNull(taskManager.getTask(taskName), taskName);
        }
    }

    private static List<String> rewriteTaskNames(MergePartitionJob job) {
        return job.getOptimizeTasks().stream().map(Task::getName).collect(Collectors.toList());
    }

    /**
     * Makes the rewrite tasks built in the calling thread look as if they were created so long ago that the job
     * timeout and the task TTL have both elapsed since.
     */
    private static MockedStatic<TaskBuilder> backdateRewriteTasks(long timeoutMs) {
        long ageMs = timeoutMs + Config.task_ttl_second * 1000L + 1000L;
        return Mockito.mockStatic(TaskBuilder.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (result instanceof OptimizeTask) {
                OptimizeTask task = (OptimizeTask) result;
                task.setCreateTime(task.getCreateTime() - ageMs);
            }
            return result;
        });
    }

    /**
     * A follower gets the expiry of each task from the task creation journal entry, and keeps it across an image
     * save and load.
     */
    private static void assertExpireTimesPersisted(Map<String, Long> expireTimes) throws Exception {
        TaskManager follower = new TaskManager();
        for (int i = 0; i < expireTimes.size(); i++) {
            follower.replayCreateTask(
                    (Task) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_TASK));
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        follower.saveTasksV2(image.getImageWriter());
        TaskManager restored = new TaskManager();
        restored.loadTasksV2(image.getMetaBlockReader());

        for (Map.Entry<String, Long> entry : expireTimes.entrySet()) {
            for (TaskManager taskManager : List.of(follower, restored)) {
                Task task = taskManager.getTask(entry.getKey());
                Assertions.assertNotNull(task, entry.getKey());
                Assertions.assertEquals(entry.getValue(), task.getExpireTime(), entry.getKey());
            }
        }
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
        Mockito.doReturn(false).when(job).hasCommittedNotVisible(Mockito.anyLong());
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
     * No two formal partitions overlap, and the sorted range index that SHOW PARTITIONS reads lists every one of them.
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
