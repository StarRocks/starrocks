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
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.history.TaskRunHistory;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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

    // ---- pruning the per-partition data of a done job -------------------------------------------

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_prune_finished");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        Assertions.assertEquals(List.of(1, 3, 3), partitionMapSizes(job));

        runToFinished(job);

        assertPruned(job);
        List<MergePartitionJob> entries = readJournaledEntries(job.getJobId());
        MergePartitionJob finishedEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(JobState.FINISHED, finishedEntry.getJobState());
        Assertions.assertEquals(List.of(1, 3, 3), partitionMapSizes(finishedEntry));
        // The merged temp partition replaced the daily ones before the prune.
        assertMergedPartitionReplacedDailyOnes(table, finishedEntry);
        assertShowAlterUnchangedByPrune(finishedEntry);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_prune_cancelled");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        List<Long> tempTabletIds = tabletIds(table.getTempPartitions());
        assertInInvertedIndex(tempTabletIds);

        Assertions.assertTrue(job.cancel("cancel"));

        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        assertTempPartitionsDropped(table, tempTabletIds);
        List<MergePartitionJob> entries = readJournaledEntries(job.getJobId());
        MergePartitionJob cancelledEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(JobState.CANCELLED, cancelledEntry.getJobState());
        Assertions.assertEquals(List.of(1, 3, 3), partitionMapSizes(cancelledEntry));
        assertShowAlterUnchangedByPrune(cancelledEntry);
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_follower_finished");
        // The follower starts from the catalog as it was before the merge.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        runToFinished(job);

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore, table);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        MergePartitionJob finishedEntry =
                (MergePartitionJob) replayUpToFinalEntry(followerMetastore, followerHandler, job.getJobId(), 1);
        // The handler keeps the first replayed entry and applies the later ones to it.
        MergePartitionJob retained = (MergePartitionJob) followerHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertEquals(JobState.WAITING_TXN, retained.getJobState());
        Assertions.assertEquals(List.of(0, 3, 0), partitionMapSizes(retained));
        assertMergedPartitionReplacedDailyOnes(followerTable, finishedEntry);
        Assertions.assertEquals(OlapTableState.OPTIMIZE, followerTable.getState());

        replayOn(followerMetastore, followerHandler, finishedEntry);

        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        // The retained job took over the partition name map of the entry; pruning it left the entry intact.
        Assertions.assertEquals(List.of(1, 3, 3), partitionMapSizes(finishedEntry));
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testFollowerReplayDropsTempPartitionsOfCancelledJob() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_follower_cancelled");
        // The follower starts from the catalog as it was before the merge.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        Assertions.assertTrue(job.cancel("cancel"));

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore, table);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        MergePartitionJob cancelledEntry =
                (MergePartitionJob) replayUpToFinalEntry(followerMetastore, followerHandler, job.getJobId(), 0);
        MergePartitionJob retained = (MergePartitionJob) followerHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertEquals(List.of(0, 3, 0), partitionMapSizes(retained));
        Assertions.assertEquals(new HashSet<>(retained.getTmpPartitionIds()),
                partitionIds(followerTable.getTempPartitions()));
        List<Long> tempTabletIds = tabletIds(followerTable.getTempPartitions());
        assertInInvertedIndex(tempTabletIds);

        replayOn(followerMetastore, followerHandler, cancelledEntry);

        // The CANCELLED replay drops the temp partitions the retained job maps; the prune comes after it.
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(JobState.CANCELLED, retained.getJobState());
        assertPruned(retained);
        assertTempPartitionsDropped(followerTable, tempTabletIds);
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testLoadPrunesDoneJobFromImage() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_prune_image");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        runToFinished(job);
        // Images written by older versions hold the done job with its partition maps, as the journal entry does.
        List<MergePartitionJob> entries = readJournaledEntries(job.getJobId());
        MergePartitionJob doneJob = entries.get(entries.size() - 1);
        Assertions.assertEquals(List.of(1, 3, 3), partitionMapSizes(doneJob));
        AlterJobMgr alterJobMgr = newAlterJobMgr();
        alterJobMgr.getSchemaChangeHandler().addAlterJobV2(doneJob);
        // SHOW ALTER of the loaded job must match that of a job that went through the same serialization.
        List<List<Comparable>> expectedRows = new ArrayList<>();
        gsonCopy(doneJob).getInfo(expectedRows);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr = loadAlterJobMgr(image);

        MergePartitionJob restored =
                (MergePartitionJob) restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertPruned(restored);
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        Assertions.assertFalse(expectedRows.isEmpty());
        Assertions.assertEquals(expectedRows, restoredRows);
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
        Mockito.doReturn(false).when(job).hasCommittedNotVisibleTxn(Mockito.any());
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

    private static AlterJobV2 nextJobEntry(long jobId) throws Exception {
        AlterJobV2 entry =
                (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(jobId, entry.getJobId());
        return entry;
    }

    /**
     * Reads the journal entries of the job, from the one that submitted it to the one of its final state.
     */
    private static List<MergePartitionJob> readJournaledEntries(long jobId) throws Exception {
        List<MergePartitionJob> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = nextJobEntry(jobId);
            entries.add((MergePartitionJob) entry);
        } while (!entry.isDone());
        Assertions.assertEquals(JobState.PENDING, entries.get(0).getJobState());
        return entries;
    }

    /**
     * Replays what the leader journaled for the job on a follower, in journal order: the PENDING entry, the temp
     * partitions, the WAITING_TXN entry and the given number of partition swaps. Returns the entry of the final
     * state without replaying it.
     */
    private static AlterJobV2 replayUpToFinalEntry(LocalMetastore metastore, SchemaChangeHandler handler, long jobId,
                                                   int partitionSwaps) throws Exception {
        AlterJobV2 pending = nextJobEntry(jobId);
        Assertions.assertEquals(JobState.PENDING, pending.getJobState());
        replayOn(metastore, handler, pending);
        AddPartitionsInfoV2 tempPartitions = (AddPartitionsInfoV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_PARTITIONS_V2);
        for (PartitionPersistInfoV2 info : tempPartitions.getAddPartitionInfos()) {
            Assertions.assertTrue(info.isTempPartition());
            metastore.replayAddPartition(info);
        }
        AlterJobV2 waitingTxn = nextJobEntry(jobId);
        Assertions.assertEquals(JobState.WAITING_TXN, waitingTxn.getJobState());
        replayOn(metastore, handler, waitingTxn);
        for (int i = 0; i < partitionSwaps; i++) {
            ReplacePartitionOperationLog swap = (ReplacePartitionOperationLog) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_REPLACE_TEMP_PARTITION);
            forgetRecycledSourcePartitions(metastore, swap);
            metastore.replayReplaceTempPartition(swap);
        }
        AlterJobV2 finalEntry = nextJobEntry(jobId);
        Assertions.assertTrue(finalEntry.isDone());
        return finalEntry;
    }

    /**
     * The swap moves the source partitions to the recycle bin, which the leader already did in the one recycle bin
     * the test has; take them out so that the follower can move them there again.
     */
    private static void forgetRecycledSourcePartitions(LocalMetastore metastore, ReplacePartitionOperationLog swap) {
        OlapTable olapTable = (OlapTable) metastore.getTable(swap.getDbId(), swap.getTblId());
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        for (String partitionName : swap.getPartitions()) {
            long partitionId = olapTable.getPartition(partitionName).getId();
            if (recycleBin.getPartition(partitionId) != null) {
                recycleBin.removePartitionFromRecycleBin(partitionId);
            }
        }
    }

    /**
     * Replays the journal entry into the handler against the given catalog, as a follower does.
     */
    private static void replayOn(LocalMetastore metastore, SchemaChangeHandler handler, AlterJobV2 entry) {
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(metastore);
        try {
            handler.replayAlterJobV2(entry);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }
    }

    private static UtFrameUtils.PseudoImage saveMetastore() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(image.getImageWriter());
        return image;
    }

    private static LocalMetastore loadMetastore(UtFrameUtils.PseudoImage image) throws Exception {
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        metastore.load(reader);
        reader.close();
        return metastore;
    }

    private static OlapTable tableIn(LocalMetastore metastore, OlapTable table) {
        return (OlapTable) metastore.getTable(GlobalStateMgrTestUtil.testDb1, table.getName());
    }

    private static AlterJobV2 gsonCopy(AlterJobV2 job) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, AlterJobV2.class), AlterJobV2.class);
    }

    private static AlterJobMgr newAlterJobMgr() {
        return new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
    }

    private static AlterJobMgr loadAlterJobMgr(UtFrameUtils.PseudoImage image) throws Exception {
        AlterJobMgr alterJobMgr = newAlterJobMgr();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        alterJobMgr.load(reader);
        reader.close();
        return alterJobMgr;
    }

    /**
     * What the job changes in the catalog: the table state, its partitions and temp partitions.
     */
    private static List<Object> catalogSnapshot(OlapTable olapTable) {
        Map<String, Long> partitions = new TreeMap<>();
        for (Partition partition : olapTable.getPartitions()) {
            partitions.put(partition.getName(), partition.getId());
        }
        return List.of(olapTable.getState(), partitions, partitionIds(olapTable.getTempPartitions()));
    }

    private static Set<Long> partitionIds(Collection<Partition> partitions) {
        return partitions.stream().map(Partition::getId).collect(Collectors.toSet());
    }

    private static List<Long> tabletIds(Collection<Partition> partitions) {
        List<Long> tabletIds = new ArrayList<>();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                    .getAllMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                index.getTablets().forEach(tablet -> tabletIds.add(tablet.getId()));
            }
        }
        return tabletIds;
    }

    /**
     * The table holds the merged temp partition of the job and none of the daily partitions it merged.
     */
    private static void assertMergedPartitionReplacedDailyOnes(OlapTable olapTable, MergePartitionJob finishedJob) {
        Set<Long> partitionIds = partitionIds(olapTable.getPartitions());
        Assertions.assertTrue(partitionIds.containsAll(finishedJob.getTmpPartitionIds()), partitionIds.toString());
        for (long dailyPartitionId : finishedJob.getTempPartitionIdToSourcePartitionIds().values()) {
            Assertions.assertFalse(partitionIds.contains(dailyPartitionId), partitionIds.toString());
        }
    }

    private static void assertInInvertedIndex(List<Long> tabletIds) {
        Assertions.assertFalse(tabletIds.isEmpty());
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long tabletId : tabletIds) {
            Assertions.assertNotNull(invertedIndex.getTabletMeta(tabletId), "tablet " + tabletId);
        }
    }

    private static void assertTempPartitionsDropped(OlapTable olapTable, List<Long> tempTabletIds) {
        Assertions.assertTrue(olapTable.getTempPartitions().isEmpty());
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long tabletId : tempTabletIds) {
            Assertions.assertNull(invertedIndex.getTabletMeta(tabletId), "tablet " + tabletId);
        }
        Assertions.assertEquals(OlapTableState.NORMAL, olapTable.getState());
    }

    /**
     * SHOW ALTER of a done job reads none of the data the prune drops.
     */
    private static void assertShowAlterUnchangedByPrune(MergePartitionJob doneJob) {
        List<List<Comparable>> rowsBefore = new ArrayList<>();
        doneJob.getInfo(rowsBefore);
        doneJob.pruneMeta();
        assertPruned(doneJob);
        List<List<Comparable>> rowsAfter = new ArrayList<>();
        doneJob.getInfo(rowsAfter);
        Assertions.assertEquals(1, rowsBefore.size());
        Assertions.assertEquals(rowsBefore, rowsAfter);
    }

    /**
     * The numbers of rewrite tasks, temp to source partition ids and temp to source partition names of the job.
     */
    private static List<Integer> partitionMapSizes(MergePartitionJob job) {
        return List.of(job.getOptimizeTasks().size(), job.tempPartitionIdToSourcePartitionIds.size(),
                job.tempPartitionNameToSourcePartitionNames.size());
    }

    private static void assertPruned(MergePartitionJob job) {
        Assertions.assertEquals(List.of(0, 0, 0), partitionMapSizes(job));
        Assertions.assertNull(Deencapsulation.getField(job, "optimizeClause"));
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
        Mockito.doCallRealMethod().when(job).hasCommittedNotVisibleTxn(Mockito.any());
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
