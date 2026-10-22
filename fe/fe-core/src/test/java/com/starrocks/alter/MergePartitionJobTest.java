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

import com.google.common.collect.Multimap;
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MergePartitionJobTest extends DDLTestBase {

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
    public void testFollowerReplayOfFinishedJobKeepsLiveTabletsUnmarked() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_follower_force_delete");
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
        // The swap already made the merged temp partition of the entry a live partition of the table.
        assertMergedPartitionReplacedDailyOnes(followerTable, finishedEntry);
        List<Long> liveTabletIds = tabletIds(followerTable.getPartitions());
        assertInInvertedIndex(liveTabletIds);
        assertNotMarkedForceDelete(liveTabletIds);

        replayOn(followerMetastore, followerHandler, finishedEntry);

        assertNotMarkedForceDelete(liveTabletIds);
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

    // ---- expiry of the rewrite tasks registered in TaskManager ------------------------------------

    @Test
    public void testRewriteTasksOutliveJobDeadline() throws Exception {
        OlapTable table = createDailyPartitionedTable("t_merge_task_expiry");
        MergePartitionJob job = submitMerge(table);
        runToRunning(job);
        // Read the names now: a done job drops its rewrite task list.
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

    private static void assertNotMarkedForceDelete(List<Long> tabletIds) {
        Assertions.assertFalse(tabletIds.isEmpty());
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (long tabletId : tabletIds) {
            Assertions.assertFalse(invertedIndex.getForceDeleteTablets().containsKey(tabletId), "tablet " + tabletId);
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
}
