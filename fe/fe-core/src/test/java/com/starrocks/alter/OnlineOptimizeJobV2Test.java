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

import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.scheduler.Constants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

public class OnlineOptimizeJobV2Test extends DDLTestBase {
    private AlterTableStmt alterTableStmt;

    private static final Logger LOG = LogManager.getLogger(OnlineOptimizeJobV2Test.class);

    @BeforeAll
    public static void beforeAll() {
        DDLTestBase.beforeAll();
        UtFrameUtils.setUpForPersistTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable7 distributed by hash(v1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        Config.enable_online_optimize_table = true;
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @AfterEach
    public void clear() throws Exception {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
        Config.enable_online_optimize_table = false;
    }

    @Test
    public void testOptimizeTable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        Assertions.assertEquals(OlapTableState.OPTIMIZE, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testOptimizeTableFinish() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        String rewriteColumns = olapTable.getBaseSchema().stream()
                .filter(column -> !column.isGeneratedColumn())
                .map(column -> "`" + column.getName() + "`")
                .collect(Collectors.joining(", "));
        for (OptimizeTask optimizeTask : optimizeTasks) {
            Assertions.assertTrue(optimizeTask.getDefinition()
                    .contains(") (" + rewriteColumns + ") select " + rewriteColumns + " from "));
            optimizeTask.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        optimizeJob.runRunningJob();

        // finish alter tasks
        Assertions.assertEquals(JobState.FINISHED, optimizeJob.getJobState());
    }

    @Test
    public void testOptimizeTableFailed() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        int retryCount = 0;
        int maxRetry = 5;

        try {
            optimizeJob.runRunningJob();
            while (retryCount < maxRetry) {
                ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
                if (optimizeJob.getJobState() == JobState.CANCELLED) {
                    break;
                }
                retryCount++;
                LOG.info("testOptimizeTable is waiting for JobState retryCount:" + retryCount);
            }
            optimizeJob.cancel("");
        } catch (AlterCancelException e) {
            optimizeJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assertions.assertEquals(JobState.CANCELLED, optimizeJob.getJobState());

        OnlineOptimizeJobV2 replayOptimizeJob = new OnlineOptimizeJobV2(
                    optimizeJob.getJobId(), db.getId(), olapTable.getId(), olapTable.getName(), 1000);
        replayOptimizeJob.replay(optimizeJob);
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getLatestBaseIndex();
        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.PENDING, optimizeJob.getJobState());

        // table is stable runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());
    }

    @Test
    public void testOptimizeReplay() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        OnlineOptimizeJobV2 replayOptimizeJob = new OnlineOptimizeJobV2(
                    optimizeJob.getJobId(), db.getId(), olapTable.getId(), olapTable.getName(), 1000);

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.PENDING, replayOptimizeJob.getJobState());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.WAITING_TXN, replayOptimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (OptimizeTask optimizeTask : optimizeTasks) {
            optimizeTask.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        try {
            optimizeJob.runRunningJob();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }

        // finish alter tasks
        Assertions.assertEquals(JobState.FINISHED, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.FINISHED, replayOptimizeJob.getJobState());
    }

    @Test
    public void testOptimizeReplayPartialSuccess() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "testTable2");

        String stmt = "alter table testTable2 distributed by hash(v1)";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        schemaChangeHandler.process(alterStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        OnlineOptimizeJobV2 replayOptimizeJob = new OnlineOptimizeJobV2(
                    optimizeJob.getJobId(), db.getId(), olapTable.getId(), olapTable.getName(), 1000);

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.PENDING, replayOptimizeJob.getJobState());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.WAITING_TXN, replayOptimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        Assertions.assertEquals(2, optimizeTasks.size());
        optimizeTasks.get(0).setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        optimizeTasks.get(1).setOptimizeTaskState(Constants.TaskRunState.FAILED);
        optimizeJob.runRunningJob();

        // finish alter tasks
        Assertions.assertEquals(JobState.FINISHED, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.FINISHED, replayOptimizeJob.getJobState());
    }

    @Test
    public void testReplayFinishedWithNullDistributionInfo() throws Exception {
        // Regression: a job persisted with allPartitionOptimized=true but no distribution change
        // (e.g. from a previously-accepted empty alter clause) must not clobber the table's
        // defaultDistributionInfo during replay.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);
        Assertions.assertNotNull(olapTable.getDefaultDistributionInfo());

        OnlineOptimizeJobV2 badPersistedJob = new OnlineOptimizeJobV2(
                9999L, db.getId(), olapTable.getId(), olapTable.getName(), 1000);
        badPersistedJob.setJobState(JobState.FINISHED);
        java.lang.reflect.Field allOptField = OnlineOptimizeJobV2.class.getDeclaredField("allPartitionOptimized");
        allOptField.setAccessible(true);
        allOptField.set(badPersistedJob, true);
        // distributionInfo stays null - this is the corruption shape

        OnlineOptimizeJobV2 replayJob = new OnlineOptimizeJobV2(
                9999L, db.getId(), olapTable.getId(), olapTable.getName(), 1000);
        replayJob.replay(badPersistedJob);

        Assertions.assertNotNull(olapTable.getDefaultDistributionInfo(),
                "replay must not null out defaultDistributionInfo when persisted job has null distributionInfo");
    }

    @Test
    public void testDoubleWriteEnableAssignsWatershedAndDisableClears() throws Exception {
        // Window-A regression: enabling the double-write mapping and assigning the watershed txn id must
        // happen together (atomically, under the table write lock). Otherwise a load can obtain a txn id
        // >= watershed yet still observe an empty double-write mapping, write only the source partition,
        // and then be dropped by the partition replacement (silent data loss) or fail at publish against a
        // force-deleted source tablet. This asserts the enable+watershed timing rather than relying on the
        // always-finished isPreviousLoadFinished() mock.
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        optimizeJob.runPendingJob();
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        OptimizeTask task = optimizeJob.getOptimizeTasks().get(0);

        // Before any task is activated, the double-write mapping is empty.
        Assertions.assertTrue(olapTable.getDoubleWritePartitions().isEmpty());

        // Activate double-write for the task's source/temp partition pair (the path runRunningJob takes
        // when a task transitions PENDING -> RUNNING).
        java.lang.reflect.Method enable = OnlineOptimizeJobV2.class.getDeclaredMethod(
                "enableDoubleWritePartition", Database.class, OlapTable.class, String.class, String.class);
        enable.setAccessible(true);
        enable.invoke(optimizeJob, db, olapTable, task.getPartitionName(), task.getTempPartitionName());

        // The mapping is now visible AND the watershed has been assigned in the same critical section.
        Assertions.assertFalse(olapTable.getDoubleWritePartitions().isEmpty(),
                "double-write mapping must be enabled");
        Assertions.assertTrue(optimizeJob.getTransactionId().isPresent(),
                "watershed txn id must be assigned atomically when double-write is enabled");
        Assertions.assertTrue(optimizeJob.getTransactionId().get() > 0);

        // Disabling double-write clears the mapping (the replacement / cancel cleanup path).
        java.lang.reflect.Method disable = OnlineOptimizeJobV2.class.getDeclaredMethod(
                "disableDoubleWritePartition", Database.class, OlapTable.class);
        disable.setAccessible(true);
        disable.invoke(optimizeJob, db, olapTable);
        Assertions.assertTrue(olapTable.getDoubleWritePartitions().isEmpty(),
                "double-write mapping must be cleared after disable");
    }

    @Test
    public void testHasCommittedNotVisibleFalseWhenNoCommittedTxns() throws Exception {
        // The visibility gate must report "safe to replace" when there are no committed-but-unpublished
        // loads on the source partition, so the optimization converges in the common case (we delay the
        // replacement only while a committed load is still unpublished, we never abandon it).
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());
        optimizeJob.runPendingJob();
        optimizeJob.runWaitingTxnJob();

        OptimizeTask task = optimizeJob.getOptimizeTasks().get(0);
        java.lang.reflect.Method m = OnlineOptimizeJobV2.class.getDeclaredMethod(
                "hasCommittedNotVisible", Database.class, OlapTable.class, String.class);
        m.setAccessible(true);
        boolean committedNotVisible = (boolean) m.invoke(optimizeJob, db, olapTable, task.getPartitionName());
        Assertions.assertFalse(committedNotVisible);
    }

    @Test
    public void testCommittedNotVisibleDefersReplacementWithoutRerunningRewrite() throws Exception {
        // When the rewrite SQL has finished (future done = SUCCESS) but a load is committed-but-not-visible
        // on the source partition, runRunningJob must DEFER the partition replacement: stay in RUNNING and
        // leave the (completed) future in place so the rewrite INSERT is NOT resubmitted on the next round.
        // This preserves the online contract (no abandon) while never replacing under an unpublished load.
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        OnlineOptimizeJobV2 optimizeJob =
                spyPreviousTxnFinished((OnlineOptimizeJobV2) alterJobsV2.values().stream().findAny().get());
        optimizeJob.runPendingJob();
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // Drive the first task straight into the "future done = SUCCESS" branch without running the real
        // INSERT: mark it RUNNING and inject an already-completed future.
        OptimizeTask task = optimizeJob.getOptimizeTasks().get(0);
        task.setOptimizeTaskState(Constants.TaskRunState.RUNNING);
        java.lang.reflect.Field futureField = OnlineOptimizeJobV2.class.getDeclaredField("future");
        futureField.setAccessible(true);
        futureField.set(optimizeJob,
                java.util.concurrent.CompletableFuture.completedFuture(Constants.TaskRunState.SUCCESS));

        // A committed-but-not-visible load exists on the source partition -> must defer.
        Mockito.doReturn(true).when(optimizeJob)
                .hasCommittedNotVisible(Mockito.any(), Mockito.any(), Mockito.anyString());

        optimizeJob.runRunningJob();

        // Deferred: still RUNNING, task still RUNNING, and the completed future is retained so the rewrite
        // SQL is not resubmitted next round.
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());
        Assertions.assertEquals(Constants.TaskRunState.RUNNING, task.getOptimizeTaskState());
        Assertions.assertNotNull(futureField.get(optimizeJob),
                "completed future must be retained while deferring, so the rewrite INSERT is not re-run");

        // Run a second scheduler round while still committed-not-visible: it must defer again and, crucially,
        // must NOT resubmit the rewrite INSERT (the retained completed future short-circuits re-execution).
        optimizeJob.runRunningJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());
        Assertions.assertEquals(Constants.TaskRunState.RUNNING, task.getOptimizeTaskState());
        Assertions.assertNotNull(futureField.get(optimizeJob));

        // The rewrite SQL is never (re-)executed across the deferred rounds: the injected future stands in for
        // the completed INSERT, and the defer branch returns before any resubmission path.
        Mockito.verify(optimizeJob, Mockito.never()).executeSql(Mockito.anyString());
    }

    private OnlineOptimizeJobV2 spyPreviousTxnFinished(OnlineOptimizeJobV2 job) throws AnalysisException {
        // Detach the job from schema change handler to prevent background scheduler from changing state
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        schemaChangeHandler.getAlterJobsV2().remove(job.getJobId());

        // Reset job state to PENDING if it has been changed by background scheduler before removal
        if (job.getJobState() != JobState.PENDING) {
            job.setJobState(JobState.PENDING);
        }

        OnlineOptimizeJobV2 spy = Mockito.spy(job);
        Mockito.doReturn(true).when(spy).isPreviousLoadFinished();
        return spy;
    }

    // ---- pruning the per-partition data of a done job -------------------------------------------

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        OlapTable table = createPartitionedTable("t_online_optimize_prune_finished");
        OnlineOptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(job));

        runToFinished(job);

        assertPruned(job);
        Assertions.assertTrue(table.getDoubleWritePartitions().isEmpty());
        List<OnlineOptimizeJobV2> entries = readJournaledEntries(job.getJobId());
        OnlineOptimizeJobV2 finishedEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(JobState.FINISHED, finishedEntry.getJobState());
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(finishedEntry));
        // The temp partitions replaced the source ones before the prune.
        Assertions.assertEquals(new HashSet<>(finishedEntry.getTmpPartitionIds()), partitionIds(table.getPartitions()));
        Assertions.assertEquals(2, table.getDefaultDistributionInfo().getBucketNum());
        assertShowAlterUnchangedByPrune(finishedEntry);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        OlapTable table = createPartitionedTable("t_online_optimize_prune_cancelled");
        OnlineOptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        // Start the double write of the first partition and wait for the loads before it.
        Mockito.doReturn(false).when(job).isPreviousLoadFinished();
        job.runRunningJob();
        Assertions.assertEquals(1, table.getDoubleWritePartitions().size());
        List<Long> tempTabletIds = tabletIds(table.getTempPartitions());
        assertInInvertedIndex(tempTabletIds);

        Assertions.assertTrue(job.cancel("cancel"));

        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        Assertions.assertTrue(table.getDoubleWritePartitions().isEmpty());
        assertTempPartitionsDropped(table, tempTabletIds);
        List<OnlineOptimizeJobV2> entries = readJournaledEntries(job.getJobId());
        OnlineOptimizeJobV2 cancelledEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(JobState.CANCELLED, cancelledEntry.getJobState());
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(cancelledEntry));
        assertShowAlterUnchangedByPrune(cancelledEntry);
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        OlapTable table = createPartitionedTable("t_online_optimize_follower_finished");
        // The follower starts from the catalog as it was before the optimize.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        OnlineOptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        runToFinished(job);

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore, table);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        // The leader swaps in the temp partition of each partition as soon as its rewrite is done.
        OnlineOptimizeJobV2 finishedEntry =
                (OnlineOptimizeJobV2) replayUpToFinalEntry(followerMetastore, followerHandler, job.getJobId(), 2);
        // The handler keeps the first replayed entry and applies the later ones to it.
        OnlineOptimizeJobV2 retained = (OnlineOptimizeJobV2) followerHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertEquals(JobState.WAITING_TXN, retained.getJobState());
        Assertions.assertEquals(List.of(0, 2, 0, 0), partitionListSizes(retained));
        Assertions.assertEquals(new HashSet<>(finishedEntry.getTmpPartitionIds()),
                partitionIds(followerTable.getPartitions()));
        Assertions.assertEquals(OlapTableState.OPTIMIZE, followerTable.getState());

        replayOn(followerMetastore, followerHandler, finishedEntry);

        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(finishedEntry));
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testFollowerReplayDropsTempPartitionsOfCancelledJob() throws Exception {
        OlapTable table = createPartitionedTable("t_online_optimize_follower_cancelled");
        // The follower starts from the catalog as it was before the optimize.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        OnlineOptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        Assertions.assertTrue(job.cancel("cancel"));

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore, table);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        OnlineOptimizeJobV2 cancelledEntry =
                (OnlineOptimizeJobV2) replayUpToFinalEntry(followerMetastore, followerHandler, job.getJobId(), 0);
        OnlineOptimizeJobV2 retained = (OnlineOptimizeJobV2) followerHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertEquals(List.of(0, 2, 0, 0), partitionListSizes(retained));
        Assertions.assertEquals(new HashSet<>(retained.getTmpPartitionIds()),
                partitionIds(followerTable.getTempPartitions()));
        List<Long> tempTabletIds = tabletIds(followerTable.getTempPartitions());
        assertInInvertedIndex(tempTabletIds);
        // The double-write routing is not journaled; seed one to see the replay stop it as the leader's cancel does.
        followerTable.addDoubleWritePartition(followerTable.getPartition("p1").getId(), retained.getTmpPartitionIds().get(0));

        replayOn(followerMetastore, followerHandler, cancelledEntry);

        // The CANCELLED replay drops the temp partitions the retained job lists; the prune comes after it.
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(JobState.CANCELLED, retained.getJobState());
        assertPruned(retained);
        Assertions.assertTrue(followerTable.getDoubleWritePartitions().isEmpty());
        assertTempPartitionsDropped(followerTable, tempTabletIds);
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testLoadPrunesDoneJobFromImage() throws Exception {
        OlapTable table = createPartitionedTable("t_online_optimize_prune_image");
        OnlineOptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        runToFinished(job);
        // Images written by older versions hold the done job with its partition lists, as the journal entry does.
        List<OnlineOptimizeJobV2> entries = readJournaledEntries(job.getJobId());
        OnlineOptimizeJobV2 doneJob = entries.get(entries.size() - 1);
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(doneJob));
        AlterJobMgr alterJobMgr = newAlterJobMgr();
        alterJobMgr.getSchemaChangeHandler().addAlterJobV2(doneJob);
        // SHOW ALTER of the loaded job must match that of a job that went through the same serialization.
        List<List<Comparable>> expectedRows = new ArrayList<>();
        gsonCopy(doneJob).getInfo(expectedRows);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr = loadAlterJobMgr(image);

        OnlineOptimizeJobV2 restored =
                (OnlineOptimizeJobV2) restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertPruned(restored);
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        Assertions.assertFalse(expectedRows.isEmpty());
        Assertions.assertEquals(expectedRows, restoredRows);
    }

    private static OlapTable createPartitionedTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (k1 INT, v1 INT) DUPLICATE KEY(k1) " +
                "PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'), PARTITION p2 VALUES LESS THAN ('20')) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
    }

    /**
     * Submits OPTIMIZE through a handler whose daemon is never started, so only the test runs the job. The returned
     * job sees every earlier load as finished, and its rewrites do nothing (no backend).
     */
    private static OnlineOptimizeJobV2 submitOptimize(OlapTable table) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " DISTRIBUTED BY HASH(k1) BUCKETS 2", starRocksAssert.getCtx());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        SchemaChangeHandler handler = new SchemaChangeHandler();
        handler.process(stmt.getAlterClauseList(), db, table);
        Assertions.assertEquals(1, handler.getAlterJobsV2().size());
        OnlineOptimizeJobV2 job =
                Mockito.spy((OnlineOptimizeJobV2) handler.getAlterJobsV2().values().iterator().next());
        Mockito.doReturn(true).when(job).isPreviousLoadFinished();
        Mockito.doNothing().when(job).executeSql(Mockito.anyString());
        return job;
    }

    private static void runToRunning(OnlineOptimizeJobV2 job) throws Exception {
        job.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
    }

    /**
     * Drives the job to FINISHED through run(), the scheduler's entry point: one round starts the rewrite of a
     * partition, a later one swaps in its temp partition once the rewrite is done.
     */
    private static void runToFinished(OnlineOptimizeJobV2 job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(100);
        }
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
    private static List<OnlineOptimizeJobV2> readJournaledEntries(long jobId) throws Exception {
        List<OnlineOptimizeJobV2> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = nextJobEntry(jobId);
            entries.add((OnlineOptimizeJobV2) entry);
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
     * What the job changes in the catalog: the table state, its partitions and temp partitions, and its bucket count.
     */
    private static List<Object> catalogSnapshot(OlapTable olapTable) {
        Map<String, Long> partitions = new TreeMap<>();
        for (Partition partition : olapTable.getPartitions()) {
            partitions.put(partition.getName(), partition.getId());
        }
        return List.of(olapTable.getState(), partitions, partitionIds(olapTable.getTempPartitions()),
                olapTable.getDefaultDistributionInfo().getBucketNum());
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
    private static void assertShowAlterUnchangedByPrune(OnlineOptimizeJobV2 doneJob) {
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
     * The numbers of rewrite tasks, temp partition ids, source partition names and temp partition names of the job.
     */
    private static List<Integer> partitionListSizes(OnlineOptimizeJobV2 job) {
        List<String> sourcePartitionNames = Deencapsulation.getField(job, "sourcePartitionNames");
        List<String> tmpPartitionNames = Deencapsulation.getField(job, "tmpPartitionNames");
        return List.of(job.getOptimizeTasks().size(), job.getTmpPartitionIds().size(),
                sourcePartitionNames.size(), tmpPartitionNames.size());
    }

    private static void assertPruned(OnlineOptimizeJobV2 job) {
        Assertions.assertEquals(List.of(0, 0, 0, 0), partitionListSizes(job));
        Assertions.assertNull(Deencapsulation.getField(job, "optimizeClause"));
    }
}
