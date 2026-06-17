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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class OnlineOptimizeJobV2Test extends DDLTestBase {
    private AlterTableStmt alterTableStmt;

    private static final Logger LOG = LogManager.getLogger(OnlineOptimizeJobV2Test.class);

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable7 distributed by hash(v1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        Config.enable_online_optimize_table = true;
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
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable7);

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
        for (OptimizeTask optimizeTask : optimizeTasks) {
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
}
