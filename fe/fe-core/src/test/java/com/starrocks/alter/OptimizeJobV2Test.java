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
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.persist.TaskRunStatus;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class OptimizeJobV2Test extends DDLTestBase {
    private static final String TEST_FILE_NAME = OptimizeJobV2Test.class.getCanonicalName();
    private AlterTableStmt alterTableStmt;

    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2Test.class);

    @BeforeAll
    public static void beforeAll() {
        DDLTestBase.beforeAll();
        // The cases drive optimize jobs by hand. A running scheduler can pick a job up between process() and
        // spyPreviousTxnFinished() and run its pending step on the original, which shares tmpPartitionIds with
        // the spy, so its failure cancels the job and drops the spy's temp partitions.
        try {
            UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        UtFrameUtils.setUpForPersistTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable7 distributed by hash(v1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        Config.enable_online_optimize_table = false;
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @AfterEach
    public void clear() {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
        Config.enable_online_optimize_table = false;
    }

    @Test
    public void testOptimizeParser() throws Exception {
        String stmt = "alter table testTable7 distributed by hash(v1)";
        UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());

        stmt = "alter table testTable7 primary key(v1)";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 order by (v1)";
        UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());

        stmt = "alter table testTable7 partition (t1) duplicate key(v1)";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 duplicate key(v1)";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 partition (t1) distributed by hash(v1)";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assertions.assertTrue(e.getMessage().contains("does not exist"));
        }

        stmt = "alter table testTable7 temporary partition (t1) distributed by hash(v1)";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support optimize temp partition"));
        }

        stmt = "alter table testTable7 partition (t1) distributed by random";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 partition (t1) distributed by hash(v3)";
        try {
            UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 distributed by random";
        UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
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
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

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
        for (int i = 0; i < optimizeTasks.size(); ++i) {
            OptimizeTask optimizeTask = optimizeTasks.get(i);
            Assertions.assertTrue(optimizeTask.getDefinition()
                    .contains(") (" + rewriteColumns + ") select " + rewriteColumns + " from "));
            removeTaskFromScheduler(optimizeTask);
            TaskRunStatus taskRunStatus = new TaskRunStatus();
            taskRunStatus.setTaskName(optimizeTask.getName());
            taskRunStatus.setState(Constants.TaskRunState.SUCCESS);
            taskRunStatus.setDbName(db.getFullName());
            GlobalStateMgr.getCurrentState().getTaskManager()
                        .getTaskRunManager().getTaskRunHistory().addHistory(taskRunStatus);
        }
        optimizeJob.runRunningJob();

        // finish alter tasks
        Assertions.assertEquals(JobState.FINISHED, optimizeJob.getJobState());
    }

    @Test
    public void testTaskSuccessButNotVisibleMarkedFailed() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 job = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());
        // Force visibility check to return true (has committed-not-visible)
        Mockito.doReturn(true).when(job).hasCommittedNotVisible(Mockito.anyLong());

        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());

        // Mark all tasks SQL SUCCESS and add history
        for (OptimizeTask t : job.getOptimizeTasks()) {
            removeTaskFromScheduler(t);
            TaskRunStatus s = new TaskRunStatus();
            s.setTaskName(t.getName());
            s.setDbName(db.getFullName());
            s.setState(Constants.TaskRunState.SUCCESS);
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager().getTaskRunHistory().addHistory(s);
        }
        // Keep the tasks: the job drops them once it is done.
        List<OptimizeTask> tasks = new ArrayList<>(job.getOptimizeTasks());
        Assertions.assertFalse(tasks.isEmpty());

        try {
            job.runRunningJob();
        } catch (AlterCancelException e) {
            job.cancel(e.getMessage());
        }

        // All tasks should be turned to FAILED due to not visible
        for (OptimizeTask t : tasks) {
            Assertions.assertEquals(Constants.TaskRunState.FAILED, t.getOptimizeTaskState());
        }
        // All partitions rewrite failed, job should be CANCELLED
        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testTaskSuccessAndVisibleKeepsSuccess() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 job = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());
        // Visibility check returns false (no committed-not-visible), so SUCCESS remains SUCCESS
        Mockito.doReturn(false).when(job).hasCommittedNotVisible(Mockito.anyLong());

        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());

        for (OptimizeTask t : job.getOptimizeTasks()) {
            removeTaskFromScheduler(t);
            TaskRunStatus s = new TaskRunStatus();
            s.setTaskName(t.getName());
            s.setDbName(db.getFullName());
            s.setState(Constants.TaskRunState.SUCCESS);
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager().getTaskRunHistory().addHistory(s);
        }

        // Should proceed to finish
        job.runRunningJob();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState());
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
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

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

        OptimizeJobV2 replayOptimizeJob = new OptimizeJobV2(
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
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

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
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        OptimizeJobV2 replayOptimizeJob = new OptimizeJobV2(
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
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        OptimizeJobV2 replayOptimizeJob = new OptimizeJobV2(
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

        int retryTimes = 3;
        do {
            try {
                optimizeJob.runRunningJob();
            } catch (Exception e) {
                LOG.info(e.getMessage());
            }
            if (--retryTimes < 0) {
                return;
            }
        } while (optimizeJob.getJobState() != JobState.FINISHED);


        // finish alter tasks
        Assertions.assertEquals(JobState.FINISHED, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assertions.assertEquals(JobState.FINISHED, replayOptimizeJob.getJobState());
    }

    @Test
    public void testOptimizeDistributionColumnPartialFail() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "testTable2");

        String stmt = "alter table testTable2 distributed by hash(v2)";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        schemaChangeHandler.process(alterStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        Assertions.assertEquals(2, optimizeTasks.size());
        optimizeTasks.get(0).setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        optimizeTasks.get(1).setOptimizeTaskState(Constants.TaskRunState.FAILED);

        try {
            optimizeJob.runRunningJob();
        } catch (AlterCancelException e) {
            optimizeJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assertions.assertEquals(JobState.CANCELLED, optimizeJob.getJobState());
    }

    @Test
    public void testOptimizeDistributionTypePartialFail() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "testTable2");

        String stmt = "alter table testTable2 distributed by random";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        schemaChangeHandler.process(alterStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        Assertions.assertEquals(2, optimizeTasks.size());
        optimizeTasks.get(0).setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        optimizeTasks.get(1).setOptimizeTaskState(Constants.TaskRunState.FAILED);

        try {
            optimizeJob.runRunningJob();
        } catch (AlterCancelException e) {
            optimizeJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assertions.assertEquals(JobState.CANCELLED, optimizeJob.getJobState());
    }

    @Test
    public void testOptimizeFailedByVersion() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "testTable2");

        String stmt = "alter table testTable2 distributed by hash(v1)";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        schemaChangeHandler.process(alterStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        Assertions.assertEquals(2, optimizeTasks.size());
        optimizeTasks.get(0).setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        optimizeTasks.get(1).setOptimizeTaskState(Constants.TaskRunState.SUCCESS);

        for (Partition p : olapTable.getPartitions()) {
            p.getDefaultPhysicalPartition().setVisibleVersion(
                    p.getDefaultPhysicalPartition().getVisibleVersion() + 1, 0);
        }

        try {
            optimizeJob.runRunningJob();
        } catch (AlterCancelException e) {
            optimizeJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assertions.assertEquals(JobState.CANCELLED, optimizeJob.getJobState());
    }

    @Test
    public void testOptimizeDistributionTypeSuccess() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable =
                (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "testTable2");

        String stmt = "alter table testTable2 distributed by random";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        schemaChangeHandler.process(alterStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // Make all tasks SUCCESS to cover allPartitionOptimized branch
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        // Expect 2 tasks (2 partitions in test env)
        Assertions.assertEquals(2, optimizeTasks.size());
        for (OptimizeTask t : optimizeTasks) {
            t.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }

        int retryTimes = 3;
        do {
            try {
                optimizeJob.runRunningJob();
            } catch (Exception e) {
                LOG.info(e.getMessage());
            }
            if (--retryTimes < 0) {
                return;
            }
        } while (optimizeJob.getJobState() != JobState.FINISHED);

        // Verify job finished and default distribution updated
        Assertions.assertEquals(JobState.FINISHED, optimizeJob.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, olapTable.getState());
        Assertions.assertEquals(
                com.starrocks.catalog.DistributionInfo.DistributionInfoType.RANDOM,
                olapTable.getDefaultDistributionInfo().getType()
        );
    }

    @Test
    public void testRunRunningJobSubmitPendingTasks() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        // Drive job to PENDING -> WAITING_TXN -> RUNNING
        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // Set all tasks to PENDING and clear scheduler state to trigger executeTask path in runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (OptimizeTask t : optimizeTasks) {
            t.setOptimizeTaskState(Constants.TaskRunState.PENDING);
            removeTaskFromScheduler(t);
        }

        // Trigger path: executeTask for PENDING tasks should set state to RUNNING or FAILED
        optimizeJob.runRunningJob();

        // Assert: all tasks should not be PENDING
        for (OptimizeTask t : optimizeTasks) {
            Assertions.assertNotEquals(Constants.TaskRunState.PENDING, t.getOptimizeTaskState());
        }
        // Job should remain RUNNING because tasks are not finished
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());
    }

    @Test
    public void testRunRunningJobSubmitPendingTasksFailed() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);

        // Drive job to PENDING -> WAITING_TXN -> RUNNING
        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // Create a fake PENDING task that is not registered in TaskManager to force executeTask -> FAILED
        String fakeTaskName = optimizeJob.getName() + "_fake_pending";
        OptimizeTask fakeTask = TaskBuilder.buildOptimizeTask(fakeTaskName, optimizeJob.getProperties(),
                "select 1", db.getFullName(), 0L);
        fakeTask.setOptimizeTaskState(Constants.TaskRunState.PENDING);
        optimizeJob.getOptimizeTasks().add(fakeTask);

        // Trigger runRunningJob: PENDING task should try to execute and become FAILED
        optimizeJob.runRunningJob();

        // Verify the fake task failed due to executeTask returning FAILED
        Assertions.assertEquals(Constants.TaskRunState.FAILED, fakeTask.getOptimizeTaskState());
        // Job should remain RUNNING because other tasks are not finished
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());
    }

    private void removeTaskFromScheduler(OptimizeTask task) {
        TaskRunManager trm = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager();
        TaskRunScheduler trs = trm.getTaskRunScheduler();
        if (trm.tryTaskRunLock()) {
            try {
                trs.removePendingTask(task);
                trs.removeRunningTask(task.getId());
            } finally {
                trm.taskRunUnlock();
            }
        }
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

        OptimizeJobV2 badPersistedJob = new OptimizeJobV2(
                9999L, db.getId(), olapTable.getId(), olapTable.getName(), 1000);
        badPersistedJob.setJobState(JobState.FINISHED);
        java.lang.reflect.Field allOptField = OptimizeJobV2.class.getDeclaredField("allPartitionOptimized");
        allOptField.setAccessible(true);
        allOptField.set(badPersistedJob, true);
        // distributionInfo stays null - this is the corruption shape

        OptimizeJobV2 replayJob = new OptimizeJobV2(
                9999L, db.getId(), olapTable.getId(), olapTable.getName(), 1000);
        replayJob.replay(badPersistedJob);

        Assertions.assertNotNull(olapTable.getDefaultDistributionInfo(),
                "replay must not null out defaultDistributionInfo when persisted job has null distributionInfo");
    }

    private OptimizeJobV2 spyPreviousTxnFinished(OptimizeJobV2 job) {
        // The test drives the spy from here on, so the handler must not hold the original. Detaching alone does
        // not keep the scheduler away from it, which is why beforeAll() stops the scheduler.
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        schemaChangeHandler.getAlterJobsV2().remove(job.getJobId());

        OptimizeJobV2 spy = Mockito.spy(job);
        try {
            Mockito.doReturn(true).when(spy).isPreviousLoadFinished();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return spy;
    }

    // ---- pruning the per-partition data of a done job -------------------------------------------

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        OlapTable table = createPartitionedTable("t_optimize_prune_finished");
        OptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(job));

        runToFinished(job);

        assertPruned(job);
        List<OptimizeJobV2> entries = readJournaledEntries(job.getJobId());
        OptimizeJobV2 finishedEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(JobState.FINISHED, finishedEntry.getJobState());
        Assertions.assertEquals(List.of(2, 2, 2, 2), partitionListSizes(finishedEntry));
        // The temp partitions replaced the source ones before the prune.
        Assertions.assertEquals(new HashSet<>(finishedEntry.getTmpPartitionIds()), partitionIds(table.getPartitions()));
        Assertions.assertEquals(2, table.getDefaultDistributionInfo().getBucketNum());
        assertShowAlterUnchangedByPrune(finishedEntry);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        OlapTable table = createPartitionedTable("t_optimize_prune_cancelled");
        OptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        List<Long> tempTabletIds = tabletIds(table.getTempPartitions());
        assertInInvertedIndex(tempTabletIds);

        Assertions.assertTrue(job.cancel("cancel"));

        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        assertTempPartitionsDropped(table, tempTabletIds);
        List<OptimizeJobV2> entries = readJournaledEntries(job.getJobId());
        OptimizeJobV2 cancelledEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(JobState.CANCELLED, cancelledEntry.getJobState());
        Assertions.assertEquals(List.of(2, 2, 0, 0), partitionListSizes(cancelledEntry));
        assertShowAlterUnchangedByPrune(cancelledEntry);
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        OlapTable table = createPartitionedTable("t_optimize_follower_finished");
        // The follower starts from the catalog as it was before the optimize.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        OptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        runToFinished(job);

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore, table);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        OptimizeJobV2 finishedEntry =
                (OptimizeJobV2) replayUpToFinalEntry(followerMetastore, followerHandler, job.getJobId(), 1);
        // The handler keeps the first replayed entry and applies the later ones to it.
        OptimizeJobV2 retained = (OptimizeJobV2) followerHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertEquals(JobState.WAITING_TXN, retained.getJobState());
        Assertions.assertEquals(List.of(0, 2, 0, 0), partitionListSizes(retained));
        Assertions.assertEquals(new HashSet<>(finishedEntry.getTmpPartitionIds()),
                partitionIds(followerTable.getPartitions()));
        Assertions.assertEquals(OlapTableState.OPTIMIZE, followerTable.getState());

        replayOn(followerMetastore, followerHandler, finishedEntry);

        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        // The retained job took over the partition names of the entry; pruning it left the entry intact.
        Assertions.assertEquals(List.of(2, 2, 2, 2), partitionListSizes(finishedEntry));
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testFollowerReplayDropsTempPartitionsOfCancelledJob() throws Exception {
        OlapTable table = createPartitionedTable("t_optimize_follower_cancelled");
        // The follower starts from the catalog as it was before the optimize.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        OptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        Assertions.assertTrue(job.cancel("cancel"));

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore, table);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        OptimizeJobV2 cancelledEntry =
                (OptimizeJobV2) replayUpToFinalEntry(followerMetastore, followerHandler, job.getJobId(), 0);
        OptimizeJobV2 retained = (OptimizeJobV2) followerHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertEquals(List.of(0, 2, 0, 0), partitionListSizes(retained));
        Assertions.assertEquals(new HashSet<>(retained.getTmpPartitionIds()),
                partitionIds(followerTable.getTempPartitions()));
        List<Long> tempTabletIds = tabletIds(followerTable.getTempPartitions());
        assertInInvertedIndex(tempTabletIds);

        replayOn(followerMetastore, followerHandler, cancelledEntry);

        // The CANCELLED replay drops the temp partitions the retained job lists; the prune comes after it.
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(JobState.CANCELLED, retained.getJobState());
        assertPruned(retained);
        assertTempPartitionsDropped(followerTable, tempTabletIds);
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testLoadPrunesDoneJobFromImage() throws Exception {
        OlapTable table = createPartitionedTable("t_optimize_prune_image");
        OptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        runToFinished(job);
        // Images written by older versions hold the done job with its partition lists, as the journal entry does.
        List<OptimizeJobV2> entries = readJournaledEntries(job.getJobId());
        OptimizeJobV2 doneJob = entries.get(entries.size() - 1);
        Assertions.assertEquals(List.of(2, 2, 2, 2), partitionListSizes(doneJob));
        AlterJobMgr alterJobMgr = newAlterJobMgr();
        alterJobMgr.getSchemaChangeHandler().addAlterJobV2(doneJob);
        // SHOW ALTER of the loaded job must match that of a job that went through the same serialization.
        List<List<Comparable>> expectedRows = new ArrayList<>();
        gsonCopy(doneJob).getInfo(expectedRows);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr = loadAlterJobMgr(image);

        OptimizeJobV2 restored =
                (OptimizeJobV2) restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertPruned(restored);
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        Assertions.assertFalse(expectedRows.isEmpty());
        Assertions.assertEquals(expectedRows, restoredRows);
    }

    // ---- expiry of the rewrite tasks registered in TaskManager ------------------------------------

    @Test
    public void testRewriteTasksOutliveJobDeadline() throws Exception {
        OlapTable table = createPartitionedTable("t_optimize_task_expiry");
        OptimizeJobV2 job = submitOptimize(table);
        runToRunning(job);
        // Read the names now: a done job drops its rewrite task list.
        List<String> taskNames = rewriteTaskNames(job);
        Assertions.assertEquals(2, taskNames.size());

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
        OlapTable table = createPartitionedTable("t_optimize_task_expired");
        OptimizeJobV2 job = submitOptimize(table);
        try (MockedStatic<TaskBuilder> ignored = backdateRewriteTasks(job.getTimeoutMs())) {
            runToRunning(job);
        }
        List<String> taskNames = rewriteTaskNames(job);
        Assertions.assertEquals(2, taskNames.size());
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        for (String taskName : taskNames) {
            Assertions.assertNotNull(taskManager.getTask(taskName), taskName);
        }

        taskManager.removeExpiredTasks();

        for (String taskName : taskNames) {
            Assertions.assertNull(taskManager.getTask(taskName), taskName);
        }
    }

    private static List<String> rewriteTaskNames(OptimizeJobV2 job) {
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

    private static OlapTable createPartitionedTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + " (k1 INT, v1 INT) DUPLICATE KEY(k1) " +
                "PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'), PARTITION p2 VALUES LESS THAN ('20')) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
    }

    /**
     * Submits OPTIMIZE through a handler whose daemon is never started, so only the test runs the job. The returned
     * job sees every earlier load as finished.
     */
    private static OptimizeJobV2 submitOptimize(OlapTable table) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " DISTRIBUTED BY HASH(k1) BUCKETS 2", starRocksAssert.getCtx());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        SchemaChangeHandler handler = new SchemaChangeHandler();
        handler.process(stmt.getAlterClauseList(), db, table);
        Assertions.assertEquals(1, handler.getAlterJobsV2().size());
        OptimizeJobV2 job = Mockito.spy((OptimizeJobV2) handler.getAlterJobsV2().values().iterator().next());
        Mockito.doReturn(true).when(job).isPreviousLoadFinished();
        return job;
    }

    private static void runToRunning(OptimizeJobV2 job) throws Exception {
        job.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState());
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
    }

    /**
     * Marks every rewrite task as succeeded and drives the job to FINISHED through run(), the scheduler's entry point.
     */
    private static void runToFinished(OptimizeJobV2 job) {
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
    private static List<OptimizeJobV2> readJournaledEntries(long jobId) throws Exception {
        List<OptimizeJobV2> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = nextJobEntry(jobId);
            entries.add((OptimizeJobV2) entry);
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
    private static void assertShowAlterUnchangedByPrune(OptimizeJobV2 doneJob) {
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
    private static List<Integer> partitionListSizes(OptimizeJobV2 job) {
        List<String> sourcePartitionNames = Deencapsulation.getField(job, "sourcePartitionNames");
        List<String> tmpPartitionNames = Deencapsulation.getField(job, "tmpPartitionNames");
        return List.of(job.getOptimizeTasks().size(), job.getTmpPartitionIds().size(),
                sourcePartitionNames.size(), tmpPartitionNames.size());
    }

    private static void assertPruned(OptimizeJobV2 job) {
        Assertions.assertEquals(List.of(0, 0, 0, 0), partitionListSizes(job));
        Assertions.assertNull(Deencapsulation.getField(job, "optimizeClause"));
    }
}
