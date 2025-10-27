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
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OptimizeJobV2Test extends DDLTestBase {
    private static final String TEST_FILE_NAME = OptimizeJobV2Test.class.getCanonicalName();
    private AlterTableStmt alterTableStmt;

    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2Test.class);

    @BeforeAll
    public static void beforeAll() {
        // do nothing
    }

    @BeforeEach
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_new_publish_mechanism = false;
        Config.tablet_sched_checker_interval_seconds = 100;
        Config.tablet_sched_repair_delay_factor_second = 100;
        Config.alter_scheduler_interval_millisecond = 10000;

        super.beforeAll();
        super.setUp();
        String stmt = "alter table testTable7 distributed by hash(v1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        Config.enable_online_optimize_table = false;

    }

    @AfterEach
    public void clear() {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
        Config.enable_online_optimize_table = true;
    }

    @Test
    public void testOptimizeParser() throws Exception {
        String stmt = "alter table testTable7 distributed by hash(v1)";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());

        stmt = "alter table testTable7 primary key(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 order by (v1)";
        alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());

        stmt = "alter table testTable7 partition (t1) duplicate key(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 duplicate key(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 partition (t1) distributed by hash(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assertions.assertTrue(e.getMessage().contains("does not exist"));
        }

        stmt = "alter table testTable7 temporary partition (t1) distributed by hash(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("not support optimize temp partition"));
        }

        stmt = "alter table testTable7 partition (t1) distributed by random";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assertions.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assertions.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable7 partition (t1) distributed by hash(v3)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
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
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable7);

        schemaChangeHandler.process(alterTableStmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (int i = 0; i < optimizeTasks.size(); ++i) {
            OptimizeTask optimizeTask = optimizeTasks.get(i);
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                        .getTaskRunScheduler().removeRunningTask(optimizeTask.getId());
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                        .getTaskRunScheduler().removePendingTask(optimizeTask);
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
        OptimizeJobV2 realJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        OptimizeJobV2 job = Mockito.spy(realJob);
        Mockito.doReturn(true).when(job).isPreviousLoadFinished();
        // Force visibility check to return true (has committed-not-visible)
        Mockito.doReturn(true).when(job).hasCommittedNotVisible(Mockito.anyLong());

        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());

        // Mark all tasks SQL SUCCESS and add history
        for (OptimizeTask t : job.getOptimizeTasks()) {
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                .getTaskRunScheduler().removeRunningTask(t.getId());
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                .getTaskRunScheduler().removePendingTask(t);
            TaskRunStatus s = new TaskRunStatus();
            s.setTaskName(t.getName());
            s.setDbName(db.getFullName());
            s.setState(Constants.TaskRunState.SUCCESS);
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager().getTaskRunHistory().addHistory(s);
        }

        try {
            job.runRunningJob();
        } catch (AlterCancelException e) {
            job.cancel(e.getMessage());
        }

        // All tasks should be turned to FAILED due to not visible
        for (OptimizeTask t : job.getOptimizeTasks()) {
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
        OptimizeJobV2 realJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        OptimizeJobV2 job = Mockito.spy(realJob);
        Mockito.doReturn(true).when(job).isPreviousLoadFinished();
        // Visibility check returns false (no committed-not-visible), so SUCCESS remains SUCCESS
        Mockito.doReturn(false).when(job).hasCommittedNotVisible(Mockito.anyLong());

        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());

        for (OptimizeTask t : job.getOptimizeTasks()) {
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                .getTaskRunScheduler().removeRunningTask(t.getId());
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                .getTaskRunScheduler().removePendingTask(t);
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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
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
    public void testSerializeOfOptimizeJob() throws IOException {
        // prepare file
        File file = new File(TEST_FILE_NAME);
        file.createNewFile();
        file.deleteOnExit();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        OptimizeJobV2 optimizeJobV2 = new OptimizeJobV2(1, 1, 1, "test", 600000);
        Deencapsulation.setField(optimizeJobV2, "jobState", AlterJobV2.JobState.FINISHED);

        // write schema change job
        optimizeJobV2.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        OptimizeJobV2 result = (OptimizeJobV2) AlterJobV2.read(in);
        Assertions.assertEquals(1, result.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, result.getJobState());
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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        optimizeJob.runPendingJob();
        Assertions.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        optimizeJob.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // Set all tasks to PENDING and clear scheduler state to trigger executeTask path in runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (OptimizeTask t : optimizeTasks) {
            t.setOptimizeTaskState(Constants.TaskRunState.PENDING);
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                    .getTaskRunScheduler().removeRunningTask(t.getId());
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager()
                    .getTaskRunScheduler().removePendingTask(t);
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
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

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

}
