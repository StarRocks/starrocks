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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.scheduler.Constants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OptimizeJobV2Test extends DDLTestBase {
    private static String fileName = "./SchemaChangeV2Test";
    private AlterTableStmt alterTableStmt;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2Test.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable1 distributed by hash(v1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
    }

    @After
    public void clear() {
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    @Test
    public void testOptimizeParser() throws Exception {
        String stmt = "alter table testTable1 distributed by hash(v1)";
        AlterTableStmt alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());

        stmt = "alter table testTable1 primary key(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable1 order by (v1)";
        alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());

        stmt = "alter table testTable1 duplicate key(v1) partition (t1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not support"));
        }

        stmt = "alter table testTable1 duplicate key(v1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not support optimize"));
        }

        stmt = "alter table testTable1 distributed by hash(v1) partition (t1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }

        stmt = "alter table testTable1 distributed by hash(v1) temporary partition (t1)";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not support optimize temp partition"));
        }

        stmt = "alter table testTable1 distributed by random";
        try {
            alterStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            LOG.warn("Alter fail:", e);
            Assert.assertTrue(e.getMessage().contains("not support change default distribution type"));
        }
    }

    @Test
    public void testOptimizeTable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.SCHEMA_CHANGE, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testOptimizeTableFinish() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        optimizeJob.runPendingJob();
        Assert.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assert.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (OptimizeTask optimizeTask : optimizeTasks) {
            optimizeTask.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        optimizeJob.runRunningJob();

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, optimizeJob.getJobState());
    }

    @Test
    public void testOptimizeTableFailed() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        optimizeJob.runPendingJob();
        Assert.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assert.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

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
        } catch (AlterCancelException e) {
            optimizeJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assert.assertEquals(JobState.CANCELLED, optimizeJob.getJobState());

        OptimizeJobV2 replayOptimizeJob = new OptimizeJobV2(
                optimizeJob.getJobId(), db.getId(), olapTable.getId(), olapTable.getName(), 1000);
        replayOptimizeJob.replay(optimizeJob);
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        optimizeJob.runPendingJob();
        Assert.assertEquals(JobState.PENDING, optimizeJob.getJobState());

        // table is stable runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        optimizeJob.runPendingJob();
        Assert.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assert.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (OptimizeTask optimizeTask : optimizeTasks) {
            optimizeTask.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        optimizeJob.runRunningJob();

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, optimizeJob.getJobState());
    }

    @Test
    public void testSerializeOfOptimizeJob() throws IOException {
        // prepare file
        File file = new File(fileName);
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
        Assert.assertEquals(1, result.getJobId());
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, result.getJobState());
    }

    @Test
    public void testOptimizeReplay() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);

        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 optimizeJob = (OptimizeJobV2) alterJobsV2.values().stream().findAny().get();

        OptimizeJobV2 replayOptimizeJob = new OptimizeJobV2(
                optimizeJob.getJobId(), db.getId(), olapTable.getId(), olapTable.getName(), 1000);

        replayOptimizeJob.replay(optimizeJob);
        Assert.assertEquals(JobState.PENDING, replayOptimizeJob.getJobState());

        // runPendingJob
        optimizeJob.runPendingJob();
        Assert.assertEquals(JobState.WAITING_TXN, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assert.assertEquals(JobState.WAITING_TXN, replayOptimizeJob.getJobState());

        // runWaitingTxnJob
        optimizeJob.runWaitingTxnJob();
        Assert.assertEquals(JobState.RUNNING, optimizeJob.getJobState());

        // runRunningJob
        List<OptimizeTask> optimizeTasks = optimizeJob.getOptimizeTasks();
        for (OptimizeTask optimizeTask : optimizeTasks) {
            optimizeTask.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        }
        optimizeJob.runRunningJob();

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, optimizeJob.getJobState());

        replayOptimizeJob.replay(optimizeJob);
        Assert.assertEquals(JobState.FINISHED, replayOptimizeJob.getJobState());
    }

}