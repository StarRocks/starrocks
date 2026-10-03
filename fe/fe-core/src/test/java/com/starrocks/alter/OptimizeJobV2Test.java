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
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
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
import java.util.stream.Collectors;

public class OptimizeJobV2Test extends DDLTestBase {
    private static final String TEST_FILE_NAME = OptimizeJobV2Test.class.getCanonicalName();
    private AlterTableStmt alterTableStmt;
    // transactions a test started, so that they can be driven to a final state afterwards
    private final Map<Long, Long> testTxnIdToDbId = Maps.newLinkedHashMap();

    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2Test.class);

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable7 distributed by hash(v1)";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        Config.enable_online_optimize_table = false;
    }

    @AfterEach
    public void clear() throws Exception {
        // transactions started by a test must reach a final state, a lingering COMMITTED one would keep
        // the next test from dropping the database in setUp
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

    /**
     * A rewrite that is committed but not published yet must not be swapped in, and must not be thrown
     * away either: the job waits for it and then finishes. Driven with a real transaction on the temp
     * partition, so that the whole lookup path is exercised rather than stubbed away.
     */
    @Test
    public void testTempPartitionNotVisibleWaitsThenFinishes() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);
        OptimizeJobV2 job = startOptimizeJobUpToRunning(db, olapTable, alterTableStmt);

        // the rewrite of the temp partition is committed but not published yet
        List<Long> rewriteTxnIds = Lists.newArrayList();
        for (Long tmpPartitionId : job.getTmpPartitionIds()) {
            rewriteTxnIds.add(commitTxnWithoutPublish(db, olapTable, olapTable.getPartition(tmpPartitionId)));
        }
        markAllRewriteTasksSucceeded(db, job);

        // the job keeps waiting instead of dropping a rewrite that only misses its publish
        job.runRunningJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
        for (Long tmpPartitionId : job.getTmpPartitionIds()) {
            Assertions.assertNotNull(olapTable.getPartition(tmpPartitionId));
        }

        // once published, the same job goes through
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        for (Long txnId : rewriteTxnIds) {
            transactionMgr.finishTransaction(db.getId(), txnId, Sets.newHashSet());
        }
        job.runRunningJob();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState());
    }

    /**
     * Regression for silent data loss: a load that is still in flight on the source partition has written
     * its rows into tablets that the replacement force deletes, while the load itself keeps reporting
     * success. The job must give up instead of replacing the partition.
     */
    @Test
    public void testInFlightIngestionOnSourcePartitionCancelsJob() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);
        Partition sourcePartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable7);
        long sourcePartitionId = sourcePartition.getId();

        OptimizeJobV2 job = startOptimizeJobUpToRunning(db, olapTable, alterTableStmt);

        // a load starts writing into the source partition while the rewrite is running
        long txnId = beginTxnWriting(db, olapTable, sourcePartition);
        markAllRewriteTasksSucceeded(db, job);

        try {
            job.runRunningJob();
        } catch (AlterCancelException e) {
            job.cancel(e.getMessage());
        }

        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        Assertions.assertTrue(job.errMsg.contains("has ingestion during optimize"), job.errMsg);
        Assertions.assertTrue(job.errMsg.contains(String.valueOf(txnId)), job.errMsg);
        // the source partition, and therefore the rows of the in flight load, must still be there
        Partition survivingPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable7);
        Assertions.assertNotNull(survivingPartition);
        Assertions.assertEquals(sourcePartitionId, survivingPartition.getId());
    }

    @Test
    public void testOptimizeTableFinishWithoutConcurrentIngestion() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable7);
        OptimizeJobV2 job = startOptimizeJobUpToRunning(db, olapTable, alterTableStmt);

        markAllRewriteTasksSucceeded(db, job);

        job.runRunningJob();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState());
    }

    /**
     * Ingestion into a partition that is not being optimized must not make the job give up: the detection
     * is scoped to the physical partitions that are actually replaced.
     */
    @Test
    public void testIngestionOnNonTargetPartitionDoesNotCancelJob() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `testOptimizePartitioned` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "PARTITION BY RANGE(`v1`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"100\"),\n" +
                " PARTITION p2 VALUES LESS THAN (\"200\"))\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "testOptimizePartitioned");
        Partition optimizedPartition = olapTable.getPartition("p1");
        Partition untouchedPartition = olapTable.getPartition("p2");

        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table testOptimizePartitioned partition (p1) distributed by hash(v1) buckets 5",
                starRocksAssert.getCtx());
        OptimizeJobV2 job = startOptimizeJobUpToRunning(db, olapTable, stmt);

        // a load is writing into p2, which this job does not replace
        beginTxnWriting(db, olapTable, untouchedPartition);
        markAllRewriteTasksSucceeded(db, job);

        job.runRunningJob();

        Assertions.assertEquals(JobState.FINISHED, job.getJobState());
        // p1 was replaced by its rewritten partition, p2 was left alone
        Assertions.assertNotEquals(optimizedPartition.getId(), olapTable.getPartition("p1").getId());
        Assertions.assertEquals(untouchedPartition.getId(), olapTable.getPartition("p2").getId());
    }

    /**
     * Submit the given alter statement and drive its optimize job until the rewrite tasks are outstanding,
     * which is the state every concurrency test starts from.
     */
    private OptimizeJobV2 startOptimizeJobUpToRunning(Database db, OlapTable olapTable, AlterTableStmt stmt)
            throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        schemaChangeHandler.process(stmt.getAlterClauseList(), db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = schemaChangeHandler.getAlterJobsV2();
        Assertions.assertEquals(1, alterJobsV2.size());
        OptimizeJobV2 job = spyPreviousTxnFinished((OptimizeJobV2) alterJobsV2.values().stream().findAny().get());

        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState());
        return job;
    }

    /**
     * Begin a real transaction and register the given partition as a write target, which is what the tablet
     * sink does when a load plan is built, before any row reaches the backend.
     */
    private long beginTxnWriting(Database db, OlapTable table, Partition partition) throws Exception {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long txnId = transactionMgr.beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                "label_" + UUIDUtil.genUUID(), TransactionState.TxnCoordinator.fromThisFE(),
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        TransactionState txnState = transactionMgr.getTransactionState(db.getId(), txnId);
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            txnState.addPartitionLoadedIndexes(table.getId(), physicalPartition.getId(),
                    Lists.newArrayList(table.getBaseIndexMetaId()));
        }
        testTxnIdToDbId.put(txnId, db.getId());
        return txnId;
    }

    /**
     * Write into the given partition and commit, without letting the transaction publish, which is the
     * state a rewrite is in when its data is durable but not visible yet.
     */
    private long commitTxnWithoutPublish(Database db, OlapTable table, Partition partition) throws Exception {
        long txnId = beginTxnWriting(db, table, partition);
        List<TabletCommitInfo> tabletCommitInfos = Lists.newArrayList();
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            for (MaterializedIndex index : physicalPartition.getAllMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                        tabletCommitInfos.add(new TabletCommitInfo(tablet.getId(), replica.getBackendId()));
                    }
                }
            }
        }
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .commitTransaction(db.getId(), txnId, tabletCommitInfos, Lists.newArrayList(), null);
        return txnId;
    }

    private void markAllRewriteTasksSucceeded(Database db, OptimizeJobV2 job) {
        for (OptimizeTask t : job.getOptimizeTasks()) {
            removeTaskFromScheduler(t);
            TaskRunStatus s = new TaskRunStatus();
            s.setTaskName(t.getName());
            s.setDbName(db.getFullName());
            s.setState(Constants.TaskRunState.SUCCESS);
            GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunManager().getTaskRunHistory().addHistory(s);
        }
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
        // Detach the job from schema change handler to prevent the background scheduler
        // from mutating its state in parallel with the UT driven state machine, which
        // occasionally drops temp partitions and leads to flaky failures.
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
}
