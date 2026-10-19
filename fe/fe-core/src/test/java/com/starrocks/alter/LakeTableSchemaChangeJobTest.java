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

import com.google.api.client.util.Lists;
import com.staros.proto.PlacementPolicy;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.TransactionState;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LakeTableSchemaChangeJobTest {
    private static final int NUM_BUCKETS = 4;
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_schema_change_test";
    private static Database db;
    private LakeTable table;

    public LakeTableSchemaChangeJobTest() {
    }

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        UtFrameUtils.setUpForPersistTest();
    }

    private static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    private static void alterTable(ConnectContext connectContext, String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    private LakeTableSchemaChangeJob getAlterJob(Table table) {
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        List<AlterJobV2> jobs = alterJobMgr.getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        alterJobMgr.getSchemaChangeHandler().clearJobs();
        Assertions.assertEquals(1, jobs.size());
        AlterJobV2 alterJob = jobs.get(0);
        Assertions.assertTrue(alterJob instanceof LakeTableSchemaChangeJob);
        return (LakeTableSchemaChangeJob) alterJob;
    }

    private LakeTableSchemaChangeJob alterTableAddColumn() throws Exception {
        alterTable(connectContext, "ALTER TABLE t0 ADD COLUMN c1 BIGINT key NOT NULL default \"0\"");
        return getAlterJob(table);
    }

    /**
     * Drive the PENDING phase the way the schema change daemon does. The phase spans several
     * scheduler rounds: one dispatches the CreateReplicaTasks and the later ones poll their latch,
     * so a single runPendingJob() no longer reaches WAITING_TXN.
     */
    private static void drivePendingJob(LakeTableSchemaChangeJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60000;
        while (true) {
            job.runPendingJob();
            if (job.getJobState() != AlterJobV2.JobState.PENDING) {
                return;
            }
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs,
                    "schema change job " + job.getJobId() + " never left PENDING");
            Thread.sleep(10);
        }
    }

    /**
     * Same as {@link #drivePendingJob} but through run(), the entry point the scheduler uses.
     */
    private static void driveJobPastPending(LakeTableSchemaChangeJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60000;
        while (job.getJobState() == AlterJobV2.JobState.PENDING) {
            job.run();
            if (job.getJobState() != AlterJobV2.JobState.PENDING) {
                return;
            }
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs,
                    "schema change job " + job.getJobId() + " never left PENDING");
            Thread.sleep(10);
        }
    }

    @BeforeEach
    public void before() throws Exception {
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getServingState().getLocalMetastore().getDb(DB_NAME);
        table = createTable(connectContext, "CREATE TABLE t0(c0 INT) duplicate key(c0) distributed by hash(c0) buckets "
                    + NUM_BUCKETS);
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    @Test
    public void testCancelPendingJob() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        // test cancel again
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testDropTableBeforeCancel() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        db.dropTable(table.getName());
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // Regression test for pushing the lake schema-change job's metadata lock down
    // from the whole database to just the altered table. Asserts both directions:
    //   1) an unrelated table in the same database is NOT blocked by the alter;
    //   2) a conflicting WRITE on the altered table still blocks.
    @Test
    public void testSchemaChangeLocksOnlyItsTable() throws Exception {
        LakeTable other = createTable(connectContext,
                "CREATE TABLE t_other(c0 INT) duplicate key(c0) distributed by hash(c0) buckets " + NUM_BUCKETS);
        // Kick off a schema change so t0 is a real altered table (mirrors production).
        alterTableAddColumn();
        long dbId = db.getId();
        long alteredTableId = table.getId();
        long otherTableId = other.getId();

        // The lake schema-change / rollup jobs guard their critical sections with
        // exactly this table-scoped lock (see LakeTableSchemaChangeJobBase); hold
        // it and verify the scope.
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(alteredTableId), LockType.WRITE)) {
            // 1) The contention win: an unrelated table in the same DB can still be
            // WRITE-locked from another thread (it is not blocked by the alter).
            Assertions.assertTrue(tryLockTableFromOtherThread(dbId, otherTableId, LockType.WRITE, 30000),
                    "altering t0 must not block locking an unrelated table in the same database");

            // 2) The safety: a conflicting WRITE on the altered table still blocks.
            Assertions.assertFalse(tryLockTableFromOtherThread(dbId, alteredTableId, LockType.WRITE, 500),
                    "a conflicting WRITE on the altered table must still block");
        }

        // Once the lock is released, the altered table can be WRITE-locked again.
        Assertions.assertTrue(tryLockTableFromOtherThread(dbId, alteredTableId, LockType.WRITE, 30000),
                "the altered table must be lockable again once the lock is released");
    }

    // Locker ownership is thread-based, so the contending acquire must run on a
    // separate thread: a same-thread re-acquire would be reentrant and would
    // never reflect cross-thread contention. NOTE: tryLockTableWithIntensiveDbLock
    // ignores the TimeUnit argument and treats the timeout value as milliseconds.
    private boolean tryLockTableFromOtherThread(long dbId, long tableId, LockType lockType, long timeoutMs)
            throws InterruptedException {
        AtomicBoolean acquired = new AtomicBoolean(false);
        Thread thread = new Thread(() -> {
            Locker locker = new Locker();
            boolean ok = locker.tryLockTableWithIntensiveDbLock(dbId, tableId, lockType, timeoutMs, TimeUnit.MILLISECONDS);
            acquired.set(ok);
            if (ok) {
                locker.unLockTableWithIntensiveDbLock(dbId, tableId, lockType);
            }
        });
        thread.start();
        thread.join();
        return acquired.get();
    }

    @Test
    public void testPendingJobNoAliveBackend() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return null;
            }
        };
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        mockedWarehouseManager.setComputeNodesAssignedToTablet(null);
        Exception exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("No alive backend"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testTableDroppedInPending() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1L;
            }
        };

        db.dropTable(table.getName());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("Table does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());
        exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("Database does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // Drop the CreateReplicaTasks on the floor so nothing ever reports back, and give tablet
    // creation a zero-second budget: the very next poll must give up instead of waiting.
    @Test
    public void testCreateTabletFailed() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                // never dispatched, so the latch is never counted down
            }
        };

        int savedTimeout = Config.tablet_create_timeout_second;
        Config.tablet_create_timeout_second = 0;
        try {
            Exception exception = Assertions.assertThrows(AlterCancelException.class,
                    () -> drivePendingJob(schemaChangeJob));
            Assertions.assertTrue(exception.getMessage().contains("Create tablet failed"),
                    exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
            Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());
        } finally {
            Config.tablet_create_timeout_second = savedTimeout;
        }

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // A node that restarts after its CreateReplicaTask was dispatched never reports it back, so the
    // job must fail as soon as the restart is visible rather than sit out the whole timeout. This is
    // the trigger from StarRocksTest#12202.
    @Test
    public void testRestartedNodeFailsTabletCreationImmediately() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                // record the epochs as the real dispatch does, then drop the batch: nothing will
                // ever report these tasks back
                schemaChangeJob.recordDispatchEpochs(batchTask);
            }
        };

        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes = clusterInfo.getComputeNodes();
        nodes.addAll(clusterInfo.getBackends());
        Assertions.assertFalse(nodes.isEmpty());
        Map<ComputeNode, Long> savedStartTime = new HashMap<>();
        nodes.forEach(node -> savedStartTime.put(node, node.getLastStartTime()));
        try {
            nodes.forEach(node -> node.setLastStartTime(1000L));

            // Dispatch round.
            schemaChangeJob.runPendingJob();
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());

            // Every node reboots after the dispatch. tablet_create_timeout_second keeps its value,
            // so a job that only honoured the deadline would still be PENDING here.
            nodes.forEach(node -> node.setLastStartTime(2000L));

            Exception exception = Assertions.assertThrows(AlterCancelException.class,
                    () -> schemaChangeJob.runPendingJob());
            Assertions.assertTrue(exception.getMessage().contains("its create tablet tasks are lost"),
                    exception.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        } finally {
            savedStartTime.forEach(ComputeNode::setLastStartTime);
        }

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // AlterJobV2.run() returns before the state machine when no compute resource can be acquired, so
    // a job left waiting on already-dispatched CreateReplicaTasks would never reach its poll. Its
    // creation deadline must still be honoured, or the only remaining bound is the day-long
    // alter_table_timeout_second.
    @Test
    public void testCreationDeadlineHonouredWithoutComputeResource() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                // never dispatched, so the latch is never counted down
            }
        };

        int savedTimeout = Config.tablet_create_timeout_second;
        Config.tablet_create_timeout_second = 0;
        try {
            schemaChangeJob.runPendingJob(); // dispatch round
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());

            new MockUp<WarehouseManager>() {
                @Mock
                public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                    throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE, "test_wh");
                }
            };
            // Neutralise the state machine so the assertion below can only be satisfied through
            // onComputeResourceUnavailable(). Without this the test passes either way: run() reaching
            // runPendingJob() would cancel on the same expired deadline, and a MockUp that silently
            // failed to intercept would go unnoticed.
            AtomicBoolean stateMachineRan = new AtomicBoolean(false);
            new MockUp<LakeTableSchemaChangeJob>() {
                @Mock
                protected void runPendingJob() {
                    stateMachineRan.set(true);
                }
            };

            schemaChangeJob.run();
            Assertions.assertFalse(stateMachineRan.get(),
                    "run() must not reach the state machine when no compute resource can be acquired");
            Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState(),
                    "the creation deadline must be enforced even when no compute resource is available");
        } finally {
            Config.tablet_create_timeout_second = savedTimeout;
        }
    }

    // A node that restarts before its tasks actually go out still receives them in its new process,
    // so that restart must not be read as a loss. The epoch is therefore captured at dispatch, not
    // while the batch is being built.
    @Test
    public void testRestartBeforeDispatchIsNotTreatedAsLoss() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes = clusterInfo.getComputeNodes();
        nodes.addAll(clusterInfo.getBackends());
        Assertions.assertFalse(nodes.isEmpty());
        Map<ComputeNode, Long> savedStartTime = new HashMap<>();
        nodes.forEach(node -> savedStartTime.put(node, node.getLastStartTime()));

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                // the restart lands after the batch was built but before it leaves FE
                nodes.forEach(node -> node.setLastStartTime(2000L));
                schemaChangeJob.recordDispatchEpochs(batchTask);
            }
        };

        try {
            nodes.forEach(node -> node.setLastStartTime(1000L));
            schemaChangeJob.runPendingJob(); // dispatch round
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());

            // The epoch seen by the poll matches the one recorded at dispatch, so nothing is lost and
            // the job keeps waiting for its deadline instead of being cancelled.
            schemaChangeJob.runPendingJob();
            Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState(),
                    "a restart that predates the send must not cancel the job");
        } finally {
            savedStartTime.forEach(ComputeNode::setLastStartTime);
        }

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // StarRocksTest#12202: the schema change scheduler is one LeaderDaemon thread that runs every
    // alter job of the cluster in turn. A job whose shadow tablets never get created must not keep
    // the jobs of other tables from making progress.
    @Test
    public void testStuckTabletCreationDoesNotBlockSiblingJobs() throws Exception {
        LakeTable sibling = createTable(connectContext,
                "CREATE TABLE t_sibling(c0 INT) duplicate key(c0) distributed by hash(c0) buckets " + NUM_BUCKETS);

        AtomicBoolean swallowTasks = new AtomicBoolean(true);
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                if (swallowTasks.get()) {
                    return; // the stuck job: nothing will ever report these tasks back
                }
                AgentTaskQueue.addBatchTask(batchTask);
                AgentTaskExecutor.submit(batchTask);
            }
        };

        LakeTableSchemaChangeJob stuckJob = alterTableAddColumn();
        stuckJob.runPendingJob(); // dispatch round; its tasks go nowhere
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, stuckJob.getJobState());

        swallowTasks.set(false);
        alterTable(connectContext, "ALTER TABLE t_sibling ADD COLUMN c1 BIGINT key NOT NULL default \"0\"");
        LakeTableSchemaChangeJob siblingJob = getAlterJob(sibling);

        // Stay well inside the stuck job's own tablet creation deadline so that it is still PENDING
        // at the end, which is what proves the sibling overtook it rather than outlived it.
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
        long deadlineMs = System.currentTimeMillis() + 20000;
        while (siblingJob.getJobState() == AlterJobV2.JobState.PENDING
                && System.currentTimeMillis() < deadlineMs) {
            // stuckJob comes first, exactly as the daemon would iterate it.
            handler.runAlterJobV2(List.of(stuckJob, siblingJob));
            Thread.sleep(10);
        }

        Assertions.assertNotEquals(AlterJobV2.JobState.PENDING, siblingJob.getJobState(),
                "a job stuck creating tablets must not block the alter jobs behind it");
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, stuckJob.getJobState());

        stuckJob.cancel("test");
        siblingJob.cancel("test");
    }

    @Test
    public void testCreateTabletSuccess() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testLightWeightTabletCreationSkipsSendTask() throws Exception {
        // Flip the table to light-weight tablet creation; the schema-change PENDING phase
        // must skip CreateReplicaTask building and dispatch while still
        // advancing the job to WAITING_TXN.
        alterTable(connectContext,
                "ALTER TABLE t0 SET ('light_weight_tablet_creation' = 'true')");
        Assertions.assertTrue(table.isLightWeightTabletCreation());

        AtomicBoolean sendCalled = new AtomicBoolean(false);
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                sendCalled.set(true);
            }
        };

        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assertions.assertFalse(sendCalled.get(),
                "sendCreateReplicaTasks must not be invoked when light_weight_tablet_creation is enabled");

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // ADD INDEX ... USING GIN carries a table-level index change, so light-weight tablet creation must be
    // disabled: the shadow tablet's on-demand schema would otherwise be built from the table's pre-alter
    // index set (only written back at job finish) and the rewritten segments would silently lack the index.
    @Test
    public void testIndexChangeDisablesLightWeightTabletCreation() throws Exception {
        boolean savedEnableGin = Config.enable_experimental_gin;
        Config.enable_experimental_gin = true;
        try {
            LakeTable ginTable = createTable(connectContext,
                        "CREATE TABLE t_gin(c0 INT, c1 VARCHAR(64)) duplicate key(c0) distributed by hash(c0) buckets "
                                    + NUM_BUCKETS);
            alterTable(connectContext, "ALTER TABLE t_gin SET ('light_weight_tablet_creation' = 'true')");
            Assertions.assertTrue(ginTable.isLightWeightTabletCreation());

            AtomicBoolean sendCalled = new AtomicBoolean(false);
            new MockUp<LakeTableSchemaChangeJob>() {
                @Mock
                public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                    sendCalled.set(true);
                    AgentTaskQueue.addBatchTask(batchTask);
                    AgentTaskExecutor.submit(batchTask);
                }
            };

            alterTable(connectContext,
                        "ALTER TABLE t_gin ADD INDEX idx_c1 (c1) USING GIN ('parser' = 'english')");
            LakeTableSchemaChangeJob schemaChangeJob = getAlterJob(ginTable);
            drivePendingJob(schemaChangeJob);
            Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
            Assertions.assertTrue(sendCalled.get(),
                        "an index change must fall back to normal tablet creation even when "
                                    + "light_weight_tablet_creation is enabled");

            schemaChangeJob.cancel("test");
            Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        } finally {
            Config.enable_experimental_gin = savedEnableGin;
        }
    }

    // And for a change to the per-column ZSTD set, which reaches the shadow tablets only through
    // CreateReplicaTask: light-weight creation skips that task, so the CN would build the shadow
    // tablet's schema from the table's pre-alter set and rewrite every segment without the setting.
    @Test
    public void testZstdCompressionChangeDisablesLightWeightTabletCreation() throws Exception {
        LakeTable zstdTable = createTable(connectContext,
                    "CREATE TABLE t_zstd(c0 INT, c1 VARCHAR(64), c2 VARCHAR(64)) duplicate key(c0) "
                                + "distributed by hash(c0) buckets " + NUM_BUCKETS
                                + " properties('zstd_compression_columns' = 'c1')");
        alterTable(connectContext, "ALTER TABLE t_zstd SET ('light_weight_tablet_creation' = 'true')");
        Assertions.assertTrue(zstdTable.isLightWeightTabletCreation());

        AtomicBoolean sendCalled = new AtomicBoolean(false);
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                sendCalled.set(true);
                AgentTaskQueue.addBatchTask(batchTask);
                AgentTaskExecutor.submit(batchTask);
            }
        };

        alterTable(connectContext, "ALTER TABLE t_zstd SET ('zstd_compression_columns' = 'c2:256k')");
        LakeTableSchemaChangeJob schemaChangeJob = getAlterJob(zstdTable);
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assertions.assertTrue(sendCalled.get(),
                    "a zstd compression column change must fall back to normal tablet creation even when "
                                + "light_weight_tablet_creation is enabled");

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    // Same contract for a bloom filter change; a mixed add+drop is used because a pure add or drop takes the
    // lake IDG fast path (no shadow tablet) and would not produce a LakeTableSchemaChangeJob at all.
    @Test
    public void testBloomFilterChangeDisablesLightWeightTabletCreation() throws Exception {
        LakeTable bfTable = createTable(connectContext,
                    "CREATE TABLE t_bf(c0 INT, c1 VARCHAR(64), c2 VARCHAR(64)) duplicate key(c0) "
                                + "distributed by hash(c0) buckets " + NUM_BUCKETS
                                + " properties('bloom_filter_columns' = 'c1')");
        alterTable(connectContext, "ALTER TABLE t_bf SET ('light_weight_tablet_creation' = 'true')");
        Assertions.assertTrue(bfTable.isLightWeightTabletCreation());

        AtomicBoolean sendCalled = new AtomicBoolean(false);
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                sendCalled.set(true);
                AgentTaskQueue.addBatchTask(batchTask);
                AgentTaskExecutor.submit(batchTask);
            }
        };

        alterTable(connectContext, "ALTER TABLE t_bf SET ('bloom_filter_columns' = 'c2')");
        LakeTableSchemaChangeJob schemaChangeJob = getAlterJob(bfTable);
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assertions.assertTrue(sendCalled.get(),
                    "a bloom filter change must fall back to normal tablet creation even when "
                                + "light_weight_tablet_creation is enabled");

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testPreviousTxnNotFinished() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return false;
            }
        };

        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testThrowAnalysisExceptionWhileWaitingTxn() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                throw new AnalysisException("isPreviousLoadFinished exception");
            }
        };

        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("sPreviousLoadFinished exception"));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableNotExistWhileWaitingTxn() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        db.dropTable(table.getName());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("Table does not exist."));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());
        exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("Database does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(db.getId(), db);
        db.registerTableUnlocked(table);
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableDroppedBeforeRewriting() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        db.dropTable(table.getName());
        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runRunningJob());
        Assertions.assertTrue(exception.getMessage().contains("Table or database does not exist"));

        db.registerTableUnlocked(table);
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());

        exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runRunningJob());
        Assertions.assertTrue(exception.getMessage().contains("Table or database does not exist"));

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(db.getId(), db);
        db.registerTableUnlocked(table);
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testAlterTabletFailed() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().stream().findFirst().get().failed();
                batchTask.getAllTasks().stream().findFirst().get().failed();
                batchTask.getAllTasks().stream().findFirst().get().failed();
            }
        };

        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runRunningJob());
        Assertions.assertTrue(exception.getMessage().contains("schema change task failed after try three times"));

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testAlterTabletSuccess() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());
        Assertions.assertTrue(schemaChangeJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);
        Collection<Partition> partitions = table.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getNextVersion());
        List<MaterializedIndex> shadowIndexes = partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // Does not support cancel job in FINISHED_REWRITING state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // LakeTablet alter job will not mark tablet force delete into TabletInvertedIndex
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getForceDeleteTablets().isEmpty());

        // Drop the table, now it's ok to cancel the job
        db.dropTable(table.getName());
        schemaChangeJob.cancel("table does not exist anymore");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testForceCancelBeforeFinishedRewritingDoesNotMark() throws Exception {
        // Regression: force-cancelling a job that has NOT reached
        // FINISHED_REWRITING must behave like a normal cancel — no no-op
        // publish happens (no version was reserved on BE), so the
        // forceSkippedAtCommitted marker must stay false. Otherwise replay
        // would later advance VisibleVersion to a version that was never
        // published.
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        // Job is still PENDING here (runPendingJob not called).
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());

        Assertions.assertTrue(schemaChangeJob.cancel("force-cancel-pending", /*force=*/ true));
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        Assertions.assertFalse(schemaChangeJob.isForceSkippedAtCommitted(),
                "force-cancel before FINISHED_REWRITING must NOT set the audit marker");
        Assertions.assertFalse(schemaChangeJob.copyForPersist().isForceSkippedAtCommitted(),
                "persisted copy must also carry marker=false so replay skips the version bump");
    }

    @Test
    public void testForceCancelAtFinishedRewriting() throws Exception {
        // Phase 2: CANCEL ALTER TABLE ... FORCE must bypass the FINISHED_REWRITING
        // guard so operators can unblock heavy lake schema-change jobs whose
        // publish RPC is permanently stuck. removeShadowIndex() inside the
        // existing persistStateChange callback handles the shadow-tablet cleanup.
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
            // Stub the no-op publish RPC so this unit test does not need a
            // live BE. Production force-cancel sends publish_version(no_op=true)
            // to advance the partition version chain; the BE-side behaviour
            // is exercised by integration tests, not here.
            @Mock
            public boolean lakePublishVersionWithSkip(String reason) {
                return true;
            }
        };

        drivePendingJob(schemaChangeJob);
        schemaChangeJob.runWaitingTxnJob();
        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        Partition partition = table.getPartitions().stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(1, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

        // Non-force cancel is a no-op in FINISHED_REWRITING (existing behavior).
        Assertions.assertFalse(schemaChangeJob.cancel("non-force-cancel"));
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());
        Assertions.assertFalse(schemaChangeJob.isForceSkippedAtCommitted());

        // Force cancel must succeed and clean up the shadow index.
        Assertions.assertTrue(schemaChangeJob.cancel("force-cancel-from-stuck-publish", /*force=*/ true));
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        Assertions.assertTrue(schemaChangeJob.isForceSkippedAtCommitted(),
                "forceSkippedAtCommitted must record the force-cancel for audit");
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size(),
                "shadow tablets must be cleaned up by removeShadowIndex on force-cancel");
    }

    @Test
    public void testForceCancelAuditFlagPersisted() throws Exception {
        // forceSkippedAtCommitted is the post-mortem audit marker for a FORCE
        // cancel. It is set on the live job during cancel(force=true), but the
        // edit log records a copyForPersist() copy. Regression guard: ensure
        // the copy carries the flag, so an FE replay or fresh load sees the
        // flag instead of silently dropping it to false.
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
            // See testForceCancelAtFinishedRewriting for why we stub this.
            @Mock
            public boolean lakePublishVersionWithSkip(String reason) {
                return true;
            }
        };
        drivePendingJob(schemaChangeJob);
        schemaChangeJob.runWaitingTxnJob();
        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        Assertions.assertTrue(schemaChangeJob.cancel("force-cancel-persist-test", /*force=*/ true));
        Assertions.assertTrue(schemaChangeJob.isForceSkippedAtCommitted());

        // The same copyForPersist() path used by EditLog.logAlterJob must carry the flag.
        AlterJobV2 persistCopy = schemaChangeJob.copyForPersist();
        Assertions.assertTrue(persistCopy.isForceSkippedAtCommitted(),
                "copyForPersist must propagate forceSkippedAtCommitted so the edit log records it");
    }

    @Test
    public void testLakePublishVersionWithSkipBuildsNoOpTxnInfo() throws Exception {
        // Cover lakePublishVersionWithSkip's body by intercepting the
        // underlying Utils.publishVersion call. The helper must build a
        // TxnInfoPB whose noOpPublish=true so BE's transactions.cpp
        // short-circuit kicks in and writes V-1 content as V.
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };
        java.util.concurrent.atomic.AtomicInteger publishCalls = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean lastNoOp = new java.util.concurrent.atomic.AtomicBoolean();
        new MockUp<Utils>() {
            @Mock
            public void publishVersion(List<Tablet> tablets, TxnInfoPB txnInfo, long baseVersion,
                                       long newVersion, com.starrocks.warehouse.cngroup.ComputeResource computeResource,
                                       boolean useAggregatePublish) {
                publishCalls.incrementAndGet();
                lastNoOp.set(txnInfo.noOpPublish);
            }
        };

        drivePendingJob(schemaChangeJob);
        schemaChangeJob.runWaitingTxnJob();
        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        Assertions.assertTrue(schemaChangeJob.cancel("force-cancel-publish-body", /*force=*/ true));
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        Assertions.assertTrue(publishCalls.get() > 0,
                "lakePublishVersionWithSkip must invoke Utils.publishVersion at least once");
        Assertions.assertTrue(lastNoOp.get(),
                "TxnInfoPB.noOpPublish must be set so BE short-circuits the txn-log apply");
    }

    @Test
    public void testForceCancelReplayBumpsVisibleVersion() throws Exception {
        // Simulate FE replaying a force-cancel edit log entry onto an
        // in-memory job loaded from a pre-cancel image. The replayed job
        // carries forceSkippedAtCommitted=true; the in-memory `this` was
        // loaded from an older image where the flag is false. Before the
        // copy-marker fix the replay branch silently skipped the version
        // bump (the if-block read this.forceSkippedAtCommitted=false), so
        // FE would resume with VisibleVersion=commitVersion-1 against BE
        // metadata already advanced by the no-op publish — the exact
        // stuck-state this PR repairs.
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
            @Mock
            public boolean lakePublishVersionWithSkip(String reason) {
                return true;
            }
        };
        drivePendingJob(schemaChangeJob);
        schemaChangeJob.runWaitingTxnJob();
        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Capture each partition's commitVersion as (current VisibleVersion + 1):
        // alter is at FINISHED_REWRITING, no other commits in flight, so the
        // reserved commit version is exactly the slot right above VisibleVersion.
        Map<Long, Long> expectedCommitVersion = new HashMap<>();
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            expectedCommitVersion.put(pp.getId(), pp.getVisibleVersion() + 1);
        }

        // Run the live force-cancel; the persisted copy must carry the marker.
        Assertions.assertTrue(schemaChangeJob.cancel("force-cancel-replay-test", /*force=*/ true));
        AlterJobV2 persistCopy = schemaChangeJob.copyForPersist();
        Assertions.assertTrue(persistCopy.isForceSkippedAtCommitted());

        // Now simulate the image: a freshly-built in-memory job WITHOUT the
        // marker, and with each partition's VisibleVersion reset back to
        // commitVersion-1 (no bump applied yet). Reset both sides to mimic
        // FE recovery from a pre-cancel image.
        LakeTableSchemaChangeJob staleInMemory = new LakeTableSchemaChangeJob(schemaChangeJob);
        staleInMemory.forceSkippedAtCommitted = false;
        staleInMemory.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            pp.setVisibleVersion(expectedCommitVersion.get(pp.getId()) - 1, 0);
        }

        // Replay: the persisted entry (`persistCopy`) carries the marker; the
        // copy-block inside replay() must propagate it onto `this` so the
        // CANCELLED branch below applies the version bump.
        staleInMemory.replay(persistCopy);
        Assertions.assertTrue(staleInMemory.isForceSkippedAtCommitted(),
                "replay must copy forceSkippedAtCommitted from the persisted entry");
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            Assertions.assertEquals(expectedCommitVersion.get(pp.getId()).longValue(), pp.getVisibleVersion(),
                    "replay must bump VisibleVersion to commitVersion when forceSkippedAtCommitted=true");
        }
    }

    @Test
    public void testParseCancelAlterTableForce() {
        // The new FORCE keyword on CANCEL ALTER TABLE must surface as
        // CancelAlterTableStmt.isForce() = true so the handler can route to
        // cancel(force=true). Without FORCE, isForce() stays false (backward
        // compatible with existing call sites).
        StatementBase withForce = SqlParser.parseSingleStatement(
                "CANCEL ALTER TABLE COLUMN FROM mydb.t1 (12345) FORCE",
                connectContext.getSessionVariable().getSqlMode());
        Assertions.assertTrue(withForce instanceof CancelAlterTableStmt);
        Assertions.assertTrue(((CancelAlterTableStmt) withForce).isForce());

        StatementBase noForce = SqlParser.parseSingleStatement(
                "CANCEL ALTER TABLE COLUMN FROM mydb.t1 (12345)",
                connectContext.getSessionVariable().getSqlMode());
        Assertions.assertTrue(noForce instanceof CancelAlterTableStmt);
        Assertions.assertFalse(((CancelAlterTableStmt) noForce).isForce());
    }

    @Test
    public void testCancelAlterForceRejectedForUnsupportedTypes() throws Exception {
        // FORCE (the publish-stuck escape hatch) is only implemented for COLUMN
        // alters on lake tables. ROLLUP -> MaterializedViewHandler and OPTIMIZE
        // -> OptimizeJobV2/OnlineOptimizeJobV2 do not honor isForce(), so the
        // analyzer must reject `... FORCE` for them up front instead of letting
        // the grammar accept a request that would silently no-op.
        for (String type : new String[] {"ROLLUP", "OPTIMIZE"}) {
            Throwable t = Assertions.assertThrows(Throwable.class, () ->
                    UtFrameUtils.parseStmtWithNewParser(
                            "CANCEL ALTER TABLE " + type + " FROM mydb.t1 (12345) FORCE", connectContext));
            Assertions.assertNotNull(t.getMessage());
            Assertions.assertTrue(t.getMessage().contains("FORCE is only supported for COLUMN"),
                    "expected FORCE-unsupported rejection for " + type + ", got: " + t.getMessage());
        }
    }

    @Test
    public void testAlterTabletSuccessEnableFileBundling() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        LakeTable table1 = createTable(connectContext, 
                    "CREATE TABLE t1(c0 INT) duplicate key(c0) distributed by hash(c0) buckets 3 " +
                                "PROPERTIES('file_bundling'='true')");
        
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        alterTable(connectContext, "ALTER TABLE t1 ADD COLUMN c1 BIGINT AS c0 + 2");
        LakeTableSchemaChangeJob schemaChangeJob1 = getAlterJob(table1);
        
        drivePendingJob(schemaChangeJob1);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob1.getJobState());

        schemaChangeJob1.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob1.getJobState());

        schemaChangeJob1.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob1.getJobState());
        Assertions.assertTrue(schemaChangeJob1.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);
        Collection<Partition> partitions = table1.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getNextVersion());
        List<MaterializedIndex> shadowIndexes = partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // Does not support cancel job in FINISHED_REWRITING state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob1.getJobState());

        while (schemaChangeJob1.getJobState() != AlterJobV2.JobState.FINISHED) {
            schemaChangeJob1.runFinishedRewritingJob();
            Thread.sleep(100);
        }
    }

    @Test
    public void testAlterTabletSuccessDisableFileBundling() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        LakeTable table1 = createTable(connectContext, 
                    "CREATE TABLE t1(c0 INT) duplicate key(c0) distributed by hash(c0) buckets 3 " +
                                "PROPERTIES('file_bundling'='false')");
        
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        alterTable(connectContext, "ALTER TABLE t1 ADD COLUMN c1 BIGINT AS c0 + 2");
        LakeTableSchemaChangeJob schemaChangeJob1 = getAlterJob(table1);
        
        drivePendingJob(schemaChangeJob1);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob1.getJobState());

        schemaChangeJob1.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob1.getJobState());

        schemaChangeJob1.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob1.getJobState());
        Assertions.assertTrue(schemaChangeJob1.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);
        Collection<Partition> partitions = table1.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getNextVersion());
        List<MaterializedIndex> shadowIndexes = partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // Does not support cancel job in FINISHED_REWRITING state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob1.getJobState());

        while (schemaChangeJob1.getJobState() != AlterJobV2.JobState.FINISHED) {
            schemaChangeJob1.runFinishedRewritingJob();
            Thread.sleep(100);
        }
    }

    @Test
    public void testPublishVersion() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        Collection<Partition> partitions = table.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);

        Assertions.assertEquals(1, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, partition.getDefaultPhysicalPartition().getNextVersion());
        // Disable send publish version
        partition.getDefaultPhysicalPartition().setNextVersion(3);

        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        List<MaterializedIndex> shadowIndexes = partition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // The partition's visible version has not catch up with the commit version of this schema change job now.
        schemaChangeJob.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Reset partition's next version
        partition.getDefaultPhysicalPartition().setVisibleVersion(2, System.currentTimeMillis());

        // Drop table
        db.dropTable(table.getName());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runFinishedRewritingJob());
        Assertions.assertTrue(exception.getMessage().contains("Table does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Add table back to database
        db.registerTableUnlocked(table);

        // Make publish version success
        new MockUp<AlterJobV2>() {
            @Mock
            public boolean publishVersion() {
                return true;
            }
        };

        while (schemaChangeJob.getJobState() != AlterJobV2.JobState.FINISHED) {
            schemaChangeJob.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, schemaChangeJob.getJobState());
        Assertions.assertTrue(schemaChangeJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);

        Assertions.assertEquals(2, table.getBaseSchema().size());
        Assertions.assertEquals("c0", table.getBaseSchema().get(0).getName());
        Assertions.assertEquals("c1", table.getBaseSchema().get(1).getName());

        Assertions.assertSame(partition, table.getPartitions().stream().findFirst().get());
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(4, partition.getDefaultPhysicalPartition().getNextVersion());

        shadowIndexes = partition.getDefaultPhysicalPartition().getLatestMaterializedIndices(IndexExtState.SHADOW);
        Assertions.assertEquals(0, shadowIndexes.size());

        List<MaterializedIndex> normalIndexes =
                    partition.getDefaultPhysicalPartition().getLatestMaterializedIndices(IndexExtState.VISIBLE);
        Assertions.assertEquals(1, normalIndexes.size());

        // Does not support cancel job in FINISHED state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, schemaChangeJob.getJobState());
    }

    @Test
    public void testPublishRetry() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        AtomicInteger numPublishRetry = new AtomicInteger(0);
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public boolean lakePublishVersion() {
                numPublishRetry.incrementAndGet();
                return false;
            }
        };
        
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        driveJobPastPending(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());
        long timeoutMs = 10 * 60 * 1000L;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (numPublishRetry.get() < 2) {
            schemaChangeJob.run();
            Thread.sleep(100);
            Assertions.assertTrue(System.currentTimeMillis() < deadline,
                    () -> String.format("publish does not retry before timeout, job state: %s", schemaChangeJob.getJobState()));
        }

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public boolean lakePublishVersion() {
                return true;
            }
        };

        deadline = System.currentTimeMillis() + timeoutMs;
        while (schemaChangeJob.getJobState() != AlterJobV2.JobState.FINISHED
                || table.getState() != OlapTable.OlapTableState.NORMAL) {
            schemaChangeJob.run();
            Thread.sleep(100);
            Assertions.assertTrue(System.currentTimeMillis() < deadline,
                    () -> String.format("publish does not finish before timeout, job state: %s, table state: %s",
                            schemaChangeJob.getJobState(), table.getState()));
        }
        Assertions.assertEquals(2, table.getBaseSchema().size());
        Assertions.assertEquals("c0", table.getBaseSchema().get(0).getName());
        Assertions.assertEquals("c1", table.getBaseSchema().get(1).getName());
    }

    /**
     * Ids drawn by other transactions between the watershed and the shadow index exposure no longer cancel
     * the job: the id generator is cluster-wide, and a writer that misses the shadow index is rejected at
     * commit instead (LakeTableTxnStateListener). The exposed shadow index carries the watershed for that check.
     */
    @Test
    public void testConcurrentTxnIdDoesNotCancelJob() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public long peekNextTransactionId() {
                return 10103L; // another id (10102) was drawn after the watershed
            }
        };

        // Runs only the PENDING phase, so the job stops at WAITING_TXN with the shadow index exposed.
        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assertions.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        for (PhysicalPartition partition : table.getPhysicalPartitions()) {
            List<MaterializedIndex> shadowIndexes = partition.getLatestMaterializedIndices(IndexExtState.SHADOW);
            Assertions.assertFalse(shadowIndexes.isEmpty());
            for (MaterializedIndex shadowIndex : shadowIndexes) {
                Assertions.assertEquals(10101L, shadowIndex.getShadowWatershedTxnId());
            }
        }

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testShow() {
        UtFrameUtils.mockInitWarehouseEnv();

        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        LakeTableSchemaChangeJob alterJobV2 =
                    new LakeTableSchemaChangeJob(12345L, db.getId(), table.getId(), table.getName(), 10);
        alterJobV2.addIndexSchema(1L, 2L, "a", (short) 1, Lists.newArrayList());

        schemaChangeHandler.addAlterJobV2(alterJobV2);
        System.out.println(schemaChangeHandler.getAlterJobInfosByDb(db));

        SchemaChangeHandler schemaChangeHandler2 = new SchemaChangeHandler();
        alterJobV2 = new LakeTableSchemaChangeJob(12345L, db.getId(), table.getId(), table.getName(), 10);
        alterJobV2.addIndexSchema(1L, 2L, "a", (short) 1, Lists.newArrayList());
        schemaChangeHandler2.addAlterJobV2(alterJobV2);
        System.out.println(schemaChangeHandler2.getAlterJobInfosByDb(db));
    }

    // Cancelling a job whose shadow tablets are still being created must drop its CreateReplicaTasks
    // from the queue. This used to be done from inside the blocking wait, which cancel() reached by
    // force-releasing the latch from outside the job monitor.
    @Test
    public void testCancelWhileCreatingTablets() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        AtomicReference<AgentBatchTask> dispatched = new AtomicReference<>();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendCreateReplicaTasks(AgentBatchTask batchTask) {
                dispatched.set(batchTask);
                AgentTaskQueue.addBatchTask(batchTask);
            }
        };

        // Dispatch round only: the tasks stay outstanding because nothing reports them back.
        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertNotNull(dispatched.get());
        Assertions.assertTrue(dispatched.get().getTaskNum() > 0);
        for (AgentTask task : dispatched.get().getAllTasks()) {
            Assertions.assertNotNull(AgentTaskQueue.getTask(task.getBackendId(), TTaskType.CREATE,
                    task.getSignature()));
        }

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        for (AgentTask task : dispatched.get().getAllTasks()) {
            Assertions.assertNull(AgentTaskQueue.getTask(task.getBackendId(), TTaskType.CREATE,
                    task.getSignature()));
        }
    }

    @Test
    public void testInsertConcurrent() throws Exception {
        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.BE, "127.0.0.1");
        // block the schema change job to simulate concurrent insert
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), List.of(table.getId()), UUID.randomUUID().toString(), coordinator,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        driveJobPastPending(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, "insert into t0 values (1)").second;
        List<Column> fullSchema = table.getFullSchema();
        Assertions.assertEquals(2, fullSchema.size());
        Assertions.assertEquals("c0", fullSchema.get(0).getName());
        Assertions.assertEquals("c1", fullSchema.get(1).getName());
        List<Expr> outputExprs = execPlan.getOutputExprs();
        Assertions.assertEquals(fullSchema.size(), outputExprs.size());
        for (int i = 0; i < fullSchema.size(); i++) {
            Assertions.assertEquals(fullSchema.get(i).getType(), outputExprs.get(i).getType());
        }
    }

    @Test
    public void testAlterAfterFseV2() throws Exception {
        // Create a table with multiple partitions
        LakeTable multiPartitionTable = createTable(connectContext,
                "CREATE TABLE t_multi_partition(c0 INT) duplicate key(c0) " +
                        "PARTITION BY RANGE(c0) (" +
                        "PARTITION p1 VALUES [(\"0\"), (\"100\"))," +
                        "PARTITION p2 VALUES [(\"100\"), (\"200\"))" +
                        ") " +
                        "distributed by hash(c0) buckets 3");

        // Alter table to add a value column which is expected to use fast schema evolution v2
        alterTable(connectContext, "ALTER TABLE t_multi_partition ADD COLUMN c1 BIGINT");
        // Alter table to add a generated column
        alterTable(connectContext, "ALTER TABLE t_multi_partition ADD COLUMN c2 BIGINT AS c0 + c1");
        LakeTableSchemaChangeJob schemaChangeJob = getAlterJob(multiPartitionTable);

        // Capture AgentBatchTask objects
        List<AgentBatchTask> capturedBatchTasks = new ArrayList<>();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                capturedBatchTasks.add(batchTask);
                // Mark tasks as finished to allow job to proceed
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        drivePendingJob(schemaChangeJob);
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Verify that we captured at least one batch task
        Assertions.assertFalse(capturedBatchTasks.isEmpty(), "Should have captured at least one AgentBatchTask");

        // Extract all AlterReplicaTask objects
        List<AlterReplicaTask> alterReplicaTasks = new ArrayList<>();
        for (AgentBatchTask batchTask : capturedBatchTasks) {
            for (com.starrocks.task.AgentTask agentTask : batchTask.getAllTasks()) {
                if (agentTask instanceof AlterReplicaTask) {
                    alterReplicaTasks.add((AlterReplicaTask) agentTask);
                }
            }
        }

        Assertions.assertFalse(alterReplicaTasks.isEmpty(), "Should have at least one AlterReplicaTask");

        // Get table and TabletInvertedIndex for verification
        OlapTable tbl = (OlapTable) multiPartitionTable;
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        // Group tasks by originIndexId (the index ID of the origin tablet)
        Map<Long, List<AlterReplicaTask>> tasksByOriginIndexId = new HashMap<>();
        Map<Long, TTabletSchema> expectedSchemasByIndexId = new HashMap<>();

        for (AlterReplicaTask task : alterReplicaTasks) {
            // Verify baseTabletReadSchema is not null
            TAlterTabletReqV2 request = task.toThrift();
            Assertions.assertTrue(request.isSetBase_tablet_read_schema(),
                    "base_tablet_read_schema should be set for tablet " + task.getBaseTabletId());
            Assertions.assertNotNull(request.getBase_tablet_read_schema(),
                    "base_tablet_read_schema should not be null for tablet " + task.getBaseTabletId());

            // Get originIndexId from TabletInvertedIndex using baseTabletId
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(task.getBaseTabletId());
            Assertions.assertNotNull(tabletMeta, "TabletMeta should exist for tablet " + task.getBaseTabletId());
            long originIndexId = tabletMeta.getIndexId();

            // Group tasks by originIndexId
            tasksByOriginIndexId.computeIfAbsent(originIndexId, k -> new ArrayList<>()).add(task);

            // Create expected schema if not already created
            if (!expectedSchemasByIndexId.containsKey(originIndexId)) {
                TTabletSchema expectedSchema = SchemaInfo.fromMaterializedIndex(
                        tbl, originIndexId, tbl.getIndexMetaByMetaId(originIndexId)).toTabletSchema();
                expectedSchemasByIndexId.put(originIndexId, expectedSchema);
            }
        }

        // Verify that tasks with the same originIndexId use the same baseTabletReadSchema
        for (Map.Entry<Long, List<AlterReplicaTask>> entry : tasksByOriginIndexId.entrySet()) {
            long originIndexId = entry.getKey();
            List<AlterReplicaTask> tasks = entry.getValue();
            TTabletSchema expectedSchema = expectedSchemasByIndexId.get(originIndexId);

            Assertions.assertNotNull(expectedSchema, "Expected schema should exist for index " + originIndexId);

            // Verify all tasks for this index use the same schema
            for (AlterReplicaTask task : tasks) {
                TAlterTabletReqV2 request = task.toThrift();
                TTabletSchema actualSchema = request.getBase_tablet_read_schema();

                // Verify schema ID matches
                Assertions.assertEquals(expectedSchema.getId(), actualSchema.getId(),
                        "Schema ID should match for index " + originIndexId);
                Assertions.assertEquals(expectedSchema.getKeys_type(), actualSchema.getKeys_type(),
                        "Keys type should match for index " + originIndexId);
                Assertions.assertEquals(expectedSchema.getShort_key_column_count(),
                        actualSchema.getShort_key_column_count(),
                        "Short key column count should match for index " + originIndexId);
                Assertions.assertEquals(expectedSchema.getColumns().size(), actualSchema.getColumns().size(),
                        "Column count should match for index " + originIndexId);
            }
        }

        // Verify that different originIndexId use different schemas (if there are multiple indices)
        if (tasksByOriginIndexId.size() > 1) {
            List<Long> indexIds = new ArrayList<>(tasksByOriginIndexId.keySet());
            TTabletSchema schema1 = expectedSchemasByIndexId.get(indexIds.get(0));
            TTabletSchema schema2 = expectedSchemasByIndexId.get(indexIds.get(1));
            // They might have the same schema if they're from the same origin index, but structure should be correct
            Assertions.assertNotNull(schema1);
            Assertions.assertNotNull(schema2);
        }
    }

    @Test
    public void testAggTableAddKeyColumnSortKeyUpdated() throws Exception {
        LakeTable aggTable = createTable(connectContext,
                "CREATE TABLE t_agg_sort_key (k0 INT, k1 INT, v0 INT SUM)"
                        + " AGGREGATE KEY(k0, k1) DISTRIBUTED BY HASH(k0) BUCKETS " + NUM_BUCKETS
                        + " ORDER BY(k1, k0)");
        doTestSortKeyUpdatedAfterAddKeyColumn(aggTable, "t_agg_sort_key");
    }

    @Test
    public void testUniqueTableAddKeyColumnSortKeyUpdated() throws Exception {
        LakeTable uniqTable = createTable(connectContext,
                "CREATE TABLE t_uniq_sort_key (k0 INT, k1 INT, v0 VARCHAR(1024))"
                        + " UNIQUE KEY(k0, k1) DISTRIBUTED BY HASH(k0) BUCKETS " + NUM_BUCKETS
                        + " ORDER BY(k1, k0)");
        doTestSortKeyUpdatedAfterAddKeyColumn(uniqTable, "t_uniq_sort_key");
    }

    private void doTestSortKeyUpdatedAfterAddKeyColumn(LakeTable tbl, String tableName) throws Exception {
        Assertions.assertEquals(Arrays.asList(1, 0),
                tbl.getIndexMetaByMetaId(tbl.getBaseIndexMetaId()).getSortKeyIdxes());

        alterTable(connectContext, "ALTER TABLE " + tableName + " ADD COLUMN k_new1 INT KEY AFTER k0");
        runSchemaChangeJobToFinish(tbl);

        Assertions.assertEquals("k0", tbl.getBaseSchema().get(0).getName());
        Assertions.assertEquals("k_new1", tbl.getBaseSchema().get(1).getName());
        Assertions.assertEquals("k1", tbl.getBaseSchema().get(2).getName());
        assertSortKey(tbl, Arrays.asList(2, 0, 1));
    }

    private void assertSortKey(LakeTable tbl, List<Integer> expectedSortKeyIndexes) {
        List<Column> columns = tbl.getBaseSchema();
        MaterializedIndexMeta indexMeta = tbl.getIndexMetaByMetaId(tbl.getBaseIndexMetaId());
        Assertions.assertEquals(expectedSortKeyIndexes, indexMeta.getSortKeyIdxes());
        List<Integer> sortKeyUniqueIds = indexMeta.getSortKeyUniqueIds();
        Assertions.assertNotNull(sortKeyUniqueIds);
        Assertions.assertEquals(expectedSortKeyIndexes.size(), sortKeyUniqueIds.size());
        for (int i = 0; i < expectedSortKeyIndexes.size(); i++) {
            Assertions.assertEquals(columns.get(expectedSortKeyIndexes.get(i)).getUniqueId(),
                    (int) sortKeyUniqueIds.get(i));
        }
    }

    private LakeTableSchemaChangeJob runSchemaChangeJobToFinish(LakeTable tbl) throws Exception {
        LakeTableSchemaChangeJob job = getAlterJob(tbl);

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };
        drivePendingJob(job);
        job.runWaitingTxnJob();
        job.runRunningJob();

        new MockUp<AlterJobV2>() {
            @Mock
            public boolean publishVersion() {
                return true;
            }
        };
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        return job;
    }

    /**
     * Shadow tablets copy their origin tablets' ranges. With lake_enable_tablet_creation_optimization the
     * job created them from one partition-shared version-1 object, which carries a single range, so every
     * shadow tablet after the first got the first one's range.
     */
    @Test
    public void testRangeTableShadowTabletsGetTheirOwnRanges() throws Exception {
        boolean oldOptimization = Config.lake_enable_tablet_creation_optimization;
        boolean oldRangeDistribution = Config.enable_range_distribution;
        boolean oldFastSchemaEvolution = Config.enable_fast_schema_evolution_in_share_data_mode;
        Config.lake_enable_tablet_creation_optimization = true;
        Config.enable_range_distribution = true;
        // Fast schema evolution would turn the type change into a metadata-only job.
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        try {
            LakeTable anchor = createTable(connectContext, "CREATE TABLE r_anchor (k1 INT, k2 INT, v INT) "
                    + "DUPLICATE KEY(k1, k2) ORDER BY (k1, k2) "
                    + "PROPERTIES('replication_num' = '1', 'file_bundling' = 'false', "
                    + "'colocate_with' = 'g_shadow_range:k1')");
            ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
            long grpId = colocateTableIndex.getGroup(anchor.getId()).grpId;
            ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
            long firstShardGroupId = rangeMgr.getColocateRanges(grpId).get(0).getShardGroupId();
            long secondShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().createShardGroup(
                    db.getId(), anchor.getId(), 0L, 0L, PlacementPolicy.PACK);
            Tuple b100 = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
            rangeMgr.setColocateRanges(grpId, Arrays.asList(
                    new ColocateRange(Range.lt(b100), firstShardGroupId),
                    new ColocateRange(Range.ge(b100), secondShardGroupId)));

            LakeTable rangeTable = createTable(connectContext, "CREATE TABLE r1 (k1 INT, k2 INT, v INT) "
                    + "DUPLICATE KEY(k1, k2) ORDER BY (k1, k2) "
                    + "PROPERTIES('replication_num' = '1', 'file_bundling' = 'false', "
                    + "'colocate_with' = 'g_shadow_range:k1')");
            Assertions.assertTrue(rangeTable.isRangeDistribution());
            Assertions.assertEquals(2, rangeTable.getPartitions().iterator().next().getDefaultPhysicalPartition()
                    .getLatestBaseIndex().getTablets().size());

            List<CreateReplicaTask> built = new ArrayList<>();
            new MockUp<CreateReplicaTask.Builder>() {
                @Mock
                public CreateReplicaTask build(Invocation inv) {
                    CreateReplicaTask task = inv.proceed();
                    built.add(task);
                    return task;
                }
            };

            // A value-column type change rewrites data, so it runs as a LakeTableSchemaChangeJob.
            alterTable(connectContext, "ALTER TABLE r1 MODIFY COLUMN v BIGINT");
            LakeTableSchemaChangeJob job = getAlterJob(rangeTable);
            drivePendingJob(job);

            List<CreateReplicaTask> shadowTasks = built.stream()
                    .filter(task -> task.getTableId() == rangeTable.getId())
                    .collect(Collectors.toList());
            List<Tablet> shadowTablets = rangeTable.getPartitions().iterator().next().getDefaultPhysicalPartition()
                    .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL).stream()
                    .filter(index -> index.getState() == MaterializedIndex.IndexState.SHADOW)
                    .flatMap(index -> index.getTablets().stream())
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, shadowTablets.size());
            Assertions.assertEquals(2, shadowTasks.size(), "every shadow tablet must get its own create task");
            for (Tablet shadowTablet : shadowTablets) {
                TCreateTabletReq request = shadowTasks.stream()
                        .filter(t -> t.getTabletId() == shadowTablet.getId())
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("no create task for " + shadowTablet.getId()))
                        .toThrift();
                Assertions.assertEquals(shadowTablet.getRange().toThrift(), request.getRange());
                Assertions.assertFalse(request.isEnable_tablet_creation_optimization());
            }
        } finally {
            Config.lake_enable_tablet_creation_optimization = oldOptimization;
            Config.enable_range_distribution = oldRangeDistribution;
            Config.enable_fast_schema_evolution_in_share_data_mode = oldFastSchemaEvolution;
        }
    }

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        completeTasksAndPublishOnSend();
        LakeTableSchemaChangeJob job = alterTableAddColumn();
        assertHasTabletData(job);

        runToFinished(job);

        assertPruned(job);
        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(job.getJobId());
        for (LakeTableSchemaChangeJob entry : entries) {
            assertHasTabletData(entry);
        }
        LakeTableSchemaChangeJob finishedEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, finishedEntry.getJobState());
        assertShowAlterUnchangedByPrune(finishedEntry);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        LakeTableSchemaChangeJob job = runningJobWithUnfinishedTasks();
        Assertions.assertTrue(job.schemaChangeBatchTask.getTaskNum() > 0);

        Assertions.assertTrue(job.cancel("cancel"));

        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(0, shadowIndexCount(table));
        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(job.getJobId());
        for (LakeTableSchemaChangeJob entry : entries) {
            assertHasTabletData(entry);
        }
        LakeTableSchemaChangeJob cancelledEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, cancelledEntry.getJobState());
        assertShowAlterUnchangedByPrune(cancelledEntry);
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        completeTasksAndPublishOnSend();
        // The follower starts from the catalog as it was before the alter.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        LakeTableSchemaChangeJob job = alterTableAddColumn();
        runToFinished(job);
        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(job.getJobId());

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        for (LakeTableSchemaChangeJob entry : entries) {
            replayOn(followerMetastore, followerHandler, entry);
        }

        // The handler keeps the first replayed entry and copies the data of each later one into it.
        LakeTableSchemaChangeJob retained = entries.get(0);
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        for (LakeTableSchemaChangeJob entry : entries.subList(1, entries.size())) {
            assertHasTabletData(entry);
        }
        OlapTable followerTable = tableIn(followerMetastore);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, followerTable.getState());
        Assertions.assertEquals(List.of("c0", "c1"),
                followerTable.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList()));
        Assertions.assertEquals(0, shadowIndexCount(followerTable));
    }

    @Test
    public void testFollowerReplayDropsShadowIndexOfCancelledJob() throws Exception {
        // The follower starts from the catalog as it was before the alter.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        LakeTableSchemaChangeJob job = runningJobWithUnfinishedTasks();
        Assertions.assertTrue(job.cancel("cancel"));
        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(job.getJobId());
        Assertions.assertEquals(
                List.of(AlterJobV2.JobState.PENDING, AlterJobV2.JobState.WAITING_TXN, AlterJobV2.JobState.CANCELLED),
                entries.stream().map(AlterJobV2::getJobState).collect(Collectors.toList()));

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        replayOn(followerMetastore, followerHandler, entries.get(0));
        replayOn(followerMetastore, followerHandler, entries.get(1));
        Assertions.assertEquals(1, shadowIndexCount(followerTable));
        replayOn(followerMetastore, followerHandler, entries.get(2));

        // The CANCELLED replay drops the shadow indexes it reads from the entry; the prune comes after it.
        LakeTableSchemaChangeJob retained = entries.get(0);
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, retained.getJobState());
        assertPruned(retained);
        Assertions.assertEquals(0, shadowIndexCount(followerTable));
        Assertions.assertEquals(catalogSnapshot(table), catalogSnapshot(followerTable));
    }

    @Test
    public void testReplaySameCancelledEntryTwice() throws Exception {
        LakeTableSchemaChangeJob job = runningJobWithUnfinishedTasks();
        // The follower starts from the catalog with the shadow index in place.
        UtFrameUtils.PseudoImage runningImage = saveMetastore();
        Assertions.assertTrue(job.cancel("cancel"));
        LakeTableSchemaChangeJob entry = lastJournaledEntry(job.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, entry.getJobState());
        List<Object> catalogAfterCancel = catalogSnapshot(table);

        LocalMetastore followerMetastore = loadMetastore(runningImage);
        Assertions.assertNotEquals(catalogAfterCancel, catalogSnapshot(tableIn(followerMetastore)));
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        replayCopyAndAssertPruned(followerHandler, entry, followerMetastore, catalogAfterCancel);
        AlterJobV2 retained = followerHandler.getAlterJobsV2().get(job.getJobId());
        replayCopyAndAssertPruned(followerHandler, entry, followerMetastore, catalogAfterCancel);
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
    }

    @Test
    public void testReplayCancelledEntryOntoJobLoadedFromImage() throws Exception {
        LakeTableSchemaChangeJob job = runningJobWithUnfinishedTasks();
        AlterJobMgr leaderMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        leaderMgr.getSchemaChangeHandler().addAlterJobV2(job);
        Assertions.assertTrue(job.cancel("cancel"));
        // An image taken after the cancel holds the already pruned job; the catalog is the one the cancel left.
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        leaderMgr.save(image.getImageWriter());
        LakeTableSchemaChangeJob entry = lastJournaledEntry(job.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, entry.getJobState());
        List<Object> catalogAfterCancel = catalogSnapshot(table);

        AlterJobMgr followerMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        followerMgr.load(reader);
        reader.close();
        SchemaChangeHandler followerHandler = followerMgr.getSchemaChangeHandler();
        AlterJobV2 loaded = followerHandler.getAlterJobsV2().get(job.getJobId());
        assertPruned((LakeTableSchemaChangeJob) loaded);

        replayCopyAndAssertPruned(followerHandler, entry, GlobalStateMgr.getCurrentState().getLocalMetastore(),
                catalogAfterCancel);
        Assertions.assertSame(loaded, followerHandler.getAlterJobsV2().get(job.getJobId()));
    }

    @Test
    public void testLoadPrunesDoneJobFromImage() throws Exception {
        LakeTableSchemaChangeJob job = runningJobWithUnfinishedTasks();
        Assertions.assertTrue(job.cancel("cancel"));
        // Images written by older versions hold the done job with its per-tablet data, as the journal entry does.
        LakeTableSchemaChangeJob doneJob = lastJournaledEntry(job.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, doneJob.getJobState());
        assertHasTabletData(doneJob);
        AlterJobMgr alterJobMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        alterJobMgr.getSchemaChangeHandler().addAlterJobV2(doneJob);
        // SHOW ALTER of the loaded job must match that of a job that went through the same serialization.
        List<List<Comparable>> expectedRows = new ArrayList<>();
        gsonCopy(doneJob).getInfo(expectedRows);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restoredMgr.load(reader);
        reader.close();

        LakeTableSchemaChangeJob restored =
                (LakeTableSchemaChangeJob) restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertPruned(restored);
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        Assertions.assertFalse(expectedRows.isEmpty());
        Assertions.assertEquals(expectedRows, restoredRows);
    }

    @Test
    public void testForceCancelWithPublishInFlightPrunesOncePublishCompletes() throws Exception {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }

            @Mock
            public boolean lakePublishVersionWithSkip(String reason) {
                return true;
            }
        };
        LakeTableSchemaChangeJob job = finishedRewritingJob();
        CompletableFuture<Boolean> publish = publishInFlight(job);
        SchemaChangeHandler handler = new SchemaChangeHandler();
        handler.addAlterJobV2(job);

        Assertions.assertTrue(job.cancel("force cancel", /*force=*/ true));

        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPrunedOncePublishCompletes(job, publish, handler);
    }

    @Test
    public void testCancelAfterTableDroppedWithPublishInFlightPrunesOncePublishCompletes() throws Exception {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };
        LakeTableSchemaChangeJob job = finishedRewritingJob();
        CompletableFuture<Boolean> publish = publishInFlight(job);
        SchemaChangeHandler handler = new SchemaChangeHandler();
        handler.addAlterJobV2(job);
        db.dropTable(table.getName());

        Assertions.assertTrue(job.cancel("table does not exist anymore"));

        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPrunedOncePublishCompletes(job, publish, handler);
    }

    /**
     * Marks every schema change task finished as soon as it is sent, and lets the publish succeed without
     * reaching a node.
     */
    private static void completeTasksAndPublishOnSend() {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }

            @Mock
            public boolean lakePublishVersion() {
                return true;
            }
        };
    }

    /**
     * Drives a new ADD COLUMN job to RUNNING, with its schema change tasks sent but never finished.
     */
    private LakeTableSchemaChangeJob runningJobWithUnfinishedTasks() throws Exception {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
            }
        };
        LakeTableSchemaChangeJob job = alterTableAddColumn();
        drivePendingJob(job);
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        return job;
    }

    /**
     * Drives a new ADD COLUMN job to FINISHED_REWRITING. The caller makes the schema change tasks finish.
     */
    private LakeTableSchemaChangeJob finishedRewritingJob() throws Exception {
        LakeTableSchemaChangeJob job = alterTableAddColumn();
        drivePendingJob(job);
        job.runWaitingTxnJob();
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        return job;
    }

    /**
     * Makes the job look as if its publish task was submitted and has not returned yet: that task reads the
     * shadow indexes without the job lock.
     */
    private static CompletableFuture<Boolean> publishInFlight(LakeTableSchemaChangeJob job) {
        CompletableFuture<Boolean> publish = new CompletableFuture<>();
        job.publishVersionFuture = publish;
        return publish;
    }

    /**
     * The cancelled job keeps its per-tablet data while its publish task may still read it, and the next
     * scheduling round of its handler prunes it once the task has completed.
     */
    private static void assertPrunedOncePublishCompletes(LakeTableSchemaChangeJob job,
                                                         CompletableFuture<Boolean> publish,
                                                         SchemaChangeHandler handler) {
        assertHasTabletData(job);
        handler.runAfterLeaseValid();
        assertHasTabletData(job);

        publish.complete(true);
        handler.runAfterLeaseValid();
        assertPruned(job);
    }

    private static void runToFinished(LakeTableSchemaChangeJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    /**
     * Reads the journal entries of the job, from the one that submitted it to the one of its final state.
     */
    private static List<LakeTableSchemaChangeJob> readJournaledEntries(long jobId) throws Exception {
        List<LakeTableSchemaChangeJob> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
            Assertions.assertEquals(jobId, entry.getJobId());
            entries.add((LakeTableSchemaChangeJob) entry);
        } while (!entry.isDone());
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, entries.get(0).getJobState());
        return entries;
    }

    private static LakeTableSchemaChangeJob lastJournaledEntry(long jobId) throws Exception {
        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(jobId);
        return entries.get(entries.size() - 1);
    }

    private static AlterJobV2 gsonCopy(AlterJobV2 job) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, AlterJobV2.class), AlterJobV2.class);
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

    private OlapTable tableIn(LocalMetastore metastore) {
        return (OlapTable) metastore.getTable(DB_NAME, table.getName());
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

    /**
     * Replays an independently deserialized copy of the CANCELLED journal entry, then checks that the job kept by
     * the handler is pruned and that the catalog is the same as right after the leader applied the entry.
     */
    private void replayCopyAndAssertPruned(SchemaChangeHandler handler, AlterJobV2 entry, LocalMetastore metastore,
                                           List<Object> expectedCatalog) {
        replayOn(metastore, handler, gsonCopy(entry));

        LakeTableSchemaChangeJob retained = (LakeTableSchemaChangeJob) handler.getAlterJobsV2().get(entry.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, retained.getJobState());
        assertPruned(retained);
        Assertions.assertEquals(expectedCatalog, catalogSnapshot(tableIn(metastore)));
    }

    /**
     * What a CANCELLED replay changes in the catalog: the table state, its indexes and those of each partition.
     */
    private static List<Object> catalogSnapshot(OlapTable olapTable) {
        List<Object> snapshot = new ArrayList<>();
        snapshot.add(olapTable.getState());
        snapshot.add(new TreeSet<>(olapTable.getIndexNameToMetaId().keySet()));
        for (PhysicalPartition physicalPartition : olapTable.getPhysicalPartitions()) {
            snapshot.add(physicalPartition.getLatestMaterializedIndices(IndexExtState.ALL).size());
        }
        return snapshot;
    }

    private static int shadowIndexCount(OlapTable olapTable) {
        int count = 0;
        for (PhysicalPartition physicalPartition : olapTable.getPhysicalPartitions()) {
            count += physicalPartition.getLatestMaterializedIndices(IndexExtState.SHADOW).size();
        }
        return count;
    }

    /**
     * SHOW ALTER of a done job reads none of the data the prune drops.
     */
    private static void assertShowAlterUnchangedByPrune(LakeTableSchemaChangeJob doneJob) {
        List<List<Comparable>> rowsBefore = new ArrayList<>();
        doneJob.getInfo(rowsBefore);
        doneJob.pruneMeta();
        assertPruned(doneJob);
        List<List<Comparable>> rowsAfter = new ArrayList<>();
        doneJob.getInfo(rowsAfter);
        Assertions.assertFalse(rowsBefore.isEmpty());
        Assertions.assertEquals(rowsBefore, rowsAfter);
    }

    /**
     * Sizes of the job's per-tablet data: the shadow tablet map, the shadow index map, the shadow schemas and
     * their short key counts.
     */
    private static List<Integer> tabletDataSizes(LakeTableSchemaChangeJob job) {
        com.google.common.collect.Table<?, ?, ?> tabletMap =
                Deencapsulation.getField(job, "physicalPartitionIndexTabletMap");
        com.google.common.collect.Table<?, ?, ?> indexMap = Deencapsulation.getField(job, "physicalPartitionIndexMap");
        Map<?, ?> schemas = Deencapsulation.getField(job, "indexMetaIdToSchema");
        Map<?, ?> shortKeys = Deencapsulation.getField(job, "indexMetaIdToShortKey");
        return List.of(tabletMap.size(), indexMap.size(), schemas.size(), shortKeys.size());
    }

    private static void assertHasTabletData(LakeTableSchemaChangeJob job) {
        Assertions.assertFalse(tabletDataSizes(job).contains(0), () -> "per-tablet data " + tabletDataSizes(job));
    }

    private static void assertPruned(LakeTableSchemaChangeJob job) {
        Assertions.assertEquals(List.of(0, 0, 0, 0), tabletDataSizes(job));
        Assertions.assertEquals(0, job.schemaChangeBatchTask.getTaskNum());
    }

    /**
     * A schema change of a lake table that modifies a column its MVs read inactivates every one of them, in any
     * database, and the MVs above them, and journals each inactivation with the clear of the version map: a
     * follower and the next leader see the MVs inactive too.
     */
    @Test
    public void testFinishedJobInactivatesAndJournalsRelatedMvs() throws Exception {
        String otherDb = DB_NAME + "_mv";
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(otherDb);
        // The column change has to go through this job rather than a fast schema evolution.
        boolean oldFastSchemaEvolution = Config.enable_fast_schema_evolution_in_share_data_mode;
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        try {
            createTable(connectContext, "CREATE TABLE t_mv_base(c0 INT, c1 INT) duplicate key(c0) " +
                    "distributed by hash(c0) buckets " + NUM_BUCKETS);
            StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
            String mvProps = "DISTRIBUTED BY HASH(c0) BUCKETS 1 REFRESH DEFERRED MANUAL ";
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + DB_NAME + ".mv_same1 " + mvProps +
                    "AS SELECT c0, c1 FROM " + DB_NAME + ".t_mv_base");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + DB_NAME + ".mv_same2 " + mvProps +
                    "AS SELECT c0, c1 FROM " + DB_NAME + ".t_mv_base");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + otherDb + ".mv_other " + mvProps +
                    "AS SELECT c0, c1 FROM " + DB_NAME + ".t_mv_base");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + otherDb + ".mv_above " + mvProps +
                    "AS SELECT c0, c1 FROM " + DB_NAME + ".mv_same1");
            List<MaterializedView> mvs = new ArrayList<>();
            for (String name : List.of(DB_NAME + ".mv_same1", DB_NAME + ".mv_same2", otherDb + ".mv_other",
                    otherDb + ".mv_above")) {
                String[] parts = name.split("\\.");
                MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(parts[0], parts[1]);
                Assertions.assertTrue(mv.isActive());
                mvs.add(mv);
            }

            LakeTable baseTable = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(DB_NAME, "t_mv_base");
            alterTable(connectContext, "ALTER TABLE t_mv_base MODIFY COLUMN c1 BIGINT");
            LakeTableSchemaChangeJob schemaChangeJob = getAlterJob(baseTable);
            new MockUp<LakeTableSchemaChangeJob>() {
                @Mock
                public void sendAgentTask(AgentBatchTask batchTask) {
                    batchTask.getAllTasks().forEach(t -> t.setFinished(true));
                }
            };
            new MockUp<AlterJobV2>() {
                @Mock
                public boolean publishVersion() {
                    return true;
                }
            };
            List<AlterMaterializedViewStatusLog> journaled = new ArrayList<>();
            new MockUp<EditLog>() {
                @Mock
                public void logAlterMvStatus(Invocation invocation, AlterMaterializedViewStatusLog log,
                                             WALApplier walApplier) {
                    journaled.add(log);
                    invocation.proceed();
                }
            };

            drivePendingJob(schemaChangeJob);
            schemaChangeJob.runWaitingTxnJob();
            schemaChangeJob.runRunningJob();
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());
            long deadlineMs = System.currentTimeMillis() + 60000;
            while (schemaChangeJob.getJobState() != AlterJobV2.JobState.FINISHED) {
                Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "the schema change never finished");
                schemaChangeJob.runFinishedRewritingJob();
                Thread.sleep(10);
            }

            for (MaterializedView mv : mvs) {
                Assertions.assertFalse(mv.isActive(), mv.getName() + " is still active");
                AlterMaterializedViewStatusLog log = journaled.stream()
                        .filter(l -> l.getTableId() == mv.getId())
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("the inactivation of " + mv.getName()
                                + " was not journaled"));
                Assertions.assertEquals(AlterMaterializedViewStatusClause.INACTIVE, log.getStatus());
                Assertions.assertTrue(log.isClearVersionMap());

                // What a follower replays.
                mv.setActive();
                GlobalStateMgr.getCurrentState().getAlterJobMgr().replayAlterMaterializedViewStatus(log);
                Assertions.assertFalse(mv.isActive());
            }
        } finally {
            Config.enable_fast_schema_evolution_in_share_data_mode = oldFastSchemaEvolution;
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, otherDb, true);
        }
    }
}
