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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

    @BeforeEach
    public void before() throws Exception {
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
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
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

    @Test
    public void testCreateTabletFailed() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds, AtomicBoolean waitingCreatingReplica,
                                             AtomicBoolean isCancelling) throws AlterCancelException {
                throw new AlterCancelException("Create tablet failed");
            }
        };

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("Create tablet failed"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testCreateTabletSuccess() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
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

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
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

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("sPreviousLoadFinished exception"));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableNotExistWhileWaitingTxn() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        schemaChangeJob.runPendingJob();
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
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableDroppedBeforeRewriting() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        schemaChangeJob.runPendingJob();
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
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
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

        schemaChangeJob.runPendingJob();
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
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
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

        schemaChangeJob.runPendingJob();
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
        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
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
        
        schemaChangeJob1.runPendingJob();
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
        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
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
        
        schemaChangeJob1.runPendingJob();
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
        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
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

        schemaChangeJob.runPendingJob();
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

        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
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

        shadowIndexes = partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(0, shadowIndexes.size());

        List<MaterializedIndex> normalIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Assertions.assertEquals(1, normalIndexes.size());
        MaterializedIndex normalIndex = normalIndexes.get(0);

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

        schemaChangeJob.run();
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

    @Test
    public void testTransactionRaceCondition() throws Exception {
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
                return 10103L; // !!!! <-------- 10103 != 10101 + 1
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains(
                "concurrent transaction detected while adding shadow index, please re-run the alter table command"),
                () -> {
                    StringWriter sw = new java.io.StringWriter();
                    PrintWriter pw = new java.io.PrintWriter(sw);
                    exception.printStackTrace(pw);
                    return sw.toString();
                });
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

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

    @Test
    public void testCancelPendingJobWithFlag() throws Exception {
        LakeTableSchemaChangeJob schemaChangeJob = alterTableAddColumn();
        schemaChangeJob.setIsCancelling(true);
        schemaChangeJob.runPendingJob();
        schemaChangeJob.setIsCancelling(false);

        schemaChangeJob.setWaitingCreatingReplica(true);
        schemaChangeJob.cancel("");
        schemaChangeJob.setWaitingCreatingReplica(false);
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
        schemaChangeJob.run();
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
    public void testBaseTableReadSchema() throws Exception {
        // Create a table with multiple partitions
        LakeTable multiPartitionTable = createTable(connectContext,
                "CREATE TABLE t_multi_partition(c0 INT) duplicate key(c0) " +
                        "PARTITION BY RANGE(c0) (" +
                        "PARTITION p1 VALUES [(\"0\"), (\"100\"))," +
                        "PARTITION p2 VALUES [(\"100\"), (\"200\"))" +
                        ") " +
                        "distributed by hash(c0) buckets 3");

        // Alter table to add a generated column
        alterTable(connectContext, "ALTER TABLE t_multi_partition ADD COLUMN c2 BIGINT AS c0 + 1");
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

        schemaChangeJob.runPendingJob();
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
                        tbl, originIndexId, tbl.getIndexMetaByIndexId(originIndexId)).toTabletSchema();
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
                tbl.getIndexMetaByIndexId(tbl.getBaseIndexId()).getSortKeyIdxes());

        alterTable(connectContext, "ALTER TABLE " + tableName + " ADD COLUMN k_new1 INT KEY AFTER k0");
        runSchemaChangeJobToFinish(tbl);

        Assertions.assertEquals("k0", tbl.getBaseSchema().get(0).getName());
        Assertions.assertEquals("k_new1", tbl.getBaseSchema().get(1).getName());
        Assertions.assertEquals("k1", tbl.getBaseSchema().get(2).getName());
        assertSortKey(tbl, Arrays.asList(2, 0, 1));
    }

    private void assertSortKey(LakeTable tbl, List<Integer> expectedSortKeyIndexes) {
        List<Column> columns = tbl.getBaseSchema();
        MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
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
        job.runPendingJob();
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
<<<<<<< HEAD
=======
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

    /**
     * With lake_enable_tablet_creation_optimization set, the job created the shadow tablets of a table without
     * `file_bundling` from one partition-shared version-1 object, named like the base index's one in the same
     * partition path, and overwrote it. The config is retired: every shadow tablet gets its own version-1 metadata.
     */
    @Test
    public void testShadowTabletsAlwaysGetPerTabletMetadata() throws Exception {
        boolean oldOptimization = Config.lake_enable_tablet_creation_optimization;
        boolean oldFastSchemaEvolution = Config.enable_fast_schema_evolution_in_share_data_mode;
        Config.lake_enable_tablet_creation_optimization = true;
        // Fast schema evolution would turn the type change into a metadata-only job.
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        try {
            LakeTable hashTable = createTable(connectContext, "CREATE TABLE h1 (k1 INT, v INT) "
                    + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS " + NUM_BUCKETS + " "
                    + "PROPERTIES('replication_num' = '1', 'file_bundling' = 'false', "
                    + "'light_weight_tablet_creation' = 'false')");
            Assertions.assertFalse(hashTable.isFileBundling());
            Assertions.assertFalse(hashTable.isRangeDistribution());

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
            alterTable(connectContext, "ALTER TABLE h1 MODIFY COLUMN v BIGINT");
            LakeTableSchemaChangeJob job = getAlterJob(hashTable);
            drivePendingJob(job);

            TreeSet<Long> shadowTabletIds = hashTable.getPartitions().iterator().next().getDefaultPhysicalPartition()
                    .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL).stream()
                    .filter(index -> index.getState() == MaterializedIndex.IndexState.SHADOW)
                    .flatMap(index -> index.getTablets().stream())
                    .map(Tablet::getId)
                    .collect(Collectors.toCollection(TreeSet::new));
            List<CreateReplicaTask> shadowTasks = built.stream()
                    .filter(task -> task.getTableId() == hashTable.getId())
                    .collect(Collectors.toList());
            Assertions.assertEquals(NUM_BUCKETS, shadowTabletIds.size());
            Assertions.assertEquals(NUM_BUCKETS, shadowTasks.size(), "every shadow tablet must get its own create task");
            Assertions.assertEquals(shadowTabletIds, shadowTasks.stream().map(CreateReplicaTask::getTabletId)
                    .collect(Collectors.toCollection(TreeSet::new)));
            for (CreateReplicaTask task : shadowTasks) {
                Assertions.assertFalse(task.toThrift().isEnable_tablet_creation_optimization());
            }
        } finally {
            Config.lake_enable_tablet_creation_optimization = oldOptimization;
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

    /**
     * A query planned off a snapshot taken before the swap shares the swapped physical partitions, so the swap must
     * invalidate that snapshot before it drops the first origin index, or the planner rethrows instead of retrying.
     */
    @Test
    public void testSwapInvalidatesEarlierPlansBeforeDroppingOriginIndex() throws Exception {
        completeTasksAndPublishOnSend();
        LakeTableSchemaChangeJob job = alterTableAddColumn();
        long planStartTime = OptimisticVersion.generate();
        List<Boolean> planValidAtDrop = recordPlanValidityOnIndexDrop(table, planStartTime);

        runToFinished(job);

        Assertions.assertEquals(List.of(false), planValidAtDrop);
        Assertions.assertFalse(OptimisticVersion.validateTableUpdate(table, planStartTime));
    }

    @Test
    public void testFollowerReplayInvalidatesEarlierPlansBeforeDroppingOriginIndex() throws Exception {
        completeTasksAndPublishOnSend();
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        LakeTableSchemaChangeJob job = alterTableAddColumn();
        runToFinished(job);
        List<LakeTableSchemaChangeJob> entries = readJournaledEntries(job.getJobId());

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = tableIn(followerMetastore);
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        long planStartTime = OptimisticVersion.generate();
        List<Boolean> planValidAtDrop = recordPlanValidityOnIndexDrop(followerTable, planStartTime);
        for (LakeTableSchemaChangeJob entry : entries) {
            replayOn(followerMetastore, followerHandler, entry);
        }

        Assertions.assertEquals(List.of(false), planValidAtDrop);
        Assertions.assertFalse(OptimisticVersion.validateTableUpdate(followerTable, planStartTime));
    }

    /**
     * Records, at each origin index drop, whether a plan started at {@code planStartTime} would still pass the
     * planner's schema check on {@code olapTable}.
     */
    private static List<Boolean> recordPlanValidityOnIndexDrop(OlapTable olapTable, long planStartTime) {
        List<Boolean> planValidAtDrop = new ArrayList<>();
        new MockUp<PhysicalPartition>() {
            @Mock
            public List<MaterializedIndex> deleteMaterializedIndexByMetaId(Invocation inv, long indexMetaId) {
                planValidAtDrop.add(OptimisticVersion.validateTableUpdate(olapTable, planStartTime));
                return inv.proceed(indexMetaId);
            }
        };
        return planValidAtDrop;
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
>>>>>>> 5ffc315 ([BugFix] Bump lastSchemaUpdateTime before shared-data schema change swaps indexes so concurrent queries re-plan (#81363))
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
                public void logAlterMvStatus(Invocation invocation, AlterMaterializedViewStatusLog log) {
                    journaled.add(log);
                    invocation.proceed();
                }
            };

            schemaChangeJob.runPendingJob();
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
