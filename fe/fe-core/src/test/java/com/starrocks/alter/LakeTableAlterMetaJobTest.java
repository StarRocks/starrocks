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

import com.google.common.collect.Table;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LakeTableAlterMetaJobTest {
    private static final String DB_NAME = "test";
    private static Database db;
    private static ConnectContext connectContext;
    private LakeTable table;
    private LakeTableAlterMetaJob job;

    public LakeTableAlterMetaJobTest() {
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Before
    public void setUp() throws Exception {
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        table = createTable(connectContext,
                    "CREATE TABLE t0(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('enable_persistent_index'='false')");
        Assert.assertFalse(table.enablePersistentIndex());
        job = new LakeTableAlterMetaJob(GlobalStateMgr.getCurrentState().getNextId(), db.getId(), table.getId(),
                    table.getName(), 60 * 1000, TTabletMetaType.ENABLE_PERSISTENT_INDEX, true);
    }

    @After
    public void tearDown() throws DdlException, MetaNotFoundException {
        db.dropTable(table.getName());
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(DB_NAME, true);
        } catch (MetaNotFoundException ignored) {
        }
    }

    private static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    @Test
    public void testJobState() throws Exception {
        Assert.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assert.assertNotEquals(-1L, job.getTransactionId().orElse(-1L).longValue());
        job.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        job.runFinishedRewritingJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        Assert.assertTrue(table.enablePersistentIndex());
    }

    @Test
    public void testUpdatePartitonMetaFailed() {
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                            WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
                return null;
            }
        };
        Assert.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        System.err.println(job.errMsg);
        Assert.assertTrue(job.errMsg.contains("no alive node"));
    }

    @Test
    public void testCancelPendingJob() {
        job.cancel("cancel test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropTable01() {
        db.dropTable(table.getId());
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropDb01() throws DdlException, MetaNotFoundException {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(db.getFullName(), true);
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropTable02() throws AlterCancelException {
        job.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        db.dropTable(table.getId());
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropDb02() throws DdlException, MetaNotFoundException {
        job.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(db.getFullName(), true);
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropTable03() throws AlterCancelException {
        job.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        job.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());

        db.dropTable(table.getId());
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropDb03() throws DdlException, MetaNotFoundException {
        job.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        job.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(db.getFullName(), true);
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testReplay() {
        job.run();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        LakeTableAlterMetaJob replayAlterMetaJob = new LakeTableAlterMetaJob(job.jobId,
                    job.dbId, job.tableId, job.tableName,
                    job.timeoutMs, TTabletMetaType.ENABLE_PERSISTENT_INDEX, true);

        Table<Long, Long, MaterializedIndex> partitionIndexMap = job.getPartitionIndexMap();
        Map<Long, Long> commitVersionMap = job.getCommitVersionMap();

        // for replay will check partition.getVisibleVersion()
        // here we reduce the visibleVersion for test
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Assert.assertEquals(partition.getDefaultPhysicalPartition().getVisibleVersion(), commitVersion);
            partition.getDefaultPhysicalPartition().updateVisibleVersion(commitVersion - 1);
        }

        replayAlterMetaJob.replay(job);

        Assert.assertEquals(AlterJobV2.JobState.FINISHED, replayAlterMetaJob.getJobState());
        Assert.assertEquals(job.getFinishedTimeMs(), replayAlterMetaJob.getFinishedTimeMs());
        Assert.assertEquals(job.getTransactionId(), replayAlterMetaJob.getTransactionId());
        Assert.assertEquals(job.getJobId(), replayAlterMetaJob.getJobId());
        Assert.assertEquals(job.getTableId(), replayAlterMetaJob.getTableId());
        Assert.assertEquals(job.getDbId(), replayAlterMetaJob.getDbId());
        Assert.assertEquals(job.getCommitVersionMap(), replayAlterMetaJob.getCommitVersionMap());
        Assert.assertEquals(job.getPartitionIndexMap(), replayAlterMetaJob.getPartitionIndexMap());

        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Assert.assertEquals(partition.getDefaultPhysicalPartition().getVisibleVersion(), commitVersion);
        }
    }

    @Test
    public void testUpdateTabletMetaInfoTaskToThrift() throws AlterCancelException {
        long backend = 1L;
        long txnId = 1L;
        Set<Long> tabletSet = new HashSet<>();
        tabletSet.add(1L);
        MarkedCountDownLatch<Long, Set<Long>> latch = new MarkedCountDownLatch<>(1);
        TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory.createEnablePersistentIndexUpdateTask(
                    backend, tabletSet, true);
        task.setLatch(latch);
        task.setTxnId(txnId);
        TUpdateTabletMetaInfoReq result = task.toThrift();
        Assert.assertEquals(result.txn_id, txnId);
        Assert.assertEquals(result.tablet_type, TTabletType.TABLET_TYPE_LAKE);
    }

    @Test
    public void testSetPropertyNotSupport() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM, "all");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        Assertions.assertThrows(DdlException.class,
                    () -> schemaChangeHandler.createAlterMetaJob(modify, db, table));
    }
}
