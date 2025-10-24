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
import com.staros.client.StarClientException;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.thrift.TCompactionStrategy;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Drop database if it already exists from a previous failed test run
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
        } catch (Exception ignored) {
        }
        
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        table = createTable(connectContext,
                    "CREATE TABLE t0(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('enable_persistent_index'='true')");
        Assertions.assertTrue(table.enablePersistentIndex());
        job = new LakeTableAlterMetaJob(GlobalStateMgr.getCurrentState().getNextId(), db.getId(), table.getId(),
                    table.getName(), 60 * 1000, TTabletMetaType.ENABLE_PERSISTENT_INDEX, true, "CLOUD_NATIVE");
    }

    @AfterEach
    public void tearDown() throws DdlException, MetaNotFoundException {
        try {
            if (db != null && table != null) {
                db.dropTable(table.getName());
            }
        } catch (Exception ignored) {
        }
        
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
        } catch (Exception ignored) {
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
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assertions.assertNotEquals(-1L, job.getTransactionId().orElse(-1L).longValue());
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        Assertions.assertTrue(table.enablePersistentIndex());
    }

    @Test
    public void testJobStateEnableFileBundling() throws Exception {
        LakeTable table1 = createTable(connectContext,
                    "CREATE TABLE t1(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('enable_persistent_index'='true', " + 
                                "'file_bundling'='true')");
        LakeTableAlterMetaJob job1 = new LakeTableAlterMetaJob(GlobalStateMgr.getCurrentState().getNextId(), db.getId(), 
                        table1.getId(), table1.getName(), 60 * 1000, TTabletMetaType.ENABLE_PERSISTENT_INDEX, true, 
                        "CLOUD_NATIVE");

        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job1.getJobState());
        job1.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job1.getJobState());
        Assertions.assertNotEquals(-1L, job1.getTransactionId().orElse(-1L).longValue());
        job1.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job1.getJobState());
        while (job1.getJobState() != AlterJobV2.JobState.FINISHED) {
            job1.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job1.getJobState());

        Assertions.assertTrue(table1.enablePersistentIndex());
    }

    @Test
    public void testSetEnablePersistentWithoutType() throws Exception {
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assertions.assertNotEquals(-1L, job.getTransactionId().orElse(-1L).longValue());
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertTrue(table.enablePersistentIndex());
        // check persistent index type been set
        Assertions.assertTrue(table.getPersistentIndexType() == (Config.enable_cloud_native_persistent_index_by_default
                ? TPersistentIndexType.CLOUD_NATIVE : TPersistentIndexType.LOCAL));
    }

    @Test
    public void testSetEnablePersistentWithLocalindex() throws Exception {
        LakeTableAlterMetaJob job2 = new LakeTableAlterMetaJob(GlobalStateMgr.getCurrentState().getNextId(),
                    db.getId(), table.getId(), table.getName(), 60 * 1000,
                    TTabletMetaType.ENABLE_PERSISTENT_INDEX, true, "LOCAL");
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job2.getJobState());
        job2.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job2.getJobState());
        Assertions.assertNotEquals(-1L, job2.getTransactionId().orElse(-1L).longValue());
        job2.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job2.getJobState());
        while (job2.getJobState() != AlterJobV2.JobState.FINISHED) {
            job2.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job2.getJobState());
        Assertions.assertTrue(table.enablePersistentIndex());
        // check persistent index type been set
        Assertions.assertTrue(table.getPersistentIndexType() == TPersistentIndexType.LOCAL);
    }

    @Test
    public void testSetDisblePersistentIndex() throws Exception {
        LakeTable table2 = createTable(connectContext,
                    "CREATE TABLE t1(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('enable_persistent_index'='true', 'persistent_index_type'='LOCAL')");
        LakeTableAlterMetaJob job2 = new LakeTableAlterMetaJob(GlobalStateMgr.getCurrentState().getNextId(),
                    db.getId(), table2.getId(), table2.getName(), 60 * 1000,
                    TTabletMetaType.ENABLE_PERSISTENT_INDEX, false, "LOCAL");
        Assertions.assertTrue(table2.enablePersistentIndex());
        Assertions.assertTrue(table2.getPersistentIndexType() == TPersistentIndexType.LOCAL);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job2.getJobState());
        job2.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job2.getJobState());
        Assertions.assertNotEquals(-1L, job2.getTransactionId().orElse(-1L).longValue());
        job2.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job2.getJobState());
        while (job2.getJobState() != AlterJobV2.JobState.FINISHED) {
            job2.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job2.getJobState());
        Assertions.assertFalse(table2.enablePersistentIndex());

        db.dropTable(table2.getName());
    }

    @Test
    public void testUpdatePartitonMetaFailed() {
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };
        mockedWarehouseManager.setComputeNodeId(null);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        System.err.println(job.errMsg);
        Assertions.assertTrue(job.errMsg.contains("no alive node"));
    }

    @Test
    public void testCancelPendingJob() {
        job.cancel("cancel test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropTable01() {
        db.dropTable(table.getId());
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropDb01() throws DdlException, MetaNotFoundException {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, db.getFullName(), true);
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropTable02() throws AlterCancelException {
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        db.dropTable(table.getId());
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropDb02() throws DdlException, MetaNotFoundException {
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, db.getFullName(), true);
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropTable03() throws AlterCancelException {
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());

        db.dropTable(table.getId());
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testDropDb03() throws DdlException, MetaNotFoundException {
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());

        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, db.getFullName(), true);
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testReplay() throws Exception {
        job.run();
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.run();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());

        LakeTableAlterMetaJob replayAlterMetaJob = new LakeTableAlterMetaJob(job.jobId,
                    job.dbId, job.tableId, job.tableName,
                    job.timeoutMs, TTabletMetaType.ENABLE_PERSISTENT_INDEX, true, "CLOUD_NATIVE");

        Table<Long, Long, MaterializedIndex> partitionIndexMap = job.getPartitionIndexMap();
        Map<Long, Long> commitVersionMap = job.getCommitVersionMap();

        // for replay will check partition.getVisibleVersion()
        // here we reduce the visibleVersion for test
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Assertions.assertEquals(physicalPartition.getVisibleVersion(), commitVersion);
            physicalPartition.updateVisibleVersion(commitVersion - 1);
        }

        replayAlterMetaJob.replay(job);

        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, replayAlterMetaJob.getJobState());
        Assertions.assertEquals(job.getFinishedTimeMs(), replayAlterMetaJob.getFinishedTimeMs());
        Assertions.assertEquals(job.getTransactionId(), replayAlterMetaJob.getTransactionId());
        Assertions.assertEquals(job.getJobId(), replayAlterMetaJob.getJobId());
        Assertions.assertEquals(job.getTableId(), replayAlterMetaJob.getTableId());
        Assertions.assertEquals(job.getDbId(), replayAlterMetaJob.getDbId());
        Assertions.assertEquals(job.getCommitVersionMap(), replayAlterMetaJob.getCommitVersionMap());
        Assertions.assertEquals(job.getPartitionIndexMap(), replayAlterMetaJob.getPartitionIndexMap());

        for (long partitionId : partitionIndexMap.rowKeySet()) {
            PhysicalPartition physicalPartition = table.getPhysicalPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Assertions.assertEquals(physicalPartition.getVisibleVersion(), commitVersion);
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
        Assertions.assertEquals(result.txn_id, txnId);
        Assertions.assertEquals(result.tablet_type, TTabletType.TABLET_TYPE_LAKE);
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

    @Test
    public void testModifyPropertyWithIndex() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("enable_persistent_index", "true");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        AlterJobV2 job = schemaChangeHandler.createAlterMetaJob(modify, db, table);
        Assertions.assertNull(job);
    }

    @Test
    public void testModifyPropertyWithIndexType() throws Exception {
        LakeTable table2 = createTable(connectContext,
                    "CREATE TABLE t11(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('enable_persistent_index'='true', 'persistent_index_type'='LOCAL')");
        Map<String, String> properties = new HashMap<>();
        properties.put("persistent_index_type", "CLOUD_NATIVE");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        // success
        AlterJobV2 job2 = schemaChangeHandler.createAlterMetaJob(modify, db, table2);
        Assertions.assertNotNull(job2);
    }

    @Test
    public void testModifyPropertyWithIndexTypeFailure() throws Exception {
        LakeTable table2 = createTable(connectContext,
                    "CREATE TABLE t11(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('enable_persistent_index'='true', 'persistent_index_type'='LOCAL')");
        Map<String, String> properties = new HashMap<>();
        properties.put("enable_persistent_index", "false");
        properties.put("persistent_index_type", "CLOUD_NATIVE");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        // should throw exception
        ExceptionChecker.expectThrows(DdlException.class,
                () -> schemaChangeHandler.createAlterMetaJob(modify, db, table2));

    }

    @Test
    public void testModifyPropertyFileBundling() throws Exception {
        LakeTable table2 = createTable(connectContext,
                    "CREATE TABLE t12(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                                "PROPERTIES('file_bundling'='false')");
        Assertions.assertFalse(table2.isFileBundling());
        try {
            String alterStmtStr = "alter table test.t12 set ('file_bundling'='true')";
            List<ShardInfo> shardInfos = new ArrayList<>();
            new MockUp<StarOSAgent>() {
                @Mock
                public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId)
                        throws StarClientException {
                    throw new StarClientException(
                        StarStatus.newBuilder().setStatusCode(StatusCode.INTERNAL).setErrorMsg("injected error")
                                .build());
                }
            };
            Assertions.assertFalse(table2.checkLakeRollupAllowFileBundling());
            new MockUp<StarOSAgent>() {
                @Mock
                public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId)
                        throws StarClientException {
                    return shardInfos;
                }
            };

            Assertions.assertTrue(table2.checkLakeRollupAllowFileBundling());
            ShardInfo shardInfo1 = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/10002/")).build();
            shardInfos.add(shardInfo1);
            Assertions.assertFalse(table2.checkLakeRollupAllowFileBundling());
            shardInfos.clear();

            ShardInfo shardInfo2 = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/10003")).build();
            ShardInfo shardInfo3 = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/10002")).build();
            shardInfos.add(shardInfo2);
            shardInfos.add(shardInfo3);
    
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
        } catch (Exception e) {
            Assertions.assertFalse(table2.isFileBundling());
        }

        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardInfo> getShardInfo(List<Long> shardIds, long workerGroupId)
                    throws StarClientException {
                return new ArrayList<>();
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put("file_bundling", "true");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        AlterJobV2 job = schemaChangeHandler.createAlterMetaJob(modify, db, table2);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assertions.assertNotEquals(-1L, job.getTransactionId().orElse(-1L).longValue());
        Assertions.assertTrue(((LakeTableAlterMetaJob) job).enableFileBundling());
        Assertions.assertFalse(((LakeTableAlterMetaJob) job).disableFileBundling());
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertTrue(table2.isFileBundling());
        Assertions.assertFalse(table2.allowUpdateFileBundling());

        properties.put("file_bundling", "true");
        ModifyTablePropertiesClause modify1 = new ModifyTablePropertiesClause(properties);
        AlterJobV2 job1 = schemaChangeHandler.createAlterMetaJob(modify1, db, table2);
        Assertions.assertNull(job1);
        Assertions.assertFalse(table2.allowUpdateFileBundling());

        try {
            String alterStmtStr = "alter table test.t12 set ('file_bundling'='true')";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(table2.isFileBundling());
        }

        try {
            String alterStmtStr = "alter table test.t12 set ('file_bundling'='false')";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
        } catch (Exception e) {
            Assertions.assertFalse(table2.allowUpdateFileBundling());
            Assertions.assertTrue(table2.isFileBundling());
        }

        properties.clear();
        properties.put("file_bundling", "false");
        ModifyTablePropertiesClause modify2 = new ModifyTablePropertiesClause(properties);
        AlterJobV2 job2 = schemaChangeHandler.createAlterMetaJob(modify2, db, table2);
        Assertions.assertNotNull(job2);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job2.getJobState());
        job2.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job2.getJobState());
        Assertions.assertNotEquals(-1L, job2.getTransactionId().orElse(-1L).longValue());
        Assertions.assertTrue(((LakeTableAlterMetaJob) job2).disableFileBundling());
        Assertions.assertFalse(((LakeTableAlterMetaJob) job2).enableFileBundling());
        job2.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job2.getJobState());
        while (job2.getJobState() != AlterJobV2.JobState.FINISHED) {
            job2.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job2.getJobState());
        Assertions.assertFalse(table2.isFileBundling());
    }

    @Test
    public void testModifyPropertyCompactionStrategy() throws Exception {
        try {
            LakeTable nonPKTable = createTable(connectContext,
                        "CREATE TABLE non_pk(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                        "PROPERTIES('compaction_strategy'='real_time')");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Only default compaction strategy is allowed"));
        }

        try {
            LakeTable nonPKTable = createTable(connectContext,
                        "CREATE TABLE non_pk(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1");
            String alterStmtStr = "alter table test.non_pk set ('compaction_strategy'='real_time')";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("can be only update for a primary key table"));
        }
    
        LakeTable table2 = createTable(connectContext,
                    "CREATE TABLE t13(c0 INT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1");
        Assertions.assertEquals(table2.getCompactionStrategy(), TCompactionStrategy.DEFAULT);
        try {
            String alterStmtStr = "alter table test.t13 set ('compaction_strategy'='unknown')";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals(table2.getCompactionStrategy(), TCompactionStrategy.DEFAULT);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put("compaction_strategy", "real_time");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        AlterJobV2 job = schemaChangeHandler.createAlterMetaJob(modify, db, table2);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assertions.assertNotEquals(-1L, job.getTransactionId().orElse(-1L).longValue());
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        while (job.getJobState() != AlterJobV2.JobState.FINISHED) {
            job.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertEquals(table2.getCompactionStrategy(), TCompactionStrategy.REAL_TIME);
        try {
            String alterStmtStr = "alter table test.t13 set ('compaction_strategy'='DEFAULT')";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals(table2.getCompactionStrategy(), TCompactionStrategy.REAL_TIME);
        }
        while (table2.getState() != OlapTable.OlapTableState.NORMAL) {
            Thread.sleep(100);
        }
        Assertions.assertEquals(table2.getCompactionStrategy(), TCompactionStrategy.DEFAULT);
    }
}
