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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.BatchDropInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class MaterializedViewHandlerEditLogTest {
    private static final String DB_NAME = "test_mv_handler_editlog";
    private static final String TABLE_NAME = "tbl";

    private Database db;
    private OlapTable table;
    private ConnectContext connectContext;
    private long backendId;
    private long replicaId;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(DB_NAME);

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        long dbId = GlobalStateMgr.getCurrentState().getNextId();
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        long partitionId = GlobalStateMgr.getCurrentState().getNextId();
        long physicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        long tabletId = GlobalStateMgr.getCurrentState().getNextId();
        long indexId = GlobalStateMgr.getCurrentState().getNextId();
        backendId = GlobalStateMgr.getCurrentState().getNextId();
        replicaId = GlobalStateMgr.getCurrentState().getNextId();

        db = new Database(dbId, DB_NAME);
        metastore.unprotectCreateDb(db);
        if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(backendId) == null) {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .addBackend(new Backend(backendId, "127.0.0.1", 9050));
        }
        table = createHashOlapTable(dbId, tableId, indexId, TABLE_NAME, partitionId, physicalPartitionId, tabletId,
                backendId, replicaId);
        db.registerTableUnlocked(table);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testProcessCreateMaterializedViewEditLog() throws Exception {
        String sql = "create materialized view mv1 as select k1, sum(v1) from " + TABLE_NAME + " group by k1";
        CreateSyncMVStmt stmt = (CreateSyncMVStmt) UtFrameUtils
                .parseStmtWithNewParser(sql, connectContext);

        MaterializedViewHandler handler = new MaterializedViewHandler();
        handler.processCreateMaterializedView(stmt, db, table);

        Assertions.assertEquals(OlapTable.OlapTableState.ROLLUP, table.getState());
        AlterJobV2 loggedJob = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, loggedJob.getJobState());
        Assertions.assertTrue(handler.getAlterJobsV2().containsKey(loggedJob.getJobId()));
    }

    @Test
    public void testProcessCreateMaterializedViewEditLogException() throws Exception {
        String sql = "create materialized view mv2 as select k1, sum(v1) from " + TABLE_NAME + " group by k1";
        CreateSyncMVStmt stmt = (CreateSyncMVStmt) UtFrameUtils
                .parseStmtWithNewParser(sql, connectContext);

        MaterializedViewHandler handler = new MaterializedViewHandler();
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterJob(any(AlterJobV2.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> handler.processCreateMaterializedView(stmt, db, table));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Assertions.assertTrue(handler.getAlterJobsV2().isEmpty());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testProcessBatchAddRollupEditLog() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        AddRollupClause clause = new AddRollupClause("r1", List.of("k1", "v1"), null, TABLE_NAME, null);
        List<AlterClause> alterClauses = List.of(clause);

        handler.processBatchAddRollup(alterClauses, db, table);

        Assertions.assertEquals(OlapTable.OlapTableState.ROLLUP, table.getState());
        Assertions.assertEquals(1, handler.getAlterJobsV2().size());

        BatchAlterJobPersistInfo logInfo = (BatchAlterJobPersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_BATCH_ADD_ROLLUP_V2);
        Assertions.assertEquals(1, logInfo.getAlterJobV2List().size());
        AlterJobV2 loggedJob = logInfo.getAlterJobV2List().get(0);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, loggedJob.getJobState());
        Assertions.assertTrue(handler.getAlterJobsV2().containsKey(loggedJob.getJobId()));
    }

    @Test
    public void testProcessBatchAddRollupEditLogException() {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        AddRollupClause clause = new AddRollupClause("r2", List.of("k1", "v1"), null, TABLE_NAME, null);
        List<AlterClause> alterClauses = List.of(clause);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logBatchAlterJob(any(BatchAlterJobPersistInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> handler.processBatchAddRollup(alterClauses, db, table));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Assertions.assertTrue(handler.getAlterJobsV2().isEmpty());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testProcessDropMaterializedViewEditLog() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        long rollupIndexId = GlobalStateMgr.getCurrentState().getNextId();
        String rollupName = "mv_drop_1";
        addRollupIndex(table, rollupName, rollupIndexId, GlobalStateMgr.getCurrentState().getNextId());
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName));

        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false,
                new TableRef(QualifiedName.of(rollupName), null, NodePosition.ZERO));
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            handler.processDropMaterializedView(stmt, db, table);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        Assertions.assertFalse(table.hasMaterializedIndex(rollupName));

        DropInfo replayInfo = (DropInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_ROLLUP_V2);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(table.getId(), replayInfo.getTableId());
        Assertions.assertEquals(rollupIndexId, replayInfo.getIndexMetaId());

        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(db.getId(), DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        Partition leaderPartition = table.getPartitions().iterator().next();
        long basePartitionId = leaderPartition.getId();
        long basePhysicalPartitionId = leaderPartition.getDefaultPhysicalPartition().getId();
        long baseIndexId = table.getBaseIndexMetaId();
        long baseTabletId = leaderPartition.getDefaultPhysicalPartition().getIndex(baseIndexId).getTablets().get(0).getId();
        OlapTable followerTable = createHashOlapTable(db.getId(), table.getId(), baseIndexId, TABLE_NAME,
                basePartitionId, basePhysicalPartitionId, baseTabletId, backendId, replicaId);
        addRollupIndex(followerTable, rollupName, rollupIndexId, GlobalStateMgr.getCurrentState().getNextId());
        followerDb.registerTableUnlocked(followerTable);

        LocalMetastore originalMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            handler.replayDropRollup(replayInfo, GlobalStateMgr.getCurrentState());
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }
        Assertions.assertFalse(followerTable.hasMaterializedIndex(rollupName));
    }

    @Test
    public void testProcessDropMaterializedViewEditLogException() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        long rollupIndexId = GlobalStateMgr.getCurrentState().getNextId();
        String rollupName = "mv_drop_error";
        addRollupIndex(table, rollupName, rollupIndexId, GlobalStateMgr.getCurrentState().getNextId());
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropRollup(any(DropInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false,
                    new TableRef(QualifiedName.of(rollupName), null, NodePosition.ZERO));
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            try {
                RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                        () -> handler.processDropMaterializedView(stmt, db, table));
                Assertions.assertEquals("EditLog write failed", exception.getMessage());
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
            Assertions.assertTrue(table.hasMaterializedIndex(rollupName));
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testProcessBatchDropRollupEditLog() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        long rollupIndexId1 = GlobalStateMgr.getCurrentState().getNextId();
        long rollupIndexId2 = GlobalStateMgr.getCurrentState().getNextId();
        String rollupName1 = "r_drop_1";
        String rollupName2 = "r_drop_2";
        addRollupIndex(table, rollupName1, rollupIndexId1, GlobalStateMgr.getCurrentState().getNextId());
        addRollupIndex(table, rollupName2, rollupIndexId2, GlobalStateMgr.getCurrentState().getNextId());
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName1));
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName2));

        List<AlterClause> alterClauses = List.of(
                new DropRollupClause(rollupName1, new HashMap<>()),
                new DropRollupClause(rollupName2, new HashMap<>()));
        handler.processBatchDropRollup(alterClauses, db, table);

        Assertions.assertFalse(table.hasMaterializedIndex(rollupName1));
        Assertions.assertFalse(table.hasMaterializedIndex(rollupName2));

        BatchDropInfo replayInfo = (BatchDropInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_BATCH_DROP_ROLLUP);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(table.getId(), replayInfo.getTableId());
        Assertions.assertEquals(Set.of(rollupIndexId1, rollupIndexId2), replayInfo.getIndexMetaIdSet());

        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(db.getId(), DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        Partition leaderPartition = table.getPartitions().iterator().next();
        long basePartitionId = leaderPartition.getId();
        long basePhysicalPartitionId = leaderPartition.getDefaultPhysicalPartition().getId();
        long baseIndexId = table.getBaseIndexMetaId();
        long baseTabletId = leaderPartition.getDefaultPhysicalPartition().getIndex(baseIndexId).getTablets().get(0).getId();
        OlapTable followerTable = createHashOlapTable(db.getId(), table.getId(), baseIndexId, TABLE_NAME,
                basePartitionId, basePhysicalPartitionId, baseTabletId, backendId, replicaId);
        addRollupIndex(followerTable, rollupName1, rollupIndexId1, GlobalStateMgr.getCurrentState().getNextId());
        addRollupIndex(followerTable, rollupName2, rollupIndexId2, GlobalStateMgr.getCurrentState().getNextId());
        followerDb.registerTableUnlocked(followerTable);

        LocalMetastore originalMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            for (long indexMetaId : replayInfo.getIndexMetaIdSet()) {
                handler.replayDropRollup(new DropInfo(replayInfo.getDbId(), replayInfo.getTableId(), indexMetaId, false),
                        GlobalStateMgr.getCurrentState());
            }
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }
        Assertions.assertFalse(followerTable.hasMaterializedIndex(rollupName1));
        Assertions.assertFalse(followerTable.hasMaterializedIndex(rollupName2));
    }

    @Test
    public void testProcessDropMaterializedViewForceDropEditLog() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        long rollupIndexId = GlobalStateMgr.getCurrentState().getNextId();
        String rollupName = "mv_force_drop";
        addRollupIndex(table, rollupName, rollupIndexId, GlobalStateMgr.getCurrentState().getNextId());
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName));

        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, true,
                new TableRef(QualifiedName.of(rollupName), null, NodePosition.ZERO), NodePosition.ZERO);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            handler.processDropMaterializedView(stmt, db, table);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        Assertions.assertFalse(table.hasMaterializedIndex(rollupName));

        DropInfo replayInfo = (DropInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_ROLLUP_V2);
        Assertions.assertNotNull(replayInfo);
        Assertions.assertTrue(replayInfo.isForceDrop());
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(table.getId(), replayInfo.getTableId());
        Assertions.assertEquals(rollupIndexId, replayInfo.getIndexMetaId());
    }

    @Test
    public void testForceDropWhenMvNameEqualsTableName() {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        // Force drop with MV name same as base table name should throw
        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, true,
                new TableRef(QualifiedName.of(TABLE_NAME), null, NodePosition.ZERO), NodePosition.ZERO);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> handler.processDropMaterializedView(stmt, db, table));
            Assertions.assertTrue(e.getMessage().contains("Cannot drop base index"));
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Test
    public void testForceDropWhenMvNotExist() {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        String nonexistentMv = "nonexistent_mv";
        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, true,
                new TableRef(QualifiedName.of(nonexistentMv), null, NodePosition.ZERO), NodePosition.ZERO);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            MetaNotFoundException e = Assertions.assertThrows(MetaNotFoundException.class,
                    () -> handler.processDropMaterializedView(stmt, db, table));
            Assertions.assertTrue(e.getMessage().contains(nonexistentMv));
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Test
    public void testCancelRollupJobsForForceDropNoMatchingJob() {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        boolean found = handler.cancelRollupJobsForForceDrop(table.getId(), "no_such_mv", "reason");
        Assertions.assertFalse(found);
    }

    @Test
    public void testCancelRollupJobsForForceDropWithMatchingJob() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        String rollupName = "mv_cancel_test";
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        RollupJobV2 job = new RollupJobV2(jobId, db.getId(), table.getId(), TABLE_NAME, 3600000,
                0, 0, TABLE_NAME, rollupName, 1,
                null, null, 1, 1, null, (short) 0,
                null, null, false);
        handler.addAlterJobV2(job);
        boolean found = handler.cancelRollupJobsForForceDrop(table.getId(), rollupName, "force drop test");
        Assertions.assertTrue(found);
        Assertions.assertTrue(job.isDone());
    }

    @Test
    public void testAlterJobMgrForceDropWithStuckRollupJob() throws Exception {
        String rollupName = "mv_force_integration";
        long rollupIndexId = GlobalStateMgr.getCurrentState().getNextId();
        addRollupIndex(table, rollupName, rollupIndexId, GlobalStateMgr.getCurrentState().getNextId());
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName));
        table.setState(OlapTableState.ROLLUP);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        RollupJobV2 job = new RollupJobV2(jobId, db.getId(), table.getId(), TABLE_NAME, 3600000,
                0, 0, TABLE_NAME, rollupName, 1,
                null, null, 1, 1, null, (short) 0,
                null, null, false);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().getMaterializedViewHandler().addAlterJobV2(job);

        DropMaterializedViewStmt stmt = new DropMaterializedViewStmt(false, true,
                new TableRef(QualifiedName.of(DB_NAME, rollupName), null, NodePosition.ZERO), NodePosition.ZERO);
        // cancelRollupJobsForForceDrop cancels the job, which removes the rollup index. Then
        // processDropMaterializedView throws MetaNotFoundException because the index is already gone.
        try {
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processDropMaterializedView(stmt);
        } catch (MetaNotFoundException e) {
            // Expected: cancel already removed the index before we tried to drop it.
        }
        Assertions.assertFalse(table.hasMaterializedIndex(rollupName));
    }

    @Test
    public void testProcessBatchDropRollupEditLogException() throws Exception {
        MaterializedViewHandler handler = new MaterializedViewHandler();
        long rollupIndexId1 = GlobalStateMgr.getCurrentState().getNextId();
        long rollupIndexId2 = GlobalStateMgr.getCurrentState().getNextId();
        String rollupName1 = "r_drop_error_1";
        String rollupName2 = "r_drop_error_2";
        addRollupIndex(table, rollupName1, rollupIndexId1, GlobalStateMgr.getCurrentState().getNextId());
        addRollupIndex(table, rollupName2, rollupIndexId2, GlobalStateMgr.getCurrentState().getNextId());
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName1));
        Assertions.assertTrue(table.hasMaterializedIndex(rollupName2));

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logBatchDropRollup(any(BatchDropInfo.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            List<AlterClause> alterClauses = List.of(
                    new DropRollupClause(rollupName1, new HashMap<>()),
                    new DropRollupClause(rollupName2, new HashMap<>()));
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> handler.processBatchDropRollup(alterClauses, db, table));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertTrue(table.hasMaterializedIndex(rollupName1));
            Assertions.assertTrue(table.hasMaterializedIndex(rollupName2));
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private void addRollupIndex(OlapTable targetTable, String rollupName, long rollupIndexId, long rollupTabletId) {
        List<Column> rollupColumns = List.of(targetTable.getColumn("k1"), targetTable.getColumn("v1"));
        targetTable.setIndexMeta(rollupIndexId, rollupName, rollupColumns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);

        MaterializedIndex rollupIndex = new MaterializedIndex(rollupIndexId, MaterializedIndex.IndexState.NORMAL);
        LocalTablet rollupTablet = new LocalTablet(rollupTabletId);
        Partition basePartition = targetTable.getPartitions().iterator().next();
        long basePhysicalPartitionId = basePartition.getDefaultPhysicalPartition().getId();
        TabletMeta rollupTabletMeta = new TabletMeta(db.getId(), targetTable.getId(), basePhysicalPartitionId,
                rollupIndexId, TStorageMedium.HDD);
        rollupIndex.addTablet(rollupTablet, rollupTabletMeta);
        rollupTablet.addReplica(new Replica(GlobalStateMgr.getCurrentState().getNextId(), backendId,
                Replica.ReplicaState.NORMAL, 1, 0), false);

        basePartition.getDefaultPhysicalPartition().createRollupIndex(rollupIndex);
    }

    private static OlapTable createHashOlapTable(long dbId, long tableId, long indexId, String tableName,
                                                 long partitionId, long physicalPartitionId, long tabletId,
                                                 long backendId, long replicaId) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("k1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v1", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(1, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(new com.starrocks.catalog.Replica(replicaId, backendId,
                com.starrocks.catalog.Replica.ReplicaState.NORMAL, 1, 0), false);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(partitionId, physicalPartitionId, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(indexId, tableName, columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId);
        olapTable.setDefaultDistributionInfo(distributionInfo);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }
}
