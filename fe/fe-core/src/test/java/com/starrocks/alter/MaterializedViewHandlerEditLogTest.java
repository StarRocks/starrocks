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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.KeysType;
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
        CreateMaterializedViewStmt stmt = (CreateMaterializedViewStmt) UtFrameUtils
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
        CreateMaterializedViewStmt stmt = (CreateMaterializedViewStmt) UtFrameUtils
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
