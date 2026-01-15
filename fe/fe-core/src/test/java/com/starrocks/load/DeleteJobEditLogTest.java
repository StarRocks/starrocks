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

package com.starrocks.load;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DeleteJobEditLogTest {
    private static final String DB_NAME = "test_delete_job_editlog";
    private static final String TABLE_NAME = "test_table";
    private static final long DB_ID = 30001L;
    private static final long TABLE_ID = 30002L;
    private static final long JOB_ID = 30003L;
    private static final long TRANSACTION_ID = 30004L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // Create database and table directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static OlapTable createOlapTable(long tableId, String tableName) {
        List<com.starrocks.catalog.Column> columns = new ArrayList<>();
        com.starrocks.catalog.Column col1 = new com.starrocks.catalog.Column("v1", com.starrocks.type.IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new com.starrocks.catalog.Column("v2", com.starrocks.type.IntegerType.BIGINT));

        com.starrocks.catalog.PartitionInfo partitionInfo = new com.starrocks.catalog.SinglePartitionInfo();
        partitionInfo.setDataProperty(30004L, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(30004L, (short) 1);

        com.starrocks.catalog.DistributionInfo distributionInfo = 
                new com.starrocks.catalog.HashDistributionInfo(3, List.of(col1));

        com.starrocks.catalog.MaterializedIndex baseIndex = 
                new com.starrocks.catalog.MaterializedIndex(30006L, com.starrocks.catalog.MaterializedIndex.IndexState.NORMAL);
        com.starrocks.catalog.LocalTablet tablet = new com.starrocks.catalog.LocalTablet(30007L);
        com.starrocks.catalog.TabletMeta tabletMeta = 
                new com.starrocks.catalog.TabletMeta(DB_ID, tableId, 30004L, 30006L, com.starrocks.thrift.TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        com.starrocks.catalog.Partition partition = 
                new com.starrocks.catalog.Partition(30004L, 30005L, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, 
                com.starrocks.sql.ast.KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(30006L, tableName, columns, 0, 0, (short) 1, 
                com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(30006L);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    @Test
    public void testAfterVisibleNormalCase() throws Exception {
        // 1. Create database and table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // 2. Create MultiDeleteInfo
        List<String> deleteConditions = new ArrayList<>();
        deleteConditions.add("v1 > 100");
        MultiDeleteInfo deleteInfo = new MultiDeleteInfo(DB_ID, TABLE_ID, TABLE_NAME, deleteConditions);
        deleteInfo.setPartitions(false, List.of(30004L), List.of("p1"));

        // 3. Create OlapDeleteJob
        Map<Long, Short> partitionToReplicateNum = new HashMap<>();
        partitionToReplicateNum.put(30004L, (short) 1);
        OlapDeleteJob deleteJob = new OlapDeleteJob(JOB_ID, TRANSACTION_ID, "test_label", 
                partitionToReplicateNum, deleteInfo);

        // Verify initial state
        Assertions.assertEquals(DeleteJob.DeleteState.DELETING, deleteJob.getState());

        // 4. Create mock TransactionState
        TransactionState txnState = mock(TransactionState.class);
        when(txnState.getTransactionId()).thenReturn(TRANSACTION_ID);
        when(txnState.getDbId()).thenReturn(DB_ID);
        when(txnState.getTransactionStatus()).thenReturn(TransactionStatus.VISIBLE);

        // 5. Execute afterVisible
        deleteJob.afterVisible(txnState, true);

        // 6. Verify master state
        Assertions.assertEquals(DeleteJob.DeleteState.FINISHED, deleteJob.getState());

        // 7. Test follower replay
        MultiDeleteInfo replayInfo = (MultiDeleteInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_FINISH_MULTI_DELETE);

        DeleteMgr followerDeleteMgr = new DeleteMgr();
        // Replay the operation
        followerDeleteMgr.replayMultiDelete(replayInfo, GlobalStateMgr.getCurrentState());
        MultiDeleteInfo followerMutiDeleteInfo = followerDeleteMgr.getDeleteInfosForDb(DB_ID).get(0);
        Assertions.assertNotNull(followerMutiDeleteInfo);
        Assertions.assertEquals(DB_ID, followerMutiDeleteInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, followerMutiDeleteInfo.getTableId());
        Assertions.assertEquals(TABLE_NAME, followerMutiDeleteInfo.getTableName());
        Assertions.assertEquals(1, followerMutiDeleteInfo.getDeleteConditions().size());
        Assertions.assertEquals("v1 > 100", followerMutiDeleteInfo.getDeleteConditions().get(0));
        Assertions.assertNotNull(followerMutiDeleteInfo.getPartitionIds());
        Assertions.assertEquals(1, followerMutiDeleteInfo.getPartitionIds().size());
        Assertions.assertEquals(30004L, followerMutiDeleteInfo.getPartitionIds().get(0));
    }

    @Test
    public void testAfterVisibleEditLogException() throws Exception {
        // 1. Create database and table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // 2. Create MultiDeleteInfo
        List<String> deleteConditions = new ArrayList<>();
        deleteConditions.add("v1 > 100");
        MultiDeleteInfo deleteInfo = new MultiDeleteInfo(DB_ID, TABLE_ID, TABLE_NAME, deleteConditions);
        deleteInfo.setPartitions(false, List.of(30004L), List.of("p1"));

        // 3. Create OlapDeleteJob
        Map<Long, Short> partitionToReplicateNum = new HashMap<>();
        partitionToReplicateNum.put(30004L, (short) 1);
        OlapDeleteJob deleteJob = new OlapDeleteJob(JOB_ID, TRANSACTION_ID, "test_label", 
                partitionToReplicateNum, deleteInfo);

        // Verify initial state
        Assertions.assertEquals(DeleteJob.DeleteState.DELETING, deleteJob.getState());

        // 4. Mock EditLog.logFinishMultiDelete to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logFinishMultiDelete(any(MultiDeleteInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 5. Create mock TransactionState
        TransactionState txnState = mock(TransactionState.class);
        when(txnState.getTransactionId()).thenReturn(TRANSACTION_ID);
        when(txnState.getDbId()).thenReturn(DB_ID);
        when(txnState.getTransactionStatus()).thenReturn(TransactionStatus.VISIBLE);

        // 6. Execute afterVisible and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            deleteJob.afterVisible(txnState, true);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 7. Verify job state remains unchanged after exception
        Assertions.assertEquals(DeleteJob.DeleteState.DELETING, deleteJob.getState());
    }

    @Test
    public void testAfterVisibleNotOperated() throws Exception {
        // 1. Create database and table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // 2. Create MultiDeleteInfo
        List<String> deleteConditions = new ArrayList<>();
        deleteConditions.add("v1 > 100");
        MultiDeleteInfo deleteInfo = new MultiDeleteInfo(DB_ID, TABLE_ID, TABLE_NAME, deleteConditions);

        // 3. Create OlapDeleteJob
        Map<Long, Short> partitionToReplicateNum = new HashMap<>();
        partitionToReplicateNum.put(30004L, (short) 1);
        OlapDeleteJob deleteJob = new OlapDeleteJob(JOB_ID, TRANSACTION_ID, "test_label", 
                partitionToReplicateNum, deleteInfo);

        // Verify initial state
        Assertions.assertEquals(DeleteJob.DeleteState.DELETING, deleteJob.getState());

        // 4. Create mock TransactionState
        TransactionState txnState = mock(TransactionState.class);

        // 5. Execute afterVisible with txnOperated = false (should return early)
        deleteJob.afterVisible(txnState, false);

        // 6. Verify job state remains unchanged (no EditLog should be written)
        Assertions.assertEquals(DeleteJob.DeleteState.DELETING, deleteJob.getState());
    }
}

