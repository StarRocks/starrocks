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

package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class StreamLoadMgrEditLogTest {
    private StreamLoadMgr masterStreamLoadMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create StreamLoadMgr instance
        masterStreamLoadMgr = new StreamLoadMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create a database with table
    private Database createDatabaseWithTable(long dbId, String dbName, long tableId, String tableName,
                                             long partitionId, long indexId) throws Exception {
        // Create database
        Database database = new Database(dbId, dbName);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(database);
        
        // Create table columns
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", IntegerType.BIGINT);
        columns.add(col2);
        
        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);
        
        // Create distribution info
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(3);
        
        // Create MaterializedIndex
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        
        // Create Partition
        Partition partition = new Partition(partitionId, partitionId + 100, tableName, 
                materializedIndex, distributionInfo);
        
        // Create OlapTable
        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        olapTable.setIndexMeta(indexId, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId);
        olapTable.addPartition(partition);
        
        // Register table to database
        database.registerTableUnlocked(olapTable);
        
        return database;
    }

    @Test
    public void testBeginLoadTaskFromFrontendNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db";
        String tableName = "test_table";
        String label = "test_label";
        String user = "test_user";
        String clientIp = "127.0.0.1";
        long timeoutMillis = 60000L;
        int channelNum = 1;
        int channelId = 0;
        long dbId = 10001L;
        long tableId = 20001L;
        long partitionId = 30001L;
        long indexId = 40001L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // 2. Verify initial state
        AbstractStreamLoadTask initialTask = masterStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(initialTask);
        
        // 3. Execute beginLoadTaskFromFrontend operation (master side)
        TransactionResult resp = new TransactionResult();
        masterStreamLoadMgr.beginLoadTaskFromFrontend(dbName, tableName, label, user, clientIp, 
                timeoutMillis, channelNum, channelId, resp, WarehouseManager.DEFAULT_RESOURCE);
        
        // 4. Verify master state
        AbstractStreamLoadTask createdTask = masterStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(label, createdTask.getLabel());
        Assertions.assertEquals(dbId, createdTask.getDBId());
        Assertions.assertTrue(createdTask instanceof StreamLoadTask);
        StreamLoadTask streamLoadTask = (StreamLoadTask) createdTask;
        Assertions.assertEquals(tableName, streamLoadTask.getTableName());
        
        // 5. Test follower replay functionality
        StreamLoadMgr followerStreamLoadMgr = new StreamLoadMgr();
        
        // Verify follower initial state
        AbstractStreamLoadTask followerInitialTask = followerStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(followerInitialTask);
        
        StreamLoadTask replayTask = (StreamLoadTask) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_STREAM_LOAD_TASK_V2);
        
        // Execute follower replay
        followerStreamLoadMgr.replayCreateLoadTask(replayTask);
        
        // 6. Verify follower state is consistent with master
        AbstractStreamLoadTask followerTask = followerStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNotNull(followerTask);
        Assertions.assertEquals(label, followerTask.getLabel());
        Assertions.assertEquals(dbId, followerTask.getDBId());
        Assertions.assertEquals(createdTask.getId(), followerTask.getId());
        Assertions.assertTrue(followerTask instanceof StreamLoadTask);
        StreamLoadTask followerStreamLoadTask = (StreamLoadTask) followerTask;
        Assertions.assertEquals(tableName, followerStreamLoadTask.getTableName());
    }

    @Test
    public void testBeginLoadTaskFromFrontendEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db";
        String tableName = "exception_table";
        String label = "exception_label";
        String user = "test_user";
        String clientIp = "127.0.0.1";
        long timeoutMillis = 60000L;
        int channelNum = 1;
        int channelId = 0;
        long dbId = 10002L;
        long tableId = 20002L;
        long partitionId = 30002L;
        long indexId = 40002L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, tableName, partitionId, indexId);
        
        // 2. Create a separate StreamLoadMgr for exception testing
        StreamLoadMgr exceptionStreamLoadMgr = new StreamLoadMgr();
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 3. Mock EditLog.logCreateStreamLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateStreamLoadJob(any(StreamLoadTask.class), any());
        
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Verify initial state
        AbstractStreamLoadTask initialTask = exceptionStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(initialTask);
        
        // 4. Execute beginLoadTaskFromFrontend operation and expect exception
        TransactionResult resp = new TransactionResult();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionStreamLoadMgr.beginLoadTaskFromFrontend(dbName, tableName, label, user, clientIp, 
                    timeoutMillis, channelNum, channelId, resp, WarehouseManager.DEFAULT_RESOURCE);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 5. Verify leader memory state remains unchanged after exception
        AbstractStreamLoadTask taskAfterException = exceptionStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(taskAfterException);
    }

    @Test
    public void testBeginMultiStatementLoadTaskNormalCase() throws Exception {
        // 1. Prepare test data
        String dbName = "test_db";
        String label = "test_multi_stmt_label";
        String user = "test_user";
        String clientIp = "127.0.0.1";
        long timeoutMillis = 60000L;
        long dbId = 10003L;
        long tableId = 20003L;
        long partitionId = 30003L;
        long indexId = 40003L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, "test_table", partitionId, indexId);
        
        // 2. Verify initial state
        AbstractStreamLoadTask initialTask = masterStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(initialTask);
        
        // 3. Execute beginMultiStatementLoadTask operation (master side)
        TransactionResult resp = new TransactionResult();
        masterStreamLoadMgr.beginMultiStatementLoadTask(dbName, label, user, clientIp, 
                timeoutMillis, resp, WarehouseManager.DEFAULT_RESOURCE);
        
        // 4. Verify master state
        AbstractStreamLoadTask createdTask = masterStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNotNull(createdTask);
        Assertions.assertEquals(label, createdTask.getLabel());
        Assertions.assertEquals(dbId, createdTask.getDBId());
        Assertions.assertTrue(createdTask instanceof StreamLoadMultiStmtTask);
        StreamLoadMultiStmtTask multiStmtTask = (StreamLoadMultiStmtTask) createdTask;
        
        // 5. Test follower replay functionality
        StreamLoadMgr followerStreamLoadMgr = new StreamLoadMgr();
        
        // Verify follower initial state
        AbstractStreamLoadTask followerInitialTask = followerStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(followerInitialTask);
        
        StreamLoadMultiStmtTask replayTask = (StreamLoadMultiStmtTask) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_MULTI_STMT_STREAM_LOAD_TASK);
        
        // Execute follower replay
        followerStreamLoadMgr.replayCreateLoadTask(replayTask);
        
        // 6. Verify follower state is consistent with master
        AbstractStreamLoadTask followerTask = followerStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNotNull(followerTask);
        Assertions.assertEquals(label, followerTask.getLabel());
        Assertions.assertEquals(dbId, followerTask.getDBId());
        Assertions.assertEquals(createdTask.getId(), followerTask.getId());
        Assertions.assertTrue(followerTask instanceof StreamLoadMultiStmtTask);
    }

    @Test
    public void testBeginMultiStatementLoadTaskEditLogException() throws Exception {
        // 1. Prepare test data
        String dbName = "exception_db";
        String label = "exception_multi_stmt_label";
        String user = "test_user";
        String clientIp = "127.0.0.1";
        long timeoutMillis = 60000L;
        long dbId = 10004L;
        long tableId = 20004L;
        long partitionId = 30004L;
        long indexId = 40004L;
        
        // Create database with table
        createDatabaseWithTable(dbId, dbName, tableId, "test_table", partitionId, indexId);
        
        // 2. Create a separate StreamLoadMgr for exception testing
        StreamLoadMgr exceptionStreamLoadMgr = new StreamLoadMgr();
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        
        // 3. Mock EditLog.logCreateMultiStmtStreamLoadJob to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateMultiStmtStreamLoadJob(any(StreamLoadMultiStmtTask.class), any());
        
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // Verify initial state
        AbstractStreamLoadTask initialTask = exceptionStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(initialTask);
        
        // 4. Execute beginMultiStatementLoadTask operation and expect exception
        TransactionResult resp = new TransactionResult();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionStreamLoadMgr.beginMultiStatementLoadTask(dbName, label, user, clientIp, 
                    timeoutMillis, resp, WarehouseManager.DEFAULT_RESOURCE);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());
        
        // 5. Verify leader memory state remains unchanged after exception
        AbstractStreamLoadTask taskAfterException = exceptionStreamLoadMgr.getTaskByLabel(label);
        Assertions.assertNull(taskAfterException);
    }
}

