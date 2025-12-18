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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ExportMgrEditLogTest {
    private ExportMgr masterExportMgr;
    private Database testDatabase;
    private OlapTable testTable;
    private static final String TEST_DB_NAME = "test_db";
    private static final long TEST_DB_ID = 100L;
    private static final long TEST_TABLE_ID = 1000L;
    private static final String TEST_TABLE_NAME = "test_table";

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create test database and table
        testDatabase = new Database(TEST_DB_ID, TEST_DB_NAME);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(testDatabase);

        // Create a complete table with partition info
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("k1", IntegerType.INT);
        col1.setIsKey(true);
        columns.add(col1);
        Column col2 = new Column("v1", IntegerType.BIGINT);
        columns.add(col2);
        
        long partitionId = TEST_TABLE_ID + 1;
        long indexId = TEST_TABLE_ID + 2;
        
        // Create partition info
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 1);
        
        // Create distribution info
        HashDistributionInfo distributionInfo = new HashDistributionInfo(3, Lists.newArrayList(col1));
        
        // Create MaterializedIndex
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        
        // Create Partition
        Partition partition = new Partition(partitionId, partitionId + 100, TEST_TABLE_NAME, 
                materializedIndex, distributionInfo);
        
        // Create OlapTable
        testTable = new OlapTable(TEST_TABLE_ID, TEST_TABLE_NAME, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        testTable.setIndexMeta(indexId, TEST_TABLE_NAME, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        testTable.setBaseIndexMetaId(indexId);
        testTable.addPartition(partition);
        
        testDatabase.registerTableUnlocked(testTable);

        // Create ExportMgr instance
        masterExportMgr = GlobalStateMgr.getCurrentState().getExportMgr();

        // Setup ConnectContext for addExportJob
        ConnectContext connectContext = new ConnectContext(null);
        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        connectContext.setThreadLocalInfo();
        
        // Verify ConnectContext is set
        Assertions.assertNotNull(ConnectContext.get());
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Add Export Job Tests ====================

    @Test
    public void testAddExportJobNormalCase() throws Exception {
        // Ensure ConnectContext is set
        if (ConnectContext.get() == null) {
            ConnectContext connectContext = new ConnectContext(null);
            connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            connectContext.setThreadLocalInfo();
        }
        
        // 1. Prepare test data
        UUID queryId = UUIDUtil.genUUID();
        ExportStmt stmt = createMockExportStmt();

        // 2. Verify initial state
        Assertions.assertEquals(0, masterExportMgr.getIdToJob().size());

        // 3. Execute addExportJob operation
        masterExportMgr.addExportJob(queryId, stmt);

        // 4. Verify master state
        Assertions.assertEquals(1, masterExportMgr.getIdToJob().size());
        ExportJob addedJob = masterExportMgr.getExportJob(TEST_DB_NAME, queryId);
        Assertions.assertNotNull(addedJob);
        Assertions.assertEquals(TEST_DB_ID, addedJob.getDbId());

        // 5. Test follower replay functionality
        ExportMgr followerExportMgr = new ExportMgr();
        Assertions.assertEquals(0, followerExportMgr.getIdToJob().size());

        ExportJob replayJob = (ExportJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_EXPORT_CREATE_V2);

        followerExportMgr.replayCreateExportJob(replayJob);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(1, followerExportMgr.getIdToJob().size());
        ExportJob followerJob = followerExportMgr.getExportJob(TEST_DB_NAME, queryId);
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(TEST_DB_ID, followerJob.getDbId());
    }

    @Test
    public void testAddExportJobEditLogException() throws Exception {
        // 1. Prepare test data
        UUID queryId = UUIDUtil.genUUID();
        ExportStmt stmt = createMockExportStmt();

        // Ensure ConnectContext is set
        if (ConnectContext.get() == null) {
            ConnectContext connectContext = new ConnectContext(null);
            connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            connectContext.setThreadLocalInfo();
        }

        // 2. Create a separate ExportMgr for exception testing
        ExportMgr exceptionExportMgr = new ExportMgr();
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logExportCreate to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logExportCreate(any(ExportJob.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(0, exceptionExportMgr.getIdToJob().size());

        // 4. Execute addExportJob operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionExportMgr.addExportJob(queryId, stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(0, exceptionExportMgr.getIdToJob().size());
        ExportJob retrievedJob = exceptionExportMgr.getExportJob(TEST_DB_NAME, queryId);
        Assertions.assertNull(retrievedJob);
    }

    private ExportStmt createMockExportStmt() {
        ExportStmt stmt = mock(ExportStmt.class);
        when(stmt.getTblName()).thenReturn(new TableName(TEST_DB_NAME, TEST_TABLE_NAME));
        when(stmt.getBrokerDesc()).thenReturn(new BrokerDesc("test_broker", Maps.newHashMap()));
        when(stmt.getColumnSeparator()).thenReturn("\t");
        when(stmt.getRowDelimiter()).thenReturn("\n");
        when(stmt.isIncludeQueryId()).thenReturn(true);
        when(stmt.getProperties()).thenReturn(Maps.newHashMap());
        when(stmt.getPath()).thenReturn("hdfs://test/path");
        when(stmt.getFileNamePrefix()).thenReturn("export_");
        when(stmt.getPartitions()).thenReturn(null);
        when(stmt.getColumnNames()).thenReturn(null);
        return stmt;
    }
}

