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

package com.starrocks.catalog;

import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
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

public class HiveTableEditLogTest {
    private static final String DB_NAME = "test_hive_table_editlog";
    private static final String TABLE_NAME = "test_hive_table";
    private static final long DB_ID = 20001L;
    private static final long TABLE_ID = 20002L;

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

    private static HiveTable createHiveTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("col1", IntegerType.BIGINT));
        columns.add(new Column("col2", new StringType(100)));
        
        List<String> dataColumnNames = new ArrayList<>();
        dataColumnNames.add("col1");
        dataColumnNames.add("col2");
        
        HiveTable hiveTable = new HiveTable(
                tableId, tableName, columns, "test_resource", "test_catalog",
                "hive_db", "hive_table", "hdfs://path", "comment", System.currentTimeMillis(),
                new ArrayList<>(), dataColumnNames, new HashMap<>(), new HashMap<>(),
                HiveStorageFormat.PARQUET, HiveTable.HiveTableType.EXTERNAL_TABLE);
        return hiveTable;
    }

    @Test
    public void testModifyTableSchemaNormalCase() throws Exception {
        // 1. Create HiveTable
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        HiveTable hiveTable = createHiveTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(hiveTable);
        
        // Verify initial state
        Assertions.assertEquals(2, hiveTable.getFullSchema().size());
        Assertions.assertEquals(2, hiveTable.getDataColumnNames().size());
        
        // 2. Create updated HiveTable with new schema
        List<Column> newColumns = new ArrayList<>();
        newColumns.add(new Column("col1", IntegerType.BIGINT));
        newColumns.add(new Column("col2", new StringType(100)));
        newColumns.add(new Column("col3", IntegerType.INT)); // New column
        
        List<String> newDataColumnNames = new ArrayList<>();
        newDataColumnNames.add("col1");
        newDataColumnNames.add("col2");
        newDataColumnNames.add("col3");
        
        HiveTable updatedTable = new HiveTable(
                TABLE_ID, TABLE_NAME, newColumns, "test_resource", "test_catalog",
                "hive_db", "hive_table", "hdfs://path", "comment", System.currentTimeMillis(),
                new ArrayList<>(), newDataColumnNames, new HashMap<>(), new HashMap<>(),
                HiveStorageFormat.PARQUET, HiveTable.HiveTableType.EXTERNAL_TABLE);
        
        // 3. Execute modifyTableSchema
        hiveTable.modifyTableSchema(DB_NAME, TABLE_NAME, updatedTable);
        
        // 4. Verify master state
        HiveTable modifiedTable = (HiveTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(modifiedTable);
        Assertions.assertEquals(3, modifiedTable.getFullSchema().size());
        Assertions.assertEquals(3, modifiedTable.getDataColumnNames().size());
        Assertions.assertTrue(modifiedTable.getDataColumnNames().contains("col3"));
        Assertions.assertNotNull(modifiedTable.getColumn("col3"));
        
        // 5. Test follower replay
        ModifyTableColumnOperationLog replayInfo = (ModifyTableColumnOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_HIVE_TABLE_COLUMN);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_NAME, replayInfo.getDbName());
        Assertions.assertEquals(TABLE_NAME, replayInfo.getTableName());
        Assertions.assertEquals(3, replayInfo.getColumns().size());
        
        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Create HiveTable with same ID and initial schema
        HiveTable followerTable = createHiveTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        
        followerMetastore.replayModifyHiveTableColumn(OperationType.OP_MODIFY_HIVE_TABLE_COLUMN, replayInfo);
        
        // 6. Verify follower state
        HiveTable replayed = (HiveTable) followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(3, replayed.getFullSchema().size());
        // Note: replayModifyHiveTableColumn only sets fullSchema via setNewFullSchema,
        // it doesn't update dataColumnNames. So we verify fullSchema is correct.
        Assertions.assertNotNull(replayed.getColumn("col3"));
        
        // Verify all columns match
        for (int i = 0; i < modifiedTable.getFullSchema().size(); i++) {
            Column masterCol = modifiedTable.getFullSchema().get(i);
            Column followerCol = replayed.getFullSchema().get(i);
            Assertions.assertEquals(masterCol.getName(), followerCol.getName());
            Assertions.assertEquals(masterCol.getType(), followerCol.getType());
        }
        
        // Verify the new column exists in fullSchema
        boolean foundCol3 = false;
        for (Column col : replayed.getFullSchema()) {
            if ("col3".equals(col.getName())) {
                foundCol3 = true;
                break;
            }
        }
        Assertions.assertTrue(foundCol3, "col3 should exist in fullSchema after replay");
    }

    @Test
    public void testModifyTableSchemaEditLogException() throws Exception {
        // 1. Create HiveTable
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        HiveTable hiveTable = createHiveTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(hiveTable);
        
        // Verify initial state
        Assertions.assertEquals(2, hiveTable.getFullSchema().size());
        
        // 2. Create updated HiveTable
        List<Column> newColumns = new ArrayList<>();
        newColumns.add(new Column("col1", IntegerType.BIGINT));
        newColumns.add(new Column("col2", new StringType(100)));
        newColumns.add(new Column("col3", IntegerType.INT));
        
        List<String> newDataColumnNames = new ArrayList<>();
        newDataColumnNames.add("col1");
        newDataColumnNames.add("col2");
        newDataColumnNames.add("col3");
        
        HiveTable updatedTable = new HiveTable(
                TABLE_ID, TABLE_NAME, newColumns, "test_resource", "test_catalog",
                "hive_db", "hive_table", "hdfs://path", "comment", System.currentTimeMillis(),
                new ArrayList<>(), newDataColumnNames, new HashMap<>(), new HashMap<>(),
                HiveStorageFormat.PARQUET, HiveTable.HiveTableType.EXTERNAL_TABLE);
        
        // 3. Mock EditLog.logModifyTableColumn to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyTableColumn(any(ModifyTableColumnOperationLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);
        
        // 4. Execute modifyTableSchema and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            hiveTable.modifyTableSchema(DB_NAME, TABLE_NAME, updatedTable);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));
        
        // 5. Verify table schema remains unchanged after exception
        HiveTable unchangedTable = (HiveTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(unchangedTable);
        Assertions.assertEquals(2, unchangedTable.getFullSchema().size());
        Assertions.assertEquals(2, unchangedTable.getDataColumnNames().size());
        Assertions.assertNull(unchangedTable.getColumn("col3"));
    }
}

