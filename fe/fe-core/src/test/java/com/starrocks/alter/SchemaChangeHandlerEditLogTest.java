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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
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
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SchemaChangeHandlerEditLogTest {
    private static final String DB_NAME = "test_schema_change_handler_editlog";
    private static final String TABLE_NAME = "test_table";
    private static final long DB_ID = 11001L;
    private static final long TABLE_ID = 11002L;
    private static final long PARTITION_ID = 11003L;
    private static final long PHYSICAL_PARTITION_ID = 11004L;
    private static final long INDEX_ID = 11005L;
    private static final long TABLET_ID = 11006L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();

        // Create database and table directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        db.registerTableUnlocked(table);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testUpdateTableConstraintUniqueConstraintNormalCase() throws Exception {
        // 1. Get table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);

        // 2. Create properties with unique constraint
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, "v1");
        
        // 3. Execute updateTableConstraint
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        handler.updateTableConstraint(db, TABLE_NAME, properties);

        // 4. Verify master state
        table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.hasUniqueConstraints());
        List<UniqueConstraint> uniqueConstraints = table.getUniqueConstraints();
        Assertions.assertNotNull(uniqueConstraints);
        Assertions.assertEquals(1, uniqueConstraints.size());
        UniqueConstraint uniqueConstraint = uniqueConstraints.get(0);
        List<String> columnNames = uniqueConstraint.getUniqueColumnNames(table);
        Assertions.assertEquals(1, columnNames.size());
        Assertions.assertEquals("v1", columnNames.get(0));
        Assertions.assertEquals(TABLE_NAME, uniqueConstraint.getTableName());
        Assertions.assertEquals(DB_NAME, uniqueConstraint.getDbName());

        // 5. Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY);

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerTable);

        LocalMetastore original = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY, replayInfo);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(original);
        }

        // 6. Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(replayed);
        Assertions.assertTrue(replayed.hasUniqueConstraints());
        List<UniqueConstraint> replayedUniqueConstraints = replayed.getUniqueConstraints();
        Assertions.assertNotNull(replayedUniqueConstraints);
        Assertions.assertEquals(1, replayedUniqueConstraints.size());
        UniqueConstraint replayedUniqueConstraint = replayedUniqueConstraints.get(0);
        List<String> replayedColumnNames = replayedUniqueConstraint.getUniqueColumnNames(replayed);
        Assertions.assertEquals(1, replayedColumnNames.size());
        Assertions.assertEquals("v1", replayedColumnNames.get(0));
        Assertions.assertEquals(TABLE_NAME, replayedUniqueConstraint.getTableName());
        Assertions.assertEquals(DB_NAME, replayedUniqueConstraint.getDbName());
    }

    @Test
    public void testUpdateTableConstraintForeignKeyConstraintNormalCase() throws Exception {
        // 1. Create a referenced table first with unique constraint
        String refTableName = "ref_table";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable refTable = createHashOlapTable(TABLE_ID + 10, refTableName, 3);
        refTable.setUniqueConstraints(List.of(new UniqueConstraint(null, null, null, List.of(ColumnId.create("v1")))));
        db.registerTableUnlocked(refTable);

        // 2. Get table
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);

        // 3. Create properties with foreign key constraint
        // For non-MV tables, format is: (column) REFERENCES catalog.db.table(column)
        Map<String, String> properties = new HashMap<>();
        String catalogName = table.getCatalogName();
        String fkConstraint = String.format("(v1) REFERENCES %s.%s.%s(v1)",
                catalogName, DB_NAME, refTableName);
        properties.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, fkConstraint);
        
        // 4. Execute updateTableConstraint
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        handler.updateTableConstraint(db, TABLE_NAME, properties);

        // 5. Verify master state
        table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.hasForeignKeyConstraints());
        List<ForeignKeyConstraint> foreignKeyConstraints = table.getForeignKeyConstraints();
        Assertions.assertNotNull(foreignKeyConstraints);
        Assertions.assertEquals(1, foreignKeyConstraints.size());
        ForeignKeyConstraint foreignKeyConstraint = foreignKeyConstraints.get(0);
        List<Pair<String, String>> columnRefPairs = foreignKeyConstraint.getColumnNameRefPairs(table);
        Assertions.assertEquals(1, columnRefPairs.size());
        Assertions.assertEquals("v1", columnRefPairs.get(0).first);
        Assertions.assertEquals("v1", columnRefPairs.get(0).second);
        Assertions.assertEquals(refTableName, foreignKeyConstraint.getParentTableInfo().getTableName());
        Assertions.assertEquals(DB_NAME, foreignKeyConstraint.getParentTableInfo().getDbName());
        Assertions.assertEquals(catalogName, foreignKeyConstraint.getParentTableInfo().getCatalogName());

        // 6. Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY);

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerTable);
        OlapTable followerRefTable = createHashOlapTable(TABLE_ID + 10, refTableName, 3);
        followerRefTable.setUniqueConstraints(List.of(new UniqueConstraint(null, null, null, List.of(ColumnId.create("v1")))));
        followerDb.registerTableUnlocked(followerRefTable);

        LocalMetastore original = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY, replayInfo);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(original);
        }

        // 7. Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(replayed);
        Assertions.assertTrue(replayed.hasForeignKeyConstraints());
        List<ForeignKeyConstraint> replayedForeignKeyConstraints = replayed.getForeignKeyConstraints();
        Assertions.assertNotNull(replayedForeignKeyConstraints);
        Assertions.assertEquals(1, replayedForeignKeyConstraints.size());
        ForeignKeyConstraint replayedForeignKeyConstraint = replayedForeignKeyConstraints.get(0);
        List<Pair<String, String>> replayedColumnRefPairs = replayedForeignKeyConstraint.getColumnNameRefPairs(replayed);
        Assertions.assertEquals(1, replayedColumnRefPairs.size());
        Assertions.assertEquals("v1", replayedColumnRefPairs.get(0).first);
        Assertions.assertEquals("v1", replayedColumnRefPairs.get(0).second);
        Assertions.assertEquals(refTableName, replayedForeignKeyConstraint.getParentTableInfo().getTableName());
        Assertions.assertEquals(DB_NAME, replayedForeignKeyConstraint.getParentTableInfo().getDbName());
        Assertions.assertEquals(catalogName, replayedForeignKeyConstraint.getParentTableInfo().getCatalogName());
    }

    @Test
    public void testUpdateTableConstraintBothConstraintsNormalCase() throws Exception {
        // 1. Create a referenced table first with unique constraint
        String refTableName = "ref_table2";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable refTable = createHashOlapTable(TABLE_ID + 20, refTableName, 3);
        refTable.setUniqueConstraints(List.of(new UniqueConstraint(null, null, null, List.of(ColumnId.create("v1")))));
        db.registerTableUnlocked(refTable);

        // 2. Get table
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);

        // 3. Create properties with both constraints
        // For non-MV tables, format is: (column) REFERENCES catalog.db.table(column)
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, "v1");
        String catalogName = table.getCatalogName();
        String fkConstraint = String.format("(v1) REFERENCES %s.%s.%s(v1)",
                catalogName, DB_NAME, refTableName);
        properties.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, fkConstraint);
        
        // 4. Execute updateTableConstraint
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        handler.updateTableConstraint(db, TABLE_NAME, properties);

        // 5. Verify master state
        table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.hasUniqueConstraints());
        Assertions.assertTrue(table.hasForeignKeyConstraints());
        
        // Verify unique constraint details
        List<UniqueConstraint> uniqueConstraints = table.getUniqueConstraints();
        Assertions.assertNotNull(uniqueConstraints);
        Assertions.assertEquals(1, uniqueConstraints.size());
        UniqueConstraint uniqueConstraint = uniqueConstraints.get(0);
        List<String> uniqueColumnNames = uniqueConstraint.getUniqueColumnNames(table);
        Assertions.assertEquals(1, uniqueColumnNames.size());
        Assertions.assertEquals("v1", uniqueColumnNames.get(0));
        Assertions.assertEquals(TABLE_NAME, uniqueConstraint.getTableName());
        Assertions.assertEquals(DB_NAME, uniqueConstraint.getDbName());
        
        // Verify foreign key constraint details
        List<ForeignKeyConstraint> foreignKeyConstraints = table.getForeignKeyConstraints();
        Assertions.assertNotNull(foreignKeyConstraints);
        Assertions.assertEquals(1, foreignKeyConstraints.size());
        ForeignKeyConstraint foreignKeyConstraint = foreignKeyConstraints.get(0);
        List<Pair<String, String>> columnRefPairs = foreignKeyConstraint.getColumnNameRefPairs(table);
        Assertions.assertEquals(1, columnRefPairs.size());
        Assertions.assertEquals("v1", columnRefPairs.get(0).first);
        Assertions.assertEquals("v1", columnRefPairs.get(0).second);
        Assertions.assertEquals(refTableName, foreignKeyConstraint.getParentTableInfo().getTableName());
        Assertions.assertEquals(DB_NAME, foreignKeyConstraint.getParentTableInfo().getDbName());
        Assertions.assertEquals(catalogName, foreignKeyConstraint.getParentTableInfo().getCatalogName());

        // 6. Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY);

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerTable);
        OlapTable followerRefTable = createHashOlapTable(TABLE_ID + 20, refTableName, 3);
        followerRefTable.setUniqueConstraints(List.of(new UniqueConstraint(null, null, null, List.of(ColumnId.create("v1")))));
        followerDb.registerTableUnlocked(followerRefTable);

        LocalMetastore original = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY, replayInfo);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(original);
        }

        // 7. Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(replayed);
        Assertions.assertTrue(replayed.hasUniqueConstraints());
        Assertions.assertTrue(replayed.hasForeignKeyConstraints());
        
        // Verify unique constraint details
        List<UniqueConstraint> replayedUniqueConstraints = replayed.getUniqueConstraints();
        Assertions.assertNotNull(replayedUniqueConstraints);
        Assertions.assertEquals(1, replayedUniqueConstraints.size());
        UniqueConstraint replayedUniqueConstraint = replayedUniqueConstraints.get(0);
        List<String> replayedUniqueColumnNames = replayedUniqueConstraint.getUniqueColumnNames(replayed);
        Assertions.assertEquals(1, replayedUniqueColumnNames.size());
        Assertions.assertEquals("v1", replayedUniqueColumnNames.get(0));
        Assertions.assertEquals(TABLE_NAME, replayedUniqueConstraint.getTableName());
        Assertions.assertEquals(DB_NAME, replayedUniqueConstraint.getDbName());
        
        // Verify foreign key constraint details
        List<ForeignKeyConstraint> replayedForeignKeyConstraints = replayed.getForeignKeyConstraints();
        Assertions.assertNotNull(replayedForeignKeyConstraints);
        Assertions.assertEquals(1, replayedForeignKeyConstraints.size());
        ForeignKeyConstraint replayedForeignKeyConstraint = replayedForeignKeyConstraints.get(0);
        List<Pair<String, String>> replayedColumnRefPairs = replayedForeignKeyConstraint.getColumnNameRefPairs(replayed);
        Assertions.assertEquals(1, replayedColumnRefPairs.size());
        Assertions.assertEquals("v1", replayedColumnRefPairs.get(0).first);
        Assertions.assertEquals("v1", replayedColumnRefPairs.get(0).second);
        Assertions.assertEquals(refTableName, replayedForeignKeyConstraint.getParentTableInfo().getTableName());
        Assertions.assertEquals(DB_NAME, replayedForeignKeyConstraint.getParentTableInfo().getDbName());
        Assertions.assertEquals(catalogName, replayedForeignKeyConstraint.getParentTableInfo().getCatalogName());
    }

    @Test
    public void testUpdateTableConstraintEditLogException() throws Exception {
        // 1. Get table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);

        // 2. Mock EditLog.logModifyConstraint to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logModifyConstraint(any(ModifyTablePropertyOperationLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Create properties with unique constraint
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, "v1");
        
        // 4. Execute updateTableConstraint and expect exception
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            handler.updateTableConstraint(db, TABLE_NAME, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));
    }

    @Test
    public void testUpdateTableConstraintNoChange() throws Exception {
        // 1. Get table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);

        // 2. First set a constraint
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, "v1");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        handler.updateTableConstraint(db, TABLE_NAME, properties1);

        // 3. Set the same constraint again (should not change)
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, "v1");
        
        // This should return early without writing to EditLog
        handler.updateTableConstraint(db, TABLE_NAME, properties2);

        // 4. Verify state
        table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
    }

    @Test
    public void testUpdateTableConstraintInvalidTable() throws Exception {
        // 1. Get database
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);

        // 2. Create properties
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, "v1");
        
        // 3. Execute updateTableConstraint with non-existent table and expect exception
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        com.starrocks.common.DdlException exception = Assertions.assertThrows(com.starrocks.common.DdlException.class, () -> {
            handler.updateTableConstraint(db, "non_existent_table", properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("does not exist"));
    }

    private static OlapTable createHashOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = Lists.newArrayList();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, Lists.newArrayList(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }
}

