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

import com.google.common.collect.Sets;
import com.starrocks.catalog.CatalogRecycleBin.RecycleTableInfo;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EraseDbLog;
import com.starrocks.persist.ErasePartitionLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class CatalogRecycleBinEditLogTest {
    private CatalogRecycleBin recycleBin;
    private static final String DB_NAME = "test_recycle_bin_db";
    private static final String TABLE_NAME = "test_recycle_bin_table";
    private static final String PARTITION_NAME = "p1";
    private static final long DB_ID = 40001L;
    private static final long TABLE_ID = 40002L;
    private static final long PARTITION_ID = 40003L;
    private static final long PHYSICAL_PARTITION_ID = 40004L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        recycleBin.clear();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private Database createDatabase(long dbId, String dbName) {
        return new Database(dbId, dbName);
    }

    private OlapTable createOlapTable(long tableId, String tableName) {
        List<Column> columns = new java.util.ArrayList<>();
        Column col1 = new Column("v1", com.starrocks.type.IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", com.starrocks.type.IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(10001L, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(10002L);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, 10001L, 
                com.starrocks.thrift.TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, PARTITION_NAME, 
                baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, 
                com.starrocks.sql.ast.KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(10001L, tableName, columns, 0, 0, (short) 1, 
                com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(10001L);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new java.util.HashMap<>()));
        return olapTable;
    }

    private OlapTable createRangePartitionedOlapTable(long tableId, String tableName) {
        List<Column> columns = new java.util.ArrayList<>();
        Column partitionCol = new Column("dt", com.starrocks.type.DateType.DATE);
        partitionCol.setIsKey(true);
        columns.add(partitionCol);
        columns.add(new Column("v2", com.starrocks.type.IntegerType.BIGINT));

        // Create RangePartitionInfo with DATE partition column
        RangePartitionInfo partitionInfo = new RangePartitionInfo(List.of(partitionCol));
        partitionInfo.setDataProperty(PARTITION_ID, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        // Create partition range: [2024-01-01, 2024-02-01)
        try {
            com.starrocks.catalog.PartitionKey lowerKey = com.starrocks.catalog.PartitionKey.ofDate(
                    java.time.LocalDate.parse("2024-01-01"));
            com.starrocks.catalog.PartitionKey upperKey = com.starrocks.catalog.PartitionKey.ofDate(
                    java.time.LocalDate.parse("2024-02-01"));
            com.google.common.collect.Range<com.starrocks.catalog.PartitionKey> partitionRange = 
                    com.google.common.collect.Range.closedOpen(lowerKey, upperKey);
            partitionInfo.addPartition(PARTITION_ID, false, partitionRange,
                    DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(partitionCol));

        MaterializedIndex baseIndex = new MaterializedIndex(10001L, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(10002L);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, 10001L, 
                com.starrocks.thrift.TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, PARTITION_NAME, 
                baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, 
                com.starrocks.sql.ast.KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(10001L, tableName, columns, 0, 0, (short) 1, 
                com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(10001L);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new java.util.HashMap<>()));
        return olapTable;
    }

    @Test
    public void testEraseDatabaseNormalCase() throws Exception {
        // 1. Create and recycle a database
        Database db = createDatabase(DB_ID, DB_NAME);
        Set<String> tableNames = Sets.newHashSet();
        recycleBin.recycleDatabase(db, tableNames, true);
        
        Assertions.assertNotNull(recycleBin.getDatabase(DB_ID));

        // 2. Set recycle time to expired
        long oldRecycleTime = System.currentTimeMillis() - 
                (Config.catalog_trash_expire_second + 100) * 1000L;
        // Access protected field through a test helper class that extends CatalogRecycleBin
        TestCatalogRecycleBin testRecycleBin = new TestCatalogRecycleBin(recycleBin);
        testRecycleBin.setRecycleTime(DB_ID, oldRecycleTime);

        // 3. Execute eraseDatabase
        recycleBin.eraseDatabase(System.currentTimeMillis());

        // 4. Verify master state - database should be removed
        Assertions.assertNull(recycleBin.getDatabase(DB_ID));
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(DB_ID));

        // 5. Test follower replay
        EraseDbLog replayLog = (EraseDbLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ERASE_DB_V2);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(DB_ID, replayLog.getDbId());

        // Create follower recycle bin and the same database, then replay
        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        Database followerDb = createDatabase(DB_ID, DB_NAME);
        followerRecycleBin.recycleDatabase(followerDb, tableNames, true);
        TestCatalogRecycleBin testFollowerRecycleBin = new TestCatalogRecycleBin(followerRecycleBin);
        testFollowerRecycleBin.setRecycleTime(DB_ID, oldRecycleTime);
        Assertions.assertNotNull(followerRecycleBin.getDatabase(DB_ID));

        followerRecycleBin.replayEraseDatabase(DB_ID);

        // 6. Verify follower state
        Assertions.assertNull(followerRecycleBin.getDatabase(DB_ID));
        Assertions.assertFalse(followerRecycleBin.isContainedInidToRecycleTime(DB_ID));
    }

    @Test
    public void testEraseDatabaseEditLogException() throws Exception {
        // 1. Create and recycle a database
        Database db = createDatabase(DB_ID, DB_NAME);
        Set<String> tableNames = Sets.newHashSet();
        recycleBin.recycleDatabase(db, tableNames, true);
        
        // 2. Set recycle time to expired
        long oldRecycleTime = System.currentTimeMillis() - 
                (Config.catalog_trash_expire_second + 100) * 1000L;
        // Access protected field through a test helper class that extends CatalogRecycleBin
        TestCatalogRecycleBin testRecycleBin = new TestCatalogRecycleBin(recycleBin);
        testRecycleBin.setRecycleTime(DB_ID, oldRecycleTime);

        // 3. Mock EditLog.logEraseDb to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logEraseDb(any(Long.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute eraseDatabase and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            recycleBin.eraseDatabase(System.currentTimeMillis());
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") ||
                            exception.getCause() != null &&
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 5. Verify database is still present after exception
        Assertions.assertNotNull(recycleBin.getDatabase(DB_ID));
    }

    @Test
    public void testPickTablesToEraseNormalCase() throws Exception {
        // 1. Create and recycle a table
        Database db = createDatabase(DB_ID, DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        recycleBin.recycleTable(DB_ID, table, true);
        
        Assertions.assertNotNull(recycleBin.getTable(DB_ID, TABLE_ID));

        // 2. Set recycle time to expired and mark as non-recoverable
        long oldRecycleTime = System.currentTimeMillis() - 
                (Config.catalog_trash_expire_second + 100) * 1000L;
        TestCatalogRecycleBin testRecycleBin = new TestCatalogRecycleBin(recycleBin);
        testRecycleBin.setRecycleTime(TABLE_ID, oldRecycleTime);
        RecycleTableInfo tableInfo = recycleBin.getRecycleTableInfo(TABLE_ID);
        tableInfo.setRecoverable(false);

        // 3. Execute pickTablesToErase
        List<RecycleTableInfo> tablesToErase = recycleBin.pickTablesToErase(System.currentTimeMillis());

        // 4. Verify master state - should return tables to erase
        Assertions.assertNotNull(tablesToErase);
        Assertions.assertFalse(tablesToErase.isEmpty());
        Assertions.assertEquals(TABLE_ID, tablesToErase.get(0).getTable().getId());

        // 5. Test follower replay - pickTablesToErase logs erase tables
        // The actual erase happens through logEraseMultiTables
        MultiEraseTableInfo replayLog = (MultiEraseTableInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ERASE_MULTI_TABLES);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertNotNull(replayLog.getTableIds());
        Assertions.assertTrue(replayLog.getTableIds().contains(TABLE_ID));

        // Create follower recycle bin and the same table, then replay
        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerRecycleBin.recycleTable(DB_ID, followerTable, true);
        TestCatalogRecycleBin testFollowerRecycleBin = new TestCatalogRecycleBin(followerRecycleBin);
        testFollowerRecycleBin.setRecycleTime(TABLE_ID, oldRecycleTime);
        RecycleTableInfo followerTableInfo = followerRecycleBin.getRecycleTableInfo(TABLE_ID);
        followerTableInfo.setRecoverable(false);
        Assertions.assertNotNull(followerRecycleBin.getTable(DB_ID, TABLE_ID));

        followerRecycleBin.replayEraseTable(replayLog.getTableIds());

        // 6. Verify follower state
        Assertions.assertNull(followerRecycleBin.getTable(DB_ID, TABLE_ID));
        Assertions.assertFalse(followerRecycleBin.isContainedInidToRecycleTime(TABLE_ID));
    }

    @Test
    public void testErasePartitionNormalCase() throws Exception {
        // 1. Create and recycle a partition
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        Partition partition = table.getPartition(PARTITION_NAME);
        Assertions.assertNotNull(partition);

        RecyclePartitionInfo partitionInfo = new RecyclePartitionInfoV2(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        partitionInfo.setRecoverable(false);
        recycleBin.recyclePartition(partitionInfo);
        
        Assertions.assertNotNull(recycleBin.getPartition(PARTITION_ID));

        // 2. Set recycle time to expired
        long oldRecycleTime = System.currentTimeMillis() - 
                (Config.catalog_trash_expire_second + 100) * 1000L;
        TestCatalogRecycleBin testRecycleBin = new TestCatalogRecycleBin(recycleBin);
        testRecycleBin.setRecycleTime(PARTITION_ID, oldRecycleTime);

        // 3. Mock partition delete to return true (finished) - use a spy on the partition info
        RecyclePartitionInfo recyclePartitionInfo = recycleBin.getRecyclePartitionInfo(PARTITION_ID);
        RecyclePartitionInfo spyPartitionInfo = spy(recyclePartitionInfo);
        doReturn(true).when(spyPartitionInfo).delete();
        
        // Replace the partition info in recycle bin with the spy
        recycleBin.setPartitionInfo(PARTITION_ID, spyPartitionInfo);
        // Create a completed future that returns true
        CompletableFuture<Boolean> completedFuture = CompletableFuture.completedFuture(true);
        recycleBin.setDeleteFutureForPartition(spyPartitionInfo, completedFuture);

        // 4. Execute erasePartition - it should log EditLog since delete is finished
        recycleBin.erasePartition(System.currentTimeMillis());

        // 5. Verify master state - partition should be removed
        Assertions.assertNull(recycleBin.getPartition(PARTITION_ID));
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(PARTITION_ID));

        // 6. Test follower replay
        ErasePartitionLog replayLog = (ErasePartitionLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ERASE_PARTITION_V2);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(PARTITION_ID, replayLog.getPartitionId());

        // Create follower recycle bin and the same partition, then replay
        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        Partition followerPartition = followerTable.getPartition(PARTITION_NAME);
        RecyclePartitionInfo followerPartitionInfo = new RecyclePartitionInfoV2(
                DB_ID, TABLE_ID, followerPartition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        followerPartitionInfo.setRecoverable(false);
        followerRecycleBin.recyclePartition(followerPartitionInfo);
        TestCatalogRecycleBin testFollowerRecycleBin = new TestCatalogRecycleBin(followerRecycleBin);
        testFollowerRecycleBin.setRecycleTime(PARTITION_ID, oldRecycleTime);
        Assertions.assertNotNull(followerRecycleBin.getPartition(PARTITION_ID));

        followerRecycleBin.replayErasePartition(PARTITION_ID);

        // 7. Verify follower state
        Assertions.assertNull(followerRecycleBin.getPartition(PARTITION_ID));
        Assertions.assertFalse(followerRecycleBin.isContainedInidToRecycleTime(PARTITION_ID));
    }

    @Test
    public void testErasePartitionEditLogException() throws Exception {
        // 1. Create and recycle a partition
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        Partition partition = table.getPartition(PARTITION_NAME);
        RecyclePartitionInfo partitionInfo = new RecyclePartitionInfoV2(
                DB_ID, TABLE_ID, partition,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        partitionInfo.setRecoverable(false);
        recycleBin.recyclePartition(partitionInfo);
        
        // 2. Set recycle time to expired
        long oldRecycleTime = System.currentTimeMillis() - 
                (Config.catalog_trash_expire_second + 100) * 1000L;
        TestCatalogRecycleBin testRecycleBinForPartition = new TestCatalogRecycleBin(recycleBin);
        testRecycleBinForPartition.setRecycleTime(PARTITION_ID, oldRecycleTime);

        // 3. Mock partition delete to return true - use a spy on the partition info
        RecyclePartitionInfo recyclePartitionInfo = recycleBin.getRecyclePartitionInfo(PARTITION_ID);
        RecyclePartitionInfo spyPartitionInfo = spy(recyclePartitionInfo);
        doReturn(true).when(spyPartitionInfo).delete();
        
        // Replace the partition info in recycle bin with the spy
        recycleBin.setPartitionInfo(PARTITION_ID, spyPartitionInfo);
        // Create a completed future that returns true
        CompletableFuture<Boolean> completedFuture = CompletableFuture.completedFuture(true);
        recycleBin.setDeleteFutureForPartition(spyPartitionInfo, completedFuture);

        // 4. Mock EditLog.logErasePartition to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logErasePartition(any(Long.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 5. Execute erasePartition and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            recycleBin.erasePartition(System.currentTimeMillis());
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") ||
                            exception.getCause() != null &&
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 6. Verify partition is still present after exception
        Assertions.assertNotNull(recycleBin.getPartition(PARTITION_ID));
    }

    @Test
    public void testRecoverTableNormalCase() throws Exception {
        // 1. Create database and recycle a table
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = createDatabase(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        recycleBin.recycleTable(DB_ID, table, true);
        
        Assertions.assertNotNull(recycleBin.getTable(DB_ID, TABLE_ID));
        Assertions.assertNull(db.getTable(TABLE_NAME));

        // 2. Execute recoverTable
        boolean recovered = recycleBin.recoverTable(db, TABLE_NAME);

        // 3. Verify master state
        Assertions.assertTrue(recovered);
        Assertions.assertNull(recycleBin.getTable(DB_ID, TABLE_ID));
        Table recoveredTable = db.getTable(TABLE_ID);
        Assertions.assertNotNull(recoveredTable);
        Assertions.assertEquals(TABLE_NAME, recoveredTable.getName());
        Assertions.assertEquals(TABLE_ID, recoveredTable.getId());

        // 4. Test follower replay
        RecoverInfo replayLog = (RecoverInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RECOVER_TABLE_V2);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(DB_ID, replayLog.getDbId());
        Assertions.assertEquals(TABLE_ID, replayLog.getTableId());
        Assertions.assertEquals(-1L, replayLog.getPartitionId());

        // Create follower metastore and the same objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = createDatabase(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        followerRecycleBin.recycleTable(DB_ID, followerTable, true);
        Assertions.assertNotNull(followerRecycleBin.getTable(DB_ID, TABLE_ID));

        followerRecycleBin.replayRecoverTable(followerDb, TABLE_ID);

        // 5. Verify follower state
        Assertions.assertNull(followerRecycleBin.getTable(DB_ID, TABLE_ID));
        Table followerRecoveredTable = followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(followerRecoveredTable);
        Assertions.assertEquals(TABLE_NAME, followerRecoveredTable.getName());
        Assertions.assertEquals(TABLE_ID, followerRecoveredTable.getId());
    }

    @Test
    public void testRecoverTableEditLogException() throws Exception {
        // 1. Create database and recycle a table
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = createDatabase(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        recycleBin.recycleTable(DB_ID, table, true);

        // 2. Mock EditLog.logRecoverTable to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logRecoverTable(any(RecoverInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute recoverTable and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            recycleBin.recoverTable(db, TABLE_NAME);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") ||
                            exception.getCause() != null &&
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 4. Verify table is still in recycle bin after exception
        Assertions.assertNotNull(recycleBin.getTable(DB_ID, TABLE_ID));
        Assertions.assertNull(db.getTable(TABLE_NAME));
    }

    @Test
    public void testRecoverPartitionNormalCase() throws Exception {
        // 0. Ensure partition is not already in recycle bin - use synchronized to ensure atomicity
        recycleBin.removePartitionFromRecycleBin(PARTITION_ID);
        
        // 1. Create database and table, then recycle a partition
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = createDatabase(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Use range partitioned table for recoverPartition test (unpartitioned tables don't support partition recovery)
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        
        Partition partition = table.getPartition(PARTITION_NAME);
        Assertions.assertNotNull(partition);
        
        // Get range before dropping partition (range info is removed when partition is dropped)
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        com.google.common.collect.Range<com.starrocks.catalog.PartitionKey> partitionRange = 
                rangePartitionInfo.getRange(PARTITION_ID);
        Assertions.assertNotNull(partitionRange, "Partition range should not be null");
        
        table.dropPartition(DB_ID, PARTITION_NAME, false);

        Assertions.assertNotNull(recycleBin.getPartition(PARTITION_ID));
        Assertions.assertNull(table.getPartition(PARTITION_NAME));

        // 2. Execute recoverPartition
        recycleBin.recoverPartition(DB_ID, table, PARTITION_NAME);

        // 3. Verify master state
        Assertions.assertNull(recycleBin.getPartition(PARTITION_ID));
        Partition recoveredPartition = table.getPartition(PARTITION_NAME);
        Assertions.assertNotNull(recoveredPartition);
        Assertions.assertEquals(PARTITION_NAME, recoveredPartition.getName());
        Assertions.assertEquals(PARTITION_ID, recoveredPartition.getId());

        // 4. Test follower replay
        RecoverInfo replayLog = (RecoverInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RECOVER_PARTITION_V2);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(DB_ID, replayLog.getDbId());
        Assertions.assertEquals(TABLE_ID, replayLog.getTableId());
        Assertions.assertEquals(PARTITION_ID, replayLog.getPartitionId());

        // Create follower metastore and the same objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = createDatabase(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Use range partitioned table for follower as well
        OlapTable followerTable = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        Partition followerPartition = followerTable.getPartition(PARTITION_NAME);
        
        // Get range before dropping partition
        RangePartitionInfo followerRangePartitionInfo = (RangePartitionInfo) followerTable.getPartitionInfo();
        com.google.common.collect.Range<com.starrocks.catalog.PartitionKey> followerPartitionRange = 
                followerRangePartitionInfo.getRange(PARTITION_ID);
        Assertions.assertNotNull(followerPartitionRange, "Follower partition range should not be null");
        
        followerTable.dropPartition(DB_ID, PARTITION_NAME, false);
        
        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        RecyclePartitionInfo followerPartitionInfo = new RecycleRangePartitionInfo(
                DB_ID, TABLE_ID, followerPartition, followerPartitionRange,
                DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        followerRecycleBin.recyclePartition(followerPartitionInfo);
        Assertions.assertNotNull(followerRecycleBin.getPartition(PARTITION_ID));

        followerRecycleBin.replayRecoverPartition(followerTable, PARTITION_ID);

        // 5. Verify follower state
        Assertions.assertNull(followerRecycleBin.getPartition(PARTITION_ID));
        Partition followerRecoveredPartition = followerTable.getPartition(PARTITION_NAME);
        Assertions.assertNotNull(followerRecoveredPartition);
        Assertions.assertEquals(PARTITION_NAME, followerRecoveredPartition.getName());
        Assertions.assertEquals(PARTITION_ID, followerRecoveredPartition.getId());
    }

    @Test
    public void testRecoverPartitionEditLogException() throws Exception {
        // 0. Ensure partition is not already in recycle bin - use synchronized to ensure atomicity
        recycleBin.removePartitionFromRecycleBin(PARTITION_ID);
        
        // 1. Create database and table, then recycle a partition
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = createDatabase(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Use range partitioned table for recoverPartition test (unpartitioned tables don't support partition recovery)
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        
        Partition partition = table.getPartition(PARTITION_NAME);
        Assertions.assertNotNull(partition);
        
        // Get range before dropping partition (range info is removed when partition is dropped)
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        com.google.common.collect.Range<com.starrocks.catalog.PartitionKey> partitionRange = 
                rangePartitionInfo.getRange(PARTITION_ID);
        Assertions.assertNotNull(partitionRange, "Partition range should not be null");
        
        table.dropPartition(DB_ID, PARTITION_NAME, false);
        
        // 2. Mock EditLog.logRecoverPartition to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logRecoverPartition(any(RecoverInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute recoverPartition and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            recycleBin.recoverPartition(DB_ID, table, PARTITION_NAME);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") ||
                            exception.getCause() != null &&
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 4. Verify partition is still in recycle bin after exception
        Assertions.assertNotNull(recycleBin.getPartition(PARTITION_ID));
        Assertions.assertNull(table.getPartition(PARTITION_NAME));
    }

    // Helper class to access protected members for testing
    private static class TestCatalogRecycleBin extends CatalogRecycleBin {
        private final CatalogRecycleBin target;

        public TestCatalogRecycleBin(CatalogRecycleBin target) {
            this.target = target;
        }

        public void setRecycleTime(long id, long time) {
            try {
                java.lang.reflect.Field field = CatalogRecycleBin.class.getDeclaredField("idToRecycleTime");
                field.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.Map<Long, Long> idToRecycleTime = (java.util.Map<Long, Long>) field.get(target);
                idToRecycleTime.put(id, time);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

