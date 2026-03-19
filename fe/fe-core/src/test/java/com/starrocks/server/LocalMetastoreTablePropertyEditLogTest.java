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

package com.starrocks.server;

import com.google.common.collect.Range;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class LocalMetastoreTablePropertyEditLogTest {
    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static final String DB_NAME = "test_local_metastore_editlog";
    private static final String TABLE_NAME = "test_table";
    private static final long DB_ID = 20001L;
    private static final long TABLE_ID = 20002L;
    private static final long PARTITION_ID = 20003L;
    private static final long PHYSICAL_PARTITION_ID = 20004L;
    private static final long INDEX_ID = 20005L;
    private static final long TABLET_ID = 20006L;

    private static OlapTable createHashOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        // For unpartitioned table, partition name should be the same as table name
        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static OlapTable createRangePartitionedOlapTable(long tableId, String tableName, int bucketNum) {
        try {
            List<Column> columns = new ArrayList<>();
            // Create partition column with DATE type
            Column partitionCol = new Column("dt", DateType.DATE);
            partitionCol.setIsKey(true);
            columns.add(partitionCol);
            columns.add(new Column("v2", IntegerType.BIGINT));

            // Create RangePartitionInfo with DATE partition column
            RangePartitionInfo partitionInfo = new RangePartitionInfo(List.of(partitionCol));
            partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
            partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

            // Create partition range: [2024-01-01, 2024-02-01)
            PartitionKey lowerKey = PartitionKey.ofDate(LocalDate.parse("2024-01-01"));
            PartitionKey upperKey = PartitionKey.ofDate(LocalDate.parse("2024-02-01"));
            Range<PartitionKey> partitionRange = Range.closedOpen(lowerKey, upperKey);
            partitionInfo.addPartition(PARTITION_ID, false, partitionRange,
                    com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY, (short) 1);

            DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(partitionCol));

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
        } catch (com.starrocks.common.AnalysisException e) {
            throw new RuntimeException("Failed to create range partitioned table", e);
        }
    }

    private static LakeTable createRangePartitionedLakeTable(long tableId, String tableName, int bucketNum) {
        try {
            List<Column> columns = new ArrayList<>();
            // partition column
            Column dt = new Column("dt", DateType.DATE);
            dt.setIsKey(true);
            columns.add(dt);
            // target column for time_drift_constraint (DATE type, not necessarily a partition column)
            columns.add(new Column("dt2", DateType.DATE));
            columns.add(new Column("v2", IntegerType.BIGINT));

            RangePartitionInfo partitionInfo = new RangePartitionInfo(List.of(dt));
            partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
            partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

            // Create partition range: [2024-01-01, 2024-02-01)
            PartitionKey lowerKey = PartitionKey.ofDate(LocalDate.parse("2024-01-01"));
            PartitionKey upperKey = PartitionKey.ofDate(LocalDate.parse("2024-02-01"));
            Range<PartitionKey> partitionRange = Range.closedOpen(lowerKey, upperKey);
            partitionInfo.addPartition(PARTITION_ID, false, partitionRange,
                    com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY, (short) 1);

            DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(dt));

            LakeTable lakeTable = new LakeTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
            lakeTable.setTableProperty(new TableProperty(new HashMap<>()));
            return lakeTable;
        } catch (com.starrocks.common.AnalysisException e) {
            throw new RuntimeException("Failed to create range partitioned lake table", e);
        }
    }

    @Test
    public void testAlterTableCommentNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        db.registerTableUnlocked(table);

        // Test
        String newComment = "new table comment";
        AlterTableCommentClause clause = new AlterTableCommentClause(newComment, NodePosition.ZERO);
        metastore.alterTableComment(db, table, clause);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME);
        Assertions.assertNotNull(table);
        Assertions.assertEquals(newComment, table.getComment());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(newComment, replayInfo.getComment());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID, TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID);
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(newComment, replayed.getComment());
    }

    @Test
    public void testAlterTableCommentEditLogException() {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 1000, DB_NAME + "_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 1000, TABLE_NAME + "_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterTableProperties(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        AlterTableCommentClause clause = new AlterTableCommentClause("new comment", NodePosition.ZERO);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.alterTableComment(db, table, clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals("", table.getComment());
    }

    @Test
    public void testModifyTableDynamicPartitionNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 2000, DB_NAME + "_dynamic");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 2000, TABLE_NAME + "_dynamic", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put("dynamic_partition.enable", "true");
        properties.put("dynamic_partition.time_unit", "DAY");
        properties.put("dynamic_partition.start", "-3");
        properties.put("dynamic_partition.end", "3");
        properties.put("dynamic_partition.prefix", "p");
        properties.put("dynamic_partition.buckets", "3");

        metastore.modifyTableDynamicPartition(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_dynamic");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        com.starrocks.catalog.DynamicPartitionProperty dynamicPartitionProperty = tableProperty.getDynamicPartitionProperty();
        Assertions.assertNotNull(dynamicPartitionProperty);
        Assertions.assertTrue(dynamicPartitionProperty.isEnabled());
        Assertions.assertEquals("DAY", dynamicPartitionProperty.getTimeUnit());
        Assertions.assertEquals(-3, dynamicPartitionProperty.getStart());
        Assertions.assertEquals(3, dynamicPartitionProperty.getEnd());
        Assertions.assertEquals("p", dynamicPartitionProperty.getPrefix());
        Assertions.assertEquals(3, dynamicPartitionProperty.getBuckets());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DYNAMIC_PARTITION);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 2000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 2000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 2000, DB_NAME + "_dynamic");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 2000, TABLE_NAME + "_dynamic", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_DYNAMIC_PARTITION, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 2000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        com.starrocks.catalog.DynamicPartitionProperty replayedDynamicPartitionProperty =
                replayedProperty.getDynamicPartitionProperty();
        Assertions.assertNotNull(replayedDynamicPartitionProperty);
        Assertions.assertTrue(replayedDynamicPartitionProperty.isEnabled());
        Assertions.assertEquals("DAY", replayedDynamicPartitionProperty.getTimeUnit());
        Assertions.assertEquals(-3, replayedDynamicPartitionProperty.getStart());
        Assertions.assertEquals(3, replayedDynamicPartitionProperty.getEnd());
        Assertions.assertEquals("p", replayedDynamicPartitionProperty.getPrefix());
        Assertions.assertEquals(3, replayedDynamicPartitionProperty.getBuckets());
    }

    @Test
    public void testModifyTableDynamicPartitionEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 3000, DB_NAME + "_dynamic_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 3000, TABLE_NAME + "_dynamic_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDynamicPartition(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put("dynamic_partition.enable", "true");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableDynamicPartition(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertFalse(table.getTableProperty().getDynamicPartitionProperty().isEnabled());
    }

    @Test
    public void testAlterTablePropertiesPartitionLiveNumberNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 4000, DB_NAME + "_partition_live");
        metastore.unprotectCreateDb(db);
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID + 4000, TABLE_NAME + "_partition_live", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, "3");
        metastore.alterTableProperties(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_partition_live");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals(3, tableProperty.getPartitionTTLNumber());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 4000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 4000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 4000, DB_NAME + "_partition_live");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createRangePartitionedOlapTable(TABLE_ID + 4000, TABLE_NAME + "_partition_live", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 4000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals(3, replayedProperty.getPartitionTTLNumber());
    }

    @Test
    public void testAlterTablePropertiesStorageMediumNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 5000, DB_NAME + "_storage");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 5000, TABLE_NAME + "_storage", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        metastore.alterTableProperties(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_storage");
        Assertions.assertNotNull(table);
        String storageMedium = table.getStorageMedium();
        Assertions.assertNotNull(storageMedium);
        Assertions.assertEquals("SSD", storageMedium);

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 5000, DB_NAME + "_storage");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 5000, TABLE_NAME + "_storage", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 5000);
        Assertions.assertNotNull(replayed);
        String replayedMedium = replayed.getStorageMedium();
        Assertions.assertNotNull(replayedMedium);
        Assertions.assertEquals("SSD", replayedMedium);
    }

    @Test
    public void testAlterTablePropertiesEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 6000, DB_NAME + "_props_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 6000, TABLE_NAME + "_props_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterTableProperties(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.alterTableProperties(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals("HDD", table.getStorageMedium());
    }

    @Test
    public void testModifyTableDefaultReplicationNumNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 7000, DB_NAME + "_replication");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 7000, TABLE_NAME + "_replication", 3);
        db.registerTableUnlocked(table);

        // Initialize colocateTableIndex if null to avoid NullPointerException
        if (metastore.getColocateTableIndex() == null) {
            metastore.setColocateTableIndex(GlobalStateMgr.getCurrentState().getColocateTableIndex());
        }

        // Test - need to hold db write lock
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            metastore.modifyTableDefaultReplicationNum(db, table, properties);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_replication");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals((short) 1, tableProperty.getReplicationNum());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_REPLICATION_NUM);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 7000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 7000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 7000, DB_NAME + "_replication");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 7000, TABLE_NAME + "_replication", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_REPLICATION_NUM, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 7000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals((short) 1, replayedProperty.getReplicationNum());
    }

    @Test
    public void testModifyTableDefaultReplicationNumEditLogException() {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 8000, DB_NAME + "_replication_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 8000, TABLE_NAME + "_replication_exception", 3);
        db.registerTableUnlocked(table);

        // Initialize colocateTableIndex if null to avoid NullPointerException
        if (metastore.getColocateTableIndex() == null) {
            metastore.setColocateTableIndex(GlobalStateMgr.getCurrentState().getColocateTableIndex());
        }

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyReplicationNum(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test - need to hold db write lock
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        RuntimeException exception;
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
            exception = Assertions.assertThrows(RuntimeException.class, () -> {
                metastore.modifyTableDefaultReplicationNum(db, table, properties);
            });
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals((short) 3, table.getTableProperty().getReplicationNum());
    }

    @Test
    public void testModifyTableEnablePersistentIndexMetaNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9000, DB_NAME + "_persistent");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 9000, TABLE_NAME + "_persistent", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "true");
        metastore.modifyTableEnablePersistentIndexMeta(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_persistent");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertTrue(tableProperty.enablePersistentIndex());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 9000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 9000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 9000, DB_NAME + "_persistent");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 9000, TABLE_NAME + "_persistent", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 9000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertTrue(replayedProperty.enablePersistentIndex());
    }

    @Test
    public void testModifyTableEnablePersistentIndexMetaEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 10000, DB_NAME + "_persistent_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 10000, TABLE_NAME + "_persistent_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyEnablePersistentIndex(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "true");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableEnablePersistentIndexMeta(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertFalse(table.getTableProperty().enablePersistentIndex());
    }

    @Test
    public void testModifyFlatJsonMetaNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 11000, DB_NAME + "_flatjson");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 11000, TABLE_NAME + "_flatjson", 3);
        db.registerTableUnlocked(table);

        // Test - need to hold db write lock
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            FlatJsonConfig config = new FlatJsonConfig(true, 0.1, 0.8, 50);
            metastore.modifyFlatJsonMeta(db, table, config);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_flatjson");
        Assertions.assertNotNull(table);
        FlatJsonConfig actualConfig = table.getFlatJsonConfig();
        Assertions.assertNotNull(actualConfig);
        Assertions.assertTrue(actualConfig.getFlatJsonEnable());
        Assertions.assertEquals(0.1, actualConfig.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, actualConfig.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, actualConfig.getFlatJsonColumnMax());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_FLAT_JSON_CONFIG);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 11000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 11000, replayInfo.getTableId());
        // Verify properties are correctly serialized
        Map<String, String> replayProperties = replayInfo.getProperties();
        Assertions.assertNotNull(replayProperties);
        Assertions.assertTrue(replayProperties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertTrue(replayProperties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertTrue(replayProperties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertTrue(replayProperties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 11000, DB_NAME + "_flatjson");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 11000, TABLE_NAME + "_flatjson", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_FLAT_JSON_CONFIG, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 11000);
        Assertions.assertNotNull(replayed);
        FlatJsonConfig replayedConfig = replayed.getFlatJsonConfig();
        Assertions.assertNotNull(replayedConfig);
        Assertions.assertTrue(replayedConfig.getFlatJsonEnable());
        Assertions.assertEquals(0.1, replayedConfig.getFlatJsonNullFactor(), 0.001);
        Assertions.assertEquals(0.8, replayedConfig.getFlatJsonSparsityFactor(), 0.001);
        Assertions.assertEquals(50, replayedConfig.getFlatJsonColumnMax());
    }

    @Test
    public void testModifyFlatJsonMetaEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 12000, DB_NAME + "_flatjson_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 12000, TABLE_NAME + "_flatjson_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyFlatJsonConfig(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        FlatJsonConfig config = new FlatJsonConfig(true, 0.1, 0.8, 50);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyFlatJsonMeta(db, table, config);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertNull(table.getFlatJsonConfig());
    }

    @Test
    public void testModifyBinlogMetaNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 13000, DB_NAME + "_binlog");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 13000, TABLE_NAME + "_binlog", 3);
        db.registerTableUnlocked(table);

        // Test
        BinlogConfig config = new BinlogConfig(1, true, 86400, 1073741824);
        metastore.modifyBinlogMeta(db, table, config);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_binlog");
        Assertions.assertNotNull(table);
        BinlogConfig actualConfig = table.getCurBinlogConfig();
        Assertions.assertNotNull(actualConfig);
        Assertions.assertTrue(actualConfig.getBinlogEnable());
        Assertions.assertEquals(86400, actualConfig.getBinlogTtlSecond());
        Assertions.assertEquals(1073741824, actualConfig.getBinlogMaxSize());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_BINLOG_CONFIG);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 13000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 13000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 13000, DB_NAME + "_binlog");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 13000, TABLE_NAME + "_binlog", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_BINLOG_CONFIG, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 13000);
        Assertions.assertNotNull(replayed);
        BinlogConfig replayedConfig = replayed.getCurBinlogConfig();
        Assertions.assertNotNull(replayedConfig);
        Assertions.assertTrue(replayedConfig.getBinlogEnable());
        Assertions.assertEquals(86400, replayedConfig.getBinlogTtlSecond());
        Assertions.assertEquals(1073741824, replayedConfig.getBinlogMaxSize());
    }

    @Test
    public void testModifyBinlogMetaEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 14000, DB_NAME + "_binlog_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 14000, TABLE_NAME + "_binlog_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyBinlogConfig(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        BinlogConfig config = new BinlogConfig(1, true, 86400, 1073741824);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyBinlogMeta(db, table, config);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertNull(table.getCurBinlogConfig());
    }

    @Test
    public void testModifyTableConstraintNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 15000, DB_NAME + "_constraint");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 15000, TABLE_NAME + "_constraint", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        String catalogName = table.getCatalogName();
        String constraintStr = catalogName + "." + DB_NAME + "_constraint." + TABLE_NAME + "_constraint.v1";
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, constraintStr);
        metastore.modifyTableConstraint(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_constraint");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        List<UniqueConstraint> constraints = tableProperty.getUniqueConstraints();
        Assertions.assertNotNull(constraints);
        Assertions.assertEquals(1, constraints.size());
        Assertions.assertEquals("v1", constraints.get(0).getUniqueColumns().get(0).toString());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 15000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 15000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 15000, DB_NAME + "_constraint");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 15000, TABLE_NAME + "_constraint", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 15000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        List<UniqueConstraint> replayedConstraints = replayedProperty.getUniqueConstraints();
        Assertions.assertNotNull(replayedConstraints);
        Assertions.assertEquals(1, replayedConstraints.size());
        Assertions.assertEquals("v1", replayedConstraints.get(0).getUniqueColumns().get(0).toString());
    }

    @Test
    public void testModifyTableConstraintEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 16000, DB_NAME + "_constraint_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 16000, TABLE_NAME + "_constraint_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyConstraint(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        String catalogName = table.getCatalogName();
        String constraintStr = catalogName + "." + DB_NAME + "_constraint_exception." + TABLE_NAME + "_constraint_exception.v1";
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, constraintStr);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableConstraint(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertNull(table.getTableProperty().getUniqueConstraints());
    }

    @Test
    public void testModifyTableWriteQuorumNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 17000, DB_NAME + "_writequorum");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 17000, TABLE_NAME + "_writequorum", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM, "ALL");
        metastore.modifyTableWriteQuorum(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_writequorum");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals(com.starrocks.thrift.TWriteQuorumType.ALL, tableProperty.writeQuorum());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_WRITE_QUORUM);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 17000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 17000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 17000, DB_NAME + "_writequorum");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 17000, TABLE_NAME + "_writequorum", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_WRITE_QUORUM, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 17000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals(com.starrocks.thrift.TWriteQuorumType.ALL, replayedProperty.writeQuorum());
    }

    @Test
    public void testModifyTableWriteQuorumEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 18000, DB_NAME + "_writequorum_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 18000, TABLE_NAME + "_writequorum_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyWriteQuorum(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM, "ALL");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableWriteQuorum(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals(TWriteQuorumType.MAJORITY, table.getTableProperty().writeQuorum());
    }

    @Test
    public void testModifyTableReplicatedStorageNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 19000, DB_NAME + "_replicated");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 19000, TABLE_NAME + "_replicated", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE, "true");
        metastore.modifyTableReplicatedStorage(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_replicated");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertTrue(tableProperty.enableReplicatedStorage());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_REPLICATED_STORAGE);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 19000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 19000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 19000, DB_NAME + "_replicated");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 19000, TABLE_NAME + "_replicated", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_REPLICATED_STORAGE, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 19000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertTrue(replayedProperty.enableReplicatedStorage());
    }

    @Test
    public void testModifyTableReplicatedStorageEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 20000, DB_NAME + "_replicated_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 20000, TABLE_NAME + "_replicated_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyReplicatedStorage(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE, "true");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableReplicatedStorage(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertFalse(table.getTableProperty().enableReplicatedStorage());
    }

    @Test
    public void testModifyTableAutomaticBucketSizeNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 21000, DB_NAME + "_bucketsize");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 21000, TABLE_NAME + "_bucketsize", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE, "1073741824");
        metastore.modifyTableAutomaticBucketSize(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_bucketsize");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals(1073741824L, tableProperty.getBucketSize());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_BUCKET_SIZE);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 21000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 21000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 21000, DB_NAME + "_bucketsize");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 21000, TABLE_NAME + "_bucketsize", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_BUCKET_SIZE, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 21000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals(1073741824L, replayedProperty.getBucketSize());
    }

    @Test
    public void testModifyTableAutomaticBucketSizeEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 22000, DB_NAME + "_bucketsize_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 22000, TABLE_NAME + "_bucketsize_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyBucketSize(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        long originalBucketSize = table.getTableProperty().getBucketSize();
        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE, "1073741824");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableAutomaticBucketSize(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals(originalBucketSize, table.getTableProperty().getBucketSize());
    }

    @Test
    public void testModifyTableMutableBucketNumNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 23000, DB_NAME + "_mutablebucket");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 23000, TABLE_NAME + "_mutablebucket", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM, "5");
        metastore.modifyTableMutableBucketNum(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_mutablebucket");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals(5L, tableProperty.getMutableBucketNum());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 23000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 23000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 23000, DB_NAME + "_mutablebucket");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 23000, TABLE_NAME + "_mutablebucket", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 23000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals(5L, replayedProperty.getMutableBucketNum());
    }

    @Test
    public void testModifyTableMutableBucketNumEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 24000, DB_NAME + "_mutablebucket_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 24000, TABLE_NAME + "_mutablebucket_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyMutableBucketNum(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        long originalMutableBucketNum = table.getTableProperty().getMutableBucketNum();
        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM, "5");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableMutableBucketNum(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals(originalMutableBucketNum, table.getTableProperty().getMutableBucketNum());
    }

    @Test
    public void testModifyTableEnableLoadProfileNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 25000, DB_NAME + "_loadprofile");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 25000, TABLE_NAME + "_loadprofile", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE, "true");
        metastore.modifyTableEnableLoadProfile(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_loadprofile");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertTrue(tableProperty.enableLoadProfile());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 25000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 25000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 25000, DB_NAME + "_loadprofile");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 25000, TABLE_NAME + "_loadprofile", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 25000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertTrue(replayedProperty.enableLoadProfile());
    }

    @Test
    public void testModifyTableEnableLoadProfileEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 26000, DB_NAME + "_loadprofile_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 26000, TABLE_NAME + "_loadprofile_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyEnableLoadProfile(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE, "true");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableEnableLoadProfile(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertFalse(table.getTableProperty().enableLoadProfile());
    }

    @Test
    public void testModifyTableBaseCompactionForbiddenTimeRangesNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 27000, DB_NAME + "_compaction");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 27000, TABLE_NAME + "_compaction", 3);
        db.registerTableUnlocked(table);

        // Test - need to hold db write lock
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES, "* 0-5 * * *");
            metastore.modifyTableBaseCompactionForbiddenTimeRanges(db, table, properties);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_compaction");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals("* 0-5 * * *", tableProperty.getBaseCompactionForbiddenTimeRanges());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 27000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 27000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 27000, DB_NAME + "_compaction");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 27000, TABLE_NAME + "_compaction", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 27000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals("* 0-5 * * *", replayedProperty.getBaseCompactionForbiddenTimeRanges());
    }

    @Test
    public void testModifyTableBaseCompactionForbiddenTimeRangesEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 28000, DB_NAME + "_compaction_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 28000, TABLE_NAME + "_compaction_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyBaseCompactionForbiddenTimeRanges(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BASE_COMPACTION_FORBIDDEN_TIME_RANGES, "* 0-5 * * *");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTableBaseCompactionForbiddenTimeRanges(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals("", table.getTableProperty().getBaseCompactionForbiddenTimeRanges());
    }

    @Test
    public void testModifyTablePrimaryIndexCacheExpireSecNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 29000, DB_NAME + "_cacheexpire");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 29000, TABLE_NAME + "_cacheexpire", 3);
        db.registerTableUnlocked(table);

        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC, "3600");
        metastore.modifyTablePrimaryIndexCacheExpireSec(db, table, properties);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_cacheexpire");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals(3600, tableProperty.primaryIndexCacheExpireSec());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 29000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 29000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 29000, DB_NAME + "_cacheexpire");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 29000, TABLE_NAME + "_cacheexpire", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 29000);
        Assertions.assertNotNull(replayed);
        TableProperty replayedProperty = replayed.getTableProperty();
        Assertions.assertNotNull(replayedProperty);
        Assertions.assertEquals(3600, replayedProperty.primaryIndexCacheExpireSec());
    }

    @Test
    public void testModifyTablePrimaryIndexCacheExpireSecEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 30000, DB_NAME + "_cacheexpire_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 30000, TABLE_NAME + "_cacheexpire_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyPrimaryIndexCacheExpireSec(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        int originalExpireSec = table.getTableProperty().primaryIndexCacheExpireSec();
        // Test
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC, "3600");
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.modifyTablePrimaryIndexCacheExpireSec(db, table, properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals(originalExpireSec, table.getTableProperty().primaryIndexCacheExpireSec());
    }

    @Test
    public void testSetHasForbiddenGlobalDictNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 31000, DB_NAME + "_globaldict");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 31000, TABLE_NAME + "_globaldict", 3);
        db.registerTableUnlocked(table);

        // Test - use db.getFullName() to get the full database name
        // Note: getDb expects the name as stored in fullNameToDb, which is db.getFullName()
        // But setHasForbiddenGlobalDict uses getDb(dbName), so we need to ensure the name matches
        String dbFullName = db.getFullName();
        metastore.setHasForbiddenGlobalDict(dbFullName, TABLE_NAME + "_globaldict", true);

        // Verify leader state
        table = (OlapTable) db.getTable(TABLE_NAME + "_globaldict");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.hasForbiddenGlobalDict());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID + 31000, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID + 31000, replayInfo.getTableId());

        // Create follower metastore and replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID + 31000, DB_NAME + "_globaldict");
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createHashOlapTable(TABLE_ID + 31000, TABLE_NAME + "_globaldict", 3);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayModifyTableProperty(OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT, replayInfo);

        // Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(TABLE_ID + 31000);
        Assertions.assertNotNull(replayed);
        Assertions.assertTrue(replayed.hasForbiddenGlobalDict());
    }

    @Test
    public void testSetHasForbiddenGlobalDictEditLogException() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 32000, DB_NAME + "_globaldict_exception");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 32000, TABLE_NAME + "_globaldict_exception", 3);
        db.registerTableUnlocked(table);

        // Mock EditLog - but note that setHasForbiddenGlobalDict checks DB existence first
        // So if DB doesn't exist, it throws DdlException before reaching EditLog
        // We need to ensure DB exists, then mock EditLog to throw
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logSetHasForbiddenGlobalDict(any(ModifyTablePropertyOperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Test - DB exists, so it should reach EditLog and throw RuntimeException
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.setHasForbiddenGlobalDict(db.getFullName(), TABLE_NAME + "_globaldict_exception", true);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertFalse(table.hasForbiddenGlobalDict());
    }

    @Test
    public void testAlterTablePropertiesTableQueryTimeoutNormalCase() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 6000, DB_NAME + "_table_query_timeout");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 6000, TABLE_NAME + "_table_query_timeout", 3);
        db.registerTableUnlocked(table);

        // Test - Set table_query_timeout via alterTableProperties
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "120");
        metastore.alterTableProperties(db, table, properties);

        // Verify table_query_timeout is set correctly
        table = (OlapTable) db.getTable(TABLE_NAME + "_table_query_timeout");
        Assertions.assertNotNull(table);
        TableProperty tableProperty = table.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        Assertions.assertEquals(120, tableProperty.getTableQueryTimeout());
    }

    @Test
    public void testAlterTablePropertiesTableQueryTimeoutWithErrorCases() throws Exception {
        // Setup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 7000, DB_NAME + "_table_query_timeout_error");
        metastore.unprotectCreateDb(db);
        OlapTable table = createHashOlapTable(TABLE_ID + 7000, TABLE_NAME + "_table_query_timeout_error", 3);
        db.registerTableUnlocked(table);

        // Test 1: Invalid timeout value (0) - should throw DdlException
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "0");
        try {
            metastore.alterTableProperties(db, table, properties1);
            Assertions.fail("Should throw DdlException for invalid timeout value");
        } catch (com.starrocks.common.DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("must be greater than 0"));
        }

        // Test 2: Invalid timeout value (negative) - should throw DdlException
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "-10");
        try {
            metastore.alterTableProperties(db, table, properties2);
            Assertions.fail("Should throw DdlException for negative timeout value");
        } catch (com.starrocks.common.DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("must be greater than 0"));
        }

        // Test 3: Invalid format (not a number) - should throw DdlException
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "invalid");
        try {
            metastore.alterTableProperties(db, table, properties3);
            Assertions.fail("Should throw DdlException for invalid number format");
        } catch (com.starrocks.common.DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("must be a valid integer"));
        }

        // Test 4: Valid large timeout value (greater than cluster timeout) - should succeed
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "600");
        metastore.alterTableProperties(db, table, properties4);
        table = (OlapTable) db.getTable(TABLE_NAME + "_table_query_timeout_error");
        Assertions.assertNotNull(table);
        Assertions.assertEquals(600, table.getTableProperty().getTableQueryTimeout());

        // Test 5: Update timeout value - should succeed
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "180");
        metastore.alterTableProperties(db, table, properties5);
        table = (OlapTable) db.getTable(TABLE_NAME + "_table_query_timeout_error");
        Assertions.assertEquals(180, table.getTableProperty().getTableQueryTimeout());
    }

    @Test
    public void testTablePropertyBuildTableQueryTimeout() {
        // Test TableProperty.buildTableQueryTimeout() boundary cases
        TableProperty tableProperty = new TableProperty(new HashMap<>());
        
        // Test 1: Valid timeout value
        tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "120");
        tableProperty.buildTableQueryTimeout();
        Assertions.assertEquals(120, tableProperty.getTableQueryTimeout());
        
        // Test 2: Timeout value is 0 - should fall back to default (-1) and remove invalid property
        tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "0");
        tableProperty.buildTableQueryTimeout();
        Assertions.assertEquals(-1, tableProperty.getTableQueryTimeout());
        Assertions.assertFalse(tableProperty.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
        
        // Test 3: Timeout value is negative (but not -1) - should fall back to default (-1) and remove invalid property
        tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "-10");
        tableProperty.buildTableQueryTimeout();
        Assertions.assertEquals(-1, tableProperty.getTableQueryTimeout());
        Assertions.assertFalse(tableProperty.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
        
        // Test 4: Invalid format (not a number) - should fall back to default (-1) and remove invalid property
        tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "invalid");
        tableProperty.buildTableQueryTimeout();
        Assertions.assertEquals(-1, tableProperty.getTableQueryTimeout());
        Assertions.assertFalse(tableProperty.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));

        // Test 5: -1 means unset/reset to default (should not throw)
        tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "-1");
        tableProperty.buildTableQueryTimeout();
        Assertions.assertEquals(-1, tableProperty.getTableQueryTimeout());
        Assertions.assertFalse(tableProperty.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
        
        // Test 6: Property not set - should set to -1
        tableProperty.getProperties().remove(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT);
        tableProperty.buildTableQueryTimeout();
        Assertions.assertEquals(-1, tableProperty.getTableQueryTimeout());
    }

    @Test
    public void testAlterTablePropertiesWithTimeDriftAndFastSchemaEvolutionV2AndTableQueryTimeout() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 8000, DB_NAME + "_table_query_timeout_mix");
        metastore.unprotectCreateDb(db);

        LakeTable table = createRangePartitionedLakeTable(TABLE_ID + 8000, TABLE_NAME + "_table_query_timeout_mix", 3);
        db.registerTableUnlocked(table);

        String spec = "dt2 between days_sub(dt, 1) and days_add(dt, 1)";
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "120");
        properties.put(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, spec);
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_COLLECT_ON_FIRST_LOAD, "false");

        metastore.alterTableProperties(db, table, properties);

        LakeTable altered = (LakeTable) db.getTable(table.getName());
        Assertions.assertNotNull(altered);
        // Verify table_query_timeout via OlapTable.getTableQueryTimeout() to ensure lambda in applier was executed
        Assertions.assertEquals(120, altered.getTableQueryTimeout());
        Assertions.assertEquals(120, altered.getTableProperty().getTableQueryTimeout());
        Assertions.assertEquals(spec, altered.getTableProperty().getTimeDriftConstraintSpec());
        Assertions.assertTrue(altered.isFastSchemaEvolutionV2());
        Assertions.assertFalse(altered.enableStatisticCollectOnFirstLoad());

        // Cover OlapTable.getCommonProperties export for compaction strategy and table_query_timeout
        altered.getTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY, "real_time");
        altered.getTableProperty().buildCompactionStrategy();
        Map<String, String> exported = altered.getProperties();
        // compactionStrategyToString returns "REAL_TIME" (uppercase) as defined in TableProperty.REAL_TIME_COMPACTION_STRATEGY
        Assertions.assertEquals("REAL_TIME", exported.get(PropertyAnalyzer.PROPERTIES_COMPACTION_STRATEGY));
        Assertions.assertEquals("120", exported.get(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
    }

    /**
     * Test alterTableProperties with only table_query_timeout property.
     * Covers LocalMetastore.alterTableQueryTimeout method (lines 3888-3903).
     */
    @Test
    public void testAlterTableQueryTimeoutOnly() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9100, DB_NAME + "_timeout_only");
        metastore.unprotectCreateDb(db);

        OlapTable table = createHashOlapTable(TABLE_ID + 9100, TABLE_NAME + "_timeout_only", 3);
        db.registerTableUnlocked(table);

        // Verify initial state
        Assertions.assertEquals(-1, table.getTableQueryTimeout());

        // Set table_query_timeout only
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "180");
        metastore.alterTableProperties(db, table, properties);

        // Verify via OlapTable.getTableQueryTimeout() - this calls tableProperty.getTableQueryTimeout()
        // The applier in alterTableQueryTimeout calls:
        // table.setTableQueryTimeout(180) -> which updates TableProperty and builds table_query_timeout.
        OlapTable altered = (OlapTable) db.getTable(TABLE_NAME + "_timeout_only");
        Assertions.assertNotNull(altered);
        Assertions.assertEquals(180, altered.getTableQueryTimeout());
    }

    @Test
    public void testAlterTableQueryTimeoutResetToDefault() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9103, DB_NAME + "_timeout_reset");
        metastore.unprotectCreateDb(db);

        OlapTable table = createHashOlapTable(TABLE_ID + 9103, TABLE_NAME + "_timeout_reset", 3);
        db.registerTableUnlocked(table);

        // Set to a positive value first.
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "120");
        metastore.alterTableProperties(db, table, properties);
        Assertions.assertEquals(120, table.getTableQueryTimeout());
        Assertions.assertEquals("120", table.getProperties().get(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));

        // Reset to default by setting -1.
        Map<String, String> resetProps = new HashMap<>();
        resetProps.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "-1");
        metastore.alterTableProperties(db, table, resetProps);
        Assertions.assertEquals(-1, table.getTableQueryTimeout());
        Assertions.assertFalse(table.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
    }

    /**
     * Test alterTableProperties with only cloud_native_fast_schema_evolution_v2 property.
     * Covers LocalMetastore.alterCloudNativeFastSchemaEvolutionV2 method (lines 3879-3886).
     */
    @Test
    public void testAlterCloudNativeFastSchemaEvolutionV2Only() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9101, DB_NAME + "_fast_schema_only");
        metastore.unprotectCreateDb(db);

        LakeTable table = createRangePartitionedLakeTable(TABLE_ID + 9101, TABLE_NAME + "_fast_schema_only", 3);
        db.registerTableUnlocked(table);

        // Verify initial state (default is true for LakeTable)
        // Set it to false first, then to true to verify the change
        table.setFastSchemaEvolutionV2(false);
        Assertions.assertFalse(table.isFastSchemaEvolutionV2());

        // Set cloud_native_fast_schema_evolution_v2 only
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, "true");
        metastore.alterTableProperties(db, table, properties);

        // Verify via LakeTable.isFastSchemaEvolutionV2()
        // The applier in alterCloudNativeFastSchemaEvolutionV2 calls:
        // ((LakeTable) table).setFastSchemaEvolutionV2(value)
        LakeTable altered = (LakeTable) db.getTable(TABLE_NAME + "_fast_schema_only");
        Assertions.assertNotNull(altered);
        Assertions.assertTrue(altered.isFastSchemaEvolutionV2());
    }

    /**
     * Test alterTableProperties with only time_drift_constraint property.
     * Covers LocalMetastore.alterTimeDriftConstraint method (line 3932).
     */
    @Test
    public void testAlterTimeDriftConstraintOnly() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9102, DB_NAME + "_time_drift_only");
        metastore.unprotectCreateDb(db);

        LakeTable table = createRangePartitionedLakeTable(TABLE_ID + 9102, TABLE_NAME + "_time_drift_only", 3);
        db.registerTableUnlocked(table);

        // Set time_drift_constraint only
        String spec = "dt2 between days_sub(dt, 1) and days_add(dt, 1)";
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, spec);
        metastore.alterTableProperties(db, table, properties);

        // Verify via TableProperty.getTimeDriftConstraintSpec()
        LakeTable altered = (LakeTable) db.getTable(TABLE_NAME + "_time_drift_only");
        Assertions.assertNotNull(altered);
        Assertions.assertEquals(spec, altered.getTableProperty().getTimeDriftConstraintSpec());
    }

    @Test
    public void testLocalMetastoreAlterHelpersAndOlapTableExport() throws Exception {
        // This test is intentionally minimal and deterministic:
        // - directly invokes LocalMetastore's private helper methods via reflection, then runs the captured appliers,

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();

        LakeTable lakeTable = createRangePartitionedLakeTable(TABLE_ID + 9200, TABLE_NAME + "_helper_lake", 3);
        lakeTable.setFastSchemaEvolutionV2(false);
        Assertions.assertFalse(lakeTable.isFastSchemaEvolutionV2());

        List<Runnable> appliers = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, "true");

        java.lang.reflect.Method alterFastSchema = LocalMetastore.class.getDeclaredMethod(
                "alterCloudNativeFastSchemaEvolutionV2", OlapTable.class, Map.class, List.class);
        alterFastSchema.setAccessible(true);
        alterFastSchema.invoke(metastore, lakeTable, props, appliers);
        Assertions.assertEquals(1, appliers.size());
        appliers.get(0).run();
        Assertions.assertTrue(lakeTable.isFastSchemaEvolutionV2());

        // Cover alterTableQueryTimeout helper + its lambda body.
        appliers.clear();
        props.clear();
        props.put(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT, "120");
        java.lang.reflect.Method alterTableTimeout = LocalMetastore.class.getDeclaredMethod(
                "alterTableQueryTimeout", OlapTable.class, Map.class, List.class);
        alterTableTimeout.setAccessible(true);
        OlapTable olapTable = createHashOlapTable(TABLE_ID + 9201, TABLE_NAME + "_helper_olap", 3);
        alterTableTimeout.invoke(metastore, olapTable, props, appliers);
        Assertions.assertEquals(1, appliers.size());
        appliers.get(0).run();
        Assertions.assertEquals(120, olapTable.getTableQueryTimeout());

        // Cover OlapTable setters/getters + getProperties export path.
        olapTable.setEnableStatisticCollectOnFirstLoad(false);
        Assertions.assertFalse(olapTable.enableStatisticCollectOnFirstLoad());
        olapTable.setTableQueryTimeout(120);
        Assertions.assertEquals(120, olapTable.getTableQueryTimeout());
        Map<String, String> exported = olapTable.getProperties();
        Assertions.assertEquals("120", exported.get(PropertyAnalyzer.PROPERTIES_TABLE_QUERY_TIMEOUT));
    }

    @Test
    public void testAlterDataCacheEnableHelper() throws Exception {
        // Tests the alterDataCacheEnable helper method via reflection (same pattern as above)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9300, DB_NAME + "_datacache_helper");
        metastore.unprotectCreateDb(db);

        LakeTable lakeTable = createRangePartitionedLakeTable(TABLE_ID + 9300, TABLE_NAME + "_datacache_helper", 3);
        lakeTable.getTableProperty().setStorageInfo(
                new StorageInfo(FilePathInfo.newBuilder().build(), FileCacheInfo.newBuilder().build()));
        db.registerTableUnlocked(lakeTable);

        // Mock StarOSAgent to no-op
        new MockUp<com.starrocks.lake.StarOSAgent>() {
            @Mock
            public void updateShardGroup(List<Partition> partitionsList, boolean enableCache) {
                // no-op
            }
        };

        List<Runnable> appliers = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        java.lang.reflect.Method alterDataCacheEnable = LocalMetastore.class.getDeclaredMethod(
                "alterDataCacheEnable", Database.class, OlapTable.class, Map.class, List.class);
        alterDataCacheEnable.setAccessible(true);
        alterDataCacheEnable.invoke(metastore, db, lakeTable, props, appliers);
        Assertions.assertEquals(1, appliers.size());
        appliers.get(0).run();

        // Verify the property has been set on the table
        Assertions.assertTrue(lakeTable.getTableProperty().getStorageInfo().isEnableDataCache());
    }

    @Test
    public void testAlterDataCacheEnableOnNonCloudNativeTableFails() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID + 9400, DB_NAME + "_datacache_non_cloud");
        metastore.unprotectCreateDb(db);

        OlapTable olapTable = createHashOlapTable(TABLE_ID + 9400, TABLE_NAME + "_datacache_non_cloud", 3);
        db.registerTableUnlocked(olapTable);

        List<Runnable> appliers = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");

        java.lang.reflect.Method alterDataCacheEnable = LocalMetastore.class.getDeclaredMethod(
                "alterDataCacheEnable", Database.class, OlapTable.class, Map.class, List.class);
        alterDataCacheEnable.setAccessible(true);

        try {
            alterDataCacheEnable.invoke(metastore, db, olapTable, props, appliers);
            Assertions.fail("Should throw exception for non-cloud-native table");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assertions.assertTrue(e.getCause() instanceof com.starrocks.common.DdlException);
            Assertions.assertTrue(e.getCause().getMessage().contains("datacache.enable"));
        }
    }

    @Test
    public void testOlapTableSetDataCacheEnable() {
        // OlapTable.setDataCacheEnable with null tableProperty
        OlapTable table = createHashOlapTable(TABLE_ID + 9500, TABLE_NAME + "_datacache_set", 3);
        table.setTableProperty(null);
        table.setDataCacheEnable(true);
        Assertions.assertNotNull(table.getTableProperty());
    }

    @Test
    public void testTablePropertyBuildDataCacheEnable() {
        // Test buildDataCacheEnable when storageInfo is null (should log warning but not throw)
        Map<String, String> propMap = new HashMap<>();
        propMap.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        TableProperty tp = new TableProperty(propMap);
        // storageInfo is null by default
        tp.buildDataCacheEnable(); // should not throw, just log warning

        // Test buildDataCacheEnable when storageInfo is present
        LakeTable lakeTable = createRangePartitionedLakeTable(TABLE_ID + 9600, TABLE_NAME + "_build_cache", 3);
        TableProperty lakeProperty = lakeTable.getTableProperty();
        lakeProperty.setStorageInfo(
                new StorageInfo(FilePathInfo.newBuilder().build(), FileCacheInfo.newBuilder().build()));
        lakeProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        lakeProperty.buildDataCacheEnable();
        Assertions.assertTrue(lakeProperty.getStorageInfo().isEnableDataCache());

        lakeProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "false");
        lakeProperty.buildDataCacheEnable();
        Assertions.assertFalse(lakeProperty.getStorageInfo().isEnableDataCache());
    }

    @Test
    public void testTablePropertyBuildDataCacheEnableNotSet() {
        // Test buildDataCacheEnable when property is not set
        Map<String, String> propMap = new HashMap<>();
        TableProperty tp = new TableProperty(propMap);
        tp.buildDataCacheEnable(); // should be a no-op, no exception
    }

}
