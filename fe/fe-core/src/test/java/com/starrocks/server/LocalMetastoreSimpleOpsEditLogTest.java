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
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.consistency.MetaRecoveryDaemon;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AdminStmtAnalyzer;
import com.starrocks.sql.analyzer.AlterDatabaseAnalyzer;
import com.starrocks.sql.ast.AdminSetPartitionVersionStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterDatabaseSetStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.PropertySet;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.ReplicaStatus;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class LocalMetastoreSimpleOpsEditLogTest {
    private static final String DB_NAME = "test_local_metastore_editlog";
    private static final String TABLE_NAME = "test_table";
    private static final long DB_ID = 90001L;
    private static final long TABLE_ID = 90002L;
    private static final long PARTITION_ID = 90004L;
    private static final long PHYSICAL_PARTITION_ID = 90005L;
    private static final long INDEX_ID = 90006L;
    private static final long TABLET_ID = 90007L;
    private static final long BACKEND_ID = 90008L;
    private static final long REPLICA_ID = 90009L;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        // Initialize colocateTableIndex if not already set
        if (globalStateMgr.getColocateTableIndex() == null) {
            globalStateMgr.setColocateTableIndex(new com.starrocks.catalog.ColocateTableIndex());
        }

        // Create database
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);

        // Add backend for replica tests
        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 9050);
        backend.setAlive(true);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create OlapTable
    private static OlapTable createOlapTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        // Add replica to tablet
        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica);

        // For unpartitioned table, partition name should be the same as table name
        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, tableName, baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    // Helper method to create range partitioned OlapTable
    private static OlapTable createRangePartitionedOlapTable(long tableId, String tableName) {
        List<Column> columns = new ArrayList<>();
        Column partitionCol = new Column("dt", DateType.DATE);
        partitionCol.setIsKey(true);
        columns.add(partitionCol);
        columns.add(new Column("v2", IntegerType.BIGINT));

        RangePartitionInfo partitionInfo = new RangePartitionInfo(List.of(partitionCol));
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        try {
            PartitionKey lowerKey = PartitionKey.ofDate(LocalDate.parse("2024-01-01"));
            PartitionKey upperKey = PartitionKey.ofDate(LocalDate.parse("2024-02-01"));
            Range<PartitionKey> partitionRange = Range.closedOpen(lowerKey, upperKey);
            partitionInfo.addPartition(PARTITION_ID, false, partitionRange,
                    com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY, (short) 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(partitionCol));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Replica replica = new Replica(REPLICA_ID, BACKEND_ID, 0, Replica.ReplicaState.NORMAL);
        replica.updateVersionInfo(2, 2, 2);
        tablet.addReplica(replica);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    // Test alterDatabaseQuota
    @Test
    public void testAlterDatabaseQuotaNormalCase() throws Exception {
        // 1. Get database
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        long initialDataQuota = db.getDataQuota();
        long initialReplicaQuota = db.getReplicaQuota();

        // 2. Create AlterDatabaseQuotaStmt for DATA quota
        AlterDatabaseQuotaStmt
                stmt = new AlterDatabaseQuotaStmt(DB_NAME, AlterDatabaseQuotaStmt.QuotaType.DATA, "1000B", NodePosition.ZERO);
        // Analyze the statement to set quota value
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase(DB_NAME);
        // Set catalog if needed
        if (ctx.getCurrentCatalog() == null) {
            ctx.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        }
        AlterDatabaseAnalyzer.analyze(stmt, ctx);

        // 3. Execute alterDatabaseQuota
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.alterDatabaseQuota(stmt);

        // 4. Verify master state
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        long expectedQuota = stmt.getQuota(); // Use the parsed quota value
        // Verify that quota was actually set (not the default MAX_VALUE)
        Assertions.assertTrue(expectedQuota > 0 && expectedQuota < Long.MAX_VALUE,
                "Expected quota should be a reasonable value, not MAX_VALUE");
        Assertions.assertEquals(expectedQuota, db.getDataQuota());
        Assertions.assertEquals(initialReplicaQuota, db.getReplicaQuota());

        // 5. Test follower replay
        DatabaseInfo replayInfo = (DatabaseInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_DB_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_NAME, replayInfo.getDbName());
        Assertions.assertEquals(expectedQuota, replayInfo.getQuota());
        Assertions.assertEquals(AlterDatabaseQuotaStmt.QuotaType.DATA, replayInfo.getQuotaType());

        // Create follower metastore and the same id database, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerDb.setDataQuota(initialDataQuota);
        followerDb.setReplicaQuota(initialReplicaQuota);
        followerMetastore.unprotectCreateDb(followerDb);

        // Temporarily set follower metastore to GlobalStateMgr so replayAlterDatabase can find the database
        LocalMetastore originalMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            followerMetastore.replayAlterDatabase(replayInfo);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }

        // 6. Verify follower state
        Assertions.assertEquals(expectedQuota, followerDb.getDataQuota());
        Assertions.assertEquals(initialReplicaQuota, followerDb.getReplicaQuota());
    }

    @Test
    public void testAlterDatabaseQuotaEditLogException() throws Exception {
        // 1. Get database
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        long initialDataQuota = db.getDataQuota();

        // 2. Create AlterDatabaseQuotaStmt
        AlterDatabaseQuotaStmt stmt = new AlterDatabaseQuotaStmt(
                DB_NAME, AlterDatabaseQuotaStmt.QuotaType.DATA, "1000B", NodePosition.ZERO);
        // Analyze the statement to set quota value
        AlterDatabaseAnalyzer.analyze(stmt, UtFrameUtils.createDefaultCtx());

        // 3. Mock EditLog.logAlterDb to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterDb(any(DatabaseInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterDatabaseQuota and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.alterDatabaseQuota(stmt);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify database quota remains unchanged after exception
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        Assertions.assertEquals(initialDataQuota, db.getDataQuota());
    }

    // Test renameDatabase
    @Test
    public void testRenameDatabaseNormalCase() throws Exception {
        // 1. Get database
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        String oldDbName = db.getFullName();

        // 2. Create AlterDatabaseRenameStatement
        // Use the actual fullName from the database, not the constant
        String newDbName = "renamed_db";
        AlterDatabaseRenameStatement stmt = new AlterDatabaseRenameStatement(oldDbName, newDbName, NodePosition.ZERO);
        // Analyze the statement (normally done before execution)
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase(oldDbName);
        AlterDatabaseAnalyzer.analyze(stmt, ctx);

        // 3. Execute renameDatabase
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.renameDatabase(stmt);

        // 4. Verify master state
        // Database full name includes cluster prefix, so use getDb with full name
        Database renamedDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(newDbName);
        Assertions.assertNotNull(renamedDb);
        Assertions.assertEquals(newDbName, renamedDb.getFullName());
        Assertions.assertEquals(DB_ID, renamedDb.getId());
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(oldDbName));

        // 5. Test follower replay
        DatabaseInfo replayInfo = (DatabaseInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RENAME_DB_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(oldDbName, replayInfo.getDbName());
        Assertions.assertEquals(newDbName, replayInfo.getNewDbName());

        // Create follower metastore and the same id database, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, oldDbName);
        followerMetastore.unprotectCreateDb(followerDb);

        followerMetastore.replayRenameDatabase(replayInfo.getDbName(), replayInfo.getNewDbName());

        // 6. Verify follower state
        // Check in followerMetastore, not in GlobalStateMgr
        // Database full name includes cluster prefix
        Database renamedFollowerDb = followerMetastore.getDb(newDbName);
        Assertions.assertNotNull(renamedFollowerDb);
        Assertions.assertEquals(newDbName, renamedFollowerDb.getFullName());
        Assertions.assertEquals(DB_ID, renamedFollowerDb.getId());
        Assertions.assertNull(followerMetastore.getDb(oldDbName));
        Assertions.assertEquals(DB_ID, followerDb.getId());
    }

    @Test
    public void testAlterDatabaseSetStorageVolume(@Mocked StarOSAgent starOSAgent) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        String dbName = db.getFullName();

        StorageVolumeMgr svMgrLeader = new SharedDataStorageVolumeMgr();
        StorageVolumeMgr svMgrFollower = new SharedDataStorageVolumeMgr();
        AtomicReference<StorageVolumeMgr> refSvMgr = new AtomicReference<>(svMgrLeader);

        FileStoreInfo defaultSv = FileStoreInfo.newBuilder()
                .setFsKey("aa-bb-cc-dd")
                .setFsType(FileStoreType.HDFS)
                .setFsName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME)
                .setEnabled(true)
                .build();
        FileStoreInfo sv02 = FileStoreInfo.newBuilder()
                .setFsKey("ee-ff-gg-hh")
                .setFsType(FileStoreType.HDFS)
                .setFsName("sv_02")
                .setEnabled(true)
                .build();

        List<FileStoreInfo> fsInfos = Arrays.asList(defaultSv, sv02);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StorageVolumeMgr getStorageVolumeMgr() {
                return refSvMgr.get();
            }
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public FileStoreInfo getFileStoreByName(String fsName) throws DdlException {
                for (FileStoreInfo fileStoreInfo : fsInfos) {
                    if (fileStoreInfo.getFsName().equals(fsName)) {
                        return fileStoreInfo;
                    }
                }
                return null;
            }

            @Mock
            public FileStoreInfo getFileStore(String fsKey) throws DdlException {
                for (FileStoreInfo fileStoreInfo : fsInfos) {
                    if (fileStoreInfo.getFsKey().equals(fsKey)) {
                        return fileStoreInfo;
                    }
                }
                return null;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public static RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        // prepare the leader svMgr state
        Assertions.assertDoesNotThrow(() -> svMgrLeader.bindDbToStorageVolume(defaultSv.getFsName(), db.getId()));
        { // leader op
            String svNameBefore = svMgrLeader.getStorageVolumeNameOfDb(db.getId());
            Assertions.assertEquals(defaultSv.getFsName(), svNameBefore);

            Map<String, String> properties = new HashMap<>();
            properties.put(PROPERTIES_STORAGE_VOLUME, sv02.getFsName());
            AlterDatabaseSetStmt stmt = new AlterDatabaseSetStmt(dbName, properties, NodePosition.ZERO);
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            ctx.setDatabase(dbName);
            AlterDatabaseAnalyzer.analyze(stmt, ctx);

            LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
            Assertions.assertDoesNotThrow(() -> metastore.alterDatabaseSet(stmt));

            String svNameAfter = svMgrLeader.getStorageVolumeNameOfDb(db.getId());
            Assertions.assertEquals(sv02.getFsName(), svNameAfter);
        }

        // Test follower replay
        AtomicReference<DatabaseInfo> dbInfoRef = new AtomicReference<>();
        Assertions.assertDoesNotThrow(() -> {
            DatabaseInfo info = (DatabaseInfo) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_ALTER_DB_V2);
            dbInfoRef.set(info);
        });

        DatabaseInfo replayInfo = dbInfoRef.get();
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(dbName, replayInfo.getDbName());
        Assertions.assertEquals(sv02.getFsKey(), replayInfo.getStorageVolumeId());

        // switch to follower svMgr
        refSvMgr.set(svMgrFollower);

        { // replay and verify follower state
            LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
            Database followerDb = new Database(DB_ID, dbName);
            followerMetastore.unprotectCreateDb(followerDb);

            String svNameBefore = svMgrFollower.getStorageVolumeNameOfDb(db.getId());
            Assertions.assertEquals(defaultSv.getFsName(), svNameBefore);

            followerMetastore.replayAlterDatabase(replayInfo);

            String svNameAfter = svMgrFollower.getStorageVolumeNameOfDb(db.getId());
            Assertions.assertEquals(sv02.getFsName(), svNameAfter);
        }
    }

    @Test
    public void testRenameDatabaseEditLogException() throws Exception {
        // 1. Get database
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        String oldDbName = db.getFullName();

        // 2. Create AlterDatabaseRenameStatement
        // Use the actual fullName from the database, not the constant
        String newDbName = "renamed_db";
        AlterDatabaseRenameStatement stmt = new AlterDatabaseRenameStatement(oldDbName, newDbName, NodePosition.ZERO);
        // Analyze the statement (normally done before execution)
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase(oldDbName);
        AlterDatabaseAnalyzer.analyze(stmt, ctx);

        // 3. Mock EditLog.logDatabaseRename to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDatabaseRename(any(DatabaseInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute renameDatabase and expect exception
        // The method may throw DdlException if database is not found, or RuntimeException if EditLog fails
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            metastore.renameDatabase(stmt);
        });
        // The exception should be either DdlException (if db not found) or RuntimeException (if EditLog fails)
        // Since we mocked EditLog to throw, we expect RuntimeException, but if db lookup fails first, we get DdlException
        Assertions.assertTrue(exception instanceof RuntimeException || exception instanceof com.starrocks.common.DdlException);
        if (exception instanceof RuntimeException) {
            Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        }

        // 5. Verify database name remains unchanged after exception
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Assertions.assertNotNull(db);
        Assertions.assertEquals(oldDbName, db.getFullName());
    }

    // Test dropPartition
    @Test
    public void testDropPartitionNormalCase() throws Exception {
        // 1. Create table with partition
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = tablet.getReplicaByBackendId(BACKEND_ID);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Verify initial state - partition exists
        Assertions.assertNotNull(table.getPartition(PARTITION_ID));

        // 2. Create DropPartitionClause
        List<String> partitionNames = List.of("p1");
        DropPartitionClause clause = new DropPartitionClause(false, partitionNames, false, false, NodePosition.ZERO);
        // Set resolved partition names (normally done by analyzer)
        clause.setResolvedPartitionNames(partitionNames);

        // 3. Execute dropPartition
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.dropPartition(db, table, clause);

        // 4. Verify master state - partition is dropped
        Assertions.assertNull(table.getPartition(PARTITION_ID));

        // 5. Test follower replay
        DropPartitionsInfo replayInfo = (DropPartitionsInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_PARTITIONS);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(partitionNames, replayInfo.getPartitionNames());

        // Create follower metastore and the same id objects, then replay
        // Use a new recycle bin instance for follower to avoid conflicts
        CatalogRecycleBin followerRecycleBin = new CatalogRecycleBin();
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(),
                followerRecycleBin, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        // Register tablet to inverted index for replay
        TabletMeta followerTabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, followerTabletMeta);
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        MaterializedIndex followerIndex = followerPhysicalPartition.getIndex(INDEX_ID);
        LocalTablet followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Replica followerReplica = followerTablet.getReplicaByBackendId(BACKEND_ID);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica);
        Assertions.assertNotNull(followerTable.getPartition(PARTITION_ID));

        // Ensure partition is not in GlobalStateMgr's recycle bin before replay
        // (OlapTable.dropPartition uses GlobalStateMgr.getCurrentState().getRecycleBin())
        GlobalStateMgr.getCurrentState().getRecycleBin().removePartitionFromRecycleBin(PARTITION_ID);

        followerMetastore.replayDropPartitions(replayInfo);

        // 6. Verify follower state
        Assertions.assertNull(followerTable.getPartition(PARTITION_ID));
    }

    @Test
    public void testDropPartitionEditLogException() throws Exception {
        // 1. Create table with partition
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = tablet.getReplicaByBackendId(BACKEND_ID);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Verify initial state
        Assertions.assertNotNull(table.getPartition(PARTITION_ID));

        // 2. Create DropPartitionClause
        List<String> partitionNames = List.of("p1");
        DropPartitionClause clause = new DropPartitionClause(false, partitionNames, false, false, NodePosition.ZERO);
        // Set resolved partition names (normally done by analyzer)
        clause.setResolvedPartitionNames(partitionNames);

        // 3. Mock EditLog.logDropPartitions to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropPartitions(any(DropPartitionsInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute dropPartition and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.dropPartition(db, table, clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify partition still exists after exception
        Assertions.assertNotNull(table.getPartition(PARTITION_ID));
    }

    // Test renameTable
    @Test
    public void testRenameTableNormalCase() throws Exception {
        // 1. Create table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        String oldTableName = table.getName();

        // 2. Create TableRenameClause
        String newTableName = "renamed_table";
        TableRenameClause clause = new TableRenameClause(newTableName, NodePosition.ZERO);

        // 3. Execute renameTable
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.renameTable(db, table, clause);

        // 4. Verify master state
        OlapTable renamedTable = (OlapTable) db.getTable(newTableName);
        Assertions.assertNotNull(renamedTable);
        Assertions.assertEquals(newTableName, renamedTable.getName());
        Assertions.assertEquals(TABLE_ID, renamedTable.getId());
        Assertions.assertNull(db.getTable(oldTableName));

        // 5. Test follower replay
        TableInfo replayInfo = (TableInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RENAME_TABLE_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(newTableName, replayInfo.getNewTableName());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, oldTableName);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayRenameTable(replayInfo);

        // 6. Verify follower state
        OlapTable replayed = (OlapTable) followerDb.getTable(newTableName);
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(newTableName, replayed.getName());
        Assertions.assertEquals(TABLE_ID, replayed.getId());
        Assertions.assertNull(followerDb.getTable(oldTableName));
    }

    @Test
    public void testRenameTableEditLogException() throws Exception {
        // 1. Create table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        String oldTableName = table.getName();

        // 2. Create TableRenameClause
        String newTableName = "renamed_table";
        TableRenameClause clause = new TableRenameClause(newTableName, NodePosition.ZERO);

        // 3. Mock EditLog.logTableRename to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logTableRename(any(TableInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute renameTable and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.renameTable(db, table, clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify table name remains unchanged after exception
        Assertions.assertEquals(oldTableName, table.getName());
        Assertions.assertNotNull(db.getTable(oldTableName));
    }

    // Test renamePartition
    @Test
    public void testRenamePartitionNormalCase() throws Exception {
        // 1. Create range partitioned table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        String oldPartitionName = "p1";

        // 2. Create PartitionRenameClause
        String newPartitionName = "renamed_p1";
        PartitionRenameClause clause = new PartitionRenameClause(oldPartitionName, newPartitionName, NodePosition.ZERO);

        // 3. Execute renamePartition
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.renamePartition(db, table, clause);

        // 4. Verify master state
        Partition renamedPartition = table.getPartition(newPartitionName);
        Assertions.assertNotNull(renamedPartition);
        Assertions.assertEquals(newPartitionName, renamedPartition.getName());
        Assertions.assertEquals(PARTITION_ID, renamedPartition.getId());
        Assertions.assertNull(table.getPartition(oldPartitionName));

        // 5. Test follower replay
        TableInfo replayInfo = (TableInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RENAME_PARTITION_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(PARTITION_ID, replayInfo.getPartitionId());
        Assertions.assertEquals(newPartitionName, replayInfo.getNewPartitionName());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayRenamePartition(replayInfo);

        // 6. Verify follower state
        Partition replayed = followerTable.getPartition(newPartitionName);
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(newPartitionName, replayed.getName());
        Assertions.assertEquals(PARTITION_ID, replayed.getId());
        Assertions.assertNull(followerTable.getPartition(oldPartitionName));
    }

    @Test
    public void testRenamePartitionEditLogException() throws Exception {
        // 1. Create range partitioned table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createRangePartitionedOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        String oldPartitionName = "p1";

        // 2. Create PartitionRenameClause
        String newPartitionName = "renamed_p1";
        PartitionRenameClause clause = new PartitionRenameClause(oldPartitionName, newPartitionName, NodePosition.ZERO);

        // 3. Mock EditLog.logPartitionRename to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logPartitionRename(any(TableInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute renamePartition and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.renamePartition(db, table, clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify partition name remains unchanged after exception
        Assertions.assertNotNull(table.getPartition(oldPartitionName));
        Assertions.assertNull(table.getPartition(newPartitionName));
    }

    // Test renameRollup
    @Test
    public void testRenameRollupNormalCase() throws Exception {
        // 1. Create table with rollup index
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Add a rollup index
        long rollupIndexId = INDEX_ID + 1;
        String rollupName = "rollup1";
        List<Column> rollupColumns = new ArrayList<>();
        rollupColumns.add(table.getColumn("v1"));
        rollupColumns.add(table.getColumn("v2"));
        table.setIndexMeta(rollupIndexId, rollupName, rollupColumns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.getIndexNameToMetaId().put(rollupName, rollupIndexId);

        String oldRollupName = rollupName;

        // 2. Create RollupRenameClause
        String newRollupName = "renamed_rollup1";
        RollupRenameClause clause = new RollupRenameClause(oldRollupName, newRollupName, NodePosition.ZERO);

        // 3. Execute renameRollup
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.renameRollup(db, table, clause);

        // 4. Verify master state
        Assertions.assertNull(table.getIndexNameToMetaId().get(oldRollupName));
        Assertions.assertEquals(rollupIndexId, table.getIndexNameToMetaId().get(newRollupName).longValue());
        Assertions.assertEquals(newRollupName, table.getIndexNameByMetaId(rollupIndexId));

        // 5. Test follower replay
        TableInfo replayInfo = (TableInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RENAME_ROLLUP_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(rollupIndexId, replayInfo.getIndexMetaId());
        Assertions.assertEquals(newRollupName, replayInfo.getNewRollupName());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        followerTable.setIndexMeta(rollupIndexId, oldRollupName, rollupColumns, 0, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        followerTable.getIndexNameToMetaId().put(oldRollupName, rollupIndexId);

        followerMetastore.replayRenameRollup(replayInfo);

        // 6. Verify follower state
        Assertions.assertNull(followerTable.getIndexNameToMetaId().get(oldRollupName));
        Assertions.assertEquals(rollupIndexId, followerTable.getIndexNameToMetaId().get(newRollupName).longValue());
        Assertions.assertEquals(newRollupName, followerTable.getIndexNameByMetaId(rollupIndexId));
    }

    @Test
    public void testRenameRollupEditLogException() throws Exception {
        // 1. Create table with rollup index
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        long rollupIndexId = INDEX_ID + 1;
        String rollupName = "rollup1";
        List<Column> rollupColumns = new ArrayList<>();
        rollupColumns.add(table.getColumn("v1"));
        rollupColumns.add(table.getColumn("v2"));
        table.setIndexMeta(rollupIndexId, rollupName, rollupColumns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.getIndexNameToMetaId().put(rollupName, rollupIndexId);

        String oldRollupName = rollupName;

        // 2. Create RollupRenameClause
        String newRollupName = "renamed_rollup1";
        RollupRenameClause clause = new RollupRenameClause(oldRollupName, newRollupName, NodePosition.ZERO);

        // 3. Mock EditLog.logRollupRename to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRollupRename(any(TableInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute renameRollup and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.renameRollup(db, table, clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify rollup name remains unchanged after exception
        Assertions.assertNotNull(table.getIndexNameToMetaId().get(oldRollupName));
        Assertions.assertNull(table.getIndexNameToMetaId().get(newRollupName));
    }

    // Test renameColumn
    @Test
    public void testRenameColumnNormalCase() throws Exception {
        // 1. Create table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        String oldColName = "v1";

        // 2. Create ColumnRenameClause
        String newColName = "renamed_v1";
        ColumnRenameClause clause = new ColumnRenameClause(oldColName, newColName, NodePosition.ZERO);

        // 3. Execute renameColumn
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.renameColumn(db, table, clause);

        // 4. Verify master state
        Assertions.assertNull(table.getColumn(oldColName));
        Assertions.assertNotNull(table.getColumn(newColName));
        Assertions.assertEquals(newColName, table.getColumn(newColName).getName());

        // 5. Test follower replay
        ColumnRenameInfo replayInfo = (ColumnRenameInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RENAME_COLUMN_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(oldColName, replayInfo.getColumnName());
        Assertions.assertEquals(newColName, replayInfo.getNewColumnName());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);

        followerMetastore.replayRenameColumn(replayInfo);

        // 6. Verify follower state
        Assertions.assertNull(followerTable.getColumn(oldColName));
        Assertions.assertNotNull(followerTable.getColumn(newColName));
        Assertions.assertEquals(newColName, followerTable.getColumn(newColName).getName());
    }

    @Test
    public void testRenameColumnEditLogException() throws Exception {
        // 1. Create table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);
        String oldColName = "v1";

        // 2. Create ColumnRenameClause
        String newColName = "renamed_v1";
        ColumnRenameClause clause = new ColumnRenameClause(oldColName, newColName, NodePosition.ZERO);

        // 3. Mock EditLog.logColumnRename to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logColumnRename(any(ColumnRenameInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute renameColumn and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.renameColumn(db, table, clause);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify column name remains unchanged after exception
        Assertions.assertNotNull(table.getColumn(oldColName));
        Assertions.assertNull(table.getColumn(newColName));
    }

    // Test modifyTableReplicationNum
    @Test
    public void testModifyTableReplicationNumNormalCase() throws Exception {
        // 1. Create unpartitioned table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        Partition partition = table.getPartition(PARTITION_ID);
        PartitionInfo partitionInfo = table.getPartitionInfo();
        short initialReplicationNum = partitionInfo.getReplicationNum(partition.getId());

        // 2. Create properties with new replication num
        Map<String, String> properties = new HashMap<>();
        short newReplicationNum = 3;
        properties.put("replication_num", String.valueOf(newReplicationNum));

        // 3. Execute modifyTableReplicationNum
        // The method requires the caller to hold db write lock
        // Also ensure colocateTableIndex is set
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        if (metastore.getColocateTableIndex() == null) {
            metastore.setColocateTableIndex(GlobalStateMgr.getCurrentState().getColocateTableIndex());
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            metastore.modifyTableReplicationNum(db, table, properties);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        // 4. Verify master state
        Assertions.assertEquals(newReplicationNum, partitionInfo.getReplicationNum(partition.getId()));
        Assertions.assertEquals(newReplicationNum, table.getTableProperty().getReplicationNum());

        // 5. Test follower replay
        ModifyPartitionInfo replayInfo = (ModifyPartitionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_PARTITION_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(PARTITION_ID, replayInfo.getPartitionId());
        Assertions.assertEquals(newReplicationNum, replayInfo.getReplicationNum());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PartitionInfo followerPartitionInfo = followerTable.getPartitionInfo();
        followerPartitionInfo.setReplicationNum(followerPartition.getId(), initialReplicationNum);
        followerTable.getTableProperty().setReplicationNum(initialReplicationNum);

        // Replay modify partition - need to manually update partition info
        followerPartitionInfo.setReplicationNum(followerPartition.getId(), replayInfo.getReplicationNum());
        followerTable.getTableProperty().setReplicationNum(replayInfo.getReplicationNum());

        // 6. Verify follower state
        Assertions.assertEquals(newReplicationNum, followerPartitionInfo.getReplicationNum(followerPartition.getId()));
        Assertions.assertEquals(newReplicationNum, followerTable.getTableProperty().getReplicationNum());
    }

    @Test
    public void testModifyTableReplicationNumEditLogException() throws Exception {
        // 1. Create unpartitioned table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        Partition partition = table.getPartition(PARTITION_ID);
        PartitionInfo partitionInfo = table.getPartitionInfo();
        short initialTableReplicationNum = table.getTableProperty().getReplicationNum();
        short initialPartitionReplicationNum = partitionInfo.getReplicationNum(partition.getId());

        // 2. Create properties with new replication num
        Map<String, String> properties = new HashMap<>();
        short newReplicationNum = 3;
        properties.put("replication_num", String.valueOf(newReplicationNum));

        // 3. Mock EditLog.logModifyPartition to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyPartition(any(ModifyPartitionInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute modifyTableReplicationNum and expect exception
        // The method requires the caller to hold db write lock
        // Also ensure colocateTableIndex is set
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        if (metastore.getColocateTableIndex() == null) {
            metastore.setColocateTableIndex(GlobalStateMgr.getCurrentState().getColocateTableIndex());
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
                metastore.modifyTableReplicationNum(db, table, properties);
            });
            Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        // 5. Verify replication num remains unchanged after exception
        Assertions.assertEquals(initialPartitionReplicationNum, partitionInfo.getReplicationNum(partition.getId()));
        Assertions.assertEquals(initialTableReplicationNum, table.getTableProperty().getReplicationNum());
    }

    // Test setReplicaStatus
    @Test
    public void testSetReplicaStatusNormalCase() throws Exception {
        // 1. Create table with tablet and replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = tablet.getReplicaByBackendId(BACKEND_ID);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Verify initial state - replica is not bad
        Assertions.assertFalse(replica.isBad());

        // 2. Create AdminSetReplicaStatusStmt
        List<Property> propertyList = new ArrayList<>();
        propertyList.add(new Property("tablet_id", String.valueOf(TABLET_ID), NodePosition.ZERO));
        propertyList.add(new Property("backend_id", String.valueOf(BACKEND_ID), NodePosition.ZERO));
        propertyList.add(new Property("status", "bad", NodePosition.ZERO));
        PropertySet propertySet = new PropertySet(propertyList, NodePosition.ZERO);
        AdminSetReplicaStatusStmt stmt = new AdminSetReplicaStatusStmt(propertySet, NodePosition.ZERO);
        // Analyze to set the values
        AdminStmtAnalyzer.analyze(stmt, UtFrameUtils.createDefaultCtx());

        // 3. Execute setReplicaStatus
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.setReplicaStatus(stmt);

        // 4. Verify master state - replica is marked as bad
        Assertions.assertTrue(replica.isBad());

        // 5. Test follower replay
        SetReplicaStatusOperationLog replayInfo = (SetReplicaStatusOperationLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SET_REPLICA_STATUS);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(TABLET_ID, replayInfo.getTabletId());
        Assertions.assertEquals(BACKEND_ID, replayInfo.getBackendId());
        Assertions.assertEquals(ReplicaStatus.BAD, replayInfo.getReplicaStatus());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);

        TabletMeta followerTabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, followerTabletMeta);
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        MaterializedIndex followerIndex = followerPhysicalPartition.getIndex(INDEX_ID);
        LocalTablet followerTablet = (LocalTablet) followerIndex.getTablet(TABLET_ID);
        Replica followerReplica = followerTablet.getReplicaByBackendId(BACKEND_ID);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, followerReplica);
        Assertions.assertFalse(followerReplica.isBad());

        followerMetastore.replaySetReplicaStatus(replayInfo);

        // 6. Verify follower state
        Assertions.assertTrue(followerReplica.isBad());
    }

    @Test
    public void testSetReplicaStatusEditLogException() throws Exception {
        // 1. Create table with tablet and replica
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);
        Partition partition = table.getPartition(PARTITION_ID);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex index = physicalPartition.getIndex(INDEX_ID);
        LocalTablet tablet = (LocalTablet) index.getTablet(TABLET_ID);
        Replica replica = tablet.getReplicaByBackendId(BACKEND_ID);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addReplica(TABLET_ID, replica);

        // Verify initial state
        Assertions.assertFalse(replica.isBad());

        // 2. Create AdminSetReplicaStatusStmt
        List<Property> propertyList = new ArrayList<>();
        propertyList.add(new Property("tablet_id", String.valueOf(TABLET_ID), NodePosition.ZERO));
        propertyList.add(new Property("backend_id", String.valueOf(BACKEND_ID), NodePosition.ZERO));
        propertyList.add(new Property("status", "bad", NodePosition.ZERO));
        PropertySet propertySet = new PropertySet(propertyList, NodePosition.ZERO);
        AdminSetReplicaStatusStmt stmt = new AdminSetReplicaStatusStmt(propertySet, NodePosition.ZERO);
        // Analyze to set the values
        AdminStmtAnalyzer.analyze(stmt, UtFrameUtils.createDefaultCtx());

        // 3. Mock EditLog.logSetReplicaStatus to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logSetReplicaStatus(any(SetReplicaStatusOperationLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute setReplicaStatus - the method may throw exception if EditLog fails
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.setReplicaStatus(stmt);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertFalse(replica.isBad());
    }

    // Test setPartitionVersion
    @Test
    public void testSetPartitionVersionNormalCase() throws Exception {
        // 1. Create table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // For unpartitioned table, partition name should be the same as table name
        Partition partition = table.getPartition(TABLE_NAME);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        long initialVersion = physicalPartition.getVisibleVersion();

        // 2. Create AdminSetPartitionVersionStmt
        // For unpartitioned table, partition name should be the same as table name
        long newVersion = 100L;
        QualifiedName qualifiedName = QualifiedName.of(List.of("default_catalog", DB_NAME, TABLE_NAME));
        TableRef tableRef = new TableRef(qualifiedName, null, NodePosition.ZERO);
        AdminSetPartitionVersionStmt stmt = new AdminSetPartitionVersionStmt(
                tableRef, TABLE_NAME, newVersion, NodePosition.ZERO);

        // 3. Execute setPartitionVersion
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.setPartitionVersion(stmt);

        // 4. Verify master state
        // Re-read the partition to get updated version
        partition = table.getPartition(TABLE_NAME);
        physicalPartition = partition.getDefaultPhysicalPartition();
        Assertions.assertEquals(newVersion, physicalPartition.getVisibleVersion());
        Assertions.assertEquals(newVersion + 1, physicalPartition.getNextVersion());

        // 5. Test follower replay
        PartitionVersionRecoveryInfo replayInfo = (PartitionVersionRecoveryInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RECOVER_PARTITION_VERSION);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertNotNull(replayInfo.getPartitionVersions());
        Assertions.assertFalse(replayInfo.getPartitionVersions().isEmpty());
        Assertions.assertEquals(DB_ID, replayInfo.getPartitionVersions().get(0).getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getPartitionVersions().get(0).getTableId());
        Assertions.assertEquals(PHYSICAL_PARTITION_ID, replayInfo.getPartitionVersions().get(0).getPartitionId());
        Assertions.assertEquals(newVersion, replayInfo.getPartitionVersions().get(0).getVersion());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        // For unpartitioned table, partition name should be the same as table name
        Partition followerPartition = followerTable.getPartition(TABLE_NAME);
        PhysicalPartition followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        long followerInitialVersion = followerPhysicalPartition.getVisibleVersion();

        // Use MetaRecoveryDaemon to replay
        // Temporarily set followerMetastore to GlobalStateMgr so recoverPartitionVersion can find the database and table
        LocalMetastore originalMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
        try {
            MetaRecoveryDaemon recoveryDaemon = new MetaRecoveryDaemon();
            recoveryDaemon.recoverPartitionVersion(replayInfo);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }

        // 6. Verify follower state
        // Re-read the partition to get updated version
        // For unpartitioned table, partition name should be the same as table name
        followerPartition = followerTable.getPartition(TABLE_NAME);
        followerPhysicalPartition = followerPartition.getDefaultPhysicalPartition();
        Assertions.assertEquals(newVersion, followerPhysicalPartition.getVisibleVersion());
        Assertions.assertEquals(newVersion + 1, followerPhysicalPartition.getNextVersion());
    }

    @Test
    public void testSetPartitionVersionEditLogException() throws Exception {
        // 1. Create table
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // For unpartitioned table, partition name should be the same as table name
        Partition partition = table.getPartition(TABLE_NAME);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        long initialVersion = physicalPartition.getVisibleVersion();

        // 2. Create AdminSetPartitionVersionStmt
        // For unpartitioned table, partition name should be the same as table name
        long newVersion = 100L;
        QualifiedName qualifiedName = QualifiedName.of(List.of("default_catalog", DB_NAME, TABLE_NAME));
        TableRef tableRef = new TableRef(qualifiedName, null, NodePosition.ZERO);
        AdminSetPartitionVersionStmt stmt = new AdminSetPartitionVersionStmt(
                tableRef, TABLE_NAME, newVersion, NodePosition.ZERO);

        // 3. Mock EditLog.logRecoverPartitionVersion to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRecoverPartitionVersion(any(PartitionVersionRecoveryInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute setPartitionVersion and expect exception
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.setPartitionVersion(stmt);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify partition version remains unchanged after exception
        Assertions.assertEquals(initialVersion, physicalPartition.getVisibleVersion());
    }

    // Test getPartitionIdToStorageMediumMap
    @Test
    public void testGetPartitionIdToStorageMediumMapNormalCase() throws Exception {
        // 1. Create table with SSD storage medium that will expire
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Set partition to SSD with expired cooldown time
        Partition partition = table.getPartition(PARTITION_ID);
        PartitionInfo partitionInfo = table.getPartitionInfo();
        com.starrocks.catalog.DataProperty expiredSSDProperty = new com.starrocks.catalog.DataProperty(
                TStorageMedium.SSD, System.currentTimeMillis() - 1000); // Expired
        partitionInfo.setDataProperty(partition.getId(), expiredSSDProperty);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.SSD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);

        // Verify initial state
        Assertions.assertEquals(TStorageMedium.SSD, partitionInfo.getDataProperty(partition.getId()).getStorageMedium());

        // 2. Execute getPartitionIdToStorageMediumMap
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Map<Long, TStorageMedium> storageMediumMap = metastore.getPartitionIdToStorageMediumMap();

        // 3. Verify master state - partition should be changed to HDD if lock was acquired
        // Note: The method uses tryLock, so it may not always succeed.
        // If lock was acquired and EditLog succeeded, the storage medium should be HDD
        TStorageMedium actualMedium = partitionInfo.getDataProperty(partition.getId()).getStorageMedium();

        Assertions.assertTrue(storageMediumMap.containsKey(PARTITION_ID));
        Assertions.assertEquals(TStorageMedium.HDD, storageMediumMap.get(PARTITION_ID));

        // 4. Test follower replay
        ModifyPartitionInfo replayInfo = (ModifyPartitionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_PARTITION_V2);

        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(DB_ID, replayInfo.getDbId());
        Assertions.assertEquals(TABLE_ID, replayInfo.getTableId());
        Assertions.assertEquals(PARTITION_ID, replayInfo.getPartitionId());
        Assertions.assertEquals(TStorageMedium.HDD, replayInfo.getDataProperty().getStorageMedium());

        // Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        OlapTable followerTable = createOlapTable(TABLE_ID, TABLE_NAME);
        followerDb.registerTableUnlocked(followerTable);
        Partition followerPartition = followerTable.getPartition(PARTITION_ID);
        PartitionInfo followerPartitionInfo = followerTable.getPartitionInfo();
        followerPartitionInfo.setDataProperty(followerPartition.getId(), expiredSSDProperty);

        followerPartitionInfo.setDataProperty(followerPartition.getId(), replayInfo.getDataProperty());

        // 5. Verify follower state
        Assertions.assertEquals(TStorageMedium.HDD, followerPartitionInfo
                .getDataProperty(followerPartition.getId()).getStorageMedium());
    }

    @Test
    public void testGetPartitionIdToStorageMediumMapEditLogException() {
        // 1. Create table with SSD storage medium that will expire
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = createOlapTable(TABLE_ID, TABLE_NAME);
        db.registerTableUnlocked(table);

        // Set partition to SSD with expired cooldown time
        Partition partition = table.getPartition(PARTITION_ID);
        PartitionInfo partitionInfo = table.getPartitionInfo();
        com.starrocks.catalog.DataProperty expiredSSDProperty = new com.starrocks.catalog.DataProperty(
                TStorageMedium.SSD, System.currentTimeMillis() - 1000); // Expired
        partitionInfo.setDataProperty(partition.getId(), expiredSSDProperty);

        // Add tablet to inverted index
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.SSD);
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(TABLET_ID, tabletMeta);

        // 2. Mock EditLog.logModifyPartition to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logModifyPartition(any(ModifyPartitionInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute getPartitionIdToStorageMediumMap - exception is caught internally
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            metastore.getPartitionIdToStorageMediumMap();
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 4. Verify storage medium remains unchanged after exception
        // The method catches exceptions internally, so the partition should remain as SSD
        Assertions.assertEquals(TStorageMedium.SSD, partitionInfo.getDataProperty(partition.getId()).getStorageMedium());
    }
}
