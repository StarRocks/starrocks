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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.PhysicalPartitionPersistInfoV2;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalMetaStoreTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_NOTHING);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable(
                                "CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                                            " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

    }

    @Test
    public void testGetNewPartitionsFromPartitions() throws DdlException {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        Partition sourcePartition = olapTable.getPartition("t1");
        List<Long> sourcePartitionIds = Lists.newArrayList(sourcePartition.getId());
        List<Long> tmpPartitionIds = Lists.newArrayList(connectContext.getGlobalStateMgr().getNextId());
        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        Map<Long, String> origPartitions = Maps.newHashMap();
        OlapTable copiedTable = localMetastore.getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions);
        Assertions.assertEquals(olapTable.getName(), copiedTable.getName());
        Set<Long> tabletIdSet = Sets.newHashSet();
        List<Partition> newPartitions = localMetastore.getNewPartitionsFromPartitions(db,
                    olapTable, sourcePartitionIds, origPartitions, copiedTable, "_100", tabletIdSet, tmpPartitionIds,
                    null, WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(sourcePartitionIds.size(), newPartitions.size());
        Assertions.assertEquals(1, newPartitions.size());
        Partition newPartition = newPartitions.get(0);
        Assertions.assertEquals("t1_100", newPartition.getName());
        olapTable.addTempPartition(newPartition);

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        partitionInfo.addPartition(newPartition.getId(), partitionInfo.getDataProperty(sourcePartition.getId()),
                    partitionInfo.getReplicationNum(sourcePartition.getId()),
                    partitionInfo.getIsInMemory(sourcePartition.getId()));
        olapTable.replacePartition(db.getId(), "t1", "t1_100");

        Assertions.assertEquals(newPartition.getId(), olapTable.getPartition("t1").getId());
    }

    @Test
    public void testLoadClusterV2() throws Exception {
        LocalMetastore localMetaStore = new LocalMetastore(GlobalStateMgr.getCurrentState(),
                    GlobalStateMgr.getCurrentState().getRecycleBin(),
                    GlobalStateMgr.getCurrentState().getColocateTableIndex());

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        localMetaStore.save(image.getImageWriter());

        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        localMetaStore.load(reader);
        reader.close();

        Assertions.assertNotNull(localMetaStore.getDb(SystemId.INFORMATION_SCHEMA_DB_ID));
        Assertions.assertNotNull(localMetaStore.getDb(InfoSchemaDb.DATABASE_NAME));
        Assertions.assertNotNull(localMetaStore.getDb(SystemId.SYS_DB_ID));
        Assertions.assertNotNull(localMetaStore.getDb(SysDb.DATABASE_NAME));
    }

    @Test
    public void testReplayAddSubPartition() throws DdlException {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        PhysicalPartition p = table.getPartitions().stream().findFirst().get().getDefaultPhysicalPartition();
        int schemaHash = table.getSchemaHashByIndexId(p.getBaseIndex().getId());
        MaterializedIndex index = new MaterializedIndex();
        TabletMeta tabletMeta = new TabletMeta(db.getId(), table.getId(), p.getId(),
                    index.getId(), table.getPartitionInfo().getDataProperty(p.getParentId()).getStorageMedium());
        index.addTablet(new LocalTablet(0), tabletMeta);
        PhysicalPartitionPersistInfoV2 info = new PhysicalPartitionPersistInfoV2(
                    db.getId(), table.getId(), p.getParentId(), new PhysicalPartition(123, "", p.getId(), index));

        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        localMetastore.replayAddSubPartition(info);
    }

    @Test
    public void testReplayTruncateTable() throws DdlException {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        Partition p = table.getPartitions().stream().findFirst().get();
        TruncateTableInfo info = new TruncateTableInfo(db.getId(), table.getId(), Lists.newArrayList(p), false);

        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        localMetastore.replayTruncateTable(info);
    }

    @Test
    public void testModifyAutomaticBucketSize() throws DdlException {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Map<String, String> properties = Maps.newHashMap();
            LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
            table.setTableProperty(null);
            localMetastore.modifyTableAutomaticBucketSize(db, table, properties);
            localMetastore.modifyTableAutomaticBucketSize(db, table, properties);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Test
    public void testCreateTableIfNotExists() throws Exception {
        // create table if not exists, if the table already exists, do nothing
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        AtomicInteger invokeCount = new AtomicInteger(0);

        new MockUp<LocalMetastore>() {
            @Mock
            public void onCreate(Invocation invocation, Database db, Table table, String storageVolumeId,
                                 boolean isSetIfNotExists) throws DdlException {
                invokeCount.incrementAndGet();
                invocation.proceed(db, table, storageVolumeId, isSetIfNotExists);
            }
        };

        starRocksAssert = new StarRocksAssert(connectContext);
        // with IF NOT EXIST
        starRocksAssert.useDatabase("test").withTable(
                    "CREATE TABLE IF NOT EXISTS test.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // w/o IF NOT EXIST
        Assertions.assertThrows(AnalysisException.class, () ->
                    starRocksAssert.useDatabase("test").withTable(
                                "CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                                            " distributed by hash(k1) buckets 3 properties('replication_num' = '1');"));

        // No invocation at all
        Assertions.assertEquals(0, invokeCount.get());
    }

    @Test
    public void testAlterTableProperties() throws Exception {
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "abcd");
        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        try {
            localMetastore.alterTableProperties(db, table, properties);
        } catch (RuntimeException e) {
            Assertions.assertEquals("Cannot parse text to Duration", e.getMessage());
        }
    }

    @Test
    public void testRenameColumnException() throws Exception {
        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();

        try {
            Table table = new HiveTable();
            localMetastore.renameColumn(null, table, null);
            Assertions.fail("should not happen");
        } catch (ErrorReportException e) {
            Assertions.assertEquals(e.getErrorCode(), ErrorCode.ERR_COLUMN_RENAME_ONLY_FOR_OLAP_TABLE);
        }

        Database db = localMetastore.getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        try {
            localMetastore.renameColumn(new Database(1, "_statistics_"), table, null);
            Assertions.fail("should not happen");
        } catch (ErrorReportException e) {
            Assertions.assertEquals(e.getErrorCode(), ErrorCode.ERR_CANNOT_RENAME_COLUMN_IN_INTERNAL_DB);
        }

        try {
            OlapTable olapTable = new OlapTable();
            olapTable.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
            localMetastore.renameColumn(db, olapTable, null);
            Assertions.fail("should not happen");
        } catch (ErrorReportException e) {
            Assertions.assertEquals(e.getErrorCode(), ErrorCode.ERR_CANNOT_RENAME_COLUMN_OF_NOT_NORMAL_TABLE);
        }

        try {
            ColumnRenameClause columnRenameClause = new ColumnRenameClause("k4", "k5");
            localMetastore.renameColumn(db, table, columnRenameClause);
            Assertions.fail("should not happen");
        } catch (ErrorReportException e) {
            Assertions.assertEquals(e.getErrorCode(), ErrorCode.ERR_BAD_FIELD_ERROR);
        }

        try {
            ColumnRenameClause columnRenameClause = new ColumnRenameClause("k3", "k2");
            localMetastore.renameColumn(db, table, columnRenameClause);
            Assertions.fail("should not happen");
        } catch (ErrorReportException e) {
            Assertions.assertEquals(e.getErrorCode(), ErrorCode.ERR_DUP_FIELDNAME);
        }
    }

    @Test
    public void testCreateTableSerializeException() {
        final long tableId = 1000010L;
        final String tableName = "test";
        Database db = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test");
        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        SerializeFailedTable table = new SerializeFailedTable(1000010L, "serialize_test");

        Assertions.assertThrows(DdlException.class, () -> localMetastore.onCreate(db, table, "", true));

        Assertions.assertNull(db.getTable(tableId));
        Assertions.assertNull(db.getTable(tableName));
    }

    @Test
    public void testTruncateInTheMiddleOfDatabaseDropped() throws Exception {
        String dbName = "db_to_be_dropped";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName)
                .withTable("CREATE TABLE t1(k1 int, k2 int, k3 int) distributed by " +
                        "hash(k1) buckets 3 properties('replication_num' = '1');");

        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        Database db = localMetastore.getDb(dbName);
        long t1TableId = db.getTable("t1").getId();
        AtomicBoolean writeLockAcquired = new AtomicBoolean(false);

        long tabletsCountBefore = connectContext.getGlobalStateMgr().getTabletInvertedIndex().getTabletCount();
        new MockUp<Locker>() {
            @Mock
            public boolean lockTableAndCheckDbExist(Invocation invocation, Database database, long tableId,
                                                    LockType lockType)
                    throws DdlException, MetaNotFoundException {
                if (lockType == LockType.WRITE && database.getId() == db.getId() && t1TableId == tableId) {
                    // simulate that the database is dropped after locking the table
                    localMetastore.dropDb(connectContext, dbName, false);
                    long tabletCountInMiddle =
                            connectContext.getGlobalStateMgr().getTabletInvertedIndex().getTabletCount();
                    // The new partition is ready, but not committed yet, so 3 more tablets are created
                    Assertions.assertEquals(tabletsCountBefore + 3, tabletCountInMiddle,
                            "3 new tablets should be created for the new partition during truncate");
                    writeLockAcquired.set(true);
                    return false;
                } else {
                    return Boolean.TRUE.equals(invocation.proceed(database, tableId, lockType));
                }
            }
        };


        TableRef tableRef = new TableRef(new TableName(dbName, "t1"), null);
        TruncateTableStmt stmt = new TruncateTableStmt(tableRef);
        DdlException exception =
                Assertions.assertThrows(DdlException.class, () -> localMetastore.truncateTable(stmt, connectContext));
        Assertions.assertEquals("Unknown database 'db_to_be_dropped'", exception.getMessage());
        long tabletsCountAfter = connectContext.getGlobalStateMgr().getTabletInvertedIndex().getTabletCount();
        Assertions.assertEquals(tabletsCountBefore, tabletsCountAfter,
                "Tablets should not be changed when truncate fails");
        Assertions.assertTrue(writeLockAcquired.get(), "Write lock should be acquired during truncate");
    }

    @Test
    public void testAddPhysicalPartitionForNonPartitionedTable() throws Exception {
        // Create a non-partitioned table with random distribution
        starRocksAssert.useDatabase("test").withTable(
                "CREATE TABLE test.add_pp_random(k1 INT, k2 VARCHAR(50), v1 INT) " +
                        "ENGINE=olap DUPLICATE KEY(k1) DISTRIBUTED BY RANDOM BUCKETS 8 " +
                        "PROPERTIES ('replication_num' = '1')");

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb("test");
        OlapTable table = (OlapTable) db.getTable("add_pp_random");
        Assertions.assertEquals(1, table.getPhysicalPartitions().size());

        // Record original mutableBucketNum to verify it is not modified
        long originalMutableBucketNum = table.getMutableBucketNum();

        // Add physical partition with custom bucket number
        metastore.addPhysicalPartition("test", "add_pp_random", null, 4);
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());

        // Verify the new physical partition has the specified bucket number
        Partition partition = table.getPartitions().iterator().next();
        List<PhysicalPartition> subPartitions = new java.util.ArrayList<>(partition.getSubPartitions());
        // The newly added physical partition (not the default one) should have bucketNum = 4
        PhysicalPartition newPartition = subPartitions.stream()
                .filter(p -> p.getId() != partition.getDefaultPhysicalPartition().getId())
                .findFirst().orElseThrow();
        Assertions.assertEquals(4, newPartition.getBucketNum());

        // Verify the original table's mutableBucketNum is NOT modified (concurrency-safe)
        Assertions.assertEquals(originalMutableBucketNum, table.getMutableBucketNum());

        starRocksAssert.dropTable("test.add_pp_random");
    }

    @Test
    public void testAddPhysicalPartitionForPartitionedTable() throws Exception {
        // Create a partitioned table with random distribution
        starRocksAssert.useDatabase("test").withTable(
                "CREATE TABLE test.add_pp_part(k1 DATE, k2 INT, v1 VARCHAR(100)) " +
                        "ENGINE=olap DUPLICATE KEY(k1, k2) " +
                        "PARTITION BY RANGE (k1) (" +
                        "  PARTITION p20240101 VALUES LESS THAN ('2024-01-02')," +
                        "  PARTITION p20240102 VALUES LESS THAN ('2024-01-03')" +
                        ") DISTRIBUTED BY RANDOM BUCKETS 10 " +
                        "PROPERTIES ('replication_num' = '1')");

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb("test");
        OlapTable table = (OlapTable) db.getTable("add_pp_part");
        Partition partition = table.getPartition("p20240101");
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());
        Assertions.assertEquals(1, partition.getSubPartitions().size());

        long originalMutableBucketNum = table.getMutableBucketNum();

        // Add physical partition to a specific partition with default bucket
        metastore.addPhysicalPartition("test", "add_pp_part", "p20240101", 0);
        Assertions.assertEquals(3, table.getPhysicalPartitions().size());
        Assertions.assertEquals(2, partition.getSubPartitions().size());

        // Add physical partition with custom bucket number to another partition
        Partition partition2 = table.getPartition("p20240102");
        metastore.addPhysicalPartition("test", "add_pp_part", "p20240102", 6);
        Assertions.assertEquals(4, table.getPhysicalPartitions().size());
        Assertions.assertEquals(2, partition2.getSubPartitions().size());

        // Verify the new physical partition in p20240102 has the specified bucket number
        PhysicalPartition newPartition = partition2.getSubPartitions().stream()
                .filter(p -> p.getId() != partition2.getDefaultPhysicalPartition().getId())
                .findFirst().orElseThrow();
        Assertions.assertEquals(6, newPartition.getBucketNum());

        // Verify the original table's mutableBucketNum is NOT modified
        Assertions.assertEquals(originalMutableBucketNum, table.getMutableBucketNum());

        starRocksAssert.dropTable("test.add_pp_part");
    }

    @Test
    public void testAddPhysicalPartitionErrors() throws Exception {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();

        // 1. Database not found
        Assertions.assertThrows(DdlException.class, () ->
                metastore.addPhysicalPartition("nonexistent_db", "t", null, 0));

        // 2. Table not found
        Assertions.assertThrows(DdlException.class, () ->
                metastore.addPhysicalPartition("test", "nonexistent_table", null, 0));

        // 3. Hash distribution table (t1 uses hash distribution)
        DdlException hashEx = Assertions.assertThrows(DdlException.class, () ->
                metastore.addPhysicalPartition("test", "t1", null, 0));
        Assertions.assertTrue(hashEx.getMessage().contains("random distribution"));

        // 4. Partitioned table without partition name & non-existent partition & negative bucket
        starRocksAssert.useDatabase("test").withTable(
                "CREATE TABLE test.add_pp_err(k1 DATE, k2 INT) ENGINE=olap DUPLICATE KEY(k1, k2) " +
                        "PARTITION BY RANGE (k1) (PARTITION p1 VALUES LESS THAN ('2024-01-02')) " +
                        "DISTRIBUTED BY RANDOM BUCKETS 8 PROPERTIES ('replication_num' = '1')");

        DdlException partEx = Assertions.assertThrows(DdlException.class, () ->
                metastore.addPhysicalPartition("test", "add_pp_err", null, 0));
        Assertions.assertTrue(partEx.getMessage().contains("Partition name must be specified"));

        DdlException notFoundEx = Assertions.assertThrows(DdlException.class, () ->
                metastore.addPhysicalPartition("test", "add_pp_err", "no_such_partition", 0));
        Assertions.assertTrue(notFoundEx.getMessage().contains("does not exist"));

        DdlException bucketEx = Assertions.assertThrows(DdlException.class, () ->
                metastore.addPhysicalPartition("test", "add_pp_err", "p1", -1));
        Assertions.assertTrue(bucketEx.getMessage().contains("non-negative"));

        starRocksAssert.dropTable("test.add_pp_err");
    }
}
