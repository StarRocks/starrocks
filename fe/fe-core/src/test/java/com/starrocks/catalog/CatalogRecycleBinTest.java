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

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class CatalogRecycleBinTest {
    private static void waitTableClearFinished(CatalogRecycleBin recycleBin, long id,
                                               long time) {
        while (recycleBin.getRecycleTableInfo(id) != null) {
            recycleBin.eraseTable(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    private static void waitPartitionClearFinished(CatalogRecycleBin recycleBin, long id,
                                                   long time) {
        while (recycleBin.getRecyclePartitionInfo(id) != null) {
            recycleBin.erasePartition(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    private static void waitTableToBeDone(CatalogRecycleBin recycleBin, long id, long time) {
        // For shared-nothing mode, table deletion is synchronous, so this is a no-op.
    }

    private static void waitPartitionToBeDone(CatalogRecycleBin recycleBin, long id, long time) {
        while (recycleBin.isDeletingPartition(id)) {
            recycleBin.erasePartition(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore spyLocalMetastore = spy(new LocalMetastore(globalStateMgr,
                globalStateMgr.getRecycleBin(), globalStateMgr.getColocateTableIndex()));
        doNothing().when(spyLocalMetastore).onEraseDatabase(anyLong());
        doNothing().when(spyLocalMetastore).onErasePartition(any());
        globalStateMgr.setLocalMetastore(spyLocalMetastore);


        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static String rowsToString(List<List<String>> rows) {
        List<String> lines = rows.stream().map(
                row -> {
                    row.remove(1);
                    return java.lang.String.join("|",
                            row.toArray(new String[0])).replaceAll("id=\\d+(,\\s+)?", "");
                }
        ).collect(Collectors.toList());
        return java.lang.String.join("\n", lines.toArray(new String[0]));
    }

    @Test
    public void testGetDb() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Database database = new Database(1, "db");
        bin.recycleDatabase(database, Sets.newHashSet(), true);
        Database database2 = new Database(2, "db");
        bin.recycleDatabase(database2, Sets.newHashSet(), true);

        Database recycledDb = bin.getDatabase(1);
        Assertions.assertNull(recycledDb);
        recycledDb = bin.getDatabase(2);
        Assertions.assertEquals(2L, recycledDb.getId());
        Assertions.assertEquals("db", recycledDb.getOriginName());

        List<Long> dbIds = bin.getAllDbIds();
        Assertions.assertEquals(Lists.newArrayList(2L), dbIds);
    }

    @Test
    public void testGetTable() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11L, table, true);
        Table table2 = new Table(2L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11L, table2, true);

        Assertions.assertFalse(bin.isTableRecoverable(11L, 1L));
        Assertions.assertNotNull(bin.getTable(11L, 1L));
        Assertions.assertTrue(bin.isTableRecoverable(11L, 2L));
        Assertions.assertNotNull(bin.getTable(11L, 2L));

        List<Table> tables = bin.getTables(11L);
        Assertions.assertEquals(2, tables.size());
    }

    @Test
    public void testGetPartition() throws Exception {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", TypeFactory.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition, range, dataProperty, (short) 1, null));
        Partition partition2 = new Partition(2L, 4L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition2, range, dataProperty, (short) 1, null));

        Partition recycledPart = bin.getPartition(1L);
        Assertions.assertNotNull(recycledPart);
        recycledPart = bin.getPartition(2L);
        Assertions.assertEquals(2L, recycledPart.getId());
        Assertions.assertEquals(range, bin.getPartitionRange(2L));
        Assertions.assertEquals(dataProperty, bin.getPartitionDataProperty(2L));
        Assertions.assertEquals((short) 1, bin.getPartitionReplicationNum(2L));
    }

    @Test
    public void testGetPhysicalPartition() throws Exception {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", TypeFactory.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition, range, dataProperty, (short) 1, null));
        Partition partition2 = new Partition(2L, 4L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition2, range, dataProperty, (short) 1, null));

        PhysicalPartition recycledPart = bin.getPhysicalPartition(3L);
        Assertions.assertNotNull(recycledPart);
        recycledPart = bin.getPhysicalPartition(4L);
        Assertions.assertEquals(4L, recycledPart.getId());

        Partition replacementPartition = new Partition(1L, 5L, "pt", new MaterializedIndex(), null);
        bin.setPartitionInfo(1L,
                new RecycleRangePartitionInfo(11L, 22L, replacementPartition, range, dataProperty, (short) 1, null));

        Assertions.assertNull(bin.getPhysicalPartition(3L));
        recycledPart = bin.getPhysicalPartition(5L);
        Assertions.assertNotNull(recycledPart);
        Assertions.assertEquals(5L, recycledPart.getId());

        bin.removePartitionFromRecycleBin(1L);
        Assertions.assertNull(bin.getPhysicalPartition(5L));
        Assertions.assertNotNull(bin.getPhysicalPartition(4L));
    }

    @Test
    public void testLoadRebuildsPhysicalPartitionIndex() throws Exception {
        CatalogRecycleBin originalBin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", TypeFactory.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        originalBin.recyclePartition(
                new RecycleRangePartitionInfo(11L, 22L, partition, range, dataProperty, (short) 1, null));
        Partition partition2 = new Partition(2L, 4L, "pt2", new MaterializedIndex(), null);
        originalBin.recyclePartition(
                new RecycleRangePartitionInfo(11L, 22L, partition2, range, dataProperty, (short) 1, null));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        originalBin.save(image.getImageWriter());

        CatalogRecycleBin loadedBin = new CatalogRecycleBin();
        loadedBin.load(image.getMetaBlockReader());

        PhysicalPartition recycledPart = loadedBin.getPhysicalPartition(3L);
        Assertions.assertNotNull(recycledPart);
        Assertions.assertEquals(3L, recycledPart.getId());
        recycledPart = loadedBin.getPhysicalPartition(4L);
        Assertions.assertNotNull(recycledPart);
        Assertions.assertEquals(4L, recycledPart.getId());
        Assertions.assertNull(loadedBin.getPhysicalPartition(5L));
    }

    @Test
    public void testEstimateMemoryTracksPhysicalPartitionIndex() throws Exception {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", TypeFactory.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition initialPartition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);

        bin.recyclePartition(
                new RecycleRangePartitionInfo(11L, 22L, initialPartition, range, dataProperty, (short) 1, null));

        Partition partitionWithTwoPhysicalPartitions = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        PhysicalPartition extraPhysicalPartition =
                new PhysicalPartition(4L, partitionWithTwoPhysicalPartitions.getId(), new MaterializedIndex());
        partitionWithTwoPhysicalPartitions.addSubPartition(extraPhysicalPartition);
        bin.setPartitionInfo(1L,
                new RecycleRangePartitionInfo(11L, 22L, partitionWithTwoPhysicalPartitions,
                        range, dataProperty, (short) 1, null));

        Assertions.assertEquals(1L, bin.estimateCount().get("Partition"));
        Assertions.assertEquals(2L, bin.estimateCount().get("PhysicalPartitionIndex"));
        Assertions.assertNotNull(bin.getPhysicalPartition(4L));

        long estimatedSizeWithTwoPhysicalPartitions = bin.estimateSize();

        Partition partitionWithOnePhysicalPartition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        bin.setPartitionInfo(1L,
                new RecycleRangePartitionInfo(11L, 22L, partitionWithOnePhysicalPartition,
                        range, dataProperty, (short) 1, null));

        Assertions.assertEquals(1L, bin.estimateCount().get("PhysicalPartitionIndex"));
        Assertions.assertNull(bin.getPhysicalPartition(4L));
        Assertions.assertTrue(estimatedSizeWithTwoPhysicalPartitions > bin.estimateSize());
    }

    @Test
    public void testReplayEraseTable() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11, table, true);
        bin.recycleTable(12, table, true);

        List<Table> tables = bin.getTables(11L);
        Assertions.assertEquals(1, tables.size());

        bin.replayEraseTable(Collections.singletonList(2L));
        tables = bin.getTables(11);
        Assertions.assertEquals(1, tables.size());

        bin.replayEraseTable(Collections.singletonList(1L));
        tables = bin.getTables(11);
        Assertions.assertEquals(0, tables.size());
    }

    @Test
    public void testReplayEraseTableEx() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11, table, true);
        Table table2 = new Table(2L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(12, table2, true);
        Table table3 = new Table(3L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(13, table3, true);

        bin.eraseTable(System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000L + 10000);
        waitTableClearFinished(bin, 1L, System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000L + 10000);
        waitTableClearFinished(bin, 2L, System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000L + 10000);
        waitTableClearFinished(bin, 3L, System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000L + 10000);

        Assertions.assertEquals(0, bin.getTables(11L).size());
        Assertions.assertEquals(0, bin.getTables(12L).size());
        Assertions.assertEquals(0, bin.getTables(13L).size());
    }

    @Test
    public void testAddTabletToInvertedIndexWithLocalTablet(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked Database db) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long physicalPartitionId = 6L;
        long replicaId = 10L;
        long backendId = 20L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", IntegerType.BIGINT, true, null, "", ""));
        columns.add(new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

        // Replica
        Replica replica1 = new Replica(replicaId, backendId, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica3 = new Replica(replicaId + 2, backendId + 2, Replica.ReplicaState.NORMAL, 1, 0);

        // Tablet
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.SSD));
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexMetaId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                result = invertedIndex;
            }
        };

        CatalogRecycleBin bin = new CatalogRecycleBin();
        bin.recycleTable(dbId, table, true);
        bin.addTabletToInvertedIndex();

        // Check
        TabletMeta tabletMeta1 = invertedIndex.getTabletMeta(tabletId);
        Assertions.assertNotNull(tabletMeta1);
        Assertions.assertFalse(tabletMeta1.isLakeTablet());
        Assertions.assertEquals(TStorageMedium.SSD, tabletMeta1.getStorageMedium());
        Assertions.assertEquals(replica1, invertedIndex.getReplica(tabletId, backendId));
        Assertions.assertEquals(replica2, invertedIndex.getReplica(tabletId, backendId + 1));
        Assertions.assertEquals(replica3, invertedIndex.getReplica(tabletId, backendId + 2));
    }


    @Test
    public void testAddTabletToInvertedIndexWithLocalTabletError(@Mocked GlobalStateMgr globalStateMgr,
                                                                 @Mocked Database db) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long physicalPartitionId = 6L;
        long replicaId = 10L;
        long backendId = 20L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", IntegerType.BIGINT, true, null, "", ""));
        columns.add(new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

        // Replica
        Replica replica1 = new Replica(replicaId, backendId, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica2 = new Replica(replicaId + 1, backendId + 1, Replica.ReplicaState.NORMAL, 1, 0);
        Replica replica3 = new Replica(replicaId + 2, backendId + 2, Replica.ReplicaState.NORMAL, 1, 0);

        // Tablet
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexMetaId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                result = invertedIndex;
            }
        };

        CatalogRecycleBin bin = new CatalogRecycleBin();
        bin.recycleTable(dbId, table, true);
        bin.addTabletToInvertedIndex();
    }

    @Test
    public void testEnsureEraseLater() {
        Config.catalog_trash_expire_second = 600; // set expire in 10 minutes
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        Database db = new Database(111, "uno");
        recycleBin.recycleDatabase(db, new HashSet<>(), true);

        // no need to set enable erase later if there are a lot of time left
        long now = System.currentTimeMillis();
        Assertions.assertTrue(recycleBin.ensureEraseLater(db.getId(), db.getId(), now));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(db.getId()));

        // no need to set enable erase later if already exipre
        long moreThanTenMinutesLater = now + 620 * 1000L;
        Assertions.assertFalse(recycleBin.ensureEraseLater(db.getId(), db.getId(), moreThanTenMinutesLater));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(db.getId()));

        // now we should set enable erase later because we are about to expire
        long moreThanNineMinutesLater = now + 550 * 1000L;
        Assertions.assertTrue(recycleBin.ensureEraseLater(db.getId(), db.getId(), moreThanNineMinutesLater));
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(db.getId()));

        // if already expired, we should return false but won't erase the flag
        Assertions.assertFalse(recycleBin.ensureEraseLater(db.getId(), db.getId(), moreThanTenMinutesLater));
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(db.getId()));
    }

    @Test
    public void testCheckValidDeletionByClusterSnapshotSharedNothingMode(@Mocked ClusterSnapshotMgr clusterSnapshotMgr) {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        long dbId = 12345L;
        // put a recycle time to make sure the old logic would call ClusterSnapshotMgr
        recycleBin.idToRecycleTime.put(dbId, System.currentTimeMillis());

        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedNothingMode() {
                return true;
            }

            @Mock
            public boolean isSharedDataMode() {
                return false;
            }
        };

        new Expectations() {
            {
                clusterSnapshotMgr.isDeletionSafeToExecute(anyLong);
                times = 0;
            }
        };

        boolean result = Deencapsulation.invoke(recycleBin, "checkValidDeletionByClusterSnapshot", dbId);
        Assertions.assertTrue(result);
    }

    @Test
    public void testRecycleDb() {
        Database db1 = new Database(111, "uno");
        Database db2SameName = new Database(22, "dos"); // samename
        Database db2 = new Database(222, "dos");

        // 1. recycle 2 dbs
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(db1, new HashSet<>(), true);
        recycleBin.recycleDatabase(db2SameName, new HashSet<>(), true);  // will remove same name
        recycleBin.recycleDatabase(db2, new HashSet<>(), true);

        Assertions.assertEquals(recycleBin.getDatabase(db1.getId()), db1);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(recycleBin.getDatabase(999), null);
        Assertions.assertEquals(2, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());

        // 2. manually set db expire time & recycle db1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(db1.getId(), expireFromNow - 1000);

        recycleBin.eraseDatabase(now);

        Assertions.assertEquals(recycleBin.getDatabase(db1.getId()), null);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assertions.assertFalse(recycleBin.ensureEraseLater(db1.getId(), db1.getId(), now));  // already erased
        Assertions.assertTrue(recycleBin.ensureEraseLater(db2.getId(), db2.getId(), now));
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow + 1000);
        Assertions.assertTrue(recycleBin.ensureEraseLater(db2.getId(), db2.getId(), now));
        Assertions.assertEquals(1, recycleBin.enableEraseLater.size());
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(db2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow - 1000);
        recycleBin.eraseDatabase(now);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow - 11000);
        Assertions.assertFalse(recycleBin.ensureEraseLater(db2.getId(), db2.getId(), now));
        recycleBin.eraseDatabase(now);
        Assertions.assertNull(recycleBin.getDatabase(db2.getId()));
        Assertions.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecycleTableMaxBatchSize() {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        for (int i = 0; i < CatalogRecycleBin.getMaxEraseOperationsPerCycle() + 1; i++) {
            Table t = new Table(i, String.format("t%d", i), Table.TableType.VIEW, null);
            recycleBin.recycleTable(10000L, t, true);
        }
        List<CatalogRecycleBin.RecycleTableInfo> recycleTableInfos = recycleBin.pickTablesToErase(
                System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000 + 1);
        Assertions.assertEquals(CatalogRecycleBin.getMaxEraseOperationsPerCycle(), recycleTableInfos.size());
    }

    @Test
    public void testRecycleTable() {
        Table table1 = new Table(111, "uno", Table.TableType.VIEW, null);
        Table table2SameName = new Table(22, "dos", Table.TableType.VIEW, null);
        Table table2 = new Table(222, "dos", Table.TableType.VIEW, null);

        // 1. add 2 tables
        long dbId = 1;
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleTable(dbId, table1, true);
        recycleBin.recycleTable(dbId, table2SameName, true);
        recycleBin.recycleTable(dbId, table2, true);

        Assertions.assertEquals(recycleBin.getTables(dbId), Arrays.asList(table1, table2SameName, table2));
        Assertions.assertSame(recycleBin.getTable(dbId, table1.getId()), table1);
        Assertions.assertSame(recycleBin.getTable(dbId, table2.getId()), table2);
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(table1.getId()));
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(table2.getId()));

        // 2. manually set table expire time & recycle table1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(table1.getId(), expireFromNow - 1000);
        recycleBin.eraseTable(now);
        waitPartitionClearFinished(recycleBin, table1.getId(), expireFromNow - 1000);

        Assertions.assertEquals(recycleBin.getTables(dbId), List.of(table2));
        Assertions.assertNull(recycleBin.getTable(dbId, table1.getId()));
        Assertions.assertSame(recycleBin.getTable(dbId, table2.getId()), table2);

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assertions.assertFalse(recycleBin.ensureEraseLater(dbId, table1.getId(), now));  // already erased
        Assertions.assertTrue(recycleBin.ensureEraseLater(dbId, table2.getId(), now));
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow + 1000);
        Assertions.assertTrue(recycleBin.ensureEraseLater(dbId, table2.getId(), now));
        Assertions.assertEquals(1, recycleBin.enableEraseLater.size());
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(table2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow - 1000);
        recycleBin.eraseTable(now);
        waitTableToBeDone(recycleBin, table2.getId(), expireFromNow - 1000);
        Assertions.assertEquals(recycleBin.getTable(dbId, table2.getId()), table2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow - 11000);
        Assertions.assertFalse(recycleBin.ensureEraseLater(dbId, table2.getId(), now));
        recycleBin.eraseTable(now);
        waitPartitionClearFinished(recycleBin, table2.getId(), now);
        Assertions.assertNull(recycleBin.getTable(dbId, table2.getId()));
        Assertions.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecyclePartition() {
        Partition p1 = new Partition(111, 112, "uno", new MaterializedIndex(), null);
        Partition p2SameName = new Partition(22, 221, "dos", new MaterializedIndex(), null);
        Partition p2 = new Partition(222, 223, "dos", new MaterializedIndex(), null);

        // 1. add 2 partitions
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null));
        recycleBin.recyclePartition(
                new RecycleRangePartitionInfo(dbId, tableId, p2SameName, null, dataProperty, (short) 2, null));
        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, null));

        Assertions.assertEquals(recycleBin.getPartition(p1.getId()), p1);
        Assertions.assertEquals(recycleBin.getPartition(p2.getId()), p2);
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(p1.getId()));
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(p2.getId()));

        // 2. manually set table expire time & recycle table1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(p1.getId(), expireFromNow - 1000);
        recycleBin.erasePartition(now);
        waitPartitionClearFinished(recycleBin, p1.getId(), now);

        Assertions.assertNull(recycleBin.getPartition(p1.getId()));
        Assertions.assertEquals(recycleBin.getPartition(p2.getId()), p2);

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assertions.assertFalse(recycleBin.ensureEraseLater(dbId, p1.getId(), now));  // already erased
        Assertions.assertTrue(recycleBin.ensureEraseLater(dbId, p2.getId(), now));
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow + 1000);
        Assertions.assertTrue(recycleBin.ensureEraseLater(dbId, p2.getId(), now));
        Assertions.assertEquals(1, recycleBin.enableEraseLater.size());
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(p2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow - 1000);
        recycleBin.erasePartition(now);
        waitPartitionToBeDone(recycleBin, p2.getId(), now);
        Assertions.assertEquals(recycleBin.getPartition(p2.getId()), p2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow - 11000);
        Assertions.assertFalse(recycleBin.ensureEraseLater(dbId, p2.getId(), now));
        recycleBin.erasePartition(now);
        waitPartitionClearFinished(recycleBin, p2.getId(), now);
        Assertions.assertEquals(recycleBin.getPartition(p2.getId()), null);
        Assertions.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testShowCatalogRecycleBinDatabase() {
        Database db1 = new Database(211, "uno");
        Database db2SameName = new Database(32, "dos"); // samename
        Database db2 = new Database(422, "dos");

        // 1. recycle 2 dbs
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(db1, new HashSet<>(), true);
        recycleBin.recycleDatabase(db2SameName, new HashSet<>(), true);  // will remove same name
        recycleBin.recycleDatabase(db2, new HashSet<>(), true);

        Assertions.assertEquals(recycleBin.getDatabase(db1.getId()), db1);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(recycleBin.getDatabase(999), null);
        Assertions.assertEquals(2, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());

        // 2. manually set db expire time & recycle db1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(db1.getId(), expireFromNow - 1000);

        recycleBin.eraseDatabase(now);

        Assertions.assertEquals(recycleBin.getDatabase(db1.getId()), null);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());

        List<List<String>> recyclebininfo = recycleBin.getCatalogRecycleBinInfo();
        Assertions.assertEquals(recyclebininfo.size(), 1);
        String actual = rowsToString(recyclebininfo);
        Assertions.assertTrue(actual.contains("422"));
    }

    @Test
    public void testShowCatalogRecycleBinTable() {
        Table table1 = new Table(111, "uno", Table.TableType.VIEW, null);
        Table table2SameName = new Table(22, "dos", Table.TableType.VIEW, null);
        Table table2 = new Table(222, "dos", Table.TableType.VIEW, null);

        // 1. add 2 tables
        long dbId = 1;
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleTable(dbId, table1, true);
        recycleBin.recycleTable(dbId, table2SameName, true);
        recycleBin.recycleTable(dbId, table2, true);

        Assertions.assertEquals(recycleBin.getTables(dbId), Arrays.asList(table1, table2SameName, table2));
        Assertions.assertSame(recycleBin.getTable(dbId, table1.getId()), table1);
        Assertions.assertSame(recycleBin.getTable(dbId, table2.getId()), table2);
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(table1.getId()));
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(table2.getId()));

        // 2. manually set table expire time & recycle table1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(table1.getId(), expireFromNow - 1000);
        recycleBin.eraseTable(now);

        Assertions.assertEquals(recycleBin.getTables(dbId), List.of(table2));
        Assertions.assertNull(recycleBin.getTable(dbId, table1.getId()));
        Assertions.assertSame(recycleBin.getTable(dbId, table2.getId()), table2);

        List<List<String>> recyclebininfo = recycleBin.getCatalogRecycleBinInfo();
        Assertions.assertEquals(recyclebininfo.size(), 1);
        String actual = rowsToString(recyclebininfo);
        Assertions.assertTrue(actual.contains("222"));        
    }

    @Test
    public void testShowCatalogRecycleBinPartition() {
        Partition p1 = new Partition(111, 112, "uno", new MaterializedIndex(), null);
        Partition p2SameName = new Partition(22, 23, "dos", new MaterializedIndex(), null);
        Partition p2 = new Partition(222, 223, "dos", new MaterializedIndex(), null);

        // 1. add 2 partitions
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null));
        recycleBin.recyclePartition(
                new RecycleRangePartitionInfo(dbId, tableId, p2SameName, null, dataProperty, (short) 2, null));
        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, null));

        Assertions.assertEquals(recycleBin.getPartition(p1.getId()), p1);
        Assertions.assertEquals(recycleBin.getPartition(p2.getId()), p2);
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(p1.getId()));
        Assertions.assertTrue(recycleBin.idToRecycleTime.containsKey(p2.getId()));

        // 2. manually set table expire time & recycle table1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(p1.getId(), expireFromNow - 1000);
        recycleBin.erasePartition(now);

        List<List<String>> recyclebininfo = recycleBin.getCatalogRecycleBinInfo();
        String actual = rowsToString(recyclebininfo);
        Assertions.assertTrue(actual.contains("222"));          
    }

    @Test
    public void testTimeExpiredWithRetentionPeriod() {
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        // Create non-recoverable partition with retention period = 7200 seconds (2 hours)
        Partition p1 = new Partition(101, 102, "p1", new MaterializedIndex(), null);
        RecycleRangePartitionInfo info1 =
                new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null);
        info1.setRecoverable(false);
        info1.setRetentionPeriod(7200);
        recycleBin.recyclePartition(info1);

        // Used to check that `catalog_trash_expire_second` will not take effect while retention period is set
        long defaultTrashExpireSecond = Config.catalog_trash_expire_second;
        Config.catalog_trash_expire_second = 3600; // default 1 hour
        long now = System.currentTimeMillis();

        // Case 1: should not expire after 1.5 hours (less than retention period)
        long recycleTime1 = now - 5400 * 1000L; // 1.5 hours ago
        recycleBin.idToRecycleTime.put(p1.getId(), recycleTime1);
        recycleBin.erasePartition(now);
        // time not expired
        Assertions.assertNotNull(recycleBin.getPartition(p1.getId()));

        // Case 2: should expire after 2.5 hours (exceeds retention period)
        long recycleTime2 = now - 9000 * 1000L; // 2.5 hours ago
        recycleBin.idToRecycleTime.put(p1.getId(), recycleTime2);
        recycleBin.erasePartition(now);
        waitPartitionClearFinished(recycleBin, p1.getId(), now);
        Assertions.assertNull(recycleBin.getPartition(p1.getId()));

        // reset default trash expire second
        Config.catalog_trash_expire_second = defaultTrashExpireSecond;
    }

    @Test
    public void testGetAdjustedRecycleTimestampWithRetentionPeriod() {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);

        // Non-recoverable partition with retention period
        Partition p1 = new Partition(201, 202, "p1", new MaterializedIndex(), null);
        RecycleRangePartitionInfo info1 =
                new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null);
        info1.setRecoverable(false);
        info1.setRetentionPeriod(3600);
        recycleBin.recyclePartition(info1);

        // Non-recoverable partition without retention period
        Partition p2 = new Partition(301, 302, "p2", new MaterializedIndex(), null);
        RecycleRangePartitionInfo info2 =
                new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, null);
        info2.setRecoverable(false);
        recycleBin.recyclePartition(info2);

        // With retention period: should return original recycle timestamp
        long adjustedTime1 = Deencapsulation.invoke(recycleBin, "getAdjustedRecycleTimestamp", dbId, p1.getId());
        Assertions.assertEquals(recycleBin.idToRecycleTime.get(p1.getId()), adjustedTime1);

        // Without retention period: should return 0 for non-recoverable partition
        long adjustedTime2 = Deencapsulation.invoke(recycleBin, "getAdjustedRecycleTimestamp", dbId, p2.getId());
        Assertions.assertEquals(0, adjustedTime2);
    }

    @Test
    public void testGetAdjustedRecycleTimestampUsesDbIdForTableLookup() {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        long dbId = 1L;
        long otherDbId = 2L;
        Table table = new Table(101L, "tbl", Table.TableType.VIEW, null);
        recycleBin.recycleTable(dbId, table, false);

        long adjustedTime = Deencapsulation.invoke(
                recycleBin, "getAdjustedRecycleTimestamp", dbId, table.getId());
        Assertions.assertEquals(0, adjustedTime);

        long adjustedTimeWithOtherDb = Deencapsulation.invoke(
                recycleBin, "getAdjustedRecycleTimestamp", otherDbId, table.getId());
        Assertions.assertEquals(recycleBin.idToRecycleTime.get(table.getId()), adjustedTimeWithOtherDb);
    }

    @Test
    public void testNonRetryableTableErasure() {
        // This test verifies that non-retryable tables (shared-nothing mode) are erased synchronously
        // and do not leak in any tracking data structures (lakeTableToPartitions, etc.)
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        long dbId = 1;

        // Create non-retryable tables (regular tables in shared-nothing mode)
        Table nonRetryableTable1 = new Table(111, "non_retryable_1", Table.TableType.OLAP, Lists.newArrayList());
        Table nonRetryableTable2 = new Table(222, "non_retryable_2", Table.TableType.OLAP, Lists.newArrayList());

        // Recycle non-retryable tables
        recycleBin.recycleTable(dbId, nonRetryableTable1, false);
        recycleBin.recycleTable(dbId, nonRetryableTable2, false);

        // Verify tables are in recycle bin
        Assertions.assertEquals(2, recycleBin.getTables(dbId).size());
        Assertions.assertNotNull(recycleBin.getTable(dbId, nonRetryableTable1.getId()));
        Assertions.assertNotNull(recycleBin.getTable(dbId, nonRetryableTable2.getId()));

        // Set expire time to trigger erasure
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(nonRetryableTable1.getId(), expireFromNow - 1000);
        recycleBin.idToRecycleTime.put(nonRetryableTable2.getId(), expireFromNow - 1000);

        // Trigger table erasure
        recycleBin.eraseTable(now);
        waitTableClearFinished(recycleBin, nonRetryableTable1.getId(), now);
        waitTableClearFinished(recycleBin, nonRetryableTable2.getId(), now);

        // Verify tables are removed from recycle bin
        Assertions.assertNull(recycleBin.getTable(dbId, nonRetryableTable1.getId()));
        Assertions.assertNull(recycleBin.getTable(dbId, nonRetryableTable2.getId()));

        // CRITICAL: Verify non-retryable tables are NOT tracked in lakeTableToPartitions
        // Non-retryable tables should be deleted synchronously without any async tracking
        java.util.Map<?, ?> lakeTableToPartitions =
                Deencapsulation.getField(recycleBin, "lakeTableToPartitions");
        Assertions.assertTrue(lakeTableToPartitions.isEmpty(),
                "lakeTableToPartitions should be empty for non-retryable tables");
    }

    /**
     * Regression test for the bug where disableRecoverPartitionWithSameName() would unconditionally
     * reset idToRecycleTime for already-non-recoverable partitions with retention periods, causing
     * them to become permanently stuck in the recycle bin.
     *
     * Scenario: Multiple non-recoverable partitions with the same name and a retention period are
     * recycled at intervals shorter than the retention period. The old implementation would reset
     * the recycle time of an already-non-recoverable partition each time a new same-name partition
     * was added, effectively restarting the retention clock and preventing erasure.
     */
    @Test
    public void testDisableRecoverPartitionWithSameNameNoClockResetForNonRecoverable() {
        long dbId = 1;
        long tableId = 2;
        long retentionPeriodSec = 1800; // 30 minutes
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        // Step 1: Recycle partition p1 (non-recoverable, with retention period)
        MaterializedIndex baseIndex1 = new MaterializedIndex(100, MaterializedIndex.IndexState.NORMAL);
        Partition p1 = new Partition(1001, 1002, "CALLS_INFO_EVENTS_ONL", baseIndex1, null);
        RecycleRangePartitionInfo info1 =
                new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null);
        info1.setRecoverable(false);
        info1.setRetentionPeriod(retentionPeriodSec);
        recycleBin.recyclePartition(info1);

        // Step 2: Simulate p1 sitting in the recycle bin for a while, then manually push its
        // recycle time back so it would be eligible for erasure after the retention period.
        // In the production scenario, this partition had been in the bin for 17+ hours.
        long simulatedOldRecycleTime = System.currentTimeMillis() - (retentionPeriodSec + 600) * 1000L;
        recycleBin.idToRecycleTime.put(p1.getId(), simulatedOldRecycleTime);

        // Step 3: Recycle a new partition p2 with the SAME name.
        // This triggers disableRecoverPartitionWithSameName().
        // Bug: the old code would reset p1's idToRecycleTime to now, restarting its 30-min clock.
        // Fix: p1 is already non-recoverable, so its recycle time should NOT be reset.
        MaterializedIndex baseIndex2 = new MaterializedIndex(101, MaterializedIndex.IndexState.NORMAL);
        Partition p2 = new Partition(2001, 2002, "CALLS_INFO_EVENTS_ONL", baseIndex2, null);
        RecycleRangePartitionInfo info2 =
                new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, null);
        info2.setRecoverable(false);
        info2.setRetentionPeriod(retentionPeriodSec);
        recycleBin.recyclePartition(info2);

        // Verify: p1's recycle time should NOT have been reset to current time
        long p1RecycleTimeAfter = recycleBin.idToRecycleTime.get(p1.getId());
        Assertions.assertEquals(simulatedOldRecycleTime, p1RecycleTimeAfter,
                "Recycle time of already-non-recoverable partition should not be reset");

        // Verify: p1 is still non-recoverable
        RecyclePartitionInfo p1Info = recycleBin.getRecyclePartitionInfo(p1.getId());
        Assertions.assertFalse(p1Info.isRecoverable());

        // Verify: p1 should be erasable now (its retention period has long expired)
        long now = System.currentTimeMillis();
        recycleBin.erasePartition(now);
        waitPartitionClearFinished(recycleBin, p1.getId(), now);
        Assertions.assertNull(recycleBin.getPartition(p1.getId()),
                "Partition p1 should have been erased since its retention period expired");

        // Verify: p2 should still be in the recycle bin (it was just added)
        Assertions.assertNotNull(
                recycleBin.getPartition(p2.getId()), "Partition p2 should still be in the recycle bin");
    }

    /**
     * Test that disableRecoverPartitionWithSameName processes ALL matching partitions,
     * not just the first one found (removed the `break` statement).
     */
    @Test
    public void testDisableRecoverPartitionWithSameNameProcessesAllMatches() {
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        // Add two recoverable partitions with the same name
        MaterializedIndex baseIndex1 = new MaterializedIndex(101, MaterializedIndex.IndexState.NORMAL);
        Partition p1 = new Partition(3001, 3002, "same_name", baseIndex1, null);
        RecycleRangePartitionInfo info1 =
                new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null);
        info1.setRecoverable(true);
        recycleBin.recyclePartition(info1);

        MaterializedIndex baseIndex2 = new MaterializedIndex(102, MaterializedIndex.IndexState.NORMAL);
        Partition p2 = new Partition(4001, 4002, "same_name", baseIndex2, null);
        RecycleRangePartitionInfo info2 =
                new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, null);
        info2.setRecoverable(true);
        // Note: recycling p2 will call disableRecoverPartitionWithSameName which marks p1 non-recoverable
        recycleBin.recyclePartition(info2);

        // p1 should now be non-recoverable (disabled by p2's recycle)
        RecyclePartitionInfo p1Info = recycleBin.getRecyclePartitionInfo(p1.getId());
        Assertions.assertFalse(
                p1Info.isRecoverable(), "p1 should be marked non-recoverable after p2 with same name was recycled");

        // Now add a third partition p3 with the same name.
        // With the fix (no break), BOTH p1 and p2 should be processed.
        // p1 is already non-recoverable — its recycle time should NOT be reset.
        // p2 is still recoverable — it should be marked non-recoverable.
        long p1RecycleTimeBefore = recycleBin.idToRecycleTime.get(p1.getId());

        MaterializedIndex baseIndex3 = new MaterializedIndex(103, MaterializedIndex.IndexState.NORMAL);
        Partition p3 = new Partition(5001, 5002, "same_name", baseIndex3, null);
        RecycleRangePartitionInfo info3 =
                new RecycleRangePartitionInfo(dbId, tableId, p3, null, dataProperty, (short) 2, null);
        info3.setRecoverable(true);
        recycleBin.recyclePartition(info3);

        // p1: already non-recoverable, recycle time should NOT be changed
        RecyclePartitionInfo p1InfoAfter = recycleBin.getRecyclePartitionInfo(p1.getId());
        Assertions.assertFalse(p1InfoAfter.isRecoverable());
        Assertions.assertEquals(p1RecycleTimeBefore, recycleBin.idToRecycleTime.get(p1.getId()),
                "p1's recycle time should not change since it was already non-recoverable");

        // p2: was recoverable, should now be non-recoverable (processed because no break)
        RecyclePartitionInfo p2Info = recycleBin.getRecyclePartitionInfo(p2.getId());
        Assertions.assertFalse(p2Info.isRecoverable(),
                "p2 should be marked non-recoverable — all matches should be processed, not just the first");

        // p3: just added, should be recoverable
        RecyclePartitionInfo p3Info = recycleBin.getRecyclePartitionInfo(p3.getId());
        Assertions.assertTrue(p3Info.isRecoverable(), "p3 is the newly added partition and should remain recoverable");
    }

    @Test
    public void testConfigMinEraseLatency() {
        long origMinLatency = Config.catalog_recycle_bin_erase_min_latency_ms;
        long origTrashExpire = Config.catalog_trash_expire_second;
        try {
            // Set a very short min erase latency (2 seconds) and short trash expire (1 second)
            Config.catalog_recycle_bin_erase_min_latency_ms = 2000L;
            Config.catalog_trash_expire_second = 1L;

            CatalogRecycleBin recycleBin = new CatalogRecycleBin();
            Database db = new Database(9001, "test_min_latency_db");
            recycleBin.recycleDatabase(db, new HashSet<>(), true);

            long now = System.currentTimeMillis();

            // trash_expire_second (1s) < min_erase_latency (2s), so min_erase_latency should take effect
            // Recycled 1.5 seconds ago: should NOT be erased (within min latency)
            recycleBin.idToRecycleTime.put(db.getId(), now - 1500L);
            recycleBin.eraseDatabase(now);
            Assertions.assertNotNull(recycleBin.getDatabase(db.getId()),
                    "DB should not be erased within min erase latency");

            // Recycled 3 seconds ago: should be erased (exceeds min latency)
            recycleBin.idToRecycleTime.put(db.getId(), now - 3000L);
            recycleBin.eraseDatabase(now);
            Assertions.assertNull(recycleBin.getDatabase(db.getId()),
                    "DB should be erased after min erase latency");
        } finally {
            Config.catalog_recycle_bin_erase_min_latency_ms = origMinLatency;
            Config.catalog_trash_expire_second = origTrashExpire;
        }
    }

    @Test
    public void testConfigMaxEraseOperationsPerCycle() {
        int origMaxOps = Config.catalog_recycle_bin_erase_max_operations_per_cycle;
        try {
            // Set a small batch size
            int batchSize = 10;
            Config.catalog_recycle_bin_erase_max_operations_per_cycle = batchSize;

            CatalogRecycleBin recycleBin = new CatalogRecycleBin();
            for (int i = 0; i < batchSize + 5; i++) {
                Table t = new Table(i, String.format("t%d", i), Table.TableType.VIEW, null);
                recycleBin.recycleTable(10000L, t, true);
            }

            List<CatalogRecycleBin.RecycleTableInfo> recycleTableInfos = recycleBin.pickTablesToErase(
                    System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000 + 1);
            Assertions.assertEquals(batchSize, recycleTableInfos.size(),
                    "Should be capped at configured max erase operations per cycle");

            // Change the config and verify the new value takes effect
            // Re-populate the recycle bin since pickTablesToErase removes non-retryable tables
            int newBatchSize = batchSize + 5;
            Config.catalog_recycle_bin_erase_max_operations_per_cycle = newBatchSize;
            for (int i = 0; i < newBatchSize + 5; i++) {
                Table t = new Table(100 + i, String.format("t%d", 100 + i), Table.TableType.VIEW, null);
                recycleBin.recycleTable(10000L, t, true);
            }
            recycleTableInfos = recycleBin.pickTablesToErase(
                    System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000 + 1);
            Assertions.assertEquals(newBatchSize, recycleTableInfos.size(),
                    "Should reflect dynamically updated config value");
        } finally {
            Config.catalog_recycle_bin_erase_max_operations_per_cycle = origMaxOps;
        }
    }

    @Test
    public void testConfigFailRetryInterval() {
        long origRetryInterval = Config.catalog_recycle_bin_erase_fail_retry_interval_ms;
        try {
            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = 5000L;
            Assertions.assertEquals(5000L, CatalogRecycleBin.getFailRetryInterval(),
                    "getFailRetryInterval should return the configured value");

            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = 120000L;
            Assertions.assertEquals(120000L, CatalogRecycleBin.getFailRetryInterval(),
                    "getFailRetryInterval should reflect runtime config change");
        } finally {
            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = origRetryInterval;
        }
    }

    @Test
    public void testConfigGettersReflectRuntimeChanges() {
        long origMinLatency = Config.catalog_recycle_bin_erase_min_latency_ms;
        int origMaxOps = Config.catalog_recycle_bin_erase_max_operations_per_cycle;
        long origRetryInterval = Config.catalog_recycle_bin_erase_fail_retry_interval_ms;
        try {
            // Verify default values
            Assertions.assertEquals(10L * 60L * 1000L, origMinLatency);
            Assertions.assertEquals(500, origMaxOps);
            Assertions.assertEquals(60L * 1000L, origRetryInterval);

            // Modify configs and verify getters return new values
            Config.catalog_recycle_bin_erase_min_latency_ms = 30000L;
            Config.catalog_recycle_bin_erase_max_operations_per_cycle = 100;
            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = 10000L;

            Assertions.assertEquals(30000L, CatalogRecycleBin.getMinEraseLatency());
            Assertions.assertEquals(100, CatalogRecycleBin.getMaxEraseOperationsPerCycle());
            Assertions.assertEquals(10000L, CatalogRecycleBin.getFailRetryInterval());
        } finally {
            Config.catalog_recycle_bin_erase_min_latency_ms = origMinLatency;
            Config.catalog_recycle_bin_erase_max_operations_per_cycle = origMaxOps;
            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = origRetryInterval;
        }
    }

    @Test
    public void testRemoveTableFromRecycleBinAlsoCleansLakeTableTracking() {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        long dbId = 1L;
        long tableId = 111L;
        long partitionId = 222L;

        // Use a LakeTable (cloud-native OlapTable) with the partition added, so that
        // removeTableFromRecycleBin can find and clean up the partition via getAllPartitions().
        Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
        List<Column> columns = Lists.newArrayList(k1);
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        SinglePartitionInfo pInfo = new SinglePartitionInfo();
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        pInfo.setDataProperty(partitionId, dataProperty);
        pInfo.setReplicationNum(partitionId, (short) 1);
        Partition partition = new Partition(partitionId, partitionId + 1, "p1", new MaterializedIndex(), null);
        LakeTable table = new LakeTable(tableId, "t1", columns, KeysType.AGG_KEYS, pInfo, distributionInfo);
        table.addPartition(partition);
        recycleBin.recycleTable(dbId, table, false);

        RecyclePartitionInfo partitionInfo =
                new RecycleRangePartitionInfo(dbId, tableId, partition, null, dataProperty, (short) 1, null);
        recycleBin.setPartitionInfo(partitionId, partitionInfo);
        recycleBin.idToRecycleTime.put(partitionId, System.currentTimeMillis());
        recycleBin.setDeleteFutureForPartition(partitionInfo, CompletableFuture.completedFuture(true));

        java.util.Map<Long, java.util.Set<Long>> lakeTableToPartitions =
                Deencapsulation.getField(recycleBin, "lakeTableToPartitions");
        lakeTableToPartitions.put(tableId, Sets.newHashSet(partitionId));

        List<CatalogRecycleBin.RecycleTableInfo> removed =
                recycleBin.removeTableFromRecycleBin(Collections.singletonList(tableId));
        Assertions.assertEquals(1, removed.size());
        Assertions.assertEquals(tableId, removed.get(0).getTable().getId());

        Assertions.assertNull(recycleBin.getRecycleTableInfo(tableId));
        Assertions.assertNull(recycleBin.getRecyclePartitionInfo(partitionId));
        Assertions.assertFalse(recycleBin.isContainedInidToRecycleTime(partitionId));
        Assertions.assertFalse(recycleBin.isPartitionFromTableDeletion(partitionId));
        Assertions.assertFalse(recycleBin.isLakeTableDeletingInProgress(tableId));
        Assertions.assertFalse(recycleBin.isDeletingPartition(partitionId));
    }

    @Test
    public void testErasePartitionFailureSetsNextEraseMinTime() {
        long origTrashExpire = Config.catalog_trash_expire_second;
        long origMinLatency = Config.catalog_recycle_bin_erase_min_latency_ms;
        long origRetryInterval = Config.catalog_recycle_bin_erase_fail_retry_interval_ms;
        try {
            Config.catalog_trash_expire_second = 1L;
            Config.catalog_recycle_bin_erase_min_latency_ms = 1000L;
            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = 5000L;

            CatalogRecycleBin recycleBin = new CatalogRecycleBin();
            long dbId = 1;
            long tableId = 2;
            DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);

            // Create a non-recoverable partition
            Partition p1 = new Partition(101, 102, "p1", new MaterializedIndex(), null);
            RecycleRangePartitionInfo info1 =
                    new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, null);
            info1.setRecoverable(false);
            recycleBin.recyclePartition(info1);

            // Set recycle time to be expired
            long now = System.currentTimeMillis();
            recycleBin.idToRecycleTime.put(p1.getId(), now - 10000L);

            // Pre-set a completed future that returns false (simulates async delete failure)
            RecyclePartitionInfo partitionInfo = recycleBin.getRecyclePartitionInfo(p1.getId());
            Assertions.assertNotNull(partitionInfo);
            recycleBin.setDeleteFutureForPartition(partitionInfo, CompletableFuture.completedFuture(false));

            // Call erasePartition - should hit the failure retry path (lines 715-716)
            // which calls setNextEraseMinTime (line 588)
            recycleBin.erasePartition(now);

            // Partition should still exist in recycle bin (not erased due to failure)
            Assertions.assertNotNull(recycleBin.getRecyclePartitionInfo(p1.getId()),
                    "Partition should still be in recycle bin after failed erase");

            // The recycle time should have been adjusted by setNextEraseMinTime
            long adjustedRecycleTime = recycleBin.idToRecycleTime.get(p1.getId());
            Assertions.assertTrue(adjustedRecycleTime > now - 10000L,
                    "Recycle time should have been adjusted forward by setNextEraseMinTime");
        } finally {
            Config.catalog_trash_expire_second = origTrashExpire;
            Config.catalog_recycle_bin_erase_min_latency_ms = origMinLatency;
            Config.catalog_recycle_bin_erase_fail_retry_interval_ms = origRetryInterval;
        }
    }
}
