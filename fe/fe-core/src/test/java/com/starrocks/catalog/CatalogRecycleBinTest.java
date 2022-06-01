// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.catalog.lake.LakeTablet;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CatalogRecycleBinTest {

    @Test
    public void testGetDb() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Database database = new Database(1, "db");
        bin.recycleDatabase(database, Sets.newHashSet());
        Database database2 = new Database(2, "db");
        bin.recycleDatabase(database2, Sets.newHashSet());

        Database recycledDb = bin.getDatabase(1);
        Assert.assertNull(recycledDb);
        recycledDb = bin.getDatabase(2);
        Assert.assertEquals(2L, recycledDb.getId());
        Assert.assertEquals("db", recycledDb.getFullName());

        List<Long> dbIds = bin.getAllDbIds();
        Assert.assertEquals(Lists.newArrayList(2L), dbIds);
    }

    @Test
    public void testGetTable() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11L, table);
        Table table2 = new Table(2L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11L, table2);

        Table recycledTable = bin.getTable(11L, 1L);
        Assert.assertNull(recycledTable);
        recycledTable = bin.getTable(11L, 2L);
        Assert.assertEquals(2L, recycledTable.getId());

        List<Table> tables = bin.getTables(11L);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(2L, tables.get(0).getId());
    }

    @Test
    public void testGetPartition() throws Exception {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", ScalarType.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition, range, dataProperty, (short) 1, false);
        Partition partition2 = new Partition(2L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition2, range, dataProperty, (short) 1, false);

        Partition recycledPart = bin.getPartition(1L);
        Assert.assertNull(recycledPart);
        recycledPart = bin.getPartition(2L);
        Assert.assertEquals(2L, recycledPart.getId());
        Assert.assertEquals(range, bin.getPartitionRange(2L));
        Assert.assertEquals(dataProperty, bin.getPartitionDataProperty(2L));
        Assert.assertEquals((short) 1, bin.getPartitionReplicationNum(2L));
        Assert.assertFalse(bin.getPartitionIsInMemory(2L));

        List<Partition> partitions = bin.getPartitions(22L);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(2L, partitions.get(0).getId());
    }

    @Test
    public void testReplayEraseTable() {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11, table);
        bin.recycleTable(12, table);

        List<Table> tables = bin.getTables(11L);
        Assert.assertEquals(1, tables.size());

        bin.replayEraseTable(2);
        tables = bin.getTables(11);
        Assert.assertEquals(1, tables.size());

        bin.replayEraseTable(1);
        tables = bin.getTables(11);
        Assert.assertEquals(0, tables.size());
    }

    @Test
    public void testReplayEraseTableEx(@Mocked GlobalStateMgr globalStateMgr) {

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getEditLog().logEraseMultiTables((List<Long>) any);
                minTimes = 0;
                result = null;
            }
        };

        CatalogRecycleBin bin = new CatalogRecycleBin();
        Table table = new Table(1L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(11, table);
        Table table2 = new Table(2L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(12, table2);
        Table table3 = new Table(3L, "tbl", Table.TableType.HIVE, Lists.newArrayList());
        bin.recycleTable(13, table3);

        bin.eraseTable(System.currentTimeMillis() + Config.catalog_trash_expire_second * 1000L + 10000);

        Assert.assertEquals(0, bin.getTables(11L).size());
        Assert.assertEquals(0, bin.getTables(12L).size());
        Assert.assertEquals(0, bin.getTables(13L).size());
    }

    @Test
    public void testAddTabletToInvertedIndexWithLocalTablet(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked Database db) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tabletId = 5L;
        long replicaId = 10L;
        long backendId = 20L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

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
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);
        partition.setPartitionInfo(partitionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentInvertedIndex();
                result = invertedIndex;
            }
        };

        CatalogRecycleBin bin = new CatalogRecycleBin();
        bin.recycleTable(dbId, table);
        bin.addTabletToInvertedIndex();

        // Check
        TabletMeta tabletMeta1 = invertedIndex.getTabletMeta(tabletId);
        Assert.assertTrue(tabletMeta1 != null);
        Assert.assertFalse(tabletMeta1.isUseStarOS());
        Assert.assertEquals(TStorageMedium.SSD, tabletMeta1.getStorageMedium());
        Assert.assertEquals(replica1, invertedIndex.getReplica(tabletId, backendId));
        Assert.assertEquals(replica2, invertedIndex.getReplica(tabletId, backendId + 1));
        Assert.assertEquals(replica3, invertedIndex.getReplica(tabletId, backendId + 2));
    }

    @Test
    public void testAddTabletToInvertedIndexWithLakeTablet(@Mocked GlobalStateMgr globalStateMgr,
                                                           @Mocked Database db) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new LakeTablet(tablet1Id, 0L);
        Tablet tablet2 = new LakeTablet(tablet2Id, 1L);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.S3));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        index.setUseStarOS(partitionInfo.isUseStarOS(partitionId));
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.S3);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);
        partition.setPartitionInfo(partitionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentInvertedIndex();
                result = invertedIndex;
            }
        };

        CatalogRecycleBin bin = new CatalogRecycleBin();
        bin.recycleTable(dbId, table);
        bin.addTabletToInvertedIndex();

        // Check
        TabletMeta tabletMeta1 = invertedIndex.getTabletMeta(tablet1Id);
        Assert.assertTrue(tabletMeta1 != null);
        Assert.assertTrue(tabletMeta1.isUseStarOS());
        Assert.assertEquals(TStorageMedium.S3, tabletMeta1.getStorageMedium());
    }
}
