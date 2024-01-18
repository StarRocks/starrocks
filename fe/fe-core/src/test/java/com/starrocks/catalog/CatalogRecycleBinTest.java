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
import com.starrocks.common.conf.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
        Assert.assertEquals("db", recycledDb.getOriginName());

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
        bin.recyclePartition(11L, 22L, partition, range, dataProperty, (short) 1, false, null);
        Partition partition2 = new Partition(2L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition2, range, dataProperty, (short) 1, false, null);

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
    public void testGetPhysicalPartition() throws Exception {
        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", ScalarType.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition, range, dataProperty, (short) 1, false, null);
        Partition partition2 = new Partition(2L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(11L, 22L, partition2, range, dataProperty, (short) 1, false, null);

        PhysicalPartition recycledPart = bin.getPhysicalPartition(1L);
        Assert.assertNull(recycledPart);
        recycledPart = bin.getPartition(2L);
        Assert.assertEquals(2L, recycledPart.getId());
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
        Assert.assertFalse(tabletMeta1.isLakeTablet());
        Assert.assertEquals(TStorageMedium.SSD, tabletMeta1.getStorageMedium());
        Assert.assertEquals(replica1, invertedIndex.getReplica(tabletId, backendId));
        Assert.assertEquals(replica2, invertedIndex.getReplica(tabletId, backendId + 1));
        Assert.assertEquals(replica3, invertedIndex.getReplica(tabletId, backendId + 2));
    }


    @Test
    public void testAddTabletToInvertedIndexWithLocalTabletError(@Mocked GlobalStateMgr globalStateMgr,
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
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);

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
    }

    @Test
    public void testEnsureEraseLater() {
        Config.catalog_trash_expire_second = 600; // set expire in 10 minutes
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        Database db = new Database(111, "uno");
        recycleBin.recycleDatabase(db, new HashSet<>());

        // no need to set enable erase later if there are a lot of time left
        long now = System.currentTimeMillis();
        Assert.assertTrue(recycleBin.ensureEraseLater(db.getId(), now));
        Assert.assertFalse(recycleBin.enableEraseLater.contains(db.getId()));

        // no need to set enable erase later if already exipre
        long moreThanTenMinutesLater = now + 620 * 1000L;
        Assert.assertFalse(recycleBin.ensureEraseLater(db.getId(), moreThanTenMinutesLater));
        Assert.assertFalse(recycleBin.enableEraseLater.contains(db.getId()));

        // now we should set enable erase later because we are about to expire
        long moreThanNineMinutesLater = now + 550 * 1000L;
        Assert.assertTrue(recycleBin.ensureEraseLater(db.getId(), moreThanNineMinutesLater));
        Assert.assertTrue(recycleBin.enableEraseLater.contains(db.getId()));

        // if already expired, we should return false but won't erase the flag
        Assert.assertFalse(recycleBin.ensureEraseLater(db.getId(), moreThanTenMinutesLater));
        Assert.assertTrue(recycleBin.enableEraseLater.contains(db.getId()));
    }

    @Test
    public void testRecycleDb(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        Database db1 = new Database(111, "uno");
        Database db2SameName = new Database(22, "dos"); // samename
        Database db2 = new Database(222, "dos");

        // 1. recycle 2 dbs
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(db1, new HashSet<>());
        recycleBin.recycleDatabase(db2SameName, new HashSet<>());  // will remove same name
        recycleBin.recycleDatabase(db2, new HashSet<>());

        Assert.assertEquals(recycleBin.getDatabase(db1.getId()), db1);
        Assert.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assert.assertEquals(recycleBin.getDatabase(999), null);
        Assert.assertEquals(2, recycleBin.idToRecycleTime.size());
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());

        // 2. manually set db expire time & recycle db1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(db1.getId(), expireFromNow - 1000);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.onEraseDatabase(anyLong);
                minTimes = 0;
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        new Expectations() {
            {
                editLog.logEraseDb(anyLong);
                minTimes = 0;
            }
        };

        recycleBin.eraseDatabase(now);

        Assert.assertEquals(recycleBin.getDatabase(db1.getId()), null);
        Assert.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assert.assertEquals(1, recycleBin.idToRecycleTime.size());
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assert.assertFalse(recycleBin.ensureEraseLater(db1.getId(), now));  // already erased
        Assert.assertTrue(recycleBin.ensureEraseLater(db2.getId(), now));
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow + 1000);
        Assert.assertTrue(recycleBin.ensureEraseLater(db2.getId(), now));
        Assert.assertEquals(1, recycleBin.enableEraseLater.size());
        Assert.assertTrue(recycleBin.enableEraseLater.contains(db2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow - 1000);
        recycleBin.eraseDatabase(now);
        Assert.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assert.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow - 11000);
        Assert.assertFalse(recycleBin.ensureEraseLater(db2.getId(), now));
        recycleBin.eraseDatabase(now);
        Assert.assertEquals(recycleBin.getDatabase(db2.getId()), null);
        Assert.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecycleTable(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        Table table1 = new Table(111, "uno", Table.TableType.VIEW, null);
        Table table2SameName = new Table(22, "dos", Table.TableType.VIEW, null);
        Table table2 = new Table(222, "dos", Table.TableType.VIEW, null);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        new Expectations() {
            {
                editLog.logEraseMultiTables((List<Long>) any);
                minTimes = 0;
            }
        };

        // 1. add 2 tables
        long dbId = 1;
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleTable(dbId, table1);
        recycleBin.recycleTable(dbId, table2SameName);
        recycleBin.recycleTable(dbId, table2);

        Assert.assertEquals(recycleBin.getTables(dbId), Arrays.asList(table1, table2));
        Assert.assertEquals(recycleBin.getTable(dbId, table1.getId()), table1);
        Assert.assertEquals(recycleBin.getTable(dbId, table2.getId()), table2);
        Assert.assertTrue(recycleBin.idToRecycleTime.containsKey(table1.getId()));
        Assert.assertTrue(recycleBin.idToRecycleTime.containsKey(table2.getId()));

        // 2. manually set table expire time & recycle table1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(table1.getId(), expireFromNow - 1000);
        recycleBin.eraseTable(now);

        Assert.assertEquals(recycleBin.getTables(dbId), Arrays.asList(table2));
        Assert.assertEquals(recycleBin.getTable(dbId, table1.getId()), null);
        Assert.assertEquals(recycleBin.getTable(dbId, table2.getId()), table2);

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assert.assertFalse(recycleBin.ensureEraseLater(table1.getId(), now));  // already erased
        Assert.assertTrue(recycleBin.ensureEraseLater(table2.getId(), now));
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow + 1000);
        Assert.assertTrue(recycleBin.ensureEraseLater(table2.getId(), now));
        Assert.assertEquals(1, recycleBin.enableEraseLater.size());
        Assert.assertTrue(recycleBin.enableEraseLater.contains(table2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow - 1000);
        recycleBin.eraseTable(now);
        Assert.assertEquals(recycleBin.getTable(dbId, table2.getId()), table2);
        Assert.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow - 11000);
        Assert.assertFalse(recycleBin.ensureEraseLater(table2.getId(), now));
        recycleBin.eraseTable(now);
        Assert.assertEquals(recycleBin.getTable(dbId, table2.getId()), null);
        Assert.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecyclePartition(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        Partition p1 = new Partition(111, "uno", null, null);
        Partition p2SameName = new Partition(22, "dos", null, null);
        Partition p2 = new Partition(222, "dos", null, null);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.onErasePartition((Partition) any);
                minTimes = 0;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        new Expectations() {
            {
                editLog.logErasePartition(anyLong);
                minTimes = 0;
            }
        };

        // 1. add 2 partitions
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        recycleBin.recyclePartition(dbId, tableId, p1, null, dataProperty, (short) 2, false, null);
        recycleBin.recyclePartition(dbId, tableId, p2SameName, null, dataProperty, (short) 2, false, null);
        recycleBin.recyclePartition(dbId, tableId, p2, null, dataProperty, (short) 2, false, null);

        Assert.assertEquals(recycleBin.getPartition(p1.getId()), p1);
        Assert.assertEquals(recycleBin.getPartition(p2.getId()), p2);
        Assert.assertTrue(recycleBin.idToRecycleTime.containsKey(p1.getId()));
        Assert.assertTrue(recycleBin.idToRecycleTime.containsKey(p2.getId()));

        // 2. manually set table expire time & recycle table1
        Config.catalog_trash_expire_second = 3600;
        long now = System.currentTimeMillis();
        long expireFromNow = now - 3600 * 1000L;
        recycleBin.idToRecycleTime.put(p1.getId(), expireFromNow - 1000);
        recycleBin.erasePartition(now);

        Assert.assertEquals(recycleBin.getPartition(p1.getId()), null);
        Assert.assertEquals(recycleBin.getPartition(p2.getId()), p2);

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assert.assertFalse(recycleBin.ensureEraseLater(p1.getId(), now));  // already erased
        Assert.assertTrue(recycleBin.ensureEraseLater(p2.getId(), now));
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow + 1000);
        Assert.assertTrue(recycleBin.ensureEraseLater(p2.getId(), now));
        Assert.assertEquals(1, recycleBin.enableEraseLater.size());
        Assert.assertTrue(recycleBin.enableEraseLater.contains(p2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow - 1000);
        recycleBin.erasePartition(now);
        Assert.assertEquals(recycleBin.getPartition(p2.getId()), p2);
        Assert.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow - 11000);
        Assert.assertFalse(recycleBin.ensureEraseLater(p2.getId(), now));
        recycleBin.erasePartition(now);
        Assert.assertEquals(recycleBin.getPartition(p2.getId()), null);
        Assert.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assert.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecyclePartitionForLakeTable(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        Partition p1 = new Partition(111, "uno", null, null);
        Partition p2SameName = new Partition(22, "dos", null, null);
        Partition p2 = new Partition(222, "dos", null, null);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.onErasePartition((Partition) any);
                minTimes = 0;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        new Expectations() {
            {
                editLog.logErasePartition(anyLong);
                minTimes = 0;
            }
        };

        // 1. add 2 partitions
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        recycleBin.recyclePartition(dbId, tableId, p1, null, dataProperty, (short) 2, false, null);
        recycleBin.recyclePartition(dbId, tableId, p2SameName, null, dataProperty, (short) 2, false, null);
        recycleBin.recyclePartition(dbId, tableId, p2, null, dataProperty, (short) 2, false, null);

        Assert.assertEquals(recycleBin.getPartition(p1.getId()), p1);
        Assert.assertEquals(recycleBin.getPartition(p2.getId()), p2);
        Assert.assertTrue(recycleBin.idToRecycleTime.containsKey(p1.getId()));
        Assert.assertTrue(recycleBin.idToRecycleTime.containsKey(p2.getId()));
    }
}
