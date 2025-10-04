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
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

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
        while (recycleBin.isDeletingTable(id)) {
            recycleBin.eraseTable(time);
            try {
                Thread.sleep(100);
            } catch (Exception ignore) {
            }
        }
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

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private VariableMgr variableMgr = new VariableMgr();
    private SessionVariable defSessionVariable = new SessionVariable();

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
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
        FakeEditLog fakeEditLog = new FakeEditLog();

        CatalogRecycleBin bin = new CatalogRecycleBin();
        List<Column> columns = Lists.newArrayList(new Column("k1", ScalarType.createVarcharType(10)));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition, range, dataProperty, (short) 1, false, null));
        Partition partition2 = new Partition(2L, 4L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition2, range, dataProperty, (short) 1, false, null));

        Partition recycledPart = bin.getPartition(1L);
        Assertions.assertNotNull(recycledPart);
        recycledPart = bin.getPartition(2L);
        Assertions.assertEquals(2L, recycledPart.getId());
        Assertions.assertEquals(range, bin.getPartitionRange(2L));
        Assertions.assertEquals(dataProperty, bin.getPartitionDataProperty(2L));
        Assertions.assertEquals((short) 1, bin.getPartitionReplicationNum(2L));
        Assertions.assertFalse(bin.getPartitionIsInMemory(2L));
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
        Partition partition = new Partition(1L, 3L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition, range, dataProperty, (short) 1, false, null));
        Partition partition2 = new Partition(2L, 4L, "pt", new MaterializedIndex(), null);
        bin.recyclePartition(new RecycleRangePartitionInfo(11L, 22L, partition2, range, dataProperty, (short) 1, false, null));

        PhysicalPartition recycledPart = bin.getPhysicalPartition(3L);
        Assertions.assertNotNull(recycledPart);
        recycledPart = bin.getPhysicalPartition(4L);
        Assertions.assertEquals(4L, recycledPart.getId());
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
    public void testReplayEraseTableEx(@Mocked GlobalStateMgr globalStateMgr) {

        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getCurrentState().getEditLog().logEraseMultiTables((List<Long>) any);
                minTimes = 1;
                maxTimes = 1;
                result = null;

                globalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;
            }
        };

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
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
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
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.SSD);
        index.addTablet(tablet, tabletMeta);

        // Partition
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", index, distributionInfo);

        // Table
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
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
        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;
            }
        };

        Config.catalog_trash_expire_second = 600; // set expire in 10 minutes
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        Database db = new Database(111, "uno");
        recycleBin.recycleDatabase(db, new HashSet<>(), true);

        // no need to set enable erase later if there are a lot of time left
        long now = System.currentTimeMillis();
        Assertions.assertTrue(recycleBin.ensureEraseLater(db.getId(), now));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(db.getId()));

        // no need to set enable erase later if already exipre
        long moreThanTenMinutesLater = now + 620 * 1000L;
        Assertions.assertFalse(recycleBin.ensureEraseLater(db.getId(), moreThanTenMinutesLater));
        Assertions.assertFalse(recycleBin.enableEraseLater.contains(db.getId()));

        // now we should set enable erase later because we are about to expire
        long moreThanNineMinutesLater = now + 550 * 1000L;
        Assertions.assertTrue(recycleBin.ensureEraseLater(db.getId(), moreThanNineMinutesLater));
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(db.getId()));

        // if already expired, we should return false but won't erase the flag
        Assertions.assertFalse(recycleBin.ensureEraseLater(db.getId(), moreThanTenMinutesLater));
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(db.getId()));
    }

    @Test
    public void testRecycleDb(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
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

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().onEraseDatabase(anyLong);
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
        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;
            }
        };

        recycleBin.eraseDatabase(now);

        Assertions.assertEquals(recycleBin.getDatabase(db1.getId()), null);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());

        // 3. set recyle later, check if recycle now
        CatalogRecycleBin.LATE_RECYCLE_INTERVAL_SECONDS = 10;
        Assertions.assertFalse(recycleBin.ensureEraseLater(db1.getId(), now));  // already erased
        Assertions.assertTrue(recycleBin.ensureEraseLater(db2.getId(), now));
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow + 1000);
        Assertions.assertTrue(recycleBin.ensureEraseLater(db2.getId(), now));
        Assertions.assertEquals(1, recycleBin.enableEraseLater.size());
        Assertions.assertTrue(recycleBin.enableEraseLater.contains(db2.getId()));

        // 4. won't erase on expire time
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow - 1000);
        recycleBin.eraseDatabase(now);
        Assertions.assertEquals(recycleBin.getDatabase(db2.getId()), db2);
        Assertions.assertEquals(1, recycleBin.idToRecycleTime.size());

        // 5. will erase after expire time + latency time
        recycleBin.idToRecycleTime.put(db2.getId(), expireFromNow - 11000);
        Assertions.assertFalse(recycleBin.ensureEraseLater(db2.getId(), now));
        recycleBin.eraseDatabase(now);
        Assertions.assertNull(recycleBin.getDatabase(db2.getId()));
        Assertions.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecycleTableMaxBatchSize(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 1;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 1;
                maxTimes = 1;
                result = editLog;
            }
        };
        new Expectations() {
            {
                editLog.logEraseMultiTables((List<Long>) any);
                minTimes = 1;
                maxTimes = 1;
                result = null;
            }
        };
        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;
            }
        };
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
                result = null;
            }
        };
        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;
            }
        };

        // 1. add 2 tables
        long dbId = 1;
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleTable(dbId, table1, true);
        recycleBin.recycleTable(dbId, table2SameName, true);
        recycleBin.recycleTable(dbId, table2, true);

        Assertions.assertEquals(Sets.newHashSet(recycleBin.getTables(dbId)), Sets.newHashSet(table1, table2SameName, table2));
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
        Assertions.assertFalse(recycleBin.ensureEraseLater(table1.getId(), now));  // already erased
        Assertions.assertTrue(recycleBin.ensureEraseLater(table2.getId(), now));
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(table2.getId(), expireFromNow + 1000);
        Assertions.assertTrue(recycleBin.ensureEraseLater(table2.getId(), now));
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
        Assertions.assertFalse(recycleBin.ensureEraseLater(table2.getId(), now));
        recycleBin.eraseTable(now);
        waitPartitionClearFinished(recycleBin, table2.getId(), now);
        Assertions.assertNull(recycleBin.getTable(dbId, table2.getId()));
        Assertions.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testRecyclePartition(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog) {
        Partition p1 = new Partition(111, 112, "uno", null, null);
        Partition p2SameName = new Partition(22, 221, "dos", null, null);
        Partition p2 = new Partition(222, 223, "dos", null, null);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().onErasePartition((Partition) any);
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
        ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getClusterSnapshotMgr();
                result = clusterSnapshotMgr;
            }
        };

        // 1. add 2 partitions
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, false, null));
        recycleBin.recyclePartition(
                new RecycleRangePartitionInfo(dbId, tableId, p2SameName, null, dataProperty, (short) 2, false, null));
        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, false, null));

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
        Assertions.assertFalse(recycleBin.ensureEraseLater(p1.getId(), now));  // already erased
        Assertions.assertTrue(recycleBin.ensureEraseLater(p2.getId(), now));
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
        recycleBin.idToRecycleTime.put(p2.getId(), expireFromNow + 1000);
        Assertions.assertTrue(recycleBin.ensureEraseLater(p2.getId(), now));
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
        Assertions.assertFalse(recycleBin.ensureEraseLater(p2.getId(), now));
        recycleBin.erasePartition(now);
        waitPartitionClearFinished(recycleBin, p2.getId(), now);
        Assertions.assertEquals(recycleBin.getPartition(p2.getId()), null);
        Assertions.assertEquals(0, recycleBin.idToRecycleTime.size());
        Assertions.assertEquals(0, recycleBin.enableEraseLater.size());
    }

    @Test
    public void testShowCatalogRecycleBinDatabase(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog,
            @Mocked LocalMetastore localMetaStore, @Mocked ClusterSnapshotMgr clusterSnapshotMgr) {
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

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getClusterSnapshotMgr();
                minTimes = 0;
                result = clusterSnapshotMgr;
            }
        };
        new Expectations(clusterSnapshotMgr) {
            {
                clusterSnapshotMgr.isDeletionSafeToExecute(anyLong);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetaStore;
            }
        };
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        new Expectations() {
            {
                localMetaStore.onEraseDatabase(anyLong);
                minTimes = 0;
            }
        };
        new Expectations() {
            {
                editLog.logEraseDb(anyLong);
                minTimes = 0;
            }
        };
        String tz = "Asia/Shanghai";
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };
        new Expectations() {
            {
                variableMgr.getDefaultSessionVariable();
                minTimes = 0;
                result = defSessionVariable;

                defSessionVariable.getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };
        new Expectations() {
            {
                connectContext.getSessionVariable().getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };

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
    public void testShowCatalogRecycleBinTable(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog,
            @Mocked VariableMgr variableMgr, @Mocked LocalMetastore localMetaStore, 
            @Mocked ClusterSnapshotMgr clusterSnapshotMgr) {
        Table table1 = new Table(111, "uno", Table.TableType.VIEW, null);
        Table table2SameName = new Table(22, "dos", Table.TableType.VIEW, null);
        Table table2 = new Table(222, "dos", Table.TableType.VIEW, null);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getClusterSnapshotMgr();
                minTimes = 0;
                result = clusterSnapshotMgr;
            }
        };
        new Expectations(clusterSnapshotMgr) {
            {
                clusterSnapshotMgr.isDeletionSafeToExecute(anyLong);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetaStore;
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
                result = null;
            }
        };
        String tz = "Asia/Shanghai";
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };
        new Expectations() {
            {
                variableMgr.getDefaultSessionVariable();
                minTimes = 0;
                result = defSessionVariable;

                defSessionVariable.getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };
        new Expectations() {
            {
                connectContext.getSessionVariable().getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };

        // 1. add 2 tables
        long dbId = 1;
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleTable(dbId, table1, true);
        recycleBin.recycleTable(dbId, table2SameName, true);
        recycleBin.recycleTable(dbId, table2, true);

        Assertions.assertEquals(Sets.newHashSet(recycleBin.getTables(dbId)), Sets.newHashSet(table1, table2SameName, table2));
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
    public void testShowCatalogRecycleBinPartition(@Mocked GlobalStateMgr globalStateMgr, @Mocked EditLog editLog,
            @Mocked LocalMetastore localMetaStore, @Mocked ClusterSnapshotMgr clusterSnapshotMgr) {
        Partition p1 = new Partition(111, 112, "uno", null, null);
        Partition p2SameName = new Partition(22, 23, "dos", null, null);
        Partition p2 = new Partition(222, 223, "dos", null, null);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getClusterSnapshotMgr();
                minTimes = 0;
                result = clusterSnapshotMgr;
            }
        };
        new Expectations(clusterSnapshotMgr) {
            {
                clusterSnapshotMgr.isDeletionSafeToExecute(anyLong);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetaStore;
            }
        };
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
        new Expectations() {
            {
                localMetaStore.onEraseDatabase(anyLong);
                minTimes = 0;
            }
        };
        new Expectations() {
            {
                editLog.logErasePartition(anyLong);
                minTimes = 0;
            }
        };
        String tz = "Asia/Shanghai";
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };
        new Expectations() {
            {
                variableMgr.getDefaultSessionVariable();
                minTimes = 0;
                result = defSessionVariable;

                defSessionVariable.getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };

        new Expectations() {
            {
                connectContext.getSessionVariable().getTimeZone();
                minTimes = 0;
                result = tz;
            }
        };

        // 1. add 2 partitions
        long dbId = 1;
        long tableId = 2;
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();

        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p1, null, dataProperty, (short) 2, false, null));
        recycleBin.recyclePartition(
                new RecycleRangePartitionInfo(dbId, tableId, p2SameName, null, dataProperty, (short) 2, false, null));
        recycleBin.recyclePartition(new RecycleRangePartitionInfo(dbId, tableId, p2, null, dataProperty, (short) 2, false, null));

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
}
