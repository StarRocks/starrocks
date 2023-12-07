// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.clone;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.clone.DiskAndTabletLoadReBalancer.BackendBalanceState;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiskAndTabletLoadReBalancerTest {

    /**
     * init state:
     * one partition with 6 tablets, 1 replica number
     * 3 tablets on be1: t1, t2, t3
     * 3 tablets on be2: t4, t5, t6
     * 0 tablets on be3
     * <p>
     * expect state:
     * 2 tablets on be1
     * 2 tablets on be2
     * 2 tablets on be3
     * two tablets moved to be3, one from be1 and the other from be2
     */
    @Test
    public void testBalance(@Mocked GlobalStateMgr globalStateMgr) {
        // system info
        long dbId = 10001L;
        long tableId = 10002L;
        long partitionId = 10003L;
        long indexId = 10004L;
        long tabletDataSize = 200 * 1024 * 1024L;
        TStorageMedium medium = TStorageMedium.HDD;
        long beId1 = 1L;
        long beId2 = 2L;
        long beId3 = 3L;
        long pathHash1 = 1111L;
        long pathHash2 = 2222L;
        long pathHash3 = 3333L;

        SystemInfoService infoService = new SystemInfoService();

        infoService.addBackend(genBackend(beId1, "host1", 2 * tabletDataSize,
                3 * tabletDataSize, 5 * tabletDataSize, pathHash1));

        infoService.addBackend(genBackend(beId2, "host2", 2 * tabletDataSize,
                3 * tabletDataSize, 5 * tabletDataSize, pathHash2));

        infoService.addBackend(genBackend(beId3, "host3", 5 * tabletDataSize,
                0, 5 * tabletDataSize, pathHash3));

        // tablet inverted index
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20001L,
                30001L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20002L,
                30002L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20003L,
                30003L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20004L,
                30004L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20005L,
                30005L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20006L,
                30006L, beId2,
                tabletDataSize, pathHash2);

        ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(infoService, invertedIndex);
        clusterLoadStatistic.init();

        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty = new DataProperty(medium);
        partitionInfo.addPartition(partitionId, dataProperty, (short) 1, false);
        DistributionInfo distributionInfo = new HashDistributionInfo(6, Lists.newArrayList());
        Partition partition = new Partition(partitionId, "partition", materializedIndex, distributionInfo);
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.createTable(table);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getDbIdsIncludeRecycleBin();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getDbIncludeRecycleBin(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getTableIncludeRecycleBin((Database) any, anyLong);
                result = table;
                minTimes = 0;

                globalStateMgr.getTablesIncludeRecycleBin((Database) any);
                result = Lists.newArrayList(table);
                minTimes = 0;

                globalStateMgr.getPartitionIncludeRecycleBin((OlapTable) any, anyLong);
                result = partition;
                minTimes = 0;

                globalStateMgr.getAllPartitionsIncludeRecycleBin((OlapTable) any);
                result = Lists.newArrayList(partition);
                minTimes = 0;

                globalStateMgr.getReplicationNumIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = (short) 1;
                minTimes = 0;

                globalStateMgr.getDataPropertyIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = dataProperty;
                minTimes = 0;
            }
        };

        Rebalancer rebalancer = new DiskAndTabletLoadReBalancer(infoService, invertedIndex);
        rebalancer.updateLoadStatistic(clusterLoadStatistic);

        List<TabletSchedCtx> tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(2, tablets.size());
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getDestBackendId() == beId3)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId2)));

        // set Config.balance_load_disk_safe_threshold to 0.9 to trigger tablet balance
        Config.tablet_sched_balance_load_disk_safe_threshold = 0.9;
        Config.storage_usage_soft_limit_reserve_bytes = 1;
        tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(2, tablets.size());
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getDestBackendId() == beId3)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId2)));

        // set table state to schema_change, balance should be ignored
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assert.assertEquals(0, rebalancer.selectAlternativeTablets().size());
    }

    /**
     * init state:
     * 1 partition with 3 tablet, 3 replica number
     * 3 tablets on be1: t1, t2, t3
     * 3 tablets on be2: t1, t2, t3
     * 3 tablets on be3: t1, t2, t3
     * 0 tablets on be4:
     * be4 and be1 are on same host
     * <p>
     * expect state:
     * nothing changed
     */
    @Test
    public void testBalanceWithSameHost(@Mocked GlobalStateMgr globalStateMgr) {
        // system info
        long dbId = 10001L;
        long tableId = 10002L;
        long partitionId = 10003L;
        long indexId = 10004L;
        long tabletDataSize = 200 * 1024 * 1024L;
        TStorageMedium medium = TStorageMedium.HDD;
        long beId1 = 1L;
        long beId2 = 2L;
        long beId3 = 3L;
        long beId4 = 4L;
        long pathHash1 = 1111L;
        long pathHash2 = 2222L;
        long pathHash3 = 3333L;
        long pathHash4 = 4444L;

        SystemInfoService infoService = new SystemInfoService();

        infoService.addBackend(genBackend(beId1, "host1", 2 * tabletDataSize,
                3 * tabletDataSize, 5 * tabletDataSize, pathHash1));

        infoService.addBackend(genBackend(beId2, "host2", 2 * tabletDataSize,
                3 * tabletDataSize, 5 * tabletDataSize, pathHash2));

        infoService.addBackend(genBackend(beId3, "host3", 2 * tabletDataSize,
                3 * tabletDataSize, 5 * tabletDataSize, pathHash3));

        infoService.addBackend(genBackend(beId4, "host1", 5 * tabletDataSize,
                0, 5 * tabletDataSize, pathHash3));

        // tablet inverted index
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20001L,
                30001L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20002L,
                30002L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20003L,
                30003L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20001L,
                30004L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20002L,
                30005L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20003L,
                30006L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20001L,
                30007L, beId3,
                tabletDataSize, pathHash3);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20002L,
                30008L, beId3,
                tabletDataSize, pathHash3);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId, indexId, 20003L,
                30009L, beId3,
                tabletDataSize, pathHash3);

        ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(infoService, invertedIndex);
        clusterLoadStatistic.init();

        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty = new DataProperty(medium);
        partitionInfo.addPartition(partitionId, dataProperty, (short) 3, false);
        DistributionInfo distributionInfo = new HashDistributionInfo(3, Lists.newArrayList());
        Partition partition = new Partition(partitionId, "partition", materializedIndex, distributionInfo);
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.createTable(table);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getDbIdsIncludeRecycleBin();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getDbIncludeRecycleBin(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getTableIncludeRecycleBin((Database) any, anyLong);
                result = table;
                minTimes = 0;

                globalStateMgr.getTablesIncludeRecycleBin((Database) any);
                result = Lists.newArrayList(table);
                minTimes = 0;

                globalStateMgr.getPartitionIncludeRecycleBin((OlapTable) any, anyLong);
                result = partition;
                minTimes = 0;

                globalStateMgr.getPartitionsIncludeRecycleBin((OlapTable) any);
                result = Lists.newArrayList(partition);
                minTimes = 0;

                globalStateMgr.getReplicationNumIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = (short) 1;
                minTimes = 0;

                globalStateMgr.getDataPropertyIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = dataProperty;
                minTimes = 0;
            }
        };

        Rebalancer rebalancer = new DiskAndTabletLoadReBalancer(infoService, invertedIndex);
        rebalancer.updateLoadStatistic(clusterLoadStatistic);

        List<TabletSchedCtx> tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(0, tablets.size());

        // set Config.balance_load_disk_safe_threshold to 0.9 to trigger tablet balance
        Config.tablet_sched_balance_load_disk_safe_threshold = 0.9;
        Config.storage_usage_soft_limit_reserve_bytes = 1;
        tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(0, tablets.size());
    }

    /**
     * init state:
     * partition 1 with 5 tablets, 1 replica number, hdd disk
     * 2 tablets on be1 data10: t1, t2
     * 1 tablets on be1 data11: t3
     * 0 tablets on be1 data12:
     * 2 tablets on be2 data20: t4, t5
     * <p>
     * partition 2 with 5 tablets, 1 replica number, ssd disk
     * 2 tablets on be1 data13: t6, t7
     * 0 tablets on be1 data14:
     * 3 tablets on be2 data21: t8, t9, t10
     * <p>
     * expect state:
     * 1 tablets on be1 data10, data11, data12
     * 1 tablets on be1 data13, data14
     * 2 tablets on be2 data20
     * 3 tablets on be2 data21
     * <p>
     * 1 tablet moved from be1 data10 to data12
     * 1 tablet moved from be1 data13 to data14
     */
    @Test
    public void testBalanceBackendTablet(@Mocked GlobalStateMgr globalStateMgr) {
        // system info
        long dbId = 10001L;
        long tableId = 10002L;
        long partitionId1 = 10010L;
        long partitionId2 = 10011L;
        long indexId = 10003L;
        long tabletDataSize = 200 * 1024 * 1024L;
        long beId1 = 1L;
        long beId2 = 2L;
        long pathHash10 = 10L;
        long pathHash11 = 11L;
        long pathHash12 = 12L;
        long pathHash13 = 13L;
        long pathHash14 = 14L;
        long pathHash20 = 20L;
        long pathHash21 = 21L;

        Backend be1 = genBackend(beId1, "host1", 2 * tabletDataSize,
                2 * tabletDataSize, 4 * tabletDataSize, pathHash10);
        DiskInfo disk10 = genDiskInfo(2 * tabletDataSize, 2 * tabletDataSize,
                4 * tabletDataSize, "/data10", pathHash10, TStorageMedium.HDD);
        DiskInfo disk11 = genDiskInfo(3 * tabletDataSize, 1 * tabletDataSize,
                4 * tabletDataSize, "/data11", pathHash11, TStorageMedium.HDD);
        DiskInfo disk12 = genDiskInfo(4 * tabletDataSize, 0 * tabletDataSize,
                4 * tabletDataSize, "/data12", pathHash12, TStorageMedium.HDD);
        DiskInfo disk13 = genDiskInfo(2 * tabletDataSize, 2 * tabletDataSize,
                4 * tabletDataSize, "/data13", pathHash13, TStorageMedium.SSD);
        DiskInfo disk14 = genDiskInfo(4 * tabletDataSize, 0 * tabletDataSize,
                4 * tabletDataSize, "/data14", pathHash14, TStorageMedium.SSD);
        Map<String, DiskInfo> diskInfoMap1 = Maps.newHashMap();
        diskInfoMap1.put(disk10.getRootPath(), disk10);
        diskInfoMap1.put(disk11.getRootPath(), disk11);
        diskInfoMap1.put(disk12.getRootPath(), disk12);
        diskInfoMap1.put(disk13.getRootPath(), disk13);
        diskInfoMap1.put(disk14.getRootPath(), disk14);
        be1.setDisks(ImmutableMap.copyOf(diskInfoMap1));

        Backend be2 = genBackend(beId2, "host2", 6 * tabletDataSize,
                2 * tabletDataSize, 8 * tabletDataSize, pathHash20);
        DiskInfo disk20 = genDiskInfo(6 * tabletDataSize, 2 * tabletDataSize,
                8 * tabletDataSize, "/data20", pathHash20, TStorageMedium.HDD);
        DiskInfo disk21 = genDiskInfo(9 * tabletDataSize, 3 * tabletDataSize,
                12 * tabletDataSize, "/data21", pathHash21, TStorageMedium.SSD);
        Map<String, DiskInfo> diskInfoMap2 = Maps.newHashMap();
        diskInfoMap2.put(disk20.getRootPath(), disk20);
        diskInfoMap2.put(disk21.getRootPath(), disk21);
        be2.setDisks(ImmutableMap.copyOf(diskInfoMap2));

        SystemInfoService infoService = new SystemInfoService();
        infoService.addBackend(be1);
        infoService.addBackend(be2);

        // tablet inverted index
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId1, indexId,
                20001L, 30001L, beId1, tabletDataSize, pathHash10);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId1, indexId,
                20002L, 30002L, beId1, tabletDataSize, pathHash10);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId1, indexId,
                20003L, 30003L, beId1, tabletDataSize, pathHash11);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId1, indexId,
                20004L, 30004L, beId2, tabletDataSize, pathHash20);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId, partitionId1, indexId,
                20005L, 30005L, beId2, tabletDataSize, pathHash20);

        addTablet(invertedIndex, materializedIndex, TStorageMedium.SSD, dbId, tableId, partitionId2, indexId,
                20006L, 30006L, beId1, tabletDataSize, pathHash13);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.SSD, dbId, tableId, partitionId2, indexId,
                20007L, 30007L, beId1, tabletDataSize, pathHash13);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.SSD, dbId, tableId, partitionId2, indexId,
                20008L, 30008L, beId2, tabletDataSize, pathHash21);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.SSD, dbId, tableId, partitionId2, indexId,
                20009L, 30009L, beId2, tabletDataSize, pathHash21);
        addTablet(invertedIndex, materializedIndex, TStorageMedium.SSD, dbId, tableId, partitionId2, indexId,
                20010L, 30010L, beId2, tabletDataSize, pathHash21);

        ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(infoService, invertedIndex);
        clusterLoadStatistic.init();

        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty1 = new DataProperty(TStorageMedium.HDD);
        partitionInfo.addPartition(partitionId1, dataProperty1, (short) 1, false);
        DataProperty dataProperty2 = new DataProperty(TStorageMedium.SSD);
        partitionInfo.addPartition(partitionId2, dataProperty2, (short) 1, false);
        DistributionInfo distributionInfo = new HashDistributionInfo(6, Lists.newArrayList());
        Partition partition1 = new Partition(partitionId1, "partition1", materializedIndex, distributionInfo);
        Partition partition2 = new Partition(partitionId2, "partition2", materializedIndex, distributionInfo);
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition1);
        table.addPartition(partition2);
        Database database = new Database(dbId, "database");
        database.createTable(table);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getDbIdsIncludeRecycleBin();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getDbIncludeRecycleBin(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getTableIncludeRecycleBin((Database) any, anyLong);
                result = table;
                minTimes = 0;

                globalStateMgr.getTablesIncludeRecycleBin((Database) any);
                result = Lists.newArrayList(table);
                minTimes = 0;

                globalStateMgr.getPartitionIncludeRecycleBin((OlapTable) any, partitionId1);
                result = partition1;
                minTimes = 0;

                globalStateMgr.getPartitionIncludeRecycleBin((OlapTable) any, partitionId2);
                result = partition2;
                minTimes = 0;

                globalStateMgr.getAllPartitionsIncludeRecycleBin((OlapTable) any);
                result = Lists.newArrayList(partition1, partition2);
                minTimes = 0;

                globalStateMgr.getReplicationNumIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = (short) 1;
                minTimes = 0;

                globalStateMgr.getDataPropertyIncludeRecycleBin((PartitionInfo) any, partitionId1);
                result = dataProperty1;
                minTimes = 0;

                globalStateMgr.getDataPropertyIncludeRecycleBin((PartitionInfo) any, partitionId2);
                result = dataProperty2;
                minTimes = 0;
            }
        };

        Rebalancer rebalancer = new DiskAndTabletLoadReBalancer(infoService, invertedIndex);
        rebalancer.updateLoadStatistic(clusterLoadStatistic);

        // set Config.balance_load_disk_safe_threshold to 0.4 to trigger backend disk balance
        Config.tablet_sched_balance_load_disk_safe_threshold = 0.4;
        Config.storage_usage_soft_limit_reserve_bytes = 1;
        List<TabletSchedCtx> tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(2, tablets.size());
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getDestBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getSrcBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getDestPathHash() == pathHash12)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getDestPathHash() == pathHash14)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcPathHash() == pathHash10)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcPathHash() == pathHash13)));

        // set Config.balance_load_disk_safe_threshold to 0.9 to trigger backend tablet distribution balance
        Config.tablet_sched_balance_load_disk_safe_threshold = 0.9;
        tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(2, tablets.size());
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getDestBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getSrcBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getDestPathHash() == pathHash12)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getDestPathHash() == pathHash14)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcPathHash() == pathHash10)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcPathHash() == pathHash13)));

        // set table state to schema_change, balance should be ignored
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assert.assertEquals(0, rebalancer.selectAlternativeTablets().size());
    }

    private Backend genBackend(long beId, String host, long availableCapB, long dataUsedCapB, long totalCapB,
                               long pathHash) {
        Backend backend = new Backend(beId, host, 0);
        backend.updateOnce(0, 0, 0);
        DiskInfo diskInfo = new DiskInfo("/data");
        diskInfo.setAvailableCapacityB(availableCapB);
        diskInfo.setDataUsedCapacityB(dataUsedCapB);
        diskInfo.setTotalCapacityB(totalCapB);
        diskInfo.setPathHash(pathHash);
        diskInfo.setState(DiskState.ONLINE);
        diskInfo.setStorageMedium(TStorageMedium.HDD);
        Map<String, DiskInfo> diskInfoMap = Maps.newHashMap();
        diskInfoMap.put("/data", diskInfo);
        backend.setDisks(ImmutableMap.copyOf(diskInfoMap));
        return backend;
    }

    private DiskInfo genDiskInfo(long availableCapB, long dataUsedCapB, long totalCapB, String path, long pathHash,
                                 TStorageMedium medium) {
        DiskInfo diskInfo = new DiskInfo(path);
        diskInfo.setAvailableCapacityB(availableCapB);
        diskInfo.setDataUsedCapacityB(dataUsedCapB);
        diskInfo.setTotalCapacityB(totalCapB);
        diskInfo.setPathHash(pathHash);
        diskInfo.setState(DiskState.ONLINE);
        diskInfo.setStorageMedium(medium);
        return diskInfo;
    }

    private void addTablet(TabletInvertedIndex invertedIndex, MaterializedIndex materializedIndex,
                           TStorageMedium medium,
                           long dbId, long tableId, long partitionId, long indexId, long tabletId, long replicaId,
                           long beId, long dataSize, long pathHash) {
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1111, medium);
        Replica replica = new Replica(replicaId, beId, 1L, 1111,
                dataSize, 1000, ReplicaState.NORMAL, -1, 1);
        invertedIndex.addTablet(tabletId, tabletMeta);
        replica.setPathHash(pathHash);
        invertedIndex.addReplica(tabletId, replica);
        LocalTablet tablet1 = new LocalTablet(tabletId, Lists.newArrayList(replica));
        materializedIndex.addTablet(tablet1, tabletMeta, true);
    }


    @Test
    public void testBalanceParallel(@Mocked GlobalStateMgr globalStateMgr) {
        // system info
        long dbId = 10001L;
        long tableId = 10002L;
        long partitionId = 10003L;
        long indexId = 10004L;
        long tabletDataSize = 200 * 1024 * 1024L;
        TStorageMedium medium = TStorageMedium.HDD;
        long beId1 = 1L;
        long beId2 = 2L;
        long beId3 = 3L;
        long beId4 = 4L;
        long pathHash1 = 1111L;
        long pathHash2 = 2222L;
        long pathHash3 = 3333L;
        long pathHash4 = 4444L;

        SystemInfoService infoService = new SystemInfoService();

        final long highTabletCnt = 10;
        infoService.addBackend(genBackend(beId1, "host1", 0,
                highTabletCnt * tabletDataSize, 10 * tabletDataSize, pathHash1));

        infoService.addBackend(genBackend(beId2, "host2", 0,
                highTabletCnt * tabletDataSize, 10 * tabletDataSize, pathHash2));

        infoService.addBackend(genBackend(beId3, "host3", 10 * tabletDataSize,
                0, 10 * tabletDataSize, pathHash3));

        infoService.addBackend(genBackend(beId4, "host4", 10 * tabletDataSize,
                0, 10 * tabletDataSize, pathHash4));

        // tablet inverted index
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        MaterializedIndex materializedIndex = new MaterializedIndex(indexId, IndexState.NORMAL);
        for (int i = 1; i <= highTabletCnt; i++) {
            addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId,
                    partitionId, indexId, 20000 + i, 30000 + i, beId1, tabletDataSize, pathHash1);
            addTablet(invertedIndex, materializedIndex, TStorageMedium.HDD, dbId, tableId,
                    partitionId, indexId, 20000 + highTabletCnt + i, 30000 + highTabletCnt + i,
                    beId2, tabletDataSize, pathHash2);
        }

        ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(infoService, invertedIndex);
        clusterLoadStatistic.init();

        PartitionInfo partitionInfo = new PartitionInfo();
        DataProperty dataProperty = new DataProperty(medium);
        partitionInfo.addPartition(partitionId, dataProperty, (short) 1, false);
        DistributionInfo distributionInfo = new HashDistributionInfo(6, Lists.newArrayList());
        Partition partition = new Partition(partitionId, "partition", materializedIndex, distributionInfo);
        OlapTable table = new OlapTable(tableId, "table", Lists.newArrayList(), KeysType.AGG_KEYS, partitionInfo,
                distributionInfo);
        table.addPartition(partition);
        Database database = new Database(dbId, "database");
        database.createTable(table);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getDbIdsIncludeRecycleBin();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                globalStateMgr.getDbIncludeRecycleBin(dbId);
                result = database;
                minTimes = 0;

                globalStateMgr.getTableIncludeRecycleBin((Database) any, anyLong);
                result = table;
                minTimes = 0;

                globalStateMgr.getTablesIncludeRecycleBin((Database) any);
                result = Lists.newArrayList(table);
                minTimes = 0;

                globalStateMgr.getPartitionIncludeRecycleBin((OlapTable) any, anyLong);
                result = partition;
                minTimes = 0;

                globalStateMgr.getAllPartitionsIncludeRecycleBin((OlapTable) any);
                result = Lists.newArrayList(partition);
                minTimes = 0;

                globalStateMgr.getReplicationNumIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = (short) 1;
                minTimes = 0;

                globalStateMgr.getDataPropertyIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = dataProperty;
                minTimes = 0;
            }
        };

        Rebalancer rebalancer = new DiskAndTabletLoadReBalancer(infoService, invertedIndex);
        rebalancer.updateLoadStatistic(clusterLoadStatistic);

        List<TabletSchedCtx> tablets = rebalancer.selectAlternativeTablets();
        int be1SourceCnt = 0;
        int be2SourceCnt = 0;
        for (TabletSchedCtx ctx : tablets) {
            if (ctx.getSrcBackendId() == beId1) {
                be1SourceCnt++;
            }

            if (ctx.getSrcBackendId() == beId2) {
                be2SourceCnt++;
            }
        }

        Assert.assertEquals(4, be1SourceCnt);
        Assert.assertEquals(4, be2SourceCnt);
    }

    @Test
    public void testBackendBalanceState() throws Exception {
        long tabletDataSize = 200 * 1024 * 1024L;
        long tabletId1 = 10000L;
        long tabletId2 = 10001L;
        long replicaId1 = 11000L;
        long replicaId2 = 11001L;
        long pathHash10 = 10L;
        long pathHash11 = 11L;
        long pathHash12 = 12L;
        long pathHash13 = 13L;
        long pathHash14 = 14L;

        Backend be1 = genBackend(1L, "host1", 2 * tabletDataSize,
                2 * tabletDataSize, 4 * tabletDataSize, pathHash10);
        DiskInfo disk10 = genDiskInfo(0, 4 * tabletDataSize,
                4 * tabletDataSize, "/data10", pathHash10, TStorageMedium.HDD);
        DiskInfo disk11 = genDiskInfo(tabletDataSize, 3 * tabletDataSize,
                4 * tabletDataSize, "/data11", pathHash11, TStorageMedium.HDD);
        DiskInfo disk12 = genDiskInfo(2 * tabletDataSize, 2 * tabletDataSize,
                4 * tabletDataSize, "/data12", pathHash12, TStorageMedium.HDD);
        DiskInfo disk13 = genDiskInfo(3 * tabletDataSize, tabletDataSize,
                4 * tabletDataSize, "/data13", pathHash13, TStorageMedium.HDD);
        DiskInfo disk14 = genDiskInfo(4 * tabletDataSize, 0,
                4 * tabletDataSize, "/data14", pathHash14, TStorageMedium.HDD);
        Map<String, DiskInfo> diskInfoMap1 = Maps.newHashMap();
        diskInfoMap1.put(disk10.getRootPath(), disk10);
        diskInfoMap1.put(disk11.getRootPath(), disk11);
        diskInfoMap1.put(disk12.getRootPath(), disk12);
        diskInfoMap1.put(disk13.getRootPath(), disk13);
        diskInfoMap1.put(disk14.getRootPath(), disk14);
        be1.setDisks(ImmutableMap.copyOf(diskInfoMap1));

        SystemInfoService infoService = new SystemInfoService();
        infoService.addBackend(be1);

        BackendLoadStatistic backendLoadStatistic = new BackendLoadStatistic(1L,
                "cluster1",
                infoService,
                new TabletInvertedIndex());

        backendLoadStatistic.init();
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        // add tablet to invertedIndex
        invertedIndex.addTablet(tabletId1,
                new TabletMeta(1, 2, 3, 4, -1, TStorageMedium.HDD));
        Replica replica = new Replica(replicaId1, 1, -1, ReplicaState.NORMAL);
        replica.setPathHash(pathHash10);
        invertedIndex.addReplica(tabletId1, replica);

        invertedIndex.addTablet(tabletId2,
                new TabletMeta(1, 2, 3, 4, -1, TStorageMedium.HDD));
        replica = new Replica(replicaId2, 1, -1, ReplicaState.NORMAL);
        replica.setPathHash(pathHash13);
        invertedIndex.addReplica(tabletId2, replica);

        BackendBalanceState hState = new BackendBalanceState(1L,
                backendLoadStatistic,
                invertedIndex,
                TStorageMedium.HDD,
                new HashMap<>(),
                new ArrayList<>());

        hState.init();
        Assert.assertEquals((Long) pathHash10, hState.sortedPath.get(0));
        Assert.assertEquals((Long) pathHash11, hState.sortedPath.get(1));
        Assert.assertEquals((Long) pathHash12, hState.sortedPath.get(2));
        Assert.assertEquals((Long) pathHash13, hState.sortedPath.get(3));
        Assert.assertEquals((Long) pathHash14, hState.sortedPath.get(4));
        Assert.assertEquals((Integer) 0, hState.pathSortIndex.get(pathHash10));
        Assert.assertEquals((Integer) 1, hState.pathSortIndex.get(pathHash11));
        Assert.assertEquals((Integer) 2, hState.pathSortIndex.get(pathHash12));
        Assert.assertEquals((Integer) 3, hState.pathSortIndex.get(pathHash13));
        Assert.assertEquals((Integer) 4, hState.pathSortIndex.get(pathHash14));
        Assert.assertEquals(10 * tabletDataSize, hState.usedCapacity);

        List<Long> highLoadPaths = hState.getTabletsInHighLoadPath(Lists.newArrayList(tabletId1, tabletId2));
        Assert.assertEquals(1, highLoadPaths.size());
        Assert.assertEquals((Long) tabletId1, highLoadPaths.get(0));

        // change threshold to 0.3, tabletId2 will be chosen
        Config.tablet_sched_balance_load_score_threshold = 0.3;
        highLoadPaths = hState.getTabletsInHighLoadPath(Lists.newArrayList(tabletId1, tabletId2));
        Assert.assertEquals(2, highLoadPaths.size());
        Assert.assertEquals((Long) tabletId1, highLoadPaths.get(0));
        Assert.assertEquals((Long) tabletId2, highLoadPaths.get(1));
        // reset
        Config.tablet_sched_balance_load_score_threshold = 0.1;

        // minus 4 tablets from pathHash10
        hState.minusUsedCapacity(pathHash10, 4 * tabletDataSize);
        Assert.assertEquals((Long) pathHash11, hState.sortedPath.get(0));
        Assert.assertEquals((Long) pathHash12, hState.sortedPath.get(1));
        Assert.assertEquals((Long) pathHash13, hState.sortedPath.get(2));
        Assert.assertEquals((Integer) 0, hState.pathSortIndex.get(pathHash11));
        Assert.assertEquals((Integer) 1, hState.pathSortIndex.get(pathHash12));
        Assert.assertEquals((Integer) 2, hState.pathSortIndex.get(pathHash13));
        Assert.assertEquals(6 * tabletDataSize, hState.usedCapacity);
        // only tabletId2 will be chosen
        highLoadPaths = hState.getTabletsInHighLoadPath(Lists.newArrayList(tabletId1, tabletId2));
        Assert.assertEquals(1, highLoadPaths.size());
        Assert.assertEquals((Long) tabletId2, highLoadPaths.get(0));


        BackendBalanceState lState = new BackendBalanceState(1L,
                backendLoadStatistic,
                invertedIndex,
                TStorageMedium.HDD,
                new HashMap<>(),
                new ArrayList<>());

        lState.init();

        // add 2 tablets to the lowest path hash, the lowest path will be pathHash13
        Long pathHash = lState.getLowestLoadPath();
        Assert.assertEquals((Long) pathHash14, pathHash);
        lState.addUsedCapacity(pathHash14, 2 * tabletDataSize);
        Assert.assertEquals((Long) pathHash13, lState.getLowestLoadPath());
    }
}
