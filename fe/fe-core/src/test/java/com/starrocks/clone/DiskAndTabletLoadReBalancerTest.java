package com.starrocks.clone;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

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
     * tow tablets moved to be3, one from be1 and the other from be2
     */
    @Test
    public void testBalance(@Mocked Catalog catalog) {
        String cluster = "cluster1";
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
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20001L, 30001L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20002L, 30002L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20003L, 30003L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20004L, 30004L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20005L, 30005L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20006L, 30006L, beId2,
                tabletDataSize, pathHash2);

        ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(cluster, infoService, invertedIndex);
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
        database.setClusterName(cluster);

        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;
                minTimes = 0;

                catalog.getDbIdsIncludeRecycleBin();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                catalog.getDbIncludeRecycleBin(dbId);
                result = database;
                minTimes = 0;

                catalog.getTableIncludeRecycleBin((Database) any, anyLong);
                result = table;
                minTimes = 0;

                catalog.getTablesIncludeRecycleBin((Database) any);
                result = Lists.newArrayList(table);
                minTimes = 0;

                catalog.getPartitionIncludeRecycleBin((OlapTable) any, anyLong);
                result = partition;
                minTimes = 0;

                catalog.getAllPartitionsIncludeRecycleBin((OlapTable) any);
                result = Lists.newArrayList(partition);
                minTimes = 0;

                catalog.getReplicationNumIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = (short) 1;
                minTimes = 0;

                catalog.getDataPropertyIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = dataProperty;
                minTimes = 0;
            }
        };

        Rebalancer rebalancer = new DiskAndTabletLoadReBalancer(infoService, invertedIndex);
        Map<String, ClusterLoadStatistic> clusterLoadStatisticMap = Maps.newHashMap();
        clusterLoadStatisticMap.put(cluster, clusterLoadStatistic);
        rebalancer.updateLoadStatistic(clusterLoadStatisticMap);

        List<TabletSchedCtx> tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(2, tablets.size());
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getDestBackendId() == beId3)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId2)));

        // set Config.balance_load_disk_safe_threshold to 0.9 to trigger tablet balance
        Config.balance_load_disk_safe_threshold = 0.9;
        Config.storage_flood_stage_left_capacity_bytes = 1;
        tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(2, tablets.size());
        Assert.assertTrue(tablets.stream().allMatch(t -> (t.getDestBackendId() == beId3)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId1)));
        Assert.assertTrue(tablets.stream().anyMatch(t -> (t.getSrcBackendId() == beId2)));
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
    public void testBalanceWithSameHost(@Mocked Catalog catalog) {
        String cluster = "cluster1";
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
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20001L, 30001L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20002L, 30002L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20003L, 30003L, beId1,
                tabletDataSize, pathHash1);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20001L, 30004L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20002L, 30005L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20003L, 30006L, beId2,
                tabletDataSize, pathHash2);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20001L, 30007L, beId3,
                tabletDataSize, pathHash3);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20002L, 30008L, beId3,
                tabletDataSize, pathHash3);
        addTablet(invertedIndex, materializedIndex, dbId, tableId, partitionId, indexId, 20003L, 30009L, beId3,
                tabletDataSize, pathHash3);

        ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(cluster, infoService, invertedIndex);
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
        database.setClusterName(cluster);

        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;
                minTimes = 0;

                catalog.getDbIdsIncludeRecycleBin();
                result = Lists.newArrayList(dbId);
                minTimes = 0;

                catalog.getDbIncludeRecycleBin(dbId);
                result = database;
                minTimes = 0;

                catalog.getTableIncludeRecycleBin((Database) any, anyLong);
                result = table;
                minTimes = 0;

                catalog.getTablesIncludeRecycleBin((Database) any);
                result = Lists.newArrayList(table);
                minTimes = 0;

                catalog.getPartitionIncludeRecycleBin((OlapTable) any, anyLong);
                result = partition;
                minTimes = 0;

                catalog.getPartitionsIncludeRecycleBin((OlapTable) any);
                result = Lists.newArrayList(partition);
                minTimes = 0;

                catalog.getReplicationNumIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = (short) 1;
                minTimes = 0;

                catalog.getDataPropertyIncludeRecycleBin((PartitionInfo) any, anyLong);
                result = dataProperty;
                minTimes = 0;
            }
        };

        Rebalancer rebalancer = new DiskAndTabletLoadReBalancer(infoService, invertedIndex);
        Map<String, ClusterLoadStatistic> clusterLoadStatisticMap = Maps.newHashMap();
        clusterLoadStatisticMap.put(cluster, clusterLoadStatistic);
        rebalancer.updateLoadStatistic(clusterLoadStatisticMap);

        List<TabletSchedCtx> tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(0, tablets.size());

        // set Config.balance_load_disk_safe_threshold to 0.9 to trigger tablet balance
        Config.balance_load_disk_safe_threshold = 0.9;
        Config.storage_flood_stage_left_capacity_bytes = 1;
        tablets = rebalancer.selectAlternativeTablets();
        Assert.assertEquals(0, tablets.size());
    }

    private Backend genBackend(long beId, String host, long availableCapB, long dataUsedCapB, long totalCapB,
                               long pathHash) {
        Backend backend = new Backend(beId, host, 0);
        backend.updateOnce(0, 0, 0);
        backend.setOwnerClusterName("cluster1");
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

    private void addTablet(TabletInvertedIndex invertedIndex, MaterializedIndex materializedIndex,
                           long dbId, long tableId, long partitionId, long indexId, long tabletId, long replicaId,
                           long beId, long dataSize, long pathHash) {
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 1111, TStorageMedium.HDD);
        Replica replica = new Replica(replicaId, beId, 1L, 1L, 1111,
                dataSize, 1000, ReplicaState.NORMAL, -1, -1, 1, 1);
        invertedIndex.addTablet(tabletId, tabletMeta);
        replica.setPathHash(pathHash);
        invertedIndex.addReplica(tabletId, replica);
        Tablet tablet1 = new Tablet(tabletId, Lists.newArrayList(replica));
        materializedIndex.addTablet(tablet1, tabletMeta, false);
    }
}
