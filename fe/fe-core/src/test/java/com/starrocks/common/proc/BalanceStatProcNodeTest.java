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

package com.starrocks.common.proc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.clone.BalanceStat;
import com.starrocks.clone.BalanceStat.BalanceType;
import com.starrocks.clone.ClusterLoadStatistic;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class BalanceStatProcNodeTest {

    @Test
    public void testFetchResult(@Mocked GlobalStateMgr globalStateMgr) throws AnalysisException {
        // 0. backend disk balance
        // system info service
        // be1
        Backend be1 = new Backend(10001L, "192.168.0.1", 9051);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(1000000L);
        diskInfo1.setAvailableCapacityB(100000L);
        diskInfo1.setDataUsedCapacityB(880000L);
        diskInfo1.setStorageMedium(TStorageMedium.HDD);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        DiskInfo diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(1000000L);
        diskInfo2.setAvailableCapacityB(900000L);
        diskInfo2.setDataUsedCapacityB(80000L);
        diskInfo2.setStorageMedium(TStorageMedium.HDD);
        disks.put(diskInfo2.getRootPath(), diskInfo2);
        be1.setDisks(ImmutableMap.copyOf(disks));
        be1.setAlive(true);

        // be2
        Backend be2 = new Backend(10002L, "192.168.0.2", 9051);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(1000000L);
        diskInfo1.setAvailableCapacityB(500000L);
        diskInfo1.setDataUsedCapacityB(480000L);
        diskInfo1.setStorageMedium(TStorageMedium.HDD);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(1000000L);
        diskInfo2.setAvailableCapacityB(500000L);
        diskInfo2.setDataUsedCapacityB(480000L);
        diskInfo2.setStorageMedium(TStorageMedium.HDD);
        disks.put(diskInfo2.getRootPath(), diskInfo2);
        be2.setDisks(ImmutableMap.copyOf(disks));
        be2.setAlive(true);

        SystemInfoService systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(be1);
        systemInfoService.addBackend(be2);

        // tablet inverted index
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        invertedIndex.addTablet(50000L, new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.HDD));
        invertedIndex.addReplica(50000L, new Replica(50001L, be1.getId(), 0, Replica.ReplicaState.NORMAL));

        invertedIndex.addTablet(60000L, new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.HDD));
        invertedIndex.addReplica(60000L, new Replica(60002L, be2.getId(), 0, Replica.ReplicaState.NORMAL));

        // cluster load statistic
        ClusterLoadStatistic clusterLoadStat = new ClusterLoadStatistic(systemInfoService, invertedIndex);
        clusterLoadStat.init();
        clusterLoadStat.updateBackendDiskBalanceStat(Pair.create(TStorageMedium.HDD, be1.getId()),
                BalanceStat.createBackendDiskBalanceStat(be1.getId(), "/path1", "/path2", 0.9, 0.1));

        // tablet scheduler
        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());
        tabletScheduler.setClusterLoadStatistic(clusterLoadStat);

        // 2 pending tablet
        TabletSchedCtx ctx1 = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, 1L, 2L, 3L, 4L, 1001L, System.currentTimeMillis());
        ctx1.setOrigPriority(TabletSchedCtx.Priority.NORMAL);
        ctx1.setBalanceType(BalanceType.BACKEND_DISK);
        ctx1.setStorageMedium(TStorageMedium.HDD);
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx1);

        TabletSchedCtx ctx2 = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, 1L, 2L, 3L, 4L, 1002L, System.currentTimeMillis());
        ctx2.setOrigPriority(TabletSchedCtx.Priority.NORMAL);
        ctx2.setBalanceType(BalanceType.BACKEND_DISK);
        ctx2.setStorageMedium(TStorageMedium.HDD);
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx2);

        // 1. cluster tablet balance
        // local meta store
        LocalMetastore localMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), new CatalogRecycleBin(), null);

        Database db = new Database(10000L, "BalanceStatProcTestDB");
        localMetastore.unprotectCreateDb(db);

        {
            List<Column> cols = Lists.newArrayList(new Column("province", Type.VARCHAR));
            PartitionInfo listPartition = new ListPartitionInfo(PartitionType.LIST, cols);
            long partitionId = 1025L;
            listPartition.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
            listPartition.setIsInMemory(partitionId, false);
            listPartition.setReplicationNum(partitionId, (short) 1);
            OlapTable olapTable = new OlapTable(1024L, "olap_table", cols, null, listPartition, null);
            MaterializedIndex index = new MaterializedIndex(1000L, MaterializedIndex.IndexState.NORMAL);
            index.setBalanceStat(BalanceStat.createClusterTabletBalanceStat(10001L, 10002L, 9L, 1L));
            Map<String, Long> indexNameToId = olapTable.getIndexNameToId();
            indexNameToId.put("index1", index.getId());
            TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(), partitionId, index.getId(), TStorageMedium.HDD);
            index.addTablet(new LocalTablet(1010L), tabletMeta);
            index.addTablet(new LocalTablet(1011L), tabletMeta);
            Partition partition = new Partition(partitionId, partitionId, "p1", index, new RandomDistributionInfo(2));
            olapTable.addPartition(partition);

            db.registerTableUnlocked(olapTable);

            // 1 running tablet
            TabletSchedCtx ctx3 = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, db.getId(), olapTable.getId(), partitionId,
                    index.getId(), 1010L, System.currentTimeMillis());
            ctx3.setOrigPriority(TabletSchedCtx.Priority.NORMAL);
            ctx3.setBalanceType(BalanceType.CLUSTER_TABLET);
            ctx3.setStorageMedium(TStorageMedium.HDD);
            Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx3);
        }

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore();
                result = localMetastore;
            }
        };

        // test
        BalanceStatProcNode proc = new BalanceStatProcNode(tabletScheduler);
        BaseProcResult result = (BaseProcResult) proc.fetchResult();
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(4, rows.size());

        // cluster disk balanced
        Assertions.assertEquals("[HDD, inter-node disk usage, true, 0, 0]", rows.get(0).toString());
        // cluster tablet not balanced, 1 running tablet
        Assertions.assertEquals("[HDD, inter-node tablet distribution, false, 0, 1]", rows.get(1).toString());
        // backend disk not balanced, 2 pending tablets
        Assertions.assertEquals("[HDD, intra-node disk usage, false, 2, 0]", rows.get(2).toString());
        // backend tablet balanced
        Assertions.assertEquals("[HDD, intra-node tablet distribution, true, 0, 0]", rows.get(3).toString());
    }
}