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
import com.google.common.collect.Maps;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.clone.BalanceStat;
import com.starrocks.clone.ClusterLoadStatistic;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ClusterLoadStatisticProcDirTest {

    @Test
    public void testFetchResult() throws AnalysisException {
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

        // test
        ClusterLoadStatisticProcDir proc = new ClusterLoadStatisticProcDir(TStorageMedium.HDD, tabletScheduler);
        BaseProcResult result = (BaseProcResult) proc.fetchResult();
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(2, rows.size());

        for (int i = 0; i < 2; ++i) {
            List<String> row = rows.get(i);
            Assertions.assertEquals(12, row.size());
            Assertions.assertEquals("true", row.get(2));
            Assertions.assertEquals("960000", row.get(3));
            Assertions.assertEquals("1960000", row.get(4));
            Assertions.assertEquals("48.980", row.get(5));
            String beId = row.get(0);
            if (beId.equals("10001")) {
                Assertions.assertEquals(
                        "{\"maxUsedPercent\":0.9,\"minUsedPercent\":0.1,\"beId\":10001,\"maxPath\":\"/path1\"," +
                                "\"minPath\":\"/path2\",\"type\":\"INTRA_NODE_DISK_USAGE\",\"balanced\":false}",
                        row.get(11));
            } else if (beId.equals("10002")) {
                Assertions.assertEquals("{\"balanced\":true}", row.get(11));
            } else {
                Assertions.fail(String.format("Unknown backend id: %s", beId));
            }
        }
    }
}
