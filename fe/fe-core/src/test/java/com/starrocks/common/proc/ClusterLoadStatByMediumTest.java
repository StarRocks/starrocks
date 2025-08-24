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
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ClusterLoadStatByMediumTest {

    @Test
    public void testFetchResult() throws AnalysisException {
        // system info service
        // be1
        Backend be1 = new Backend(10001L, "192.168.0.1", 9051);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(1000000L);
        diskInfo1.setAvailableCapacityB(500000L);
        diskInfo1.setDataUsedCapacityB(480000L);
        diskInfo1.setStorageMedium(TStorageMedium.HDD);
        disks.put(diskInfo1.getRootPath(), diskInfo1);
        DiskInfo diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(1000000L);
        diskInfo2.setAvailableCapacityB(500000L);
        diskInfo2.setDataUsedCapacityB(480000L);
        diskInfo2.setStorageMedium(TStorageMedium.SSD);
        disks.put(diskInfo2.getRootPath(), diskInfo2);
        be1.setDisks(ImmutableMap.copyOf(disks));
        be1.setAlive(true);

        SystemInfoService systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(be1);

        // tablet inverted index
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        invertedIndex.addTablet(50000L, new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.HDD));
        invertedIndex.addReplica(50000L, new Replica(50001L, be1.getId(), 0, Replica.ReplicaState.NORMAL));

        invertedIndex.addTablet(60000L, new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.SSD));
        invertedIndex.addReplica(60000L, new Replica(60002L, be1.getId(), 0, Replica.ReplicaState.NORMAL));

        // cluster load statistic
        ClusterLoadStatistic clusterLoadStat = new ClusterLoadStatistic(systemInfoService, invertedIndex);
        clusterLoadStat.init();
        clusterLoadStat.updateClusterDiskBalanceStat(
                TStorageMedium.HDD, BalanceStat.createClusterDiskBalanceStat(1L, 2L, 0.9, 0.1));

        // tablet scheduler
        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());
        tabletScheduler.setClusterLoadStatistic(clusterLoadStat);

        // test
        ClusterLoadStatByMedium proc = new ClusterLoadStatByMedium(tabletScheduler);
        BaseProcResult result = (BaseProcResult) proc.fetchResult();
        List<List<String>> rows = result.getRows();
        Assertions.assertEquals(2, rows.size());

        for (int i = 0; i < rows.size(); ++i) {
            List<String> row = rows.get(i);
            Assertions.assertEquals(2, row.size());
            String medium = row.get(0);
            String balanceStat = row.get(1);
            if (medium.equals("HDD")) {
                Assertions.assertEquals(
                        "{\"maxUsedPercent\":0.9,\"minUsedPercent\":0.1,\"maxBeId\":1,\"minBeId\":2," +
                                "\"type\":\"INTER_NODE_DISK_USAGE\",\"balanced\":false}",
                        balanceStat);
            } else if (medium.equals("SSD")) {
                Assertions.assertEquals("{\"balanced\":true}", balanceStat);
            } else {
                Assertions.fail(String.format("Unknown storage medium: %s", medium));
            }
        }
    }
}
