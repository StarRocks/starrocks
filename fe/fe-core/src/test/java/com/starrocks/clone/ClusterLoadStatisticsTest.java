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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/clone/ClusterLoadStatisticsTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.clone;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ClusterLoadStatisticsTest {

    private Backend be1;
    private Backend be2;
    private Backend be3;

    private GlobalStateMgr globalStateMgr;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;

    @BeforeEach
    public void setUp() {
        // be1
        be1 = new Backend(10001, "192.168.0.1", 9051);
        Map<String, DiskInfo> disks = Maps.newHashMap();
        DiskInfo diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(1000000);
        diskInfo1.setAvailableCapacityB(500000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        DiskInfo diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(2000000);
        diskInfo2.setAvailableCapacityB(100000);
        diskInfo2.setDataUsedCapacityB(80000);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        DiskInfo diskInfo3 = new DiskInfo("/path3");
        diskInfo3.setTotalCapacityB(500000);
        diskInfo3.setAvailableCapacityB(490000);
        diskInfo3.setDataUsedCapacityB(10000);
        disks.put(diskInfo3.getRootPath(), diskInfo3);

        be1.setDisks(ImmutableMap.copyOf(disks));
        be1.setAlive(true);

        // be2
        be2 = new Backend(10002, "192.168.0.2", 9052);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(2000000);
        diskInfo1.setAvailableCapacityB(1900000);
        diskInfo1.setDataUsedCapacityB(480000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(20000000);
        diskInfo2.setAvailableCapacityB(1000000);
        diskInfo2.setDataUsedCapacityB(80000);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        be2.setDisks(ImmutableMap.copyOf(disks));
        be2.setAlive(true);

        // be3
        be3 = new Backend(10003, "192.168.0.3", 9053);
        disks = Maps.newHashMap();
        diskInfo1 = new DiskInfo("/path1");
        diskInfo1.setTotalCapacityB(4000000);
        diskInfo1.setAvailableCapacityB(100000);
        diskInfo1.setDataUsedCapacityB(80000);
        disks.put(diskInfo1.getRootPath(), diskInfo1);

        diskInfo2 = new DiskInfo("/path2");
        diskInfo2.setTotalCapacityB(2000000);
        diskInfo2.setAvailableCapacityB(100000);
        diskInfo2.setDataUsedCapacityB(80000);
        disks.put(diskInfo2.getRootPath(), diskInfo2);

        diskInfo3 = new DiskInfo("/path3");
        diskInfo3.setTotalCapacityB(500000);
        diskInfo3.setAvailableCapacityB(490000);
        diskInfo3.setDataUsedCapacityB(10000);
        disks.put(diskInfo3.getRootPath(), diskInfo3);

        be3.setDisks(ImmutableMap.copyOf(disks));
        be3.setAlive(true);

        systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(be1);
        systemInfoService.addBackend(be2);
        systemInfoService.addBackend(be3);

        // tablet
        invertedIndex = new TabletInvertedIndex();

        invertedIndex.addTablet(50000, new TabletMeta(1, 2, 3, 4, TStorageMedium.HDD));
        invertedIndex.addReplica(50000, new Replica(50001, be1.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50003, be3.getId(), 0, ReplicaState.NORMAL));

        invertedIndex.addTablet(60000, new TabletMeta(1, 2, 3, 4, TStorageMedium.HDD));
        invertedIndex.addReplica(60000, new Replica(60002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(60000, new Replica(60003, be3.getId(), 0, ReplicaState.NORMAL));

        invertedIndex.addTablet(70000, new TabletMeta(1, 2, 3, 4, TStorageMedium.HDD));
        invertedIndex.addReplica(70000, new Replica(70002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(70000, new Replica(70003, be3.getId(), 0, ReplicaState.NORMAL));
    }

    @Test
    public void test() {
        ClusterLoadStatistic loadStatistic = new ClusterLoadStatistic(systemInfoService, invertedIndex);
        loadStatistic.init();
        List<List<String>> infos = loadStatistic.getBackendLoadStats(TStorageMedium.HDD);
        System.out.println(infos);
        Assertions.assertEquals(3, infos.size());
    }

    @Test
    public void testInit_singleMediumShortcutCountsAllReplicasUnderPhysicalMedium() throws LoadBalanceException {
        // Build a BE with HDD-only disks but a mix of HDD- and SSD-declared TabletMeta. The
        // SSD-declared entries simulate the post-cooldown / mis-placed-tablet windows where
        // TabletMeta.storageMedium diverges from physical placement on a single-medium BE that
        // ReportHandler cannot migrate. The shortcut in BackendLoadStatistic.init must count
        // every replica on the BE under HDD (its physical medium), not drop the SSD-declared
        // ones via the hasMedium() post-pass.
        Backend hddOnlyBe = new Backend(11001, "192.168.0.11", 9051);
        Map<String, DiskInfo> hddDisks = Maps.newHashMap();
        DiskInfo hddDisk = new DiskInfo("/path1");
        hddDisk.setTotalCapacityB(1_000_000);
        hddDisk.setAvailableCapacityB(900_000);
        hddDisk.setDataUsedCapacityB(100_000);
        hddDisk.setStorageMedium(TStorageMedium.HDD);
        hddDisks.put(hddDisk.getRootPath(), hddDisk);
        hddOnlyBe.setDisks(ImmutableMap.copyOf(hddDisks));
        hddOnlyBe.setAlive(true);

        SystemInfoService localInfo = new SystemInfoService();
        localInfo.addBackend(hddOnlyBe);

        TabletInvertedIndex localIndex = new TabletInvertedIndex();
        long tabletId = 80000;
        // 3 HDD-declared and 2 SSD-declared replicas, all physically on the HDD-only BE.
        for (int i = 0; i < 3; i++, tabletId++) {
            localIndex.addTablet(tabletId, new TabletMeta(1, 2, 3, 4, TStorageMedium.HDD));
            localIndex.addReplica(tabletId, new Replica(tabletId + 100, hddOnlyBe.getId(), 0, ReplicaState.NORMAL));
        }
        for (int i = 0; i < 2; i++, tabletId++) {
            localIndex.addTablet(tabletId, new TabletMeta(1, 2, 3, 5, TStorageMedium.SSD));
            localIndex.addReplica(tabletId, new Replica(tabletId + 100, hddOnlyBe.getId(), 0, ReplicaState.NORMAL));
        }

        BackendLoadStatistic stat = new BackendLoadStatistic(
                hddOnlyBe.getId(), SystemInfoService.DEFAULT_CLUSTER, localInfo, localIndex);
        stat.init();

        Assertions.assertEquals(5L, stat.getReplicaNum(TStorageMedium.HDD),
                "single-medium HDD BE must count every replica under HDD, including SSD-declared ones");
        Assertions.assertEquals(0L, stat.getReplicaNum(TStorageMedium.SSD),
                "single-medium HDD BE must report zero SSD replicas");
    }

    @Test
    public void testInit_singleMediumSsdShortcut() throws LoadBalanceException {
        // Symmetric to the HDD-only case: SSD-only BE counts every replica under SSD even when
        // some TabletMeta entries still carry the legacy HDD declaration.
        Backend ssdOnlyBe = new Backend(11002, "192.168.0.12", 9051);
        Map<String, DiskInfo> ssdDisks = Maps.newHashMap();
        DiskInfo ssdDisk = new DiskInfo("/path1");
        ssdDisk.setTotalCapacityB(1_000_000);
        ssdDisk.setAvailableCapacityB(900_000);
        ssdDisk.setDataUsedCapacityB(100_000);
        ssdDisk.setStorageMedium(TStorageMedium.SSD);
        ssdDisks.put(ssdDisk.getRootPath(), ssdDisk);
        ssdOnlyBe.setDisks(ImmutableMap.copyOf(ssdDisks));
        ssdOnlyBe.setAlive(true);

        SystemInfoService localInfo = new SystemInfoService();
        localInfo.addBackend(ssdOnlyBe);

        TabletInvertedIndex localIndex = new TabletInvertedIndex();
        long tabletId = 81000;
        for (int i = 0; i < 4; i++, tabletId++) {
            localIndex.addTablet(tabletId, new TabletMeta(1, 2, 3, 4, TStorageMedium.SSD));
            localIndex.addReplica(tabletId, new Replica(tabletId + 100, ssdOnlyBe.getId(), 0, ReplicaState.NORMAL));
        }
        for (int i = 0; i < 3; i++, tabletId++) {
            localIndex.addTablet(tabletId, new TabletMeta(1, 2, 3, 5, TStorageMedium.HDD));
            localIndex.addReplica(tabletId, new Replica(tabletId + 100, ssdOnlyBe.getId(), 0, ReplicaState.NORMAL));
        }

        BackendLoadStatistic stat = new BackendLoadStatistic(
                ssdOnlyBe.getId(), SystemInfoService.DEFAULT_CLUSTER, localInfo, localIndex);
        stat.init();

        Assertions.assertEquals(7L, stat.getReplicaNum(TStorageMedium.SSD),
                "single-medium SSD BE must count every replica under SSD, including HDD-declared ones");
        Assertions.assertEquals(0L, stat.getReplicaNum(TStorageMedium.HDD),
                "single-medium SSD BE must report zero HDD replicas");
    }

    @Test
    public void testInit_mixedMediumStillScansAndHonorsTabletMeta() throws LoadBalanceException {
        // BE with both HDD and SSD disks: the per-tablet scan must run and counts must match
        // TabletMeta.storageMedium so the migration scheduler in ReportHandler sees the right
        // intended counts.
        Backend mixedBe = new Backend(11003, "192.168.0.13", 9051);
        Map<String, DiskInfo> mixedDisks = Maps.newHashMap();
        DiskInfo hdd = new DiskInfo("/hdd");
        hdd.setTotalCapacityB(2_000_000);
        hdd.setAvailableCapacityB(1_500_000);
        hdd.setDataUsedCapacityB(500_000);
        hdd.setStorageMedium(TStorageMedium.HDD);
        mixedDisks.put(hdd.getRootPath(), hdd);
        DiskInfo ssd = new DiskInfo("/ssd");
        ssd.setTotalCapacityB(1_000_000);
        ssd.setAvailableCapacityB(700_000);
        ssd.setDataUsedCapacityB(300_000);
        ssd.setStorageMedium(TStorageMedium.SSD);
        mixedDisks.put(ssd.getRootPath(), ssd);
        mixedBe.setDisks(ImmutableMap.copyOf(mixedDisks));
        mixedBe.setAlive(true);

        SystemInfoService localInfo = new SystemInfoService();
        localInfo.addBackend(mixedBe);

        TabletInvertedIndex localIndex = new TabletInvertedIndex();
        long tabletId = 82000;
        for (int i = 0; i < 6; i++, tabletId++) {
            localIndex.addTablet(tabletId, new TabletMeta(1, 2, 3, 4, TStorageMedium.HDD));
            localIndex.addReplica(tabletId, new Replica(tabletId + 100, mixedBe.getId(), 0, ReplicaState.NORMAL));
        }
        for (int i = 0; i < 4; i++, tabletId++) {
            localIndex.addTablet(tabletId, new TabletMeta(1, 2, 3, 5, TStorageMedium.SSD));
            localIndex.addReplica(tabletId, new Replica(tabletId + 100, mixedBe.getId(), 0, ReplicaState.NORMAL));
        }

        BackendLoadStatistic stat = new BackendLoadStatistic(
                mixedBe.getId(), SystemInfoService.DEFAULT_CLUSTER, localInfo, localIndex);
        stat.init();

        Assertions.assertEquals(6L, stat.getReplicaNum(TStorageMedium.HDD),
                "mixed-medium BE must keep the per-tablet scan counts");
        Assertions.assertEquals(4L, stat.getReplicaNum(TStorageMedium.SSD),
                "mixed-medium BE must keep the per-tablet scan counts");
    }

    @Test
    public void testToString() {
        ClusterLoadStatistic clusterLoad = new ClusterLoadStatistic(systemInfoService, invertedIndex);
        clusterLoad.init();

        BackendLoadStatistic beLoad = clusterLoad.getBackendLoadStatistic(10001);
        Assertions.assertEquals("{\"beId\":10001,\"clusterName\":\"default_cluster\",\"isAvailable\":true," +
                "\"cpuCores\":0,\"memLimit\":0,\"memUsed\":0," +
                "\"mediums\":[{\"medium\":\"HDD\",\"replica\":1,\"used\":570000,\"total\":\"1.5MB\"," +
                "\"score\":1.0040447504302925}," +
                "{\"medium\":\"SSD\",\"replica\":0,\"used\":0,\"total\":\"0B\",\"score\":NaN}]," +
                "\"paths\":[" +
                "{\"beId\":10001,\"path\":\"/path3\",\"pathHash\":0,\"storageMedium\":\"HDD\"," +
                "\"total\":500000,\"used\":10000}," +
                "{\"beId\":10001,\"path\":\"/path2\",\"pathHash\":0,\"storageMedium\":\"HDD\"," +
                "\"total\":180000,\"used\":80000}," +
                "{\"beId\":10001,\"path\":\"/path1\",\"pathHash\":0,\"storageMedium\":\"HDD\"," +
                "\"total\":980000,\"used\":480000}]}", beLoad.toString());

    }

}
