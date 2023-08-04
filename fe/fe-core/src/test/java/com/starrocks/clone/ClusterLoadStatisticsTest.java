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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ClusterLoadStatisticsTest {

    private Backend be1;
    private Backend be2;
    private Backend be3;

    private GlobalStateMgr globalStateMgr;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;

    @Before
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

        invertedIndex.addTablet(50000, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        invertedIndex.addReplica(50000, new Replica(50001, be1.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(50000, new Replica(50003, be3.getId(), 0, ReplicaState.NORMAL));

        invertedIndex.addTablet(60000, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        invertedIndex.addReplica(60000, new Replica(60002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(60000, new Replica(60003, be3.getId(), 0, ReplicaState.NORMAL));

        invertedIndex.addTablet(70000, new TabletMeta(1, 2, 3, 4, 5, TStorageMedium.HDD));
        invertedIndex.addReplica(70000, new Replica(70002, be2.getId(), 0, ReplicaState.NORMAL));
        invertedIndex.addReplica(70000, new Replica(70003, be3.getId(), 0, ReplicaState.NORMAL));
    }

    @Test
    public void test() {
        ClusterLoadStatistic loadStatistic = new ClusterLoadStatistic(systemInfoService, invertedIndex);
        loadStatistic.init();
        List<List<String>> infos = loadStatistic.getClusterStatistic(TStorageMedium.HDD);
        System.out.println(infos);
        Assert.assertEquals(3, infos.size());
    }

    @Test
    public void testToString() {
        ClusterLoadStatistic clusterLoad = new ClusterLoadStatistic(systemInfoService, invertedIndex);
        clusterLoad.init();

        BackendLoadStatistic beLoad = clusterLoad.getBackendLoadStatistic(10001);
        Assert.assertEquals("{\"be_id\":10001,\"cluster_name\":\"default_cluster\",\"is_available\":true," +
                "\"cpu_cores\":0,\"mem_limit\":0,\"mem_used\":0," +
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
