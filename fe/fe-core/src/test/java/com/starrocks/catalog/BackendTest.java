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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/BackendTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class BackendTest {
    private Backend backend;
    private long backendId = 9999;
    private String host = "myhost";
    private int heartbeatPort = 21234;
    private int bePort = 21235;
    private int httpPort = 21237;
    private int beRpcPort = 21238;
    private int starletPort = 21239;

    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;
    private FakeEditLog fakeEditLog;

    @BeforeEach
    public void setUp() {
        globalStateMgr = AccessTestUtil.fetchAdminCatalog();

        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeEditLog = new FakeEditLog();

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
        FakeGlobalStateMgr.setSystemInfo(AccessTestUtil.fetchSystemInfoService());

        backend = new Backend(backendId, host, heartbeatPort);
        backend.updateOnce(bePort, httpPort, beRpcPort);
        backend.setStarletPort(starletPort);
    }

    @Test
    public void getMethodTest() {
        Assertions.assertEquals(backendId, backend.getId());
        Assertions.assertEquals(host, backend.getHost());
        Assertions.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        Assertions.assertEquals(bePort, backend.getBePort());
        Assertions.assertEquals(starletPort, backend.getStarletPort());

        // set new port
        int newBePort = 31235;
        int newHttpPort = 31237;
        backend.updateOnce(newBePort, newHttpPort, beRpcPort);
        Assertions.assertEquals(newBePort, backend.getBePort());

        // check alive
        Assertions.assertTrue(backend.isAlive());
    }

    @Test
    public void diskInfoTest() {
        Map<String, TDisk> diskInfos = new HashMap<String, TDisk>();

        TDisk disk1 = new TDisk("/data1/", 1000, 800, true);
        TDisk disk2 = new TDisk("/data2/", 2000, 700, true);
        TDisk disk3 = new TDisk("/data3/", 3000, 600, false);

        diskInfos.put(disk1.getRoot_path(), disk1);
        diskInfos.put(disk2.getRoot_path(), disk2);
        diskInfos.put(disk3.getRoot_path(), disk3);

        // first update
        backend.updateDisks(diskInfos);
        Assertions.assertEquals(disk1.getDisk_total_capacity() + disk2.getDisk_total_capacity(),
                backend.getTotalCapacityB());
        Assertions.assertEquals(1, backend.getAvailableCapacityB());

        // second update
        diskInfos.remove(disk1.getRoot_path());
        backend.updateDisks(diskInfos);
        Assertions.assertEquals(disk2.getDisk_total_capacity(), backend.getTotalCapacityB());
        Assertions.assertEquals(disk2.getDisk_available_capacity() + 1, backend.getAvailableCapacityB());
    }

    @Test
    public void testGetBackendStorageTypeCnt() {

        int backendStorageTypeCnt;
        Backend backend = new Backend(100L, "192.168.1.1", 9050);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assertions.assertEquals(0, backendStorageTypeCnt);

        backend.setAlive(true);
        DiskInfo diskInfo1 = new DiskInfo("/tmp/abc");
        diskInfo1.setStorageMedium(TStorageMedium.HDD);
        ImmutableMap<String, DiskInfo> disks = ImmutableMap.of("/tmp/abc", diskInfo1);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assertions.assertEquals(1, backendStorageTypeCnt);

        DiskInfo diskInfo2 = new DiskInfo("/tmp/abc");
        diskInfo2.setStorageMedium(TStorageMedium.SSD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1, "/tmp/abcd", diskInfo2);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assertions.assertEquals(2, backendStorageTypeCnt);

        diskInfo2.setStorageMedium(TStorageMedium.HDD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1, "/tmp/abcd", diskInfo2);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assertions.assertEquals(1, backendStorageTypeCnt);

        diskInfo1.setState(DiskInfo.DiskState.OFFLINE);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assertions.assertEquals(0, backendStorageTypeCnt);

    }

    @Test
    public void testHeartbeatOk() throws Exception {
        Backend be = new Backend();
        BackendHbResponse hbResponse = new BackendHbResponse(1, 9060, 8040, 8060, 8090,
                System.currentTimeMillis(), "1.0", 64, 20);
        boolean isChanged = be.handleHbResponse(hbResponse, false);
        Assertions.assertTrue(isChanged);
    }

}
