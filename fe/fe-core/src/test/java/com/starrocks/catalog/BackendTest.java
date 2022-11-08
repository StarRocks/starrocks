// This file is made available under Elastic License 2.0.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.CoordinatorMonitor;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.system.HeartbeatResponse;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BackendTest {
    private Backend backend;
    private long backendId = 9999;
    private String host = "myhost";
    private int heartbeatPort = 21234;
    private int bePort = 21235;
    private int httpPort = 21237;
    private int beRpcPort = 21238;

    private Catalog catalog;

    private FakeCatalog fakeCatalog;
    private FakeEditLog fakeEditLog;

    @Before
    public void setUp() {
        catalog = AccessTestUtil.fetchAdminCatalog();

        fakeCatalog = new FakeCatalog();
        fakeEditLog = new FakeEditLog();

        FakeCatalog.setCatalog(catalog);
        FakeCatalog.setMetaVersion(FeConstants.meta_version);
        FakeCatalog.setSystemInfo(AccessTestUtil.fetchSystemInfoService());

        backend = new Backend(backendId, host, heartbeatPort);
        backend.updateOnce(bePort, httpPort, beRpcPort);
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(backendId, backend.getId());
        Assert.assertEquals(host, backend.getHost());
        Assert.assertEquals(heartbeatPort, backend.getHeartbeatPort());
        Assert.assertEquals(bePort, backend.getBePort());

        // set new port
        int newBePort = 31235;
        int newHttpPort = 31237;
        backend.updateOnce(newBePort, newHttpPort, beRpcPort);
        Assert.assertEquals(newBePort, backend.getBePort());

        // check alive
        Assert.assertTrue(backend.isAlive());
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
        Assert.assertEquals(disk1.getDisk_total_capacity() + disk2.getDisk_total_capacity(),
                backend.getTotalCapacityB());
        Assert.assertEquals(1, backend.getAvailableCapacityB());

        // second update
        diskInfos.remove(disk1.getRoot_path());
        backend.updateDisks(diskInfos);
        Assert.assertEquals(disk2.getDisk_total_capacity(), backend.getTotalCapacityB());
        Assert.assertEquals(disk2.getDisk_available_capacity() + 1, backend.getAvailableCapacityB());
    }

    @Test
    public void testSerialization() throws Exception {
        // Write 100 objects to file
        File file = new File("./backendTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        List<Backend> list1 = new LinkedList<Backend>();
        List<Backend> list2 = new LinkedList<Backend>();

        for (int count = 0; count < 100; ++count) {
            Backend backend = new Backend(count, "10.120.22.32" + count, 6000 + count);
            backend.updateOnce(7000 + count, 9000 + count, beRpcPort);
            list1.add(backend);
        }
        for (int count = 100; count < 200; count++) {
            Backend backend = new Backend(count, "10.120.22.32" + count, 6000 + count);
            backend.updateOnce(7000 + count, 9000 + count, beRpcPort);
            list1.add(backend);
        }
        for (Backend backend : list1) {
            backend.write(dos);
        }
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        for (int count = 0; count < 100; ++count) {
            Backend backend = new Backend();
            backend.readFields(dis);
            list2.add(backend);
            Assert.assertEquals(count, backend.getId());
            Assert.assertEquals("10.120.22.32" + count, backend.getHost());
        }

        for (int count = 100; count < 200; ++count) {
            Backend backend = Backend.read(dis);
            list2.add(backend);
            Assert.assertEquals(count, backend.getId());
            Assert.assertEquals("10.120.22.32" + count, backend.getHost());
        }

        for (int count = 0; count < 200; count++) {
            Assert.assertTrue(list1.get(count).equals(list2.get(count)));
        }
        Assert.assertFalse(list1.get(1).equals(list1.get(2)));
        Assert.assertFalse(list1.get(1).equals(this));
        Assert.assertTrue(list1.get(1).equals(list1.get(1)));

        Backend back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        Backend back2 = new Backend(2, "a", 1);
        back2.updateOnce(1, 1, 1);
        Assert.assertFalse(back1.equals(back2));

        back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        back2 = new Backend(1, "b", 1);
        back2.updateOnce(1, 1, 1);
        Assert.assertFalse(back1.equals(back2));

        back1 = new Backend(1, "a", 1);
        back1.updateOnce(1, 1, 1);
        back2 = new Backend(1, "a", 2);
        back2.updateOnce(1, 1, 1);
        Assert.assertFalse(back1.equals(back2));

        Assert.assertEquals("Backend [id=1, host=a, heartbeatPort=1, alive=true]", back1.toString());

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testGetBackendStorageTypeCnt() {

        int backendStorageTypeCnt;
        Backend backend = new Backend(100L, "192.168.1.1", 9050);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assert.assertEquals(0, backendStorageTypeCnt);

        backend.setAlive(true);
        DiskInfo diskInfo1 = new DiskInfo("/tmp/abc");
        diskInfo1.setStorageMedium(TStorageMedium.HDD);
        ImmutableMap<String, DiskInfo> disks = ImmutableMap.of("/tmp/abc", diskInfo1);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assert.assertEquals(1, backendStorageTypeCnt);

        DiskInfo diskInfo2 = new DiskInfo("/tmp/abc");
        diskInfo2.setStorageMedium(TStorageMedium.SSD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1, "/tmp/abcd", diskInfo2);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assert.assertEquals(2, backendStorageTypeCnt);

        diskInfo2.setStorageMedium(TStorageMedium.HDD);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1, "/tmp/abcd", diskInfo2);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assert.assertEquals(1, backendStorageTypeCnt);

        diskInfo1.setState(DiskInfo.DiskState.OFFLINE);
        disks = ImmutableMap.of("/tmp/abc", diskInfo1);
        backend.setDisks(disks);
        backendStorageTypeCnt = backend.getAvailableBackendStorageTypeCnt();
        Assert.assertEquals(0, backendStorageTypeCnt);

    }

    @Test
    public void testHeartbeatOk() throws Exception {
        Backend be = new Backend();
        BackendHbResponse hbResponse = new BackendHbResponse();
        boolean isChanged = be.handleHbResponse(hbResponse, false);
        Assert.assertTrue(isChanged);
    }

    @Test
    public void testHbStatusAliveChanged() {

        class TestCase {
            final boolean isReplay;

            final HeartbeatResponse.HbStatus responseStatus;
            final HeartbeatResponse.AliveStatus aliveStatus;
            final boolean nodeAlive;

            final boolean expectedNeedSync;
            final boolean expectedNeedNotifyCoordinatorMonitor;
            final boolean expectedAlive;

            public TestCase(boolean isReplay,
                            HeartbeatResponse.HbStatus responseStatus, HeartbeatResponse.AliveStatus aliveStatus,
                            boolean nodeAlive,
                            boolean expectedNeedSync, boolean expectedNeedNotifyCoordinatorMonitor,
                            boolean expectedAlive) {
                this.isReplay = isReplay;
                this.responseStatus = responseStatus;
                this.aliveStatus = aliveStatus;
                this.nodeAlive = nodeAlive;
                this.expectedNeedSync = expectedNeedSync;
                this.expectedNeedNotifyCoordinatorMonitor = expectedNeedNotifyCoordinatorMonitor;
                this.expectedAlive = expectedAlive;
            }
        }

        BackendHbResponse hbResponse = new BackendHbResponse();
        Backend node = new Backend();
        node.setBrpcPort(hbResponse.getBrpcPort()); // Don't return needSync by different BrpcPort.
        CoordinatorMonitor coordinatorMonitor = CoordinatorMonitor.getInstance();

        List<TestCase> testCases = ImmutableList.of(
                new TestCase(false, HeartbeatResponse.HbStatus.BAD, null, true, true, true, false),
                new TestCase(false, HeartbeatResponse.HbStatus.BAD, null, false, true, false, false),
                new TestCase(false, HeartbeatResponse.HbStatus.OK, null, true, false, false, true),
                new TestCase(false, HeartbeatResponse.HbStatus.OK, null, false, true, false, true),

                new TestCase(true, HeartbeatResponse.HbStatus.BAD, null, true, false, true, false),
                new TestCase(true, HeartbeatResponse.HbStatus.BAD, null, false, false, false, false),
                new TestCase(true, HeartbeatResponse.HbStatus.OK, null, true, false, false, true),
                new TestCase(true, HeartbeatResponse.HbStatus.OK, null, false, false, false, true),

                new TestCase(true, HeartbeatResponse.HbStatus.BAD, HeartbeatResponse.AliveStatus.NOT_ALIVE, true, false, true, false),
                new TestCase(true, HeartbeatResponse.HbStatus.BAD, HeartbeatResponse.AliveStatus.NOT_ALIVE, false, false, false, false),
                new TestCase(true, HeartbeatResponse.HbStatus.BAD, HeartbeatResponse.AliveStatus.ALIVE, true, false, false, true),
                new TestCase(true, HeartbeatResponse.HbStatus.BAD, HeartbeatResponse.AliveStatus.ALIVE, false, false, false, true),
                new TestCase(true, HeartbeatResponse.HbStatus.OK, HeartbeatResponse.AliveStatus.ALIVE, true, false, false, true),
                new TestCase(true, HeartbeatResponse.HbStatus.OK, HeartbeatResponse.AliveStatus.ALIVE, false, false, false, true)

        );

        int prevHeartbeatRetryTimes = Config.heartbeat_retry_times;
        try {
            Config.heartbeat_retry_times = 0;

            long nextNodeId = 0L;
            for (TestCase tc : testCases) {
                hbResponse.setStatus(tc.responseStatus);
                hbResponse.aliveStatus = tc.aliveStatus;
                node.setId(nextNodeId++);
                node.setAlive(tc.nodeAlive);
                new Expectations(coordinatorMonitor) {
                    {
                        coordinatorMonitor.addDeadBackend(node.getId());
                        times = tc.expectedNeedNotifyCoordinatorMonitor ? 1 : 0;
                    }
                };
                boolean needSync = node.handleHbResponse(hbResponse, tc.isReplay);
                if (!tc.isReplay) { // Only check needSync for the FE leader.
                    Assert.assertEquals("nodeId: " + node.getId(), tc.expectedNeedSync, needSync);
                }
                Assert.assertEquals("nodeId: " + node.getId(), tc.expectedAlive, node.isAlive());
            }
        } finally {
            Config.heartbeat_retry_times = prevHeartbeatRetryTimes;
        }
    }
}
