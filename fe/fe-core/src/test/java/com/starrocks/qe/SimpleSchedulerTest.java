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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/SimpleSchedulerTest.java

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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.Reference;
import com.starrocks.common.util.NetUtils;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleSchedulerTest {
    static Reference<Long> ref = new Reference<>();

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;

    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                GlobalStateMgr.getServingState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.isReady();
                result = true;
                minTimes = 0;
            }
        };
        // disable updateBlackListThread
        // This is not good enough, it could be too late to run to this location where the updateBlackListThread
        // is already in running state and possibly has been run for a few cycles.
        SimpleScheduler.disableUpdateBlocklistThread();
    }

    @Test
    public void testGetHostWithBackendId() {
        Config.heartbeat_timeout_second = Integer.MAX_VALUE;
        // three locations
        List<TScanRangeLocation> nullLocations = null;
        List<TScanRangeLocation> emptyLocations = new ArrayList<>();

        List<TScanRangeLocation> twoLocations = new ArrayList<>();
        TScanRangeLocation locationA = new TScanRangeLocation();
        TScanRangeLocation locationB = new TScanRangeLocation();
        locationA.setBackend_id(20);
        locationA.setBackend_id(30);
        twoLocations.add(locationA);
        twoLocations.add(locationB);

        ImmutableMap<Long, ComputeNode> nullBackends = null;
        ImmutableMap<Long, ComputeNode> emptyBackends = ImmutableMap.of();

        // three Backends
        Backend backendA = new Backend(0L, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(1L, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(2L, "addressC", 0);
        backendC.updateOnce(0, 0, 0);

        ImmutableMap<Long, ComputeNode> immutableThreeBackends =
                ImmutableMap.<Long, ComputeNode>builder()
                        .put(backendA.getId(), backendA)
                        .put(backendB.getId(), backendB)
                        .put(backendC.getId(), backendC).build();

        // null Backends
        Assertions.assertNull(SimpleScheduler.getHost(0L, nullLocations, nullBackends, ref));

        // empty Backends
        Assertions.assertNull(SimpleScheduler.getHost(0L, emptyLocations, emptyBackends, ref));

        { // normal Backends
            TNetworkAddress address;
            // BackendId exists
            address = SimpleScheduler.getHost(0, emptyLocations, immutableThreeBackends, ref);
            Assertions.assertNotNull(address);
            Assertions.assertEquals(address.hostname, "addressA");

            address = SimpleScheduler.getHost(2, emptyLocations, immutableThreeBackends, ref);
            Assertions.assertNotNull(address);
            Assertions.assertEquals(address.hostname, "addressC");

            // BackendId not exists and location exists, choose the locations' first
            address = SimpleScheduler.getHost(3, twoLocations, immutableThreeBackends, ref);
            Assertions.assertNotNull(address);
            Assertions.assertEquals(address.hostname, "addressA");
        }

        // abnormal: BackendId not exists and location not exists
        Assertions.assertNull(SimpleScheduler.getHost(3, emptyLocations, immutableThreeBackends, ref));
    }

    @Test
    public void testGetHostWithBackendIdInSharedDataMode() {
        // locations
        List<TScanRangeLocation> locations = new ArrayList<>();
        TScanRangeLocation locationA = new TScanRangeLocation();
        locationA.setBackend_id(0);
        locations.add(locationA);

        // backends
        Backend backendA = new Backend(0, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(1, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(2, "addressC", 0);
        backendC.updateOnce(0, 0, 0);

        Map<Long, Backend> backends = Maps.newHashMap();
        backends.put((long) 0, backendA);
        backends.put((long) 1, backendB);
        backends.put((long) 2, backendC);

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        ImmutableMap<Long, ComputeNode> immutableBackends;

        { // backendA in locations is alive
            backendA.setAlive(true);
            backendB.setAlive(true);
            backendC.setAlive(true);
            immutableBackends = ImmutableMap.copyOf(backends);

            TNetworkAddress address = SimpleScheduler.getHost(0, locations, immutableBackends, ref);
            Assertions.assertNotNull(address);
            Assertions.assertEquals(address.hostname, "addressA");
        }

        { // backendA in locations is not alive
            backendA.setAlive(false);
            backendB.setAlive(false);
            backendC.setAlive(true);
            immutableBackends = ImmutableMap.copyOf(backends);

            TNetworkAddress address = SimpleScheduler.getHost(0, locations, immutableBackends, ref);
            Assertions.assertNotNull(address);
            Assertions.assertEquals(address.hostname, "addressC");
        }
    }

    @Test
    public void testGetHostWithNoParams() {
        Config.heartbeat_timeout_second = Integer.MAX_VALUE;
        ImmutableMap<Long, ComputeNode> nullBackends = null;
        ImmutableMap<Long, ComputeNode> emptyBackends = ImmutableMap.of();

        Backend backendA = new Backend(0, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(1, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(2, "addressC", 0);
        backendC.updateOnce(0, 0, 0);
        ImmutableMap<Long, ComputeNode> immutableThreeBackends =
                ImmutableMap.<Long, ComputeNode>builder()
                        .put(backendA.getId(), backendA)
                        .put(backendB.getId(), backendB)
                        .put(backendC.getId(), backendC).build();

        { // abnormal
            Assertions.assertNull(SimpleScheduler.getBackendHost(nullBackends, ref));
            Assertions.assertNull(SimpleScheduler.getBackendHost(emptyBackends, ref));
        }
        { // normal
            TNetworkAddress addressA = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
            Assertions.assertNotNull(addressA);
            String a = addressA.hostname;

            TNetworkAddress addressB = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
            Assertions.assertNotNull(addressB);
            String b = addressB.hostname;

            TNetworkAddress addressC = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
            Assertions.assertNotNull(addressC);
            String c = addressC.hostname;

            Assertions.assertTrue(!a.equals(b) && !a.equals(c) && !b.equals(c));

            addressA = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
            Assertions.assertNotNull(addressA);
            a = addressA.hostname;

            addressB = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
            Assertions.assertNotNull(addressB);
            b = addressB.hostname;

            addressC = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
            Assertions.assertNotNull(addressC);
            c = addressC.hostname;
            Assertions.assertTrue(!a.equals(b) && !a.equals(c) && !b.equals(c));
        }
    }

    @Test
    public void testBlackList() {
        Config.heartbeat_timeout_second = Integer.MAX_VALUE;
        TNetworkAddress address;

        Backend backendA = new Backend(100L, "addressA", 0);
        backendA.updateOnce(0, 0, 0);
        Backend backendB = new Backend(101L, "addressB", 0);
        backendB.updateOnce(0, 0, 0);
        Backend backendC = new Backend(102L, "addressC", 0);
        backendC.updateOnce(0, 0, 0);
        ImmutableMap<Long, ComputeNode> immutableThreeBackends =
                ImmutableMap.<Long, ComputeNode>builder()
                        .put(backendA.getId(), backendA)
                        .put(backendB.getId(), backendB)
                        .put(backendC.getId(), backendC).build();

        SimpleScheduler.addToBlocklist(backendA.getId());
        SimpleScheduler.addToBlocklist(backendB.getId());

        address = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
        Assertions.assertNotNull(address);
        // only backendC can work
        Assertions.assertEquals(address.hostname, "addressC");

        SimpleScheduler.addToBlocklist(backendC.getId());
        // no backend can work
        address = SimpleScheduler.getBackendHost(immutableThreeBackends, ref);
        Assertions.assertNull(address);
    }

    @Test
    public void testRemoveBackendFromBlackList() {
        Config.heartbeat_timeout_second = Integer.MAX_VALUE;
        TNetworkAddress address;

        Backend backendA = new Backend(100, "addressA", 0);
        backendA.setBrpcPort(1);

        backendA.updateOnce(2, 3, 4);
        Map<Long, Backend> backends = Maps.newHashMap();
        backends.put(100L, backendA);
        ImmutableMap<Long, ComputeNode> immutableBackends = ImmutableMap.copyOf(backends);

        SimpleScheduler.addToBlocklist(100L);
        address = SimpleScheduler.getBackendHost(immutableBackends, ref);
        Assertions.assertNull(address);

        String host = backendA.getHost();
        List<Integer> ports = new ArrayList<>();
        Collections.addAll(ports, backendA.getBePort(), backendA.getBrpcPort(), backendA.getHttpPort());
        boolean accessible = NetUtils.checkAccessibleForAllPorts(host, ports);
        Assertions.assertFalse(accessible);

        SimpleScheduler.removeFromBlocklist(100L);
        address = SimpleScheduler.getBackendHost(immutableBackends, ref);
        Assertions.assertNotNull(address);
        Assertions.assertEquals(address.hostname, "addressA");
    }

    @Test
    public void testEmptyBackendList() {
        Reference<Long> idRef = new Reference<>();
        TNetworkAddress address = SimpleScheduler.getBackendHost(null, idRef);
        Assertions.assertNull(address);

        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        address = SimpleScheduler.getBackendHost(builder.build(), idRef);
        Assertions.assertNull(address);
    }

    @Test
    public void testEmptyComputeNodeList() {
        Reference<Long> idRef = new Reference<>();
        TNetworkAddress address = SimpleScheduler.getComputeNodeHost(null, idRef);
        Assertions.assertNull(address);

        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        address = SimpleScheduler.getComputeNodeHost(builder.build(), idRef);
        Assertions.assertNull(address);
    }

    @Test
    public void testUpdateBlacklist(@Mocked SystemInfoService systemInfoService, @Mocked NetUtils netUtils) {
        Config.heartbeat_timeout_second = 1;
        // the node is allowed to be removed from blocklist immediately.
        long defaultBlockPenaltyTime = Config.black_host_penalty_min_ms;
        Config.black_host_penalty_min_ms = 0;

        Backend backend1 = new Backend(10001L, "host10002", 10002);
        backend1.setAlive(true);
        backend1.setBrpcPort(10002);
        backend1.setHttpPort(10012);

        ComputeNode computeNode1 = new ComputeNode(10003, "host10003", 10003);
        computeNode1.setAlive(false);
        computeNode1.setBrpcPort(10003);
        computeNode1.setHttpPort(10013);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                times = 2;

                // backend 10001 will be removed
                systemInfoService.getBackendOrComputeNode(10001L);
                result = null;
                times = 1;

                systemInfoService.getBackendOrComputeNode(10002L);
                result = backend1;
                times = 1;

                systemInfoService.checkNodeAvailable(backend1);
                result = true;
                times = 1;

                NetUtils.checkAccessibleForAllPorts("host10002", (List<Integer>) any);
                result = true;
                times = 1;

                // backend 10003, which is not available, will not be removed
                systemInfoService.getBackendOrComputeNode(10003L);
                result = computeNode1;
                times = 2;

                systemInfoService.checkNodeAvailable(computeNode1);
                result = false;
                times = 2;
            }
        };
        SimpleScheduler.addToBlocklist(10001L);
        SimpleScheduler.addToBlocklist(10002L);
        SimpleScheduler.addToBlocklist(10003L);

        SimpleScheduler.getHostBlacklist().refresh();

        Assertions.assertFalse(SimpleScheduler.isInBlocklist(10001L));
        Assertions.assertFalse(SimpleScheduler.isInBlocklist(10002L));
        Assertions.assertTrue(SimpleScheduler.isInBlocklist(10003L));

        //Having retried for Config.heartbeat_timeout_second + 1 times, backend 10003 will be removed.
        SimpleScheduler.getHostBlacklist().refresh();
        Assertions.assertTrue(SimpleScheduler.isInBlocklist(10003L));

        Config.black_host_penalty_min_ms = defaultBlockPenaltyTime;
    }

    @Test
    public void testNoAliveBackend() {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        for (int i = 0; i < 6; i++) {
            Backend backend = new Backend(i, "address" + i, 0);
            backend.setAlive(false);
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, ComputeNode> backends = builder.build();
        Reference<Long> idRef = new Reference<>();
        TNetworkAddress address = SimpleScheduler.getBackendHost(backends, idRef);
        Assertions.assertNull(address);
    }

    @Test
    public void testNoAliveComputeNode() {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        for (int i = 0; i < 6; i++) {
            ComputeNode node = new ComputeNode(i, "address" + i, 0);
            node.setAlive(false);
            builder.put(node.getId(), node);
        }
        ImmutableMap<Long, ComputeNode> nodes = builder.build();
        Reference<Long> idRef = new Reference<>();
        TNetworkAddress address = SimpleScheduler.getComputeNodeHost(nodes, idRef);
        Assertions.assertNull(address);
    }

    @Test
    public void testChooseBackendConcurrently() throws InterruptedException {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        for (int i = 0; i < 6; i++) {
            Backend backend = new Backend(i, "address" + i, 0);
            backend.setAlive(i == 0);
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, ComputeNode> backends = builder.build();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Thread t = new Thread(() -> {
                for (int i1 = 0; i1 < 50; i1++) {
                    Reference<Long> idRef = new Reference<>();
                    TNetworkAddress address = SimpleScheduler.getBackendHost(backends, idRef);
                    Assertions.assertNotNull(address);
                    Assertions.assertEquals("address0", address.hostname);
                }
            });
            threads.add(t);
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void testChooseComputeNodeConcurrently() throws InterruptedException {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        for (int i = 0; i < 6; i++) {
            ComputeNode backend = new ComputeNode(i, "address" + i, 0);
            backend.setAlive(i == 0);
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, ComputeNode> nodes = builder.build();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Thread t = new Thread(() -> {
                for (int i1 = 0; i1 < 50; i1++) {
                    Reference<Long> idRef = new Reference<>();
                    TNetworkAddress address = SimpleScheduler.getComputeNodeHost(nodes, idRef);
                    Assertions.assertNotNull(address);
                    Assertions.assertEquals("address0", address.hostname);
                }
            });
            threads.add(t);
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void testTimeUpdate(@Mocked SystemInfoService systemInfoService, @Mocked NetUtils netUtils)
            throws InterruptedException {
        Config.black_host_history_sec = 5; // 5s
        HostBlacklist blacklist = new HostBlacklist();
        blacklist.disableAutoUpdate();
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;

                // backend 10001 will be removed
                systemInfoService.getBackendOrComputeNode(10001L);
                result = null;

                // backend 10002 will be removed
                ComputeNode backend2 = new ComputeNode();
                backend2.setAlive(true);
                backend2.setHost("host10002");
                backend2.setBrpcPort(10002);
                backend2.setHttpPort(10012);
                systemInfoService.getBackendOrComputeNode(10002L);
                result = backend2;

                systemInfoService.checkNodeAvailable(backend2);
                result = true;

                NetUtils.checkAccessibleForAllPorts((String) any, (List<Integer>) any);
                result = true;

                // backend 10003, which is not available
                ComputeNode backend3 = new ComputeNode();
                backend3.setAlive(true);
                backend3.setHost("host10003");
                backend3.setBrpcPort(10003);
                backend3.setHttpPort(10013);
                systemInfoService.getBackendOrComputeNode(10003L);
                result = backend3;

                systemInfoService.checkNodeAvailable(backend3);
                result = true;
            }
        };

        blacklist.add(10001L);
        blacklist.add(10002L);
        blacklist.add(10003L);
        for (int i = 0; i < 7; i++) {
            blacklist.add(10003L);
            Thread.sleep(1000);
            Assertions.assertTrue(blacklist.contains(10003L));
        }

        Thread.sleep(2000);
        blacklist.refresh();
        Assertions.assertFalse(blacklist.contains(10003L));
    }

    @Test
    public void testManualAdd(@Mocked SystemInfoService systemInfoService) throws InterruptedException {
        Config.black_host_history_sec = 5; // 5s
        HostBlacklist blacklist = new HostBlacklist();
        blacklist.disableAutoUpdate();
        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;

                // backend 10003, which is not available
                ComputeNode backend3 = new ComputeNode();
                backend3.setAlive(true);
                backend3.setHost("host10003");
                backend3.setBrpcPort(10003);
                backend3.setHttpPort(10013);
                systemInfoService.getBackendOrComputeNode(10003L);
                result = backend3;

                systemInfoService.checkNodeAvailable(backend3);
                result = true;
            }
        };

        blacklist.addByManual(10003L);
        Thread.sleep(7000);
        Assertions.assertTrue(blacklist.contains(10003L));

        Thread.sleep(2000);
        blacklist.refresh();
        Assertions.assertTrue(blacklist.contains(10003L));
    }

    @Test
    public void testPenaltyTimeInBlockList(@Mocked SystemInfoService systemInfoService, @Mocked NetUtils netUtils)
            throws InterruptedException {
        Config.black_host_history_sec = 5; // 5s
        HostBlacklist blacklist = new HostBlacklist();
        blacklist.disableAutoUpdate();

        // stay at least 2 seconds before removed from the list
        long originPenaltyTime = Config.black_host_penalty_min_ms;
        Config.black_host_penalty_min_ms = 2000;

        long nodeId = 2000L;
        ComputeNode node = new ComputeNode(nodeId, "computeNode", 1111);
        node.setBrpcPort(0);
        node.updateOnce(1, 2, 3);
        List<Integer> ports = new ArrayList<>();
        Collections.addAll(ports, node.getBrpcPort(), node.getBePort(), node.getHttpPort(), node.getBeRpcPort());

        new Expectations() {
            {
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = systemInfoService;

                systemInfoService.getBackendOrComputeNode(nodeId);
                result = node;

                systemInfoService.checkNodeAvailable(node);
                result = true;

                NetUtils.checkAccessibleForAllPorts(anyString, (List<Integer>) any);
                result = true;
            }
        };

        long ts1 = System.currentTimeMillis();
        blacklist.add(node.getId());
        long ts2 = System.currentTimeMillis();

        long checkedCount = 0;
        while (ts1 + Config.black_host_penalty_min_ms > System.currentTimeMillis()) {
            Assertions.assertNotNull(systemInfoService.getBackendOrComputeNode(nodeId));
            Assertions.assertTrue(systemInfoService.checkNodeAvailable(node));
            Assertions.assertTrue(NetUtils.checkAccessibleForAllPorts(node.getHost(), ports));
            blacklist.refresh();
            // still in the blocklist
            Assertions.assertTrue(blacklist.contains(node.getId()));
            ++checkedCount;
            // check every 200ms
            Thread.sleep(200);
        }
        Assertions.assertTrue(checkedCount > 0);
        long remainMs = System.currentTimeMillis() - ts2 - Config.black_host_penalty_min_ms;
        if (remainMs > 0) {
            Thread.sleep(remainMs);
        }
        // must be expired in the blocklist
        Assertions.assertTrue(ts2 + Config.black_host_penalty_min_ms <= System.currentTimeMillis());
        blacklist.refresh();
        Assertions.assertFalse(blacklist.contains(node.getId()));

        Config.black_host_penalty_min_ms = originPenaltyTime;
    }
}
