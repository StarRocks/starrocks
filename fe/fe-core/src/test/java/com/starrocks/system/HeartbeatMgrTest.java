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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/system/HeartbeatMgrTest.java

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

package com.starrocks.system;

import com.starrocks.catalog.FsBroker;
import com.starrocks.common.Pair;
import com.starrocks.common.util.Util;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.HeartbeatMgr.BrokerHeartbeatHandler;
import com.starrocks.system.HeartbeatMgr.FrontendHeartbeatHandler;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import com.starrocks.thrift.HeartbeatService;
import com.starrocks.thrift.TBrokerOperationStatus;
import com.starrocks.thrift.TBrokerOperationStatusCode;
import com.starrocks.thrift.TBrokerPingBrokerRequest;
import com.starrocks.thrift.TFileBrokerService;
import com.starrocks.thrift.THeartbeatResult;
import com.starrocks.thrift.TMasterInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRunMode;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class HeartbeatMgrTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private NodeMgr nodeMgr;

    @BeforeEach
    public void setUp() {
        new Expectations() {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.isReady();
                minTimes = 0;
                result = true;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getSelfNode();
                minTimes = 0;
                result = Pair.create("192.168.1.3", 9010); // not self
            }
        };
    }

    @Test
    public void testFrontendHbHandler() {
        new MockUp<Util>() {
            @Mock
            public String getResultForUrl(String urlStr, String encodedAuthInfo, int connectTimeoutMs,
                                          int readTimeoutMs) {
                if (urlStr.contains("192.168.1.1")) {
                    return "{\"replayedJournalId\":191224,\"queryPort\":9131,\"rpcPort\":9121,\"status\":\"OK\"," +
                            "\"msg\":\"Success\",\"feStartTime\":1637288321250,\"feVersion\":\"2.0-ac45651a\"}";
                } else {
                    return "{\"replayedJournalId\":0,\"queryPort\":0,\"rpcPort\":0,\"status\":\"FAILED\",\"msg\":\"not ready\"}";
                }
            }
        };

        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "test", "192.168.1.1", 9010);
        FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(fe, 0, "abcd");
        HeartbeatResponse response = handler.call();

        Assertions.assertTrue(response instanceof FrontendHbResponse);
        FrontendHbResponse hbResponse = (FrontendHbResponse) response;
        Assertions.assertEquals(191224, hbResponse.getReplayedJournalId());
        Assertions.assertEquals(9121, hbResponse.getRpcPort());
        Assertions.assertEquals(9131, hbResponse.getQueryPort());
        Assertions.assertEquals(HbStatus.OK, hbResponse.getStatus());
        Assertions.assertEquals(1637288321250L, hbResponse.getFeStartTime());
        Assertions.assertEquals("2.0-ac45651a", hbResponse.getFeVersion());

        Frontend fe2 = new Frontend(FrontendNodeType.FOLLOWER, "test2", "192.168.1.2", 9010);
        handler = new FrontendHeartbeatHandler(fe2, 0, "abcd");
        response = handler.call();

        Assertions.assertTrue(response instanceof FrontendHbResponse);
        hbResponse = (FrontendHbResponse) response;
        Assertions.assertEquals(0, hbResponse.getReplayedJournalId());
        Assertions.assertEquals(0, hbResponse.getRpcPort());
        Assertions.assertEquals(0, hbResponse.getQueryPort());
        Assertions.assertEquals(HbStatus.BAD, hbResponse.getStatus());

    }

    @Test
    public void testBrokerHbHandler(@Mocked TFileBrokerService.Client client) throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.setStatusCode(TBrokerOperationStatusCode.OK);

        new MockUp<ThriftConnectionPool<TFileBrokerService.Client>>() {
            @Mock
            public TFileBrokerService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TFileBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                client.ping((TBrokerPingBrokerRequest) any);
                minTimes = 0;
                result = status;
            }
        };

        FsBroker broker = new FsBroker("192.168.1.1", 8111);
        BrokerHeartbeatHandler handler = new BrokerHeartbeatHandler("hdfs", broker, "abc");
        HeartbeatResponse response = handler.call();

        Assertions.assertTrue(response instanceof BrokerHbResponse);
        BrokerHbResponse hbResponse = (BrokerHbResponse) response;
        System.out.println(hbResponse.toString());
        Assertions.assertEquals(HbStatus.OK, hbResponse.getStatus());
    }

    @Test
    public void testBackendHandler(@Mocked HeartbeatService.Client client) throws Exception {
        TStatus status = new TStatus(TStatusCode.ABORTED);
        status.setError_msgs(Collections.singletonList("error_msg"));
        THeartbeatResult res = new THeartbeatResult();
        res.setStatus(status);

        new MockUp<ThriftConnectionPool<?>>() {
            @Mock
            public HeartbeatService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, HeartbeatService.Client object) {
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, HeartbeatService.Client object) {
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<HeartbeatMgr>() {
            @Mock
            public long computeMinActiveTxnId() {
                return 100L;
            }
        };

        new Expectations() {
            {
                client.heartbeat((TMasterInfo) any);
                minTimes = 1;
                result = res;
            }
        };

        // call setLeader() to init the MASTER_INFO
        new HeartbeatMgr(false).setLeader(1, "123", 1);

        ComputeNode cn = new ComputeNode(1, "192.168.1.1", 8111);
        HeartbeatMgr.BackendHeartbeatHandler handler = new HeartbeatMgr.BackendHeartbeatHandler(cn);
        HeartbeatResponse response = handler.call();
        Assertions.assertTrue(response instanceof BackendHbResponse);
        BackendHbResponse hbResponse = (BackendHbResponse) response;
        Assertions.assertEquals(HbStatus.BAD, hbResponse.getStatus());

        new Verifications() {
            {
                TMasterInfo masterInfo;
                client.heartbeat(masterInfo = withCapture());
                // verify the runMode is set in the masterInfo request
                Assertions.assertNotNull(masterInfo);
                Assertions.assertEquals(TRunMode.SHARED_DATA, masterInfo.getRun_mode());
            }
        };
    }

    @Test
    public void testObserverFeCallsAddWorkerDuringJournalReplay(@Mocked StarOSAgent starOSAgent, 
                                                               @Mocked SystemInfoService systemInfoService) {
        // Test that observer FEs call addWorker() during journal replay to maintain worker cache
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        ComputeNode computeNode = new ComputeNode(1001L, "10.1.1.100", 9050);
        computeNode.setStarletPort(9070);
        computeNode.setWorkerGroupId(2L);

        new Expectations() {
            {
                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starOSAgent;

                globalStateMgr.isLeader();
                minTimes = 0;
                result = false; // Observer FE (not leader)
                
                // Mock the system info lookup
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
                
                // First tries Backend, then ComputeNode
                systemInfoService.getBackend(1001L);
                minTimes = 0;
                result = null; // No backend found
                
                systemInfoService.getComputeNode(1001L);
                minTimes = 0;
                result = computeNode;
            }
        };

        HeartbeatMgr heartbeatMgr = new HeartbeatMgr(false);

        // Create successful heartbeat response
        BackendHbResponse response = new BackendHbResponse(1001L, 9060, 8030, 8060, 9070, 1000L, "test-version", 16, 8000000000L);

        // Test observer FE during journal replay - should call addWorker()
        boolean isChanged = heartbeatMgr.handleHbResponse(response, true /* isReplay */);

        new Verifications() {
            {
                starOSAgent.addWorker(1001L, "10.1.1.100:9070", 2L);
                times = 1; // Should be called for observer FE during replay
            }
        };
    }

    @Test  
    public void testLeaderFeSkipsAddWorkerDuringJournalReplay(@Mocked StarOSAgent starOSAgent,
                                                             @Mocked SystemInfoService systemInfoService) {
        // Test that leader FE does NOT call addWorker() during journal replay
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        ComputeNode computeNode = new ComputeNode(1002L, "10.1.1.101", 9050);
        computeNode.setStarletPort(9070);
        computeNode.setWorkerGroupId(2L);

        new Expectations() {
            {
                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starOSAgent;

                globalStateMgr.isLeader();
                minTimes = 0;
                result = true; // Leader FE
                
                // Mock the system info lookup
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
                
                // First tries Backend, then ComputeNode
                systemInfoService.getBackend(1002L);
                minTimes = 0;
                result = null; // No backend found
                
                systemInfoService.getComputeNode(1002L);
                minTimes = 0;
                result = computeNode;
            }
        };

        HeartbeatMgr heartbeatMgr = new HeartbeatMgr(false);

        // Create successful heartbeat response
        BackendHbResponse response = new BackendHbResponse(1002L, 9060, 8030, 8060, 9070, 1000L, "test-version", 16, 8000000000L);

        // Test leader FE during journal replay - should NOT call addWorker()
        boolean isChanged = heartbeatMgr.handleHbResponse(response, true /* isReplay */);

        new Verifications() {
            {
                starOSAgent.addWorker(anyLong, anyString, anyLong);
                times = 0; // Should NOT be called for leader FE during replay
            }
        };
    }

    @Test
    public void testLiveHeartbeatAlwaysCallsAddWorker(@Mocked StarOSAgent starOSAgent,
                                                     @Mocked SystemInfoService systemInfoService) {
        // Test that both leader and observer FE call addWorker() during live heartbeat processing
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        ComputeNode computeNode = new ComputeNode(1003L, "10.1.1.102", 9050);
        computeNode.setStarletPort(9070);
        computeNode.setWorkerGroupId(2L);

        new Expectations() {
            {
                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starOSAgent;

                globalStateMgr.isLeader();
                minTimes = 0;
                result = true; // Could be either leader or observer - doesn't matter for live heartbeat
                
                // Mock the system info lookup
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
                
                // First tries Backend, then ComputeNode
                systemInfoService.getBackend(1003L);
                minTimes = 0;
                result = null; // No backend found
                
                systemInfoService.getComputeNode(1003L);
                minTimes = 0;
                result = computeNode;
            }
        };

        HeartbeatMgr heartbeatMgr = new HeartbeatMgr(false);

        // Create successful heartbeat response
        BackendHbResponse response = new BackendHbResponse(1003L, 9060, 8030, 8060, 9070, 1000L, "test-version", 16, 8000000000L);

        // Test live heartbeat processing (not replay) - should call addWorker()
        boolean isChanged = heartbeatMgr.handleHbResponse(response, false /* isReplay */);

        new Verifications() {
            {
                starOSAgent.addWorker(1003L, "10.1.1.102:9070", 2L);
                times = 1; // Should be called for live heartbeat processing
            }
        };
    }
}
