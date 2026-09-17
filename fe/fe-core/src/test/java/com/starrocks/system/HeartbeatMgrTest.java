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
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.Util;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.gson.GsonUtils;
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
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

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
        HeartbeatMgr.BackendHeartbeatHandler handler = new HeartbeatMgr.BackendHeartbeatHandler(cn, true);
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
    public void testBackendHandlerCarriesLastHeartbeatTime(@Mocked HeartbeatService.Client client) throws Exception {
        TStatus shutdownStatus = new TStatus(TStatusCode.SHUTDOWN);
        shutdownStatus.setError_msgs(Collections.singletonList("BE is shutting down"));
        THeartbeatResult res = new THeartbeatResult();
        res.setStatus(shutdownStatus);

        new MockUp<ThriftConnectionPool<HeartbeatService.Client>>() {
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
        cn.setLastUpdateMs(777777L);
        HeartbeatMgr.BackendHeartbeatHandler handler = new HeartbeatMgr.BackendHeartbeatHandler(cn, true);

        handler.call();
        new Verifications() {
            {
                TMasterInfo masterInfo;
                client.heartbeat(masterInfo = withCapture());
                Assertions.assertNotNull(masterInfo);
                // The request always carries the FE's LastHeartbeat time for this BE; the BE uses
                // its advance as the shutdown ack.
                Assertions.assertTrue(masterInfo.isSetLast_heartbeat_time_ms());
                Assertions.assertEquals(777777L, masterInfo.getLast_heartbeat_time_ms());
            }
        };
    }

    @Test
    public void testShutdownHeartbeatDoesNotAbortCoordinatorTxns(@Mocked SystemInfoService clusterInfo,
                                                                 @Mocked GlobalTransactionMgr txnMgr) {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        new Expectations() {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = clusterInfo;

                clusterInfo.getBackend(1);
                minTimes = 0;
                result = cn;

                globalStateMgr.getGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        BackendHbResponse hbResponse = new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down");
        Deencapsulation.invoke(mgr, "handleHbResponse", hbResponse, false);

        // The shutdown BE keeps draining its coordinator loads; they must commit or abort normally.
        new Verifications() {
            {
                txnMgr.abortTxnWhenCoordinateBeDown(anyString, anyInt);
                times = 0;
            }
        };
        Assertions.assertFalse(cn.isAlive(), "SHUTDOWN must still mark the node not alive");
    }

    @Test
    public void testNonShutdownHeartbeatFailureAbortsCoordinatorTxns(@Mocked SystemInfoService clusterInfo,
                                                                     @Mocked GlobalTransactionMgr txnMgr) {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        cn.setAlive(false); // already marked not alive, e.g. by a previous SHUTDOWN heartbeat
        new Expectations() {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = clusterInfo;

                clusterInfo.getBackend(1);
                minTimes = 0;
                result = cn;

                globalStateMgr.getGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        BackendHbResponse hbResponse = new BackendHbResponse(1, TStatusCode.INTERNAL_ERROR, "connection refused");
        // Transient losses while the status is still SHUTDOWN/CONNECTING must not abort; the
        // node only turns DISCONNECTED (and aborts) after heartbeat_retry_times failures.
        for (int i = 0; i <= Config.heartbeat_retry_times; i++) {
            Deencapsulation.invoke(mgr, "handleHbResponse", hbResponse, false);
        }

        Assertions.assertEquals(ComputeNode.Status.DISCONNECTED, cn.getStatus());
        new Verifications() {
            {
                txnMgr.abortTxnWhenCoordinateBeDown(cn.getHost(), 100);
                times = 1;
            }
        };
    }

    @Test
    public void testOkAfterShutdownAbortsCoordinatorTxns(@Mocked SystemInfoService clusterInfo,
                                                        @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        cn.setLastStartTime(100_000L);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        new Expectations() {
            {
                txnMgr.peekNextTransactionId();
                minTimes = 1;
                result = 100L;
                txnMgr.getTransactionIdByCoordinateBe(cn.getHost(), 1L, 100L, 100);
                minTimes = 1;
                result = java.util.List.of(new Pair<>(5L, 10L), new Pair<>(6L, 20L));
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.abortTransaction(5L, 10L, anyString, false);
                times = 1;
                txnMgr.abortTransaction(6L, 20L, anyString, false);
                times = 1;
                txnMgr.abortTransaction(anyLong, anyLong, anyString, false);
                times = 2;
            }
        };
        Assertions.assertTrue(cn.isAlive());
        Assertions.assertEquals(ComputeNode.Status.OK, cn.getStatus());
        Assertions.assertEquals(0L, cn.getShutdownTxnIdWatermark());
    }

    @Test
    public void testOkAfterShutdownSkipsPostRestartTxn(@Mocked SystemInfoService clusterInfo,
                                                       @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        cn.setLastStartTime(100_000L);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        // Query layer returns only txns below the watermark.
        new Expectations() {
            {
                txnMgr.peekNextTransactionId();
                minTimes = 1;
                result = 100L;
                txnMgr.getTransactionIdByCoordinateBe(cn.getHost(), 1L, 100L, 100);
                result = java.util.List.of(new Pair<>(5L, 10L));
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.abortTransaction(5L, 10L, anyString, false);
                times = 1;
            }
        };
    }

    @Test
    public void testRepeatedShutdownRaisesWatermark(@Mocked SystemInfoService clusterInfo,
                                                   @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        new Expectations() {
            {
                txnMgr.peekNextTransactionId();
                returns(100L, 150L);
                txnMgr.getTransactionIdByCoordinateBe(cn.getHost(), 1L, 150L, 100);
                result = java.util.List.of(new Pair<>(1L, 120L));
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Assertions.assertEquals(100L, cn.getShutdownTxnIdWatermark());
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Assertions.assertEquals(150L, cn.getShutdownTxnIdWatermark());
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.abortTransaction(1L, 120L, anyString, false);
                times = 1;
            }
        };
        Assertions.assertEquals(0L, cn.getShutdownTxnIdWatermark());
    }

    @Test
    public void testRepeatedShutdownDoesNotLowerWatermark(@Mocked SystemInfoService clusterInfo,
                                                         @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        new Expectations() {
            {
                txnMgr.peekNextTransactionId();
                returns(100L, 90L);
                txnMgr.getTransactionIdByCoordinateBe(cn.getHost(), 1L, 100L, 100);
                result = java.util.List.of(new Pair<>(1L, 50L));
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Assertions.assertEquals(100L, cn.getShutdownTxnIdWatermark());
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Assertions.assertEquals(100L, cn.getShutdownTxnIdWatermark());
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.abortTransaction(1L, 50L, anyString, false);
                times = 1;
            }
        };
    }

    @Test
    public void testReplayShutdownRestoresWatermarkThenOkAborts(@Mocked SystemInfoService clusterInfo,
                                                               @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        new Expectations() {
            {
                txnMgr.peekNextTransactionId();
                minTimes = 0;
                txnMgr.getTransactionIdByCoordinateBe(cn.getHost(), 1L, 100L, 100);
                result = java.util.List.of(new Pair<>(5L, 10L));
            }
        };

        BackendHbResponse shutdown = new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down");
        shutdown.setShutdownTxnIdWatermark(100L);
        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse", shutdown, true);
        Assertions.assertEquals(100L, cn.getShutdownTxnIdWatermark());
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.abortTransaction(5L, 10L, anyString, false);
                times = 1;
            }
        };
        Assertions.assertEquals(0L, cn.getShutdownTxnIdWatermark());
    }

    @Test
    public void testWatermarkSurvivesGsonRoundTrip() {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        cn.setShutdownTxnIdWatermark(99L);
        Backend copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(cn), Backend.class);
        Assertions.assertEquals(99L, copy.getShutdownTxnIdWatermark());
    }

    @Test
    public void testSameHostPassesRestartedBackendIdToQuery(@Mocked SystemInfoService clusterInfo,
                                                           @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(7, "10.0.0.1", 8111);
        mockBackendTxn(clusterInfo, txnMgr, cn);
        new Expectations() {
            {
                clusterInfo.getBackend(1);
                minTimes = 0;
                result = cn;
                txnMgr.peekNextTransactionId();
                result = 50L;
                txnMgr.getTransactionIdByCoordinateBe("10.0.0.1", 7L, 50L, 100);
                result = java.util.List.of(new Pair<>(1L, 9L));
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.getTransactionIdByCoordinateBe("10.0.0.1", 7L, 50L, 100);
                times = 1;
                txnMgr.abortTransaction(1L, 9L, anyString, false);
                times = 1;
            }
        };
    }

    @Test
    public void testOkRebootWhileAliveDoesNotAbortCoordinatorTxns(@Mocked SystemInfoService clusterInfo,
                                                                 @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        cn.setAlive(true);
        cn.setLastStartTime(100_000L);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 200), false);

        new Verifications() {
            {
                txnMgr.abortTxnWhenCoordinateBeDown(anyString, anyInt);
                times = 0;
                txnMgr.abortTransaction(anyLong, anyLong, anyString, false);
                times = 0;
            }
        };
    }

    @Test
    public void testReplayOkAfterShutdownDoesNotAbort(@Mocked SystemInfoService clusterInfo,
                                                     @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        cn.setLastStartTime(100_000L);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        new Expectations() {
            {
                txnMgr.peekNextTransactionId();
                minTimes = 0;
                result = 100L;
            }
        };

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse",
                new BackendHbResponse(1, TStatusCode.SHUTDOWN, "BE is shutting down"), false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), true);

        new Verifications() {
            {
                txnMgr.abortTxnWhenCoordinateBeDown(anyString, anyInt);
                times = 0;
                // Replay OK after a shutdown must not abort the snapshot.
                txnMgr.abortTransaction(anyLong, anyLong, anyString, false);
                times = 0;
            }
        };
    }

    @Test
    public void testFirstJoinOkDoesNotAbortCoordinatorTxns(@Mocked SystemInfoService clusterInfo,
                                                          @Mocked GlobalTransactionMgr txnMgr) throws Exception {
        Backend cn = new Backend(1, "192.168.1.1", 8111);
        mockBackendTxn(clusterInfo, txnMgr, cn);

        HeartbeatMgr mgr = new HeartbeatMgr(false);
        Deencapsulation.invoke(mgr, "handleHbResponse", okHb(1, 100), false);

        new Verifications() {
            {
                txnMgr.abortTxnWhenCoordinateBeDown(anyString, anyInt);
                times = 0;
                txnMgr.abortTransaction(anyLong, anyLong, anyString, false);
                times = 0;
            }
        };
    }

    private void mockBackendTxn(SystemInfoService clusterInfo, GlobalTransactionMgr txnMgr, Backend cn) {
        new Expectations() {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = clusterInfo;

                clusterInfo.getBackend(1);
                minTimes = 0;
                result = cn;

                globalStateMgr.getGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;
            }
        };
    }

    private static BackendHbResponse okHb(long beId, long rebootSec) {
        BackendHbResponse hb = new BackendHbResponse(beId, 9050, 8040, 8060, 0,
                System.currentTimeMillis(), "v", 8, 0L);
        hb.setRebootTime(rebootSec);
        return hb;
    }

    @Test
    public void testOnStoppedShutsDownAndAwaitsExecutorTermination() {
        HeartbeatMgr mgr = new HeartbeatMgr(false);
        // start() lazy-inits the executor.
        mgr.start();
        ExecutorService before = mgr.executor;
        Assertions.assertNotNull(before, "executor must be initialized after start()");

        // protected onStopped() is visible from the same package.
        mgr.onStopped();

        Assertions.assertTrue(before.isShutdown(), "previous executor must be shut down");
        Assertions.assertTrue(before.isTerminated(),
                "previous executor must be terminated after onStopped() awaits drain");
        // Nulled after the drain for consistency with the other pool-owning daemons
        // (PublishVersionDaemon, AutovacuumDaemon); start() lazily rebuilds either way.
        Assertions.assertNull(mgr.executor, "executor reference is dropped after successful drain");
    }

    @Test
    public void testStartRebuildsExecutorAfterOnStopped() {
        HeartbeatMgr mgr = new HeartbeatMgr(false);
        mgr.start();
        ExecutorService originalExecutor = mgr.executor;
        mgr.onStopped();
        Assertions.assertTrue(originalExecutor.isTerminated());

        mgr.start();
        try {
            Assertions.assertNotSame(originalExecutor, mgr.executor,
                    "executor must be rebuilt on re-election");
            Assertions.assertFalse(mgr.executor.isShutdown(),
                    "rebuilt executor must accept new heartbeats");
        } finally {
            mgr.setStop();
        }
    }

}
