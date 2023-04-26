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
import com.starrocks.common.GenericPool;
import com.starrocks.common.Pair;
import com.starrocks.common.util.Util;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.HeartbeatMgr.BrokerHeartbeatHandler;
import com.starrocks.system.HeartbeatMgr.FrontendHeartbeatHandler;
import com.starrocks.system.HeartbeatResponse.HbStatus;
import com.starrocks.thrift.TBrokerOperationStatus;
import com.starrocks.thrift.TBrokerOperationStatusCode;
import com.starrocks.thrift.TBrokerPingBrokerRequest;
import com.starrocks.thrift.TFileBrokerService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HeartbeatMgrTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Before
    public void setUp() {
        new Expectations() {
            {
                globalStateMgr.getSelfNode();
                minTimes = 0;
                result = Pair.create("192.168.1.3", 9010); // not self

                globalStateMgr.isReady();
                minTimes = 0;
                result = true;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
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
        FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(fe, 12345, "abcd");
        HeartbeatResponse response = handler.call();

        Assert.assertTrue(response instanceof FrontendHbResponse);
        FrontendHbResponse hbResponse = (FrontendHbResponse) response;
        Assert.assertEquals(191224, hbResponse.getReplayedJournalId());
        Assert.assertEquals(9121, hbResponse.getRpcPort());
        Assert.assertEquals(9131, hbResponse.getQueryPort());
        Assert.assertEquals(HbStatus.OK, hbResponse.getStatus());
        Assert.assertEquals(1637288321250L, hbResponse.getFeStartTime());
        Assert.assertEquals("2.0-ac45651a", hbResponse.getFeVersion());

        Frontend fe2 = new Frontend(FrontendNodeType.FOLLOWER, "test2", "192.168.1.2", 9010);
        handler = new FrontendHeartbeatHandler(fe2, 12345, "abcd");
        response = handler.call();

        Assert.assertTrue(response instanceof FrontendHbResponse);
        hbResponse = (FrontendHbResponse) response;
        Assert.assertEquals(0, hbResponse.getReplayedJournalId());
        Assert.assertEquals(0, hbResponse.getRpcPort());
        Assert.assertEquals(0, hbResponse.getQueryPort());
        Assert.assertEquals(HbStatus.BAD, hbResponse.getStatus());

    }

    @Test
    public void testBrokerHbHandler(@Mocked TFileBrokerService.Client client) throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.setStatusCode(TBrokerOperationStatusCode.OK);

        new MockUp<GenericPool<TFileBrokerService.Client>>() {
            @Mock
            public TFileBrokerService.Client borrowObject(TNetworkAddress address) throws Exception {
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

        Assert.assertTrue(response instanceof BrokerHbResponse);
        BrokerHbResponse hbResponse = (BrokerHbResponse) response;
        System.out.println(hbResponse.toString());
        Assert.assertEquals(HbStatus.OK, hbResponse.getStatus());
    }

}
