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

package com.starrocks.qe;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class ForwardTest extends SchedulerTestBase {
    @BeforeEach
    public void before() {
        mockFrontends(FRONTENDS);
        mockFrontendService(new MockFrontendServiceClient());
    }

    TMasterOpRequest makeRequest() {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setCatalog("default");
        request.setCluster("default");
        request.setDb("information_schema");
        request.setQueryId(UUIDUtil.genTUniqueId());
        final TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("user1");
        userIdentity.setHost("%");
        userIdentity.setIs_ephemeral(true);
        request.setCurrent_user_ident(userIdentity);
        return request;
    }

    @Test
    public void testKillConnection() throws Exception {
        final FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());
        final TMasterOpRequest request = makeRequest();
        request.setConnectionId(1);
        request.setSql("kill QUERY 1");
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 1L;
            }
        };
        final TMasterOpResult result = service.forward(request);
        Assertions.assertEquals(result.errorMsg, "");
    }

    @Test
    public void testUpgradeKillConnection() throws Exception {
        final FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());
        final TMasterOpRequest request = makeRequest();
        request.setSql("kill QUERY 1");
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 1L;
            }
        };
        final TMasterOpResult result = service.forward(request);
        Assertions.assertEquals(result.errorMsg, "Unknown thread id: 1");
    }

    /**
     * Mockup {@link ConnectProcessor#proxyExecute(TMasterOpRequest, Frontend)}.
     */
    @Test
    public void testKillWithUnknownException() throws Exception {
        final FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());
        final TMasterOpRequest request = makeRequest();
        request.setSql("kill QUERY 1");
        request.setConnectionId(1);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 1L;
            }
        };
        new MockUp<FrontendServiceImpl>() {
            @Mock
            public TNetworkAddress getClientAddr() {
                return null;
            }
        };

        new MockUp<ConnectProcessor>() {
            @Mock
            public TMasterOpResult proxyExecute(TMasterOpRequest request, Frontend requestFE) {
                throw new RuntimeException("unknown exception");
            }
        };

        final TMasterOpResult result = service.forward(request);
        Assertions.assertEquals("unknown exception", result.errorMsg);
    }

    @Test
    public void testArrowFlightSQL() throws Exception {
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.scanBytes = 1L;
        statistics.scanRows = 1L;
        statistics.returnedRows = 1L;
        new MockUp<RowBatch>() {
            @Mock
            public PQueryStatistics getQueryStatistics() {
                return statistics;
            }
        };

        final FrontendServiceImpl service = spy(new FrontendServiceImpl(ExecuteEnv.getInstance()));

        TNetworkAddress address = new TNetworkAddress();
        Frontend frontend = FRONTENDS.get(0);
        frontend.setRpcPort(8030);
        address.setHostname(frontend.getHost());
        address.setPort(frontend.getRpcPort());
        doReturn(address).when(service).getClientAddr();

        final TMasterOpRequest request = makeRequest();
        request.setConnectionId(1);
        request.setSql("select 1");
        request.setIsInternalStmt(false);
        request.setIs_arrow_flight_sql(true);
        request.setUser("root");

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 1L;
            }

            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        final TMasterOpResult result = service.forward(request);
        Assertions.assertEquals("", result.errorMsg);
    }
}
