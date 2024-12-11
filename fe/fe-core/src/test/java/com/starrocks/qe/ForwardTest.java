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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class ForwardTest {

    TMasterOpRequest makeRequest() {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setCatalog("default");
        request.setCluster("default");
        request.setDb("information_schema");
        request.setQueryId(UUIDUtil.genTUniqueId());
        final TUserIdentity userIdentity = new TUserIdentity();
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
        Assert.assertEquals(result.errorMsg, "");
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
        Assert.assertEquals(result.errorMsg, "Unknown thread id: 1");
    }

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
            public TMasterOpResult proxyExecute(TMasterOpRequest request) {
                throw new RuntimeException("unknown exception");
            }
        };

        final TMasterOpResult result = service.forward(request);
        Assert.assertEquals(result.errorMsg, "unknown exception");
    }
}
