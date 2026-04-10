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
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ForwardStarMgrJournalTest extends SchedulerTestBase {
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
    public void testProxyExecute_sharedDataMode_setsStarMgrJournalId() throws Exception {
        final FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());
        final long expectedStarMgrJournalId = 12345L;

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 100L;
            }

            @Mock
            public boolean isLeader() {
                return true;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        new MockUp<StarMgrServer>() {
            @Mock
            public StarMgrServer getCurrentState() {
                return new StarMgrServer() {
                    @Override
                    public long getMaxJournalId() {
                        return expectedStarMgrJournalId;
                    }
                };
            }
        };

        final TMasterOpRequest request = makeRequest();
        request.setConnectionId(1);
        request.setSql("kill QUERY 1");

        final TMasterOpResult result = service.forward(request);
        Assertions.assertTrue(result.isSetMaxStarMgrJournalId());
        Assertions.assertEquals(expectedStarMgrJournalId, result.getMaxStarMgrJournalId());
    }

    @Test
    public void testProxyExecute_sharedNothingMode_noStarMgrJournalId() throws Exception {
        final FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 100L;
            }

            @Mock
            public boolean isLeader() {
                return true;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return false;
            }
        };

        final TMasterOpRequest request = makeRequest();
        request.setConnectionId(1);
        request.setSql("kill QUERY 1");

        final TMasterOpResult result = service.forward(request);
        Assertions.assertFalse(result.isSetMaxStarMgrJournalId());
    }
}
