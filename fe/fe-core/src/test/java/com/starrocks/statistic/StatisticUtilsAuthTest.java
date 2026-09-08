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

package com.starrocks.statistic;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.JWTTokenProvider;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class StatisticUtilsAuthTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testBuildConnectContextWithAuth() throws AuthenticationException {
        // 1. test with user connect context
        try (MockedStatic<ConnectContext> connectContextMockedStatic = Mockito.mockStatic(ConnectContext.class)) {
            ConnectContext connectContext = new ConnectContext();
            connectContext.setAuthToken("user_token");
            connectContextMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

            ConnectContext context = StatisticUtils.buildConnectContextWithAuth();
            Assert.assertEquals("user_token", context.getAuthToken());
        }

        // 2. test with bot config
        JWTTokenProvider tokenProvider = Mockito.mock(JWTTokenProvider.class);
        when(tokenProvider.getToken()).thenReturn("bot_token");
        GlobalStateMgr globalStateMgr = Mockito.mock(GlobalStateMgr.class);
        when(globalStateMgr.getTokenProvider()).thenReturn(tokenProvider);

        try (MockedStatic<GlobalStateMgr> globalStateMgrMockedStatic = Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<ConnectContext> connectContextMockedStatic = Mockito.mockStatic(ConnectContext.class)) {
            connectContextMockedStatic.when(ConnectContext::get).thenReturn(null);
            globalStateMgrMockedStatic.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
            Config.use_bot_for_background_tasks = true;
            ConnectContext context = StatisticUtils.buildConnectContextWithAuth();
            Assert.assertEquals("bot_token", context.getAuthToken());
            Config.use_bot_for_background_tasks = false;
        }

        // 3. test with no auth
        try (MockedStatic<ConnectContext> connectContextMockedStatic = Mockito.mockStatic(ConnectContext.class)) {
            connectContextMockedStatic.when(ConnectContext::get).thenReturn(null);
            ConnectContext context = StatisticUtils.buildConnectContextWithAuth();
            Assert.assertNull(context.getAuthToken());
        }
    }
}
