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

package com.starrocks.authentication;

import com.starrocks.analysis.UserDesc;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class CustomAuthenticationProviderTest {

    static String AUTH_PLUGIN = "AUTHENTICATION_CUSTOM";

    byte[] password = "".getBytes();
    byte[] randomString = "".getBytes();

    String user = "test";

    String host = "%";

    static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        Config.authorization_custom_class = "com.starrocks.authentication.FakeCustomAuthenticationProviderTest";
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @Test
    public void testCustomInNativeAuthentication() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        UserDesc userDesc = new UserDesc(testUser, AUTH_PLUGIN, "", false, NodePosition.ZERO);
        AuthenticationMgr mgr = ctx.getGlobalStateMgr().getAuthenticationMgr();
        CreateUserStmt stmt = new CreateUserStmt(true, userDesc, Collections.emptyList());
        UserAuthenticationInfo authenticationInfo = new UserAuthenticationInfo();
        authenticationInfo.setOrigUserHost(testUser.getUser(), testUser.getHost());
        authenticationInfo.setAuthPlugin(AUTH_PLUGIN);
        stmt.setAuthenticationInfo(authenticationInfo);
        mgr.createUser(stmt);

        UserIdentity check1 = mgr.checkPassword(user, host, password, randomString);
        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                mgr.getBestMatchedUserIdentity(user, host);
        Assert.assertNotNull(matchedUserIdentity);

        AuthenticationProvider provider =
                AuthenticationProviderFactory.create(matchedUserIdentity.getValue().getAuthPlugin());
        Assert.assertTrue(provider instanceof CustomAuthenticationProvider);
        try {
            provider.authenticate(user, host, password, randomString, matchedUserIdentity.getValue());
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Login error"));
        }

    }

    @Test
    public void testCustomInNonNativeAuthentication() throws Exception {
        AuthenticationMgr mgr = ctx.getGlobalStateMgr().getAuthenticationMgr();
        String authMechanism = "custom";
        SecurityIntegration securityIntegration = new CustomSecurityIntegration(authMechanism, Collections.EMPTY_MAP);

        AuthenticationProvider provider = securityIntegration.getAuthenticationProvider();
        Assert.assertTrue(provider instanceof CustomAuthenticationProvider);

        try {
            UserAuthenticationInfo userAuthenticationInfo = new UserAuthenticationInfo();
            userAuthenticationInfo.extraInfo.put(SecurityIntegration.SECURITY_INTEGRATION_KEY, securityIntegration);
            provider.authenticate(user, host, password, randomString, userAuthenticationInfo);
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Login error"));
        }
    }

}
