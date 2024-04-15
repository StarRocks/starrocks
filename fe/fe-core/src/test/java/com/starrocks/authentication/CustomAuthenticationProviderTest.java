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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CustomAuthenticationProviderTest {

    byte[] password = "".getBytes();
    byte[] randomString = "".getBytes();

    String user = "test";

    String host = "%";

    static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        Config.authorization_custom_class = "com.starrocks.authentication.FakeCustomAuthenticationProviderTest";
        Config.authentication_chain = new String[] {"custom"};
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @Test
    public void testCustomAuthentication() throws Exception {
        AuthenticationMgr mgr = ctx.getGlobalStateMgr().getAuthenticationMgr();
        UserIdentity res = mgr.checkPassword(user, host, password, randomString);
        Assert.assertNull(res);

        AuthenticationProvider provider = AuthenticationProviderFactory.create(CustomAuthenticationProviderFactory.PLUGIN_NAME);
        try {
            provider.authenticate(user, host, password, randomString, new UserAuthenticationInfo());
            Assert.fail();
        } catch (AuthenticationException ex) {
            Assert.assertTrue(ex.getMessage().contains("Login error"));
        }

    }

}
