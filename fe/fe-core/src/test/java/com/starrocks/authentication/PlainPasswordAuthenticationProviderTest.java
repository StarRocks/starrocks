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

import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.UserAuthOptionAnalyzer;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PlainPasswordAuthenticationProviderTest {
    private ConnectContext ctx = new ConnectContext();

    @Test
    public void testAuthentication() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        String[] passwords = {"asdf123", "starrocks", "testtest"};
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        ctx.setAuthDataSalt(seed);

        for (String password : passwords) {
            UserAuthOption userAuthOption = new UserAuthOption(null, password, true, NodePosition.ZERO);
            UserAuthenticationInfo info = UserAuthOptionAnalyzer.analyzeAuthOption(testUser, userAuthOption);
            PlainPasswordAuthenticationProvider provider = (PlainPasswordAuthenticationProvider) AuthenticationProviderFactory
                    .create(info.getAuthPlugin(), new String(info.getPassword()));

            byte[] scramble = MysqlPassword.scramble(seed, password);
            provider.authenticate(ctx, testUser.getUser(), "10.1.1.1", scramble);
        }

        // no password
        PlainPasswordAuthenticationProvider provider = new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD);
        UserAuthenticationInfo info = UserAuthOptionAnalyzer.analyzeAuthOption(testUser, null);
        ctx.setAuthDataSalt(new byte[0]);
        provider.authenticate(ctx, testUser.getUser(), "10.1.1.1", new byte[0]);
        try {
            ctx.setAuthDataSalt("x".getBytes(StandardCharsets.UTF_8));
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    "xx".getBytes(StandardCharsets.UTF_8));
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password length mismatch!"));
        }

        byte[] p = MysqlPassword.makeScrambledPassword("bb");

        UserAuthOption userAuthOption = new UserAuthOption(null, new String(p, StandardCharsets.UTF_8), false, NodePosition.ZERO);
        info = UserAuthOptionAnalyzer.analyzeAuthOption(testUser, userAuthOption);

        provider = new PlainPasswordAuthenticationProvider(info.getPassword());
        try {
            ctx.setAuthDataSalt(seed);
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    MysqlPassword.scramble(seed, "xx"));
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password mismatch!"));
        }

        try {
            ctx.setAuthDataSalt(seed);
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    MysqlPassword.scramble(seed, "bb"));

        } catch (AuthenticationException e) {
            Assert.fail();
        }

        try {
            byte[] remotePassword = "bb".getBytes(StandardCharsets.UTF_8);
            ctx.setAuthDataSalt(null);
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    remotePassword);

        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }
}
