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
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class PlainPasswordAuthenticationProviderTest {
    protected PlainPasswordAuthenticationProvider provider = new PlainPasswordAuthenticationProvider();
    private ConnectContext ctx = new ConnectContext();

    @Test
    public void testValidPassword() throws Exception {
        Config.enable_validate_password = true;

        // too short
        try {
            provider.validatePassword(new UserIdentity("u", "%"), "aaa");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password is too short"));
        }

        // only number
        String[] badPasswords = {"starrocks", "STARROCKS", "123456789", "STARROCKS123", "starrocks123", "STARROCKSstar"};
        for (String badPassword : badPasswords) {
            try {
                provider.validatePassword(new UserIdentity("u", "%"), badPassword);
                Assert.fail();
            } catch (AuthenticationException e) {
                Assert.assertTrue(e.getMessage().contains(
                        "password should contains at least one digit, one lowercase letter and one uppercase letter!"));
            }
        }

        provider.validatePassword(new UserIdentity("u", "%"), "aaaAAA123");
        Config.enable_validate_password = false;
        provider.validatePassword(new UserIdentity("u", "%"), "aaa");
    }

    @Test
    public void testAuthentication() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        String[] passwords = {"asdf123", "starrocks", "testtest"};
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        for (String password : passwords) {
            UserAuthOption userAuthOption = new UserAuthOption(password, null, null, true, NodePosition.ZERO);
            UserAuthenticationInfo info = provider.analyzeAuthOption(testUser, userAuthOption);
            byte[] scramble = MysqlPassword.scramble(seed, password);
            provider.authenticate(ctx, testUser.getUser(), "10.1.1.1", scramble, seed, info);
        }

        // no password
        UserAuthenticationInfo info = provider.analyzeAuthOption(testUser, null);
        provider.authenticate(ctx, testUser.getUser(), "10.1.1.1", new byte[0], new byte[0], info);
        try {
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    "xx".getBytes(StandardCharsets.UTF_8),
                    "x".getBytes(StandardCharsets.UTF_8),
                    info);
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password length mismatch!"));
        }

        byte[] p = MysqlPassword.makeScrambledPassword("bb");

        UserAuthOption userAuthOption = new UserAuthOption(
                new String(p, StandardCharsets.UTF_8), null, null, false, NodePosition.ZERO);

        info = provider.analyzeAuthOption(testUser, userAuthOption);
        try {
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    MysqlPassword.scramble(seed, "xx"),
                    seed,
                    info);
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("password mismatch!"));
        }

        try {
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    MysqlPassword.scramble(seed, "bb"),
                    seed,
                    info);

        } catch (AuthenticationException e) {
            Assert.fail();
        }

        try {
            byte[] remotePassword = "bb".getBytes(StandardCharsets.UTF_8);
            provider.authenticate(
                    ctx,
                    testUser.getUser(),
                    "10.1.1.1",
                    remotePassword,
                    null,
                    info);

        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testValidatePassword() throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(null));
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Config.enable_validate_password = true;
        Config.enable_password_reuse = false;
        ConnectContext context = new ConnectContext();

        String sql = "create user u1 identified by '123456abcD!'";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, context);
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        sql = "alter user u1 identified by '123456abcD!'";
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parse(sql, context.getSessionVariable().getSqlMode()).get(0);
        Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(alterUserStmt, context));

        Config.enable_validate_password = false;
        Config.enable_password_reuse = true;
    }
}
