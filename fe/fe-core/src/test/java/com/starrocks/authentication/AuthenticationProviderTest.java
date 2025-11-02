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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.mysql.MysqlAuthPacket;
import com.starrocks.mysql.MysqlAuthPacketTest;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.NegotiateState;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.UserAuthOptionAnalyzer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class AuthenticationProviderTest {
    private ConnectContext ctx = new ConnectContext();

    @Test
    public void testAuthentication() throws Exception {
        UserRef testUser = new UserRef("test", "%");
        UserIdentity testUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");

        String[] passwords = {"asdf123", "starrocks", "testtest"};
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        ctx.setAuthDataSalt(seed);

        for (String password : passwords) {
            UserAuthOption userAuthOption = new UserAuthOption(null, password, true, NodePosition.ZERO);
            UserAuthOptionAnalyzer.analyzeAuthOption(testUser, userAuthOption);
            UserAuthenticationInfo info = new UserAuthenticationInfo(testUser, userAuthOption);
            PlainPasswordAuthenticationProvider provider = (PlainPasswordAuthenticationProvider) AuthenticationProviderFactory
                    .create(info.getAuthPlugin(), new String(info.getPassword()));

            byte[] scramble = MysqlPassword.scramble(seed, password);
            provider.authenticate(ctx.getAccessControlContext(), testUserIdentity, scramble);
        }

        // no password
        PlainPasswordAuthenticationProvider provider = new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD);
        UserAuthOptionAnalyzer.analyzeAuthOption(testUser, null);
        UserAuthenticationInfo info = new UserAuthenticationInfo(testUser, null);
        ctx.setAuthDataSalt(new byte[0]);
        provider.authenticate(ctx.getAccessControlContext(), testUserIdentity, new byte[0]);
        try {
            ctx.setAuthDataSalt("x".getBytes(StandardCharsets.UTF_8));
            provider.authenticate(
                    ctx.getAccessControlContext(),
                    testUserIdentity,
                    "xx".getBytes(StandardCharsets.UTF_8));
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, e.getErrorCode());
        }

        byte[] p = MysqlPassword.makeScrambledPassword("bb");

        UserAuthOption userAuthOption = new UserAuthOption(null, new String(p, StandardCharsets.UTF_8), false, NodePosition.ZERO);
        UserAuthOptionAnalyzer.analyzeAuthOption(testUser, userAuthOption);
        info = new UserAuthenticationInfo(testUser, userAuthOption);

        provider = new PlainPasswordAuthenticationProvider(info.getPassword());
        try {
            ctx.setAuthDataSalt(seed);
            provider.authenticate(
                    ctx.getAccessControlContext(),
                    testUserIdentity,
                    MysqlPassword.scramble(seed, "xx"));
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, e.getErrorCode());
        }

        try {
            ctx.setAuthDataSalt(seed);
            provider.authenticate(
                    ctx.getAccessControlContext(),
                    testUserIdentity,
                    MysqlPassword.scramble(seed, "bb"));

        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        try {
            byte[] remotePassword = "bb".getBytes(StandardCharsets.UTF_8);
            ctx.setAuthDataSalt(null);
            provider.authenticate(
                    ctx.getAccessControlContext(),
                    testUserIdentity,
                    remotePassword);

        } catch (AuthenticationException e) {
            Assertions.fail();
        }
    }

    @Test
    public void testClearPassword() throws IOException, DdlException {
        new MockUp<MysqlChannel>() {
            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
                return;
            }

            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return ByteBuffer.wrap(new byte[23]);
            }
        };

        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        CreateUserStmt createUserStmt = (CreateUserStmt) SqlParser
                .parse("create user test identified with mysql_native_password by '12345'", 32).get(0);
        Analyzer.analyze(createUserStmt, ctx);
        authenticationMgr.createUser(createUserStmt);

        MysqlAuthPacket mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test",
                "12345".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);

        ConnectContext ctx = new ConnectContext();
        MysqlProto.NegotiateResult result = MysqlProto.authenticate(ctx, mysqlAuthPacket);
        Assertions.assertEquals(NegotiateState.OK, result.state());

        mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test",
                "1234".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);
        result = MysqlProto.authenticate(ctx, mysqlAuthPacket);
        Assertions.assertEquals(NegotiateState.AUTHENTICATION_FAILED, result.state());
        Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, ctx.getState().getErrorCode());
        Assertions.assertTrue(ctx.getState().getErrorMessage().contains("Access denied for user 'test'"));

        mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test",
                "".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);
        result = MysqlProto.authenticate(ctx, mysqlAuthPacket);
        Assertions.assertEquals(NegotiateState.AUTHENTICATION_FAILED, result.state());
        Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, ctx.getState().getErrorCode());
        Assertions.assertTrue(ctx.getState().getErrorMessage().contains("Access denied for user 'test'"));

        mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test",
                MysqlPassword.EMPTY_PASSWORD,
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);
        result = MysqlProto.authenticate(ctx, mysqlAuthPacket);
        Assertions.assertEquals(NegotiateState.AUTHENTICATION_FAILED, result.state());
        Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, ctx.getState().getErrorCode());
        Assertions.assertTrue(ctx.getState().getErrorMessage().contains("Access denied for user 'test'"));
    }

    @Test
    public void testLdapAuthentication() throws IOException, DdlException {
        new MockUp<MysqlChannel>() {
            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
                return;
            }

            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return ByteBuffer.wrap(new byte[23]);
            }
        };

        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        CreateUserStmt createUserStmt = (CreateUserStmt) SqlParser
                .parse("create user test identified with authentication_ldap_simple", 32).get(0);
        Analyzer.analyze(createUserStmt, ctx);
        authenticationMgr.createUser(createUserStmt);

        createUserStmt = (CreateUserStmt) SqlParser.parse(
                "create user test2 identified with authentication_ldap_simple by 'ou=People,dc=starrocks,dc=com'", 32).get(0);
        Analyzer.analyze(createUserStmt, ctx);
        authenticationMgr.createUser(createUserStmt);

        MysqlAuthPacket mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test",
                new byte[1],
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);
        MysqlProto.NegotiateResult result = MysqlProto.authenticate(ctx, mysqlAuthPacket);
        Assertions.assertEquals(NegotiateState.AUTHENTICATION_FAILED, result.state());
        Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, ctx.getState().getErrorCode());
        Assertions.assertTrue(
                ctx.getState().getErrorMessage().contains("empty password is not allowed for simple authentication"));

        mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test",
                "12345".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);
        result = MysqlProto.authenticate(ctx, mysqlAuthPacket);

        mysqlAuthPacket = MysqlAuthPacketTest.buildPacket(
                "test2",
                "12345".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD);
        result = MysqlProto.authenticate(ctx, mysqlAuthPacket);
    }
}
