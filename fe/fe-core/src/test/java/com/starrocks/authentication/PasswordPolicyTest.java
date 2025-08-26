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

import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.epack.authentication.PasswordExpiredChecker;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SetExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PasswordPolicyTest {
    @Test
    public void testCreateUserWithLock() throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        ConnectContext context = new ConnectContext();
        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!' lock",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "123456abcD!");
        context.setAuthDataSalt(seed);

        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assert.fail();
        } catch (AuthenticationException e) {

        }

        // Can login after alter user unlock
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 unlock", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertFalse(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testLockUser() throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLogEPack(null));
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "password");
        context.setAuthDataSalt(seed);
        for (int i = 0; i < 3; ++i) {
            try {
                AuthenticationHandler.authenticate(context, "u1", "%", scramble);
                Assert.fail();
            } catch (AuthenticationException e) {

            }
        }
        // Check user has locked after login with error password three times
        Assert.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        // Can login after alter user unlock
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 unlock", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertFalse(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        //Can not login after alter user lock
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 lock", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));
        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assert.fail();
        } catch (AuthenticationException e) {

        }
    }

    @Test
    public void testPasswordExpired() throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        ConnectContext context = new ConnectContext();
        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!' expire_password = true",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "123456abcD!");
        context.setAuthDataSalt(seed);
        Assert.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assert.assertTrue(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        // Can login after alter user expired = false
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 expire_password = false", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testModifyPassword() throws Exception {
        ConnectContext context = new ConnectContext();

        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLogEPack(null));

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "123456abcD!");
        context.setAuthDataSalt(seed);
        Assert.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assert.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        // Can login after alter user expired = false
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 expire_password = true", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assert.assertTrue(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        //password is too simple
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 identified by '123';", context.getSessionVariable().getSqlMode());
        AlterUserStmt finalAlterUserStmt = alterUserStmt;
        Assert.assertThrows(SemanticException.class, () -> Analyzer.analyze(finalAlterUserStmt, context));

        // Modify Password
        UserAuthenticationInfo userAuthenticationInfo =
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"));
        long lastModifiedTime = userAuthenticationInfo.getPasswordLastModifiedTimestamp();

        //sleep 100ms to avoid password modified timestamp equal
        Thread.sleep(100);
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 identified by '!Ab345678';", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        Assert.assertNotEquals(authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"))
                .getPasswordLastModifiedTimestamp(), lastModifiedTime);

        try {
            byte[] password = MysqlPassword.scramble(seed, "!Ab345678");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assert.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        //Test set password
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 expire_password = true", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assert.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "!Ab345678");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assert.assertTrue(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        lastModifiedTime = userAuthenticationInfo.getPasswordLastModifiedTimestamp();
        //sleep 100ms to avoid password modified timestamp equal
        Thread.sleep(100);

        List<SetListItem> vars = Lists.newArrayList();
        UserAuthOption userAuthOption =
                new UserAuthOption(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), "!Ab345678", true, NodePosition.ZERO);
        vars.add(new SetPassVar(new UserIdentity("u1", "%"), userAuthOption, NodePosition.ZERO));
        SetStmt setStmt = new SetStmt(vars);
        com.starrocks.sql.analyzer.Analyzer.analyze(setStmt, context);
        SetExecutor executor = new SetExecutor(context, setStmt);
        executor.execute();
        Assert.assertNotEquals(authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"))
                .getPasswordLastModifiedTimestamp(), lastModifiedTime);

        try {
            byte[] password = MysqlPassword.scramble(seed, "!Ab345678");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assert.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testPasswordExpiredThread() throws Exception {
        ConnectContext context = new ConnectContext();

        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLogEPack(null));

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "123456abcD!");
        context.setAuthDataSalt(seed);
        Assert.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assert.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        long lastModifiedTimestamp = authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"))
                .getPasswordLastModifiedTimestamp();

        PasswordExpiredChecker passwordExpiredChecker = new PasswordExpiredChecker();
        //Mock timestamp to 7 days ago
        passwordExpiredChecker.checkPasswordExpiredAndLock(lastModifiedTimestamp + 7 * 24 * 60 * 60 * 1000);

        Assert.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
    }

    @Test
    public void testAutoUnLock() throws Exception {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLogEPack(null));
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\",\n" +
                        "    \"PASSWORD_LOCKOUT_TIME_MINS\" = \"10\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "password");
        context.setAuthDataSalt(seed);
        for (int i = 0; i < 3; ++i) {
            try {
                AuthenticationHandler.authenticate(context, "u1", "%", scramble);
                Assert.fail();
            } catch (AuthenticationException e) {

            }
        }
        // Check user has locked after login with error password three times
        Assert.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        long lockTimestamp =
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%")).getLockTimestamp();

        PasswordExpiredChecker passwordExpiredChecker = new PasswordExpiredChecker();
        //Mock timestamp to 10 min ago
        passwordExpiredChecker.checkPasswordExpiredAndLock(lockTimestamp + 10 * 60 * 1000);

        // Can login after alter user unlock
        Assert.assertFalse(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assert.fail();
        }
    }

    @Test
    public void testQueryHandler() throws IOException, DdlException {
        new MockUp<EditLog>() {
            @Mock
            public void logEdit(short op, Writable writable) {
                return;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(new EditLogEPack(null));
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();

        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\",\n" +
                        "    \"PASSWORD_LOCKOUT_TIME_MINS\" = \"10\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456abcD!' expire_password = true",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "123456abcD!");
        context.setAuthDataSalt(seed);
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
        } catch (AuthenticationException e) {
            Assert.fail();
        }

        // Check user has locked after login with error password three times
        Assert.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        Assert.assertTrue(context.isPasswordExpired());

        //Init ConnectProcessor
        MetricRepo.init();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(3);
        serializer.writeEofString("select * from a");
        ByteBuffer queryPacket = serializer.toByteBuffer();

        new MockUp<MysqlChannel>() {
            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return queryPacket;
            }

            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
            }
        };

        ConnectProcessor processor = new ConnectProcessor(context);
        processor.processOnce();
        Assert.assertEquals(ErrorCode.ERR_AUTHENTICATION_PASSWORD_EXPIRED, context.getState().getErrorCode());
        Assert.assertEquals(QueryState.ErrType.ANALYSIS_ERR, context.getState().getErrType());

        serializer.reset();
        serializer.writeInt1(3);
        serializer.writeEofString("alter user u1 identified by '123';");
        ByteBuffer queryPacket2 = serializer.toByteBuffer();
        new MockUp<MysqlChannel>() {
            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return queryPacket2;
            }

            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
            }
        };
        processor.processOnce();
        Assert.assertEquals(QueryState.ErrType.ANALYSIS_ERR, context.getState().getErrType());

        serializer.reset();
        serializer.writeInt1(3);
        serializer.writeEofString("alter user u1 identified by '223456abcD!';");
        ByteBuffer queryPacket3 = serializer.toByteBuffer();
        new MockUp<MysqlChannel>() {
            @Mock
            public ByteBuffer fetchOnePacket() throws IOException {
                return queryPacket3;
            }

            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
            }
        };
        processor.processOnce();
        Assert.assertFalse(context.getState().isError());
        Assert.assertFalse(context.isPasswordExpired());
    }
}