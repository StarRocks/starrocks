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
import com.starrocks.catalog.system.sys.SysUsers;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.epack.authentication.PasswordExpiredChecker;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.qe.DDLStmtExecutorVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.privilege.AuthPlugin;
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
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TGetUsersRequest;
import com.starrocks.thrift.TGetUsersResponse;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PasswordPolicyTest {

    @BeforeEach
    public void setUpPersistJournal() {
        // Real EditLog on an auto-committing pseudo journal (shields BDB): journal writes complete so the
        // WALApplier.apply() inside logJsonObject() still runs and the DDL takes effect in memory. These
        // tests used to neutralize journaling by faking logEdit(short, Writable) on a null-queue EditLog,
        // which no longer intercepts the WAL-applier write path (it goes straight to the gated write).
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private void mockLeader() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };
    }

    @Test
    public void testCreateUserWithLock() throws Exception {

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
            Assertions.fail();
        } catch (AuthenticationException e) {

        }

        // Can login after alter user unlock
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 unlock", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assertions.assertFalse(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assertions.fail();
        }
    }

    @Test
    public void testLockUser() throws Exception {
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
                Assertions.fail();
            } catch (AuthenticationException e) {

            }
        }
        // Check user has locked after login with error password three times
        Assertions.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        // Can login after alter user unlock
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 unlock", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assertions.assertFalse(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        //Can not login after alter user lock
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 lock", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assertions.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));
        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assertions.fail();
        } catch (AuthenticationException e) {

        }
    }

    @Test
    public void testPasswordExpired() throws Exception {

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
        Assertions.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assertions.assertTrue(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        // Can login after alter user expired = false
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 expire_password = false", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assertions.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assertions.fail();
        }
    }

    @Test
    public void testModifyPassword() throws Exception {
        ConnectContext context = new ConnectContext();


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
        Assertions.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assertions.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        // Can login after alter user expired = false
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 expire_password = true", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assertions.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assertions.assertTrue(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        //password is too simple
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 identified by '123';", context.getSessionVariable().getSqlMode());
        AlterUserStmt finalAlterUserStmt = alterUserStmt;
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(finalAlterUserStmt, context));

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
        Assertions.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        Assertions.assertNotEquals(authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"))
                .getPasswordLastModifiedTimestamp(), lastModifiedTime);

        try {
            byte[] password = MysqlPassword.scramble(seed, "!Ab345678");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assertions.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        //Test set password
        alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 expire_password = true", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);
        Assertions.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "!Ab345678");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assertions.assertTrue(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        lastModifiedTime = userAuthenticationInfo.getPasswordLastModifiedTimestamp();
        //sleep 100ms to avoid password modified timestamp equal
        Thread.sleep(100);

        List<SetListItem> vars = Lists.newArrayList();
        UserAuthOption userAuthOption =
                new UserAuthOption(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name(), "!Ab345678", true, NodePosition.ZERO);
        vars.add(new SetPassVar(new UserRef("u1", "%"), userAuthOption, NodePosition.ZERO));
        SetStmt setStmt = new SetStmt(vars);
        com.starrocks.sql.analyzer.Analyzer.analyze(setStmt, context);
        SetExecutor executor = new SetExecutor(context, setStmt);
        executor.execute();
        Assertions.assertNotEquals(authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"))
                .getPasswordLastModifiedTimestamp(), lastModifiedTime);

        try {
            byte[] password = MysqlPassword.scramble(seed, "!Ab345678");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
            Assertions.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }
    }

    @Test
    public void testPasswordExpiredThread() throws Exception {
        ConnectContext context = new ConnectContext();


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
        Assertions.assertFalse(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assertions.assertFalse(context.isPasswordExpired());
        } catch (AuthenticationException e) {
            Assertions.fail();
        }

        long lastModifiedTimestamp = authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%"))
                .getPasswordLastModifiedTimestamp();

        PasswordExpiredChecker passwordExpiredChecker = new PasswordExpiredChecker();
        //Mock timestamp to 7 days ago
        passwordExpiredChecker.checkPasswordExpiredAndLock(lastModifiedTimestamp + 7 * 24 * 60 * 60 * 1000);

        Assertions.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
    }

    @Test
    public void testAutoUnLock() throws Exception {
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
                Assertions.fail();
            } catch (AuthenticationException e) {

            }
        }
        // Check user has locked after login with error password three times
        Assertions.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        long lockTimestamp =
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%")).getLockTimestamp();

        PasswordExpiredChecker passwordExpiredChecker = new PasswordExpiredChecker();
        //Mock timestamp to 10 min ago
        passwordExpiredChecker.checkPasswordExpiredAndLock(lockTimestamp + 10 * 60 * 1000);

        // Can login after alter user unlock
        Assertions.assertFalse(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));

        try {
            byte[] password = MysqlPassword.scramble(seed, "123456abcD!");
            AuthenticationHandler.authenticate(context, "u1", "%", password);
        } catch (AuthenticationException e) {
            Assertions.fail();
        }
    }

    @Test
    public void testQueryHandler() throws IOException, DdlException {
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
            Assertions.fail();
        }

        // Check user has locked after login with error password three times
        Assertions.assertTrue(authenticationMgr.checkUserPasswordExpired(new UserIdentity("u1", "%")));
        Assertions.assertTrue(context.isPasswordExpired());

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
        Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_PASSWORD_EXPIRED, context.getState().getErrorCode());
        Assertions.assertEquals(QueryState.ErrType.ANALYSIS_ERR, context.getState().getErrType());

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
        Assertions.assertEquals(QueryState.ErrType.ANALYSIS_ERR, context.getState().getErrType());

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
        Assertions.assertFalse(context.getState().isError());
        Assertions.assertFalse(context.isPasswordExpired());
    }

    @Test
    public void testDuplicatePasswordPolicyCreation() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Create first password policy
        CreatePasswordPolicyStmt createPolicyStmt1 = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt1, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt1);

        // Verify policy exists
        Assertions.assertNotNull(securityPolicyMgr.getPasswordPolicy("pp1"));

        // Try to create duplicate password policy - should throw exception
        CreatePasswordPolicyStmt createPolicyStmt2 = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment updated\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        CreatePasswordPolicyStmt finalCreatePolicyStmt2 = createPolicyStmt2;
        Assertions.assertThrows(SemanticException.class, () -> 
                com.starrocks.sql.analyzer.Analyzer.analyze(finalCreatePolicyStmt2, context));
    }

    @Test
    public void testSystemPasswordPolicySetAndUnset() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);

        // Set global password policy
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");
        Assertions.assertEquals("pp1", securityPolicyMgr.getGlobalPasswordPolicy().getPolicyName());

        // Test creating user with password policy enforced
        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123'",
                context.getSessionVariable().getSqlMode());
        
        // Should fail due to password policy during analysis
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, context));

        // Unset global password policy
        securityPolicyMgr.unsetGlobalPasswordPolicy();
        Assertions.assertNull(securityPolicyMgr.getGlobalPasswordPolicy());

        // Now creating user with simple password should succeed
        CreateUserStmt stmt2 = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(stmt2, context);
        authenticationMgr.createUser(stmt2);

        // Verify user was created successfully
        Assertions.assertNotNull(authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%")));
    }

    @Test
    public void testPasswordPolicyValidationLevels() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        // Test different password complexity levels that should fail
        String[] weakPasswords = {
                "123",           // Too short
                "12345678",      // Missing uppercase, lowercase, special chars
                "A2345678",      // Missing lowercase, special chars
                "Ab345678"       // Missing special chars
        };

        for (String password : weakPasswords) {
            CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                    "create user u1 identified by '" + password + "'",
                    context.getSessionVariable().getSqlMode());
            
            // Should fail due to password policy during analysis
            Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, context));
        }

        // Test password that should succeed
        CreateUserStmt validStmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '!Ab345678'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(validStmt, context);
        authenticationMgr.createUser(validStmt);

        // Verify user was created successfully
        Assertions.assertNotNull(authenticationMgr.getUserAuthenticationInfoByUserIdentity(new UserIdentity("u1", "%")));
    }

    @Test
    public void testShowPasswordPolicies() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);

        // Set as global policy
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        // Test show password policies
        com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt showStmt = 
                (com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "show password policies", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(showStmt, context);

        // Execute show statement
        com.starrocks.epack.qe.ShowExecutorVisitorEPack showExecutor = new com.starrocks.epack.qe.ShowExecutorVisitorEPack();
        com.starrocks.qe.ShowResultSet resultSet = showExecutor.visitShowPasswordPolicyStatement(showStmt, context);

        // Verify result contains the policy
        Assertions.assertNotNull(resultSet);
        Assertions.assertFalse(resultSet.getResultRows().isEmpty());
        
        // Check that the policy appears in the results
        boolean foundPolicy = false;
        for (List<String> row : resultSet.getResultRows()) {
            if (row.contains("pp1")) {
                foundPolicy = true;
                break;
            }
        }
        Assertions.assertTrue(foundPolicy, "Password policy pp1 should be found in show results");
    }

    @Test
    public void testShowCreatePasswordPolicy() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);

        // Test show create password policy
        com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt showStmt = 
                (com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "show create password policy pp1", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(showStmt, context);

        // Execute show statement
        com.starrocks.epack.qe.ShowExecutorVisitorEPack showExecutor = new com.starrocks.epack.qe.ShowExecutorVisitorEPack();
        com.starrocks.qe.ShowResultSet resultSet = showExecutor.visitShowCreatePasswordPolicyStatement(showStmt, context);

        // Verify result contains the policy creation statement
        Assertions.assertNotNull(resultSet);
        Assertions.assertFalse(resultSet.getResultRows().isEmpty());
        
        // Check that the create statement appears in the results
        List<String> resultRow = resultSet.getResultRows().get(0);
        Assertions.assertTrue(resultRow.contains("pp1"), "Result should contain policy name");
        Assertions.assertTrue(resultRow.stream().anyMatch(s -> s.contains("CREATE PASSWORD POLICY")), 
                "Result should contain CREATE statement");
    }

    @Test
    public void testShowCreatePasswordPolicyNotFound() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Test show create password policy for non-existent policy
        com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt showStmt = 
                (com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "show create password policy nonexistent", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(showStmt, context);

        // Execute show statement - should throw exception
        com.starrocks.epack.qe.ShowExecutorVisitorEPack showExecutor = new com.starrocks.epack.qe.ShowExecutorVisitorEPack();
        Assertions.assertThrows(SemanticException.class, () -> 
                showExecutor.visitShowCreatePasswordPolicyStatement(showStmt, context));
    }

    @Test
    public void testDropPasswordPolicy() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);

        // Verify policy exists
        Assertions.assertNotNull(securityPolicyMgr.getPasswordPolicy("pp1"));

        // Drop password policy
        com.starrocks.epack.sql.ast.DropPasswordPolicyStmt dropStmt = 
                (com.starrocks.epack.sql.ast.DropPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "drop password policy pp1", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(dropStmt, context);
        
        // Execute drop statement
        DDLStmtExecutor ddlStmtExecutor = new DDLStmtExecutor(DDLStmtExecutorVisitorEPack.getInstance());
        ddlStmtExecutor.execute(dropStmt, context);

        // Verify policy no longer exists
        Assertions.assertNull(securityPolicyMgr.getPasswordPolicy("pp1"));
    }

    @Test
    public void testDropNonExistentPasswordPolicy() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Try to drop non-existent password policy
        com.starrocks.epack.sql.ast.DropPasswordPolicyStmt dropStmt = 
                (com.starrocks.epack.sql.ast.DropPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "drop password policy nonexistent", context.getSessionVariable().getSqlMode());
        
        // Should throw exception during analysis
        Assertions.assertThrows(SemanticException.class, () -> 
                com.starrocks.sql.analyzer.Analyzer.analyze(dropStmt, context));
    }

    @Test
    public void testSystemSetPasswordPolicy() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);

        // Set system password policy
        com.starrocks.epack.sql.ast.SetPasswordPolicyStmt setStmt = 
                (com.starrocks.epack.sql.ast.SetPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "alter system set password policy pp1", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(setStmt, context);
        
        // Execute set statement
        DDLStmtExecutor ddlStmtExecutor = new DDLStmtExecutor(DDLStmtExecutorVisitorEPack.getInstance());
        ddlStmtExecutor.execute(setStmt, context);

        // Verify global password policy is set
        Assertions.assertEquals("pp1", securityPolicyMgr.getGlobalPasswordPolicy().getPolicyName());
    }

    @Test
    public void testSystemUnsetPasswordPolicy() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Create password policy and set it as global
        CreatePasswordPolicyStmt createPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"pp1 comment\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MAX_AGE_DAYS\" = \"7\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(createPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(createPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("pp1");

        // Verify global password policy is set
        Assertions.assertEquals("pp1", securityPolicyMgr.getGlobalPasswordPolicy().getPolicyName());

        // Unset system password policy
        com.starrocks.epack.sql.ast.UnsetPasswordPolicyStmt unsetStmt = 
                (com.starrocks.epack.sql.ast.UnsetPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "alter system unset password policy", context.getSessionVariable().getSqlMode());
        com.starrocks.sql.analyzer.Analyzer.analyze(unsetStmt, context);
        
        // Execute unset statement
        DDLStmtExecutor ddlStmtExecutor = new DDLStmtExecutor(DDLStmtExecutorVisitorEPack.getInstance());
        ddlStmtExecutor.execute(unsetStmt, context);

        // Verify global password policy is unset
        Assertions.assertNull(securityPolicyMgr.getGlobalPasswordPolicy());
    }

    @Test
    public void testSystemSetNonExistentPasswordPolicy() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            boolean isLeader() {
                return true;
            }
        };

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        // Try to set non-existent password policy
        com.starrocks.epack.sql.ast.SetPasswordPolicyStmt setStmt = 
                (com.starrocks.epack.sql.ast.SetPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "alter system set password policy nonexistent", context.getSessionVariable().getSqlMode());
        
        // Should throw exception during analysis
        Assertions.assertThrows(SemanticException.class, () -> 
                com.starrocks.sql.analyzer.Analyzer.analyze(setStmt, context));
    }

    @Test
    public void testCreateUserSupportsPasswordPolicyProperty() throws Exception {
        mockLeader();

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt globalPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY global_pp comment \"global\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\"\n" +
                        ")",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(globalPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(globalPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("global_pp");

        CreatePasswordPolicyStmt userPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY pp1 comment \"user\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"3\"\n" +
                        ")",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(userPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(userPolicyStmt);

        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123' properties('PASSWORD_POLICY' = 'pp1')",
                context.getSessionVariable().getSqlMode());

        Analyzer.analyze(stmt, context);
        authenticationMgr.createUser(stmt);

        Assertions.assertEquals("pp1", authenticationMgr.getPasswordPolicyByUserName("u1"));
    }

    @Test
    public void testCreateUserWithNonexistentPasswordPolicyProperty() {
        mockLeader();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(new SecurityPolicyMgr());

        ConnectContext context = new ConnectContext();
        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456Ab!' properties('PASSWORD_POLICY' = 'missing_pp')",
                context.getSessionVariable().getSqlMode());

        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(stmt, context));
    }

    @Test
    public void testUserPasswordPolicyOverridesGlobalPolicy() throws Exception {
        mockLeader();

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt globalPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY global_pp comment \"global\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MIN_UPPER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_LOWER_CASE_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_NUMERIC_CHARS\" = \"1\",\n" +
                        "    \"PASSWORD_MIN_SPECIAL_CHARS\" = \"1\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(globalPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(globalPolicyStmt);

        CreatePasswordPolicyStmt userPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY user_pp comment \"user\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(userPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(userPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("global_pp");

        CreateUserStmt createUserStmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123456Ab!'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createUserStmt, context);
        authenticationMgr.createUser(createUserStmt);

        StatementBase bindStmt = SqlParser.parseSingleStatement(
                "ALTER USER 'u1' SET PROPERTIES ('PASSWORD_POLICY' = 'user_pp')",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(bindStmt, context);
        DDLStmtExecutor.execute(bindStmt, context);
        Assertions.assertEquals("user_pp", authenticationMgr.getPasswordPolicyByUserName("u1"));

        AlterUserStmt alterUserStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 identified by '123'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(alterUserStmt, context);
        DDLStmtExecutor.execute(alterUserStmt, context);

        StatementBase unbindStmt = SqlParser.parseSingleStatement(
                "ALTER USER 'u1' SET PROPERTIES ('PASSWORD_POLICY' = '')",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(unbindStmt, context);
        DDLStmtExecutor.execute(unbindStmt, context);
        Assertions.assertEquals("", authenticationMgr.getPasswordPolicyByUserName("u1"));

        AlterUserStmt strictAlterStmt = (AlterUserStmt) SqlParser.parseSingleStatement(
                "alter user u1 identified by '123'",
                context.getSessionVariable().getSqlMode());
        Assertions.assertThrows(SemanticException.class, () -> Analyzer.analyze(strictAlterStmt, context));
    }

    @Test
    public void testUserPasswordPolicyOverridesRetryPolicy() throws Exception {
        mockLeader();

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt globalPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY global_retry comment \"global\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(globalPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(globalPolicyStmt);

        CreatePasswordPolicyStmt userPolicyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY user_retry comment \"user\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"8\",\n" +
                        "    \"PASSWORD_MAX_RETRIES\" = \"1\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(userPolicyStmt, context);
        securityPolicyMgr.createPasswordPolicy(userPolicyStmt);
        securityPolicyMgr.setGlobalPasswordPolicy("global_retry");

        CreateUserStmt createUserStmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '12345678'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createUserStmt, context);
        authenticationMgr.createUser(createUserStmt);

        StatementBase bindStmt = SqlParser.parseSingleStatement(
                "ALTER USER 'u1' SET PROPERTIES ('PASSWORD_POLICY' = 'user_retry')",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(bindStmt, context);
        DDLStmtExecutor.execute(bindStmt, context);

        byte[] seed = "data_salt".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "wrong_password");
        context.setAuthDataSalt(seed);
        try {
            AuthenticationHandler.authenticate(context, "u1", "%", scramble);
            Assertions.fail();
        } catch (AuthenticationException e) {
            // expected
        }

        Assertions.assertTrue(authenticationMgr.checkUserLocked(new UserIdentity("u1", "%")));
    }

    @Test
    public void testUserPasswordPolicyShownAndDropBlocked() throws Exception {
        mockLeader();

        AuthenticationMgrEPack authenticationMgr = new AuthenticationMgrEPack();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        SecurityPolicyMgr securityPolicyMgr = new SecurityPolicyMgr();
        GlobalStateMgr.getCurrentState().setSecurityPolicyManager(securityPolicyMgr);

        ConnectContext context = new ConnectContext();

        CreatePasswordPolicyStmt policyStmt = (CreatePasswordPolicyStmt) SqlParser.parseSingleStatement(
                "CREATE PASSWORD POLICY user_pp comment \"user\"\n" +
                        "properties (\n" +
                        "    \"PASSWORD_MIN_LENGTH\" = \"3\"\n" +
                        ")", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(policyStmt, context);
        securityPolicyMgr.createPasswordPolicy(policyStmt);

        CreateUserStmt createUserStmt = (CreateUserStmt) SqlParser.parseSingleStatement(
                "create user u1 identified by '123'",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(createUserStmt, context);
        authenticationMgr.createUser(createUserStmt);

        StatementBase bindStmt = SqlParser.parseSingleStatement(
                "ALTER USER 'u1' SET PROPERTIES ('PASSWORD_POLICY' = 'user_pp')",
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(bindStmt, context);
        DDLStmtExecutor.execute(bindStmt, context);

        ShowUserPropertyStmt showStmt = new ShowUserPropertyStmt("u1", null);
        List<List<String>> rows = showStmt.getRows(context);
        Assertions.assertTrue(rows.stream().anyMatch(row -> row.get(0).equals(UserProperty.PROP_PASSWORD_POLICY)
                && row.get(1).equals("user_pp")));

        TGetUsersResponse response = SysUsers.getUsers(new TGetUsersRequest());
        Assertions.assertTrue(response.getUsers().stream().anyMatch(
                item -> item.getUser().equals("u1") && item.getPassword_policy().equals("user_pp")));

        com.starrocks.epack.sql.ast.DropPasswordPolicyStmt dropStmt =
                (com.starrocks.epack.sql.ast.DropPasswordPolicyStmt) SqlParser.parseSingleStatement(
                        "drop password policy user_pp", context.getSessionVariable().getSqlMode());
        Analyzer.analyze(dropStmt, context);
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> securityPolicyMgr.dropPasswordPolicy(dropStmt));
        Assertions.assertTrue(exception.getMessage().contains("associated with users"));
    }
}
