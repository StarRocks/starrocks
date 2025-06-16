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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/SetPassVarTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SetExecutor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class SetPassVarTest {

    private ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUser2;
    private static AuthorizationMgr authorizationManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();
        ctxToRoot();
        testUser = createUser("CREATE USER 'test' IDENTIFIED BY ''");
        testUser2 = createUser("CREATE USER 'test2' IDENTIFIED BY ''");
    }

    @Before
    public void setUp() {
        ctx = new ConnectContext();
        UserIdentity currentUser = new UserIdentity("root", "%");
        ctx.setCurrentUserIdentity(currentUser);
    }

    @Test
    public void testNormal() throws StarRocksException {
        SetPassVar stmt;

        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        UserAuthOption userAuthOption =
                new UserAuthOption(null, "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", false, NodePosition.ZERO);
        stmt = new SetPassVar(new UserIdentity("test", "%"), userAuthOption, NodePosition.ZERO);
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), null);
        Assert.assertEquals("test", stmt.getUserIdent().getUser());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", stmt.getAuthOption().getAuthString());
        Assert.assertEquals("'test'@'%'", stmt.getUserIdent().toString());

        // empty user
        ctxToRoot();
        stmt = new SetPassVar(null, userAuthOption, NodePosition.ZERO);
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), ctx);
        Assert.assertEquals("'root'@'%'", stmt.getUserIdent().toString());
    }

    @Test
    public void testSetPassword() {
        String sql = "SET PASSWORD FOR 'test' = PASSWORD('testPass')";
        SetStmt setStmt = (SetStmt) SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(setStmt, ctx);
        SetPassVar setPassVar = (SetPassVar) setStmt.getSetListItems().get(0);
        Assert.assertEquals("test", setPassVar.getUserIdent().getUser());

        sql = "SET PASSWORD = PASSWORD('testPass')";
        setStmt = (SetStmt) SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(setStmt, ctx);
        setPassVar = (SetPassVar) setStmt.getSetListItems().get(0);
        String password = new String(setPassVar.getAuthOption().getAuthString());
        Assert.assertEquals("testPass", password);
        Assert.assertTrue(setPassVar.getAuthOption().isPasswordPlain());

        sql = "SET PASSWORD = '*88EEBA7D913688E7278E2AD071FDB5E76D76D34B'";
        setStmt = (SetStmt) SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(setStmt, ctx);
        setPassVar = (SetPassVar) setStmt.getSetListItems().get(0);
        password = new String(setPassVar.getAuthOption().getAuthString());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);
        Assert.assertFalse(setPassVar.getAuthOption().isPasswordPlain());
    }

    @Test
    public void testSetStmt() throws Exception {
        String sql = "SET PASSWORD FOR 'test2'@'%' = PASSWORD('123456');";
        String expectError =
                "Access denied; you need (at least one of) the GRANT privilege(s) on SYSTEM for this operation";
        verifyNODEAndGRANT(sql, expectError);

        ctxToTestUser();
        // user 'test' not has GRANT/NODE privilege
        sql = "set password = PASSWORD('123456')";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Authorizer.check(statement, starRocksAssert.getCtx());

        sql = "set password for test = PASSWORD('123456')";
        statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Authorizer.check(statement, starRocksAssert.getCtx());
    }

    private static void verifyNODEAndGRANT(String sql, String expectError) throws Exception {
        ctxToRoot();
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        // user 'root' has GRANT/NODE privilege
        Authorizer.check(statement, starRocksAssert.getCtx());

        try {
            ctxToTestUser();
            // user 'test' not has GRANT/NODE privilege
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    @Test
    public void testCreateTablePartitionNormal() throws Exception {
        String setSql = "set sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES');";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(setSql, ctx);
        ctx.getSessionVariable().setSqlMode(SqlModeHelper.MODE_STRICT_TRANS_TABLES);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        SystemVariable setVars = (SystemVariable) stmt.getSetListItems().get(0);

        Assert.assertTrue(setVars.getResolvedExpression().getStringValue().contains("STRICT_TRANS_TABLES"));
    }

    @Test(expected = ErrorReportException.class)
    public void testBadPassword() {
        SetPassVar stmt;
        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        UserAuthOption userAuthOption =
                new UserAuthOption(null, "*88EEBAHD913688E7278E2AD071FDB5E76D76D34B", false, NodePosition.ZERO);
        stmt = new SetPassVar(new UserIdentity("test", "%"), userAuthOption, NodePosition.ZERO);
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), null);
        Assert.fail("No exception throws.");
    }



    private static void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getUser());
    }

    private static void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getUser());
    }

    private static UserIdentity createUser(String createUserSql) throws Exception {
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
        return createUserStmt.getUserIdentity();
    }

    @Test
    public void testSetPasswordInNewPrivilege() throws Exception {

        ctxToRoot();
        UserAuthenticationInfo userAuthenticationInfo = GlobalStateMgr.getCurrentState().getAuthenticationMgr().
                getUserAuthenticationInfoByUserIdentity(testUser);
        Assert.assertEquals(0, userAuthenticationInfo.getPassword().length);
        String setSql = "SET PASSWORD FOR 'test'@'%' = PASSWORD('123456');";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(setSql, starRocksAssert.getCtx());
        SetExecutor executor = new SetExecutor(starRocksAssert.getCtx(), (SetStmt) statementBase);
        executor.execute();
        userAuthenticationInfo = GlobalStateMgr.getCurrentState().getAuthenticationMgr().
                getUserAuthenticationInfoByUserIdentity(testUser);
        Assert.assertTrue(userAuthenticationInfo.getPassword().length > 0);

    }
}