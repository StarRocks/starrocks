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
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SetExecutor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SetPassVarTest {

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        UserIdentity currentUser = new UserIdentity("root", "192.168.1.1");
        ctx.setCurrentUserIdentity(currentUser);
    }

    @Test
    public void testNormal() throws UserException {
        SetPassVar stmt;

        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B");
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), null);
        Assert.assertEquals("testUser", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", new String(stmt.getPassword()));
        Assert.assertEquals("'testUser'@'%'", stmt.getUserIdent().toString());

        // empty password
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), null);
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), null);
        Assert.assertEquals("'testUser'@'%'", stmt.getUserIdent().toString());

        // empty user
        // empty password
        stmt = new SetPassVar(null, null);
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), ctx);
        Assert.assertEquals("'root'@'192.168.1.1'", stmt.getUserIdent().toString());
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

    @Test(expected = SemanticException.class)
    public void testBadPassword() {
        SetPassVar stmt;
        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBAHD913688E7278E2AD071FDB5E76D76D34B");
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(stmt)), null);
        Assert.fail("No exception throws.");
    }

    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static PrivilegeManager privilegeManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        privilegeManager = starRocksAssert.getCtx().getGlobalStateMgr().getPrivilegeManager();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        privilegeManager.initBuiltinRolesAndUsers();
        ctxToRoot();
        createUsers();
    }

    private static void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getQualifiedUser());
    }

    private static void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
    }

    private static void createUsers() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        AuthenticationManager authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationManager();
        authenticationManager.createUser(createUserStmt);
        testUser = createUserStmt.getUserIdentity();
    }
    @Test
    public void testSetPasswordInNewPrivilege() throws Exception {

        ctxToRoot();
        UserAuthenticationInfo userAuthenticationInfo = GlobalStateMgr.getCurrentState().getAuthenticationManager().
                getUserAuthenticationInfoByUserIdentity(testUser);
        Assert.assertEquals(0, userAuthenticationInfo.getPassword().length);
        String setSql = "SET PASSWORD FOR 'test'@'%' = PASSWORD('123456');";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(setSql, starRocksAssert.getCtx());
        SetExecutor executor = new SetExecutor(starRocksAssert.getCtx(), (SetStmt) statementBase);
        executor.execute();
        userAuthenticationInfo = GlobalStateMgr.getCurrentState().getAuthenticationManager().
                getUserAuthenticationInfoByUserIdentity(testUser);
        Assert.assertTrue(userAuthenticationInfo.getPassword().length > 0);

    }

}