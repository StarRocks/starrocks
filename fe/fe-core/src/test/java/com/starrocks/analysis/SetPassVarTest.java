// This file is made available under Elastic License 2.0.
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

import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
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
        currentUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser);
    }

    @Test
    public void testNormal() throws UserException {
        SetPassVar stmt;

        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B");
        stmt.analyze();
        Assert.assertEquals("testUser", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", new String(stmt.getPassword()));

        Assert.assertEquals("'testUser'@'%'", stmt.getUserIdent().toString());

        // empty password
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), null);
        stmt.analyze();
        Assert.assertEquals("'testUser'@'%'", stmt.getUserIdent().toString());

        // empty user
        // empty password
        stmt = new SetPassVar(null, null);
        stmt.analyze();
        Assert.assertEquals("'root'@'192.168.1.1'", stmt.getUserIdent().toString());
    }

    @Test
    public void testCreateTablePartitionNormal() throws Exception {
        String setSql = "set sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES');";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(setSql, ctx);
        ctx.getSessionVariable().setSqlMode(SqlModeHelper.MODE_STRICT_TRANS_TABLES);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        SetVar setVars = stmt.getSetVars().get(0);

        Assert.assertTrue(setVars.getResolvedExpression().getStringValue().contains("STRICT_TRANS_TABLES"));
    }

    @Test(expected = SemanticException.class)
    public void testBadPassword() {
        SetPassVar stmt;
        //  mode: SET PASSWORD FOR 'testUser' = 'testPass';
        stmt = new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBAHD913688E7278E2AD071FDB5E76D76D34B");
        stmt.analyze();
        Assert.fail("No exception throws.");
    }

}