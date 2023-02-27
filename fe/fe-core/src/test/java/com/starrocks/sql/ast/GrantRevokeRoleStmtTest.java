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


package com.starrocks.sql.ast;

import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class GrantRevokeRoleStmtTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                globalStateMgr.isUsingNewPrivilege();
                minTimes = 0;
                result = false;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getGlobalStateMgr();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        // suppose current user exists
        // suppose current role exists and has GRANT privilege
        new Expectations(auth) {
            {
                auth.doesRoleExist((String) any);
                minTimes = 0;
                result = true;

                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;
            }
        };

        // grant
        // user without host
        GrantRoleStmt stmt = (GrantRoleStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant test_role to test_user", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("GRANT 'test_role' TO 'test_user'@'%'", AstToSQLBuilder.toSQL(stmt));

        // grant 2
        // user with host
        stmt = (GrantRoleStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant 'test_role' to 'test_user'@'localhost'", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("GRANT 'test_role' TO 'test_user'@'localhost'", AstToSQLBuilder.toSQL(stmt));

        // revoke
        // user with domain
        RevokeRoleStmt stmt2 = (RevokeRoleStmt) com.starrocks.sql.parser.SqlParser.parse(
                "revoke 'test_role' from 'test_user'@['starrocks.com']", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt2, ctx);
        Assert.assertEquals("REVOKE 'test_role' " +
                "FROM 'test_user'@['starrocks.com']", AstToSQLBuilder.toSQL(stmt2));
    }

    @Test(expected = SemanticException.class)
    public void testUserNotExist() throws Exception {
        // suppose current user doesn't exist, check for exception
        // suppose current role exists
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = false;

                auth.doesRoleExist((String) any);
                minTimes = 0;
                result = true;
            }
        };
        GrantRoleStmt stmt = new GrantRoleStmt(Collections.singletonList("test_role"),
                new UserIdentity("test_user", "localhost"));
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testRoleNotExist() throws Exception {
        // suppose current exists
        // suppose current role doesn't exist, check for exception
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;

                auth.doesRoleExist((String) any);
                minTimes = 0;
                result = false;
            }
        };
        GrantRoleStmt stmt = new GrantRoleStmt(Collections.singletonList("test_role"),
                new UserIdentity("test_user", "localhost"));
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }
}
