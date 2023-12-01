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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

public class ExecuteAsStmtTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private AuthenticationMgr auth;
    @Mocked
    private AuthorizationMgr authorizationMgr;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() throws PrivilegeException {
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                minTimes = 0;
                result = auth;

                globalStateMgr.isUsingNewPrivilege();
                minTimes = 0;
                result = false;

                GlobalStateMgr.getCurrentState().getAuthorizationMgr().getDefaultRoleIdsByUser((UserIdentity) any);
                minTimes = 0;
                result = new HashSet<>();
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
    public void testWithNoRevert() throws Exception {
        // suppose current user exists
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;
            }
        };

        ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                "execute as user1 with no revert", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("user1", stmt.getToUser().getUser());
        Assert.assertEquals("%", stmt.getToUser().getHost());
        Assert.assertEquals("EXECUTE AS 'user1'@'%' WITH NO REVERT", stmt.toString());
        Assert.assertFalse(stmt.isAllowRevert());

        ExecuteAsExecutor.execute(stmt, ctx);

        Assert.assertEquals(new UserIdentity("user1", "%"), ctx.getCurrentUserIdentity());
    }

    @Test(expected = SemanticException.class)
    public void testUserNotExist() throws Exception {
        // suppose current user doesn't exist, check for exception
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = false;
            }
        };
        ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                "execute as user1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testAllowRevert() throws Exception {
        // suppose current user exists
        new Expectations(auth) {
            {
                auth.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;
            }
        };

        ExecuteAsStmt stmt = (ExecuteAsStmt) com.starrocks.sql.parser.SqlParser.parse(
                "execute as user1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }
}
