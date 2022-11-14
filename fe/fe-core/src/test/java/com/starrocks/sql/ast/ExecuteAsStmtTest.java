// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExecuteAsStmtTest {

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
        Assert.assertEquals("user1", stmt.getToUser().getQualifiedUser());
        Assert.assertEquals("%", stmt.getToUser().getHost());
        Assert.assertEquals("EXECUTE AS 'user1'@'%' WITH NO REVERT", stmt.toString());
        Assert.assertFalse(stmt.isAllowRevert());
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
