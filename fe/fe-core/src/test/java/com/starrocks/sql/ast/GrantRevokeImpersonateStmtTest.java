// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.mysql.privilege.UserPrivTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GrantRevokeImpersonateStmtTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private Auth auth;
    @Mocked
    private UserPrivTable userPrivTable;
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
            }
        };
        new Expectations(auth) {
            {
                auth.getUserPrivTable();
                minTimes = 0;
                result = userPrivTable;

                auth.doesRoleExist((String) any);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getClusterName();
                minTimes = 0;
                result = "test_cluster";

                ctx.getGlobalStateMgr();
                minTimes = 0;
                result = globalStateMgr;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        // suppose current user exists
        new Expectations(userPrivTable) {
            {
                userPrivTable.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = true;
            }
        };

        // grant IMPERSONATE
        GrantImpersonateStmt stmt = (GrantImpersonateStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant IMPERSONATE on user2 to user1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("GRANT IMPERSONATE ON 'test_cluster:user2'@'%' TO 'test_cluster:user1'@'%'", stmt.toString());

        stmt = (GrantImpersonateStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant IMPERSONATE on user2 to ROLE role1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("GRANT IMPERSONATE ON 'test_cluster:user2'@'%' TO ROLE 'test_cluster:role1'", stmt.toString());

        // revoke
        RevokeImpersonateStmt stmt2 = (RevokeImpersonateStmt) com.starrocks.sql.parser.SqlParser.parse(
                "revoke IMPERSONATE on user2 from user1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt2, ctx);
        Assert.assertEquals("REVOKE IMPERSONATE ON 'test_cluster:user2'@'%' FROM 'test_cluster:user1'@'%'",
                stmt2.toString());

        stmt2 = (RevokeImpersonateStmt) com.starrocks.sql.parser.SqlParser.parse(
                "revoke IMPERSONATE on user2 from ROLE role1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt2, ctx);
        Assert.assertEquals("REVOKE IMPERSONATE ON 'test_cluster:user2'@'%' FROM ROLE 'test_cluster:role1'",
                stmt2.toString());
    }

    @Test(expected = SemanticException.class)
    public void testUserNotExist() throws Exception {
        // suppose current user doesn't exist, check for exception
        new Expectations(userPrivTable) {
            {
                userPrivTable.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = false;
            }
        };
        GrantImpersonateStmt stmt = (GrantImpersonateStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant IMPERSONATE on user2 to user1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testRoleNotExist() throws Exception {
        // suppose current user doesn't exist, check for exception
        new Expectations(userPrivTable) {
            {
                userPrivTable.doesUserExist((UserIdentity) any);
                minTimes = 0;
                result = true;
            }
        };
        new Expectations(auth) {
            {
                auth.doesRoleExist((String) any);
                minTimes = 0;
                result = false;
            }
        };
        GrantImpersonateStmt stmt = (GrantImpersonateStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant IMPERSONATE on user2 to Role role1", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }
}
