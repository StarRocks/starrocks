// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.Catalog;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.UserPrivTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GrantRevokeRoleStmtTest {

    @Mocked
    private Catalog catalog;
    @Mocked
    private Auth auth;
    @Mocked
    private UserPrivTable userPrivTable;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
        new Expectations(catalog) {
            {
                catalog.getAuth();
                minTimes = 0;
                result = auth;
            }
        };
        new Expectations(auth) {
            {
                auth.getUserPrivTable();
                minTimes = 0;
                result = userPrivTable;

                auth.checkGlobalPriv(ctx, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getClusterName();
                minTimes = 0;
                result = "test_cluster";
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

        // suppose current role exists
        new Expectations(auth) {
            {
                auth.doesRoleExist((String)any);
                minTimes = 0;
                result = true;
            }
        };

        // grant
        // user without host
        GrantRoleStmt stmt = (GrantRoleStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant test_role to test_user", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("GRANT 'test_cluster:test_role' TO 'test_cluster:test_user'@'%'", stmt.toString());

        // grant 2
        // user with host
        stmt = (GrantRoleStmt) com.starrocks.sql.parser.SqlParser.parse(
                "grant 'test_role' to 'test_user'@'localhost'", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("GRANT 'test_cluster:test_role' TO 'test_cluster:test_user'@'localhost'", stmt.toString());

        // revoke
        // user with domain
        RevokeRoleStmt stmt2 = (RevokeRoleStmt) com.starrocks.sql.parser.SqlParser.parse(
                "revoke 'test_role' from 'test_user'@['starrocks.com']", 1).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt2, ctx);
        Assert.assertEquals("REVOKE 'test_cluster:test_role' FROM 'test_cluster:test_user'@['starrocks.com']", stmt2.toString());
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
        // suppose current role exists
        new Expectations(auth) {
            {
                auth.doesRoleExist((String)any);
                minTimes = 0;
                result = true;
            }
        };
        GrantRoleStmt stmt = new GrantRoleStmt("test_role", new UserIdentity("test_user", "localhost"));
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testRoleNotExist() throws Exception {
        // suppose current exists
        new Expectations(userPrivTable) {
            {
                userPrivTable.doesUserExist((UserIdentity)any);
                minTimes = 0;
                result = true;
            }
        };
        // suppose current role doesn't exist, check for exception
        new Expectations(auth) {
            {
                auth.doesRoleExist((String)any);
                minTimes = 0;
                result = false;
            }
        };
        GrantRoleStmt stmt = new GrantRoleStmt("test_role", new UserIdentity("test_user", "localhost"));
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws.");
    }
 }
