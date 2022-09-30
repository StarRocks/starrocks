// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class PrivilegeStmtAnalyzerV2Test {
    static ConnectContext ctx;
    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        ctx.getGlobalStateMgr().getAuthenticationManager().createUser(createUserStmt);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        DropUserStmt dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "drop user test_user", ctx);
        ctx.getGlobalStateMgr().getAuthenticationManager().dropUser(dropUserStmt);
    }

    @Test
    public void testCreateUser() throws Exception {
        String sql = "create user test";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("%", stmt.getUserIdent().getHost());
        Assert.assertEquals("", stmt.getOriginalPassword());
        Assert.assertEquals(PlainPasswordAuthenticationProvider.PLUGIN_NAME, stmt.getAuthPlugin());

        sql = "create user 'test'@'10.1.1.1'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("10.1.1.1", stmt.getUserIdent().getHost());
        Assert.assertEquals("", stmt.getOriginalPassword());
        Assert.assertEquals(PlainPasswordAuthenticationProvider.PLUGIN_NAME, stmt.getAuthPlugin());

        sql = "create user 'test'@'%' identified by 'abc'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("%", stmt.getUserIdent().getHost());
        Assert.assertEquals("abc", stmt.getOriginalPassword());
        Assert.assertEquals(PlainPasswordAuthenticationProvider.PLUGIN_NAME, stmt.getAuthPlugin());

        sql = "create user 'aaa~bbb'";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("invalid user name"));
        }
    }

    @Test
    public void testAlterDropUser() throws Exception {
        String sql = "alter user test_user identified by 'abc'";
        AlterUserStmt alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_user", alterUserStmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("%", alterUserStmt.getUserIdent().getHost());
        Assert.assertEquals("abc", alterUserStmt.getOriginalPassword());
        Assert.assertEquals(PlainPasswordAuthenticationProvider.PLUGIN_NAME, alterUserStmt.getAuthPlugin());

        sql = "alter user 'test'@'10.1.1.1' identified by 'abc'";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("user 'test'@'10.1.1.1' not exist!"));
        }

        sql = "drop user test";
        DropUserStmt dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", dropUserStmt.getUserIdent().getQualifiedUser());

        sql = "drop user test_user";
        dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_user", dropUserStmt.getUserIdent().getQualifiedUser());

        sql = "drop user root";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot drop root!"));
        }
    }

    @Test
    public void testGrantRevokeSelectTablePrivilege() throws Exception {
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test", ctx);
        ctx.getGlobalStateMgr().getAuthenticationManager().createUser(createUserStmt);

        String sql = "grant select on db.tbl to test";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("TABLE", grantStmt.getPrivType());
        List<String> tokens = grantStmt.getPrivilegeObjectNameTokenList();
        Assert.assertEquals(2, tokens.size());
        Assert.assertEquals("db", tokens.get(0));
        Assert.assertEquals("tbl", tokens.get(1));
        Assert.assertEquals("test", grantStmt.getUserIdentity().getQualifiedUser());
        Assert.assertNull(grantStmt.getRole());

        sql = "revoke select on db.tbl from test";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("TABLE", revokeStmt.getPrivType());
        tokens = revokeStmt.getPrivilegeObjectNameTokenList();
        Assert.assertEquals(2, tokens.size());
        Assert.assertEquals("db", tokens.get(0));
        Assert.assertEquals("tbl", tokens.get(1));
        Assert.assertEquals("test", revokeStmt.getUserIdentity().getQualifiedUser());
        Assert.assertNull(revokeStmt.getRole());
    }

    @Test
    public void testRole() throws Exception {
        String sql = "create role test_role";
        CreateRoleStmt createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role", createStmt.getQualifiedRole());

        // bad name
        sql = "create role ___";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        sql = "drop role test_role";
        DropRoleStmt dropStmt = (DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role", createStmt.getQualifiedRole());

        sql = "drop role ___";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
      }
}
