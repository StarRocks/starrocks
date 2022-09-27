// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
    }
}
