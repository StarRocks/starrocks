// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PrivilegeStmtAnalyzerV2Test {
    ConnectContext ctx;
    @Before
    public void init() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
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
}
