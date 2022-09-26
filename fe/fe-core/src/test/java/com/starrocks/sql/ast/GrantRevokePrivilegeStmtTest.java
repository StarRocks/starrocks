// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GrantRevokePrivilegeStmtTest {

    private StarRocksAssert starRocksAssert;
    private ConnectContext ctx;
    private UserIdentity currentUser = UserIdentity.createAnalyzedUserIdentWithIp("me", "%");
    private UserIdentity existUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
    private String existRole = "test_role";
    private Auth auth;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert();
        ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        auth = GlobalStateMgr.getCurrentState().getAuth();

        auth.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user if not exists " + currentUser.getQualifiedUser(), ctx));
        auth.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user if not exists " + existUser.getQualifiedUser(), ctx));
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(existRole);
        if (! auth.doesRoleExist(existRole)) {
            auth.createRole((CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "create role " + existRole, ctx));
        }

        ctx.setCurrentUserIdentity(currentUser);
    }

    @Test
    public void testCommonBadCase() throws Exception {
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("grant IMPERSONATE on XXX db.table to me", ctx));
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("grant aaa, bbb on db.table to me", ctx));
    }

    @Test
    public void testImpersonate() throws Exception {
        // grant IMPERSONATE
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "grant IMPERSONATE on test_user to me", ctx);
        Assert.assertEquals("GRANT IMPERSONATE ON 'test_user'@'%' TO 'me'@'%'", AST2SQL.toString(stmt));

        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "grant IMPERSONATE on test_user to ROLE test_role", ctx);
        Assert.assertEquals("GRANT IMPERSONATE ON 'test_user'@'%' TO ROLE 'test_role'", AST2SQL.toString(stmt));

        // revoke
        RevokePrivilegeStmt stmt2 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke IMPERSONATE on test_user from me", ctx);
        Assert.assertEquals("REVOKE IMPERSONATE ON 'test_user'@'%' FROM 'me'@'%'",
                AST2SQL.toString(stmt2));

        stmt2 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke IMPERSONATE on test_user from ROLE test_role", ctx);
        Assert.assertEquals("REVOKE IMPERSONATE ON 'test_user'@'%' FROM ROLE 'test_role'",
                AST2SQL.toString(stmt2));

        // not exists user
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("grant IMPERSONATE on test_user to userxx", ctx));
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("grant IMPERSONATE on userxx to test_user", ctx));

        // role not exists
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("grant IMPERSONATE on test_user to Role rolexx", ctx));

        // invalide privilege
        Assert.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("grant IMPERSONATE,SELECT on test_user@'%' to me", ctx));
    }

    @Test
    public void testGlobal() throws Exception {
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT select_priv on *.* to me", ctx);
        Assert.assertEquals("GRANT SELECT ON *.* TO 'me'@'%'", AST2SQL.toString(stmt));
        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
        "GRANT usage on resource * to role test_role", ctx);
        Assert.assertEquals("GRANT USAGE ON RESOURCE * TO ROLE 'test_role'", AST2SQL.toString(stmt));
        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT usage on *.* to role test_role", ctx);
        Assert.assertEquals("GRANT USAGE ON *.* TO ROLE 'test_role'", AST2SQL.toString(stmt));

        // many privileges
        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT node_priv, select_priv, grant_priv, admin_priv, usage_priv on *.* to me", ctx);
        Assert.assertTrue(stmt.getPrivBitSet().containsPrivs(
                Privilege.NODE_PRIV, Privilege.ADMIN_PRIV, Privilege.SELECT_PRIV, Privilege.USAGE_PRIV));
        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
        "GRANT node, select, grant, admin, usage on resource * to me", ctx);
        Assert.assertTrue(stmt.getPrivBitSet().containsPrivs(
                Privilege.NODE_PRIV, Privilege.ADMIN_PRIV, Privilege.SELECT_PRIV, Privilege.USAGE_PRIV));

        // not exists user
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "REVOKE node_priv, select_pri, grant_priv, admin_priv, usage_priv on *.* from userxx", ctx));

        // invalid privilege
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "REVOKE xxx on resource * from me", ctx));
    }

    @Test
    public void testDatabase() throws Exception {
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT select_priv on db.* to me", ctx);
        Assert.assertEquals("GRANT SELECT ON db.* TO 'me'@'%'", AST2SQL.toString(stmt));

        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT usage on db.* to role test_role", ctx);
        Assert.assertEquals("GRANT USAGE ON db.* TO ROLE 'test_role'", AST2SQL.toString(stmt));

        RevokePrivilegeStmt stmt1 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke select on db.* from me", ctx);
        Assert.assertEquals("REVOKE SELECT ON db.* FROM 'me'@'%'", AST2SQL.toString(stmt1));
        stmt1 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke usage on db.* from role test_role", ctx);
        Assert.assertEquals("REVOKE USAGE ON db.* FROM ROLE 'test_role'", AST2SQL.toString(stmt1));
    }

    @Test
    public void testTable() throws Exception {
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT select_priv on db.tbl to me", ctx);
        Assert.assertEquals("GRANT SELECT ON db.tbl TO 'me'@'%'", AST2SQL.toString(stmt));

        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT usage on db.tbl to role 'test_role'", ctx);
        Assert.assertEquals("GRANT USAGE ON db.tbl TO ROLE 'test_role'", AST2SQL.toString(stmt));

        RevokePrivilegeStmt stmt1 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke select on db.tbl from me", ctx);
        Assert.assertEquals("REVOKE SELECT ON db.tbl FROM 'me'@'%'", AST2SQL.toString(stmt1));
        stmt1 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke usage on db.tbl from role test_role", ctx);
        Assert.assertEquals("REVOKE USAGE ON db.tbl FROM ROLE 'test_role'", AST2SQL.toString(stmt1));

        // invalide table name
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "REVOKE select on 'not!valid!' from me", ctx));
        // invalid privilege
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "REVOKE admin,node on db.tbl from me", ctx));
    }

    @Test
    public void testResources() throws Exception {
        GrantPrivilegeStmt stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT usage_priv on resource spark0 to me", ctx);
        Assert.assertEquals("GRANT USAGE ON RESOURCE spark0 TO 'me'@'%'", AST2SQL.toString(stmt));

        stmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT USAGE ON RESOURCE 'spark0' TO ROLE test_role", ctx);
        Assert.assertEquals("GRANT USAGE ON RESOURCE spark0 TO ROLE 'test_role'", AST2SQL.toString(stmt));

        RevokePrivilegeStmt stmt1 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke usage on RESOURCE 'spark0' from me", ctx);
        Assert.assertEquals("REVOKE USAGE ON RESOURCE spark0 FROM 'me'@'%'", AST2SQL.toString(stmt1));
        stmt1 = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "revoke usage on resource spark0 from role test_role", ctx);
        Assert.assertEquals("REVOKE USAGE ON RESOURCE spark0 FROM ROLE 'test_role'", AST2SQL.toString(stmt1));

        // invalide resource name
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "REVOKE node_priv on RESOURCE not.valid from me", ctx));
        Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(
                "REVOKE node_priv on RESOURCE 'not!valid!' from me", ctx));
    }

}


