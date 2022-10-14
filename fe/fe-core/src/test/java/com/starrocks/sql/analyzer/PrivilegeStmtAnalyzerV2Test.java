// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.utframe.StarRocksAssert;
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
    public void testGrantRevokeSelectTableDbPrivilege() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test", ctx);
        globalStateMgr.getAuthenticationManager().createUser(createUserStmt);

        String sql = "grant select on db1.tbl1 to test";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert,delete on db1.tbl1 to test";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert_priv,delete on table db1.tbl1 to test";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke create_table on database db1 from test";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke create_table,drop on db1 from test";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke select on db1 from test";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid action SELECT for DATABASE"));
        }

        sql = "grant insert on table dbx to test";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have two"));
        }

        sql = "grant insert on dbx.tblxx to test";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find db: dbx"));
        }

        sql = "grant insert on db1.tblxx to test";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find table tblxx in db db1"));
        }

        sql = "grant drop on database db1.tbl1 to test";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have one"));
        }

        sql = "grant drop on dbx to test";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find db: dbx"));
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
    public void testRole() throws Exception {
        String sql = "create role test_role";
        CreateRoleStmt createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role", createStmt.getQualifiedRole());
        ctx.getGlobalStateMgr().getPrivilegeManager().createRole(createStmt);

        // bad name
        sql = "create role ___";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid role format"));
        }

        sql = "drop role test_role";
        DropRoleStmt dropStmt = (DropRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role", dropStmt.getQualifiedRole());

        sql = "drop role ___";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid role format"));
        }

        sql = "drop role bad_role";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Can not drop role bad_role: cannot find role"));
        }
    }

    @Test
    public void testSetRole() throws Exception {
        String sql = "set role 'role1', 'role2'";
        SetRoleStmt setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(2, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertEquals("role2", setRoleStmt.getRoles().get(1));
        Assert.assertFalse(setRoleStmt.isAll());

        sql = "set role 'role1'";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(1, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertFalse(setRoleStmt.isAll());

        sql = "set role all";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertNull(setRoleStmt.getRoles());
        Assert.assertTrue(setRoleStmt.isAll());

        sql = "set role all except 'role1'";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(1, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertTrue(setRoleStmt.isAll());
    }
}
