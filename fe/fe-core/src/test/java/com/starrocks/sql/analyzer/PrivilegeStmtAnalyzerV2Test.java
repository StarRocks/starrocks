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
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        // create db1.tbl0, db1.tbl1
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withDatabase("db1");
        for (int i = 0; i < 2; ++ i) {
            starRocksAssert.withTable("create table db1.tbl" + i + createTblStmtStr);
        }
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
        String sql = "grant select on db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert,delete on db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert,delete on db1.tbl1 to test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert_priv,delete on table db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke create_table on database db1 from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke create_table,drop on database db1 from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant ALL on table db1.tbl0, db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant ALL on db1.tbl0, db1.tbl0 to test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke select on database db1 from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid action SELECT for DATABASE"));
        }

        sql = "grant insert on table dbx to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have two"));
        }

        sql = "grant insert on dbx.tblxx to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find db: dbx"));
        }

        sql = "grant insert on db1.tblxx to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find table tblxx in db db1"));
        }

        sql = "grant drop on database db1.tbl1 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have one"));
        }

        sql = "grant drop on database dbx to test_user";
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
    public void testGrantRevokeAll() throws Exception {
        String sql = "grant select on ALL tables in all databases to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        sql = "revoke select on ALL tables in all databases from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select on ALL tables in database db1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        sql = "revoke select on ALL tables in database db1 from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant create_table on ALL databases to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        sql = "revoke create_table on ALL databases from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant create_table on ALL database to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid plural privilege type DATABASE"));
        }

        sql = "grant create_table on ALL tables to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ALL TABLES must be restricted with database"));
        }

        sql = "revoke select on ALL tables IN ALL tables IN all databases from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid ALL statement for tables"));
        }

        sql = "revoke select on ALL tables IN ALL tables from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ALL TABLES must be restricted with ALL DATABASES instead of ALL TABLES"));
        }

        sql = "grant create_table on ALL databases in database db1 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid ALL statement for databases! only support ON ALL DATABASES"));
        }

    }

    @Test
    public void testGrantRevokeImpersonate() throws Exception {
        String sql = "grant impersonate on root to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant impersonate on root to test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on 'root'@'%' from test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant impersonate on user 'root'@'%', 'test_user'@'%' to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on user root, test_user from test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on user root, 'test_user'@'%' from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        try {
            UtFrameUtils.parseStmtWithNewParser("grant impersonate on xxx to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("user 'xxx'@'%' not exist!"));
        }
    }

    @Test
    public void testGrantSystem() throws Exception {
        try {
            UtFrameUtils.parseStmtWithNewParser("grant grant on system to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot grant/revoke system privilege"));
        }

    }
}
