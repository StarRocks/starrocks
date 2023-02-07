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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class PrivilegeStmtAnalyzerV2Test {
    static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        AnalyzeTestUtil.init();
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
        for (int i = 0; i < 2; ++i) {
            starRocksAssert.withTable("create table db1.tbl" + i + createTblStmtStr);
        }
        ctx.getGlobalStateMgr().getPrivilegeManager().initBuiltinRolesAndUsers();
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

        sql = "create user 'test'@'10.1.1.1'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("10.1.1.1", stmt.getUserIdent().getHost());
        Assert.assertEquals("", stmt.getOriginalPassword());

        sql = "create user 'test'@'%' identified by 'abc'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("%", stmt.getUserIdent().getHost());
        Assert.assertEquals("abc", stmt.getOriginalPassword());

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

        sql = "revoke select on tttable db1.tbl0 from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find privilege object type TTTABLE"));
        }

        sql = "revoke select on database db1 from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find action SELECT in"));
        }

        sql = "grant insert on table dbx to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No database selected"));
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

        sql = "grant select on table db1.tbl1 to role test_role";
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        sql = "grant select on table db1.tbl1 to role xxx";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Can not grant/revoke to role: cannot find role"));
        }
    }

    @Test
    public void testAlterDropUser() throws Exception {
        String sql = "alter user test_user identified by 'abc'";
        AlterUserStmt alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_user", alterUserStmt.getUserIdent().getQualifiedUser());
        Assert.assertEquals("%", alterUserStmt.getUserIdent().getHost());
        Assert.assertEquals("abc", alterUserStmt.getOriginalPassword());

        sql = "alter user 'test'@'10.1.1.1' identified by 'abc'";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("cannot find user 'test'@'10.1.1.1'!"));
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

        sql = "create role test_role2";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role2", createStmt.getQualifiedRole());
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
            Assert.assertTrue(e.getMessage().contains("Can not drop role: cannot find role bad_role"));
        }

        sql = "grant test_role to test_user";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("[test_role]", grantRoleStmt.getGranteeRole().toString());
        Assert.assertEquals("'test_user'@'%'", grantRoleStmt.getUserIdent().toString());

        sql = "grant test_role, test_role2 to test_user";
        grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("[test_role, test_role2]", grantRoleStmt.getGranteeRole().toString());
        Assert.assertEquals("'test_user'@'%'", grantRoleStmt.getUserIdent().toString());

        sql = "grant ___ to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid role format"));
        }
    }

    @Test
    public void testSetRole() throws Exception {
        for (int i = 1; i != 4; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create role role" + i, ctx), ctx);
        }

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

        sql = "set role all except 'role1', 'role2', 'role3'";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(3, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertEquals("role2", setRoleStmt.getRoles().get(1));
        Assert.assertEquals("role3", setRoleStmt.getRoles().get(2));
        Assert.assertTrue(setRoleStmt.isAll());

        // invalidate rolename
        try {
            UtFrameUtils.parseStmtWithNewParser("set role 'role1', 'bad_role'", ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot set role: cannot find role bad_role"));
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
        sql = "grant impersonate on ALL users to test_user";
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
            Assert.assertTrue(e.getMessage().contains("You have an error in your SQL syntax"));
        }

        sql = "revoke select on ALL tables IN ALL tables from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("You have an error in your SQL syntax"));
        }

        sql = "grant create_table on ALL databases in database db1 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have one"));
        }

        sql = "grant impersonate on ALL users in all databases to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid ALL statement for user! only support ON ALL USERS"));
        }

    }

    @Test
    public void testGrantRevokeImpersonate() throws Exception {
        String sql = "grant impersonate on USER root to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant impersonate on USER root to test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on USER 'root'@'%' from test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant impersonate on user 'root'@'%', 'test_user'@'%' to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on user root, test_user from test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on user root, 'test_user'@'%' from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        try {
            UtFrameUtils.parseStmtWithNewParser("grant impersonate on USER xxx to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find user 'xxx'@'%'"));
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

    @Test
    public void testResourceException() throws Exception {
        try {
            UtFrameUtils.parseStmtWithNewParser(
                    "grant alter on all resources in all databases to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have one: [*, *]"));
        }

        try {
            UtFrameUtils.parseStmtWithNewParser(
                    "grant alter on resource db.resource to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("invalid object tokens, should have one"));
        }

        try {
            UtFrameUtils.parseStmtWithNewParser(
                    "grant alter on resource 'not_exists' to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("cannot find resource: not_exists"));
        }
    }

    @Test
    public void testViewException() throws Exception {
        String sql;
        sql = "grant alter, select on view db1 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No database selected"));
        }

        sql = "grant drop, select on view xxx.xx to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find db: xxx"));
        }

        sql = "grant drop on view db1.tbl1 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find view tbl1 in db db1"));
        }

        sql = "grant select on ALL views to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ALL VIEWS must be restricted with database"));
        }

        sql = "revoke select on ALL views IN ALL views IN all databases from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("You have an error in your SQL syntax"));
        }

        sql = "revoke select on ALL views IN ALL views from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("You have an error in your SQL syntax"));
        }
    }

    @Test
    public void testExecuteAs() throws Exception {
        ExecuteAsStmt stmt = (ExecuteAsStmt) UtFrameUtils.parseStmtWithNewParser(
                "execute as root with no revert", ctx);
        Assert.assertEquals(UserIdentity.ROOT, stmt.getToUser());
        Assert.assertFalse(stmt.isAllowRevert());

        try {
            UtFrameUtils.parseStmtWithNewParser("execute as root", ctx);
            Assert.fail();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("`EXECUTE AS` must use with `WITH NO REVERT` for now"));
        }
    }

    @Test
    public void testGrantMultiObject() {
        String sql = "grant SELECT on TABLE test.t0 to test_user";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT ON TABLE test.t0 TO 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "GRANT SELECT on TABLE test.t0, test.t1 to role public";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT ON TABLE test.t0, test.t1 TO ROLE 'public'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));
    }
}
