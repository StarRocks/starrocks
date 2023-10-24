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

import com.starrocks.common.AnalysisException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SetDefaultRoleExecutor;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetRoleType;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
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
        ctx.getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        ctx.getGlobalStateMgr().getAuthenticationMgr().createUser(createUserStmt);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        DropUserStmt dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "drop user test_user", ctx);
        ctx.getGlobalStateMgr().getAuthenticationMgr().dropUser(dropUserStmt);
    }

    @Test
    public void testCreateUser() throws Exception {
        String sql = "create user test";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdentity().getUser());
        Assert.assertEquals("%", stmt.getUserIdentity().getHost());
        Assert.assertEquals("", stmt.getOriginalPassword());

        sql = "create user 'test'@'10.1.1.1'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdentity().getUser());
        Assert.assertEquals("10.1.1.1", stmt.getUserIdentity().getHost());
        Assert.assertEquals("", stmt.getOriginalPassword());

        sql = "create user 'test'@'%' identified by 'abc'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test", stmt.getUserIdentity().getUser());
        Assert.assertEquals("%", stmt.getUserIdentity().getHost());
        Assert.assertEquals("abc", stmt.getOriginalPassword());

        sql = "create user 'aaa~bbb'";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("invalid user name"));
        }

        sql = "create user u1 identified with mysql_native_password by '123456'";
        CreateUserStmt createUserStmt = (CreateUserStmt) analyzeSuccess(sql);
        Assert.assertEquals("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9",
                new String(createUserStmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8));

        sql = "create user u2 identified with mysql_native_password as '123456'";
        analyzeFail(sql, "Password hash should be a 41-digit hexadecimal number");

        sql = "create user u2 identified with mysql_native_password as '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'";
        createUserStmt = (CreateUserStmt) analyzeSuccess(sql);
        Assert.assertEquals("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9",
                new String(createUserStmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8));

        sql = "create user u3 identified by '123456'";
        createUserStmt = (CreateUserStmt) analyzeSuccess(sql);
        Assert.assertEquals("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9",
                new String(createUserStmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8));

        sql = "create user u4 identified by password '123456'";
        analyzeFail(sql, "Password hash should be a 41-digit hexadecimal number");

        sql = "create user u4 identified by password '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'";
        createUserStmt = (CreateUserStmt) analyzeSuccess(sql);
        Assert.assertEquals("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9",
                new String(createUserStmt.getAuthenticationInfo().getPassword(), StandardCharsets.UTF_8));
    }

    @Test
    public void testGrantRevokeSelectTableDbPrivilege() throws Exception {
        String sql = "grant select on db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert,delete on db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert,delete on db1.tbl1 to test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant select,insert,delete on table db1.tbl1 to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke create table on database db1 from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke create table,drop on database db1 from test_user";
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
            Assert.assertTrue(e.getMessage().contains("syntax error"));
        }

        sql = "revoke select on database db1 from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Cannot grant or revoke SELECT on 'DATABASE' type object"));
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
            Assert.assertTrue(e.getMessage().contains("cannot find catalog"));
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
        Assert.assertEquals("test_user", alterUserStmt.getUserIdentity().getUser());
        Assert.assertEquals("%", alterUserStmt.getUserIdentity().getHost());
        Assert.assertEquals("abc", alterUserStmt.getOriginalPassword());

        sql = "alter user 'test'@'10.1.1.1' identified by 'abc'";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Operation ALTER USER failed for 'test'@'10.1.1.1' : user not exists"));
        }


        try {
            sql = "drop user test";
            DropUserStmt dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Operation DROP USER failed for 'test'@'%' : user not exists"));
        }

        sql = "drop user test_user";
        DropUserStmt dropUserStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_user", dropUserStmt.getUserIdentity().getUser());

        sql = "drop user root";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Operation DROP USER failed for 'root'@'%' : cannot drop user 'root'@'%'"));
        }
    }

    @Test
    public void testRole() throws Exception {
        String sql = "create role test_role";
        CreateRoleStmt createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role", createStmt.getRoles().get(0));
        ctx.getGlobalStateMgr().getAuthorizationMgr().createRole(createStmt);

        sql = "create role test_role2";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("test_role2", createStmt.getRoles().get(0));
        ctx.getGlobalStateMgr().getAuthorizationMgr().createRole(createStmt);

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
        Assert.assertEquals("test_role", dropStmt.getRoles().get(0));

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
            Assert.assertTrue(e.getMessage().contains("Operation DROP ROLE failed for bad_role : role not exists"));
        }

        sql = "grant test_role to test_user";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("[test_role]", grantRoleStmt.getGranteeRole().toString());
        Assert.assertEquals("'test_user'@'%'", grantRoleStmt.getUserIdentity().toString());

        sql = "grant test_role, test_role2 to test_user";
        grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals("[test_role, test_role2]", grantRoleStmt.getGranteeRole().toString());
        Assert.assertEquals("'test_user'@'%'", grantRoleStmt.getUserIdentity().toString());

        sql = "grant ___ to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("invalid role format"));
        }

        sql = "create role r1, r2";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
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
        Assert.assertNotEquals(setRoleStmt.getSetRoleType(), SetRoleType.ALL);

        sql = "set role 'role1'";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(1, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertNotEquals(setRoleStmt.getSetRoleType(), SetRoleType.ALL);

        sql = "set role all";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertTrue(setRoleStmt.getRoles().isEmpty());
        Assert.assertEquals(setRoleStmt.getSetRoleType(), SetRoleType.ALL);

        sql = "set role all except 'role1'";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(1, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertEquals(setRoleStmt.getSetRoleType(), SetRoleType.ALL);

        sql = "set role all except 'role1', 'role2', 'role3'";
        setRoleStmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertEquals(3, setRoleStmt.getRoles().size());
        Assert.assertEquals("role1", setRoleStmt.getRoles().get(0));
        Assert.assertEquals("role2", setRoleStmt.getRoles().get(1));
        Assert.assertEquals("role3", setRoleStmt.getRoles().get(2));
        Assert.assertEquals(setRoleStmt.getSetRoleType(), SetRoleType.ALL);

        // invalidate rolename
        try {
            UtFrameUtils.parseStmtWithNewParser("set role 'role1', 'bad_role'", ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot set role: cannot find role bad_role"));
        }

        sql = "drop role role1, role2";
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        for (int i = 1; i != 4; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role role" + i, ctx), ctx);
        }
    }

    @Test
    public void testSetDefaultRole() throws Exception {
        for (int i = 1; i != 4; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create role role" + i, ctx), ctx);
        }

        AuthorizationMgr authorizationManager = ctx.getGlobalStateMgr().getAuthorizationMgr();

        String sql = "grant role1, role2 to user test_user";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantRoleStmt, ctx);

        sql = "set default role all to test_user";
        SetDefaultRoleStmt setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        List<Long> roleId = new ArrayList<>(authorizationManager
                .getDefaultRoleIdsByUser(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%")));
        Assert.assertEquals(2, roleId.size());

        sql = "set default role none to test_user";
        setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        roleId = new ArrayList<>(authorizationManager
                .getDefaultRoleIdsByUser(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%")));
        Assert.assertEquals(0, roleId.size());

        sql = "set default role 'role1' to test_user";
        setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        roleId = new ArrayList<>(authorizationManager
                .getDefaultRoleIdsByUser(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%")));
        Assert.assertEquals(1, roleId.size());
        Assert.assertEquals("role1",
                authorizationManager.getRolePrivilegeCollectionUnlocked(roleId.get(0), true).getName());

        sql = "set default role xxx to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find role xxx!"));
        }

        sql = "set default role role3 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Role role3 is not granted to 'test_user'@'%'"));
        }

        for (int i = 1; i != 4; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role role" + i, ctx), ctx);
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

        sql = "grant create table on ALL databases to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        sql = "revoke create table on ALL databases from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        sql = "grant impersonate on ALL users to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant create table on ALL database to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("syntax error"));
        }

        sql = "grant create table on ALL tables to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid grant statement with error privilege object"));
        }

        sql = "revoke select on ALL tables IN ALL tables IN all databases from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Getting syntax error at line 1, column 35. " +
                    "Detail message: Unexpected input 'tables', the most similar input is {'DATABASES'}."));
        }

        sql = "revoke select on ALL tables IN ALL tables from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Getting syntax error at line 1, column 35. " +
                    "Detail message: Unexpected input 'tables', the most similar input is {'DATABASES'}"));
        }

        sql = "grant create table on ALL databases in database db1 to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Invalid grant statement with error privilege object"));
        }

        sql = "grant impersonate on ALL users in all databases to test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid grant statement with error privilege object"));
        }

    }

    @Test
    public void testGrantRevokeImpersonate() throws Exception {
        String sql = "grant impersonate on USER root to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant impersonate on USER root to test_user with grant option";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on USER 'root'@'%' from test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "grant impersonate on user 'root'@'%', 'test_user'@'%' to test_user";
        Assert.assertNotNull(UtFrameUtils.parseStmtWithNewParser(sql, ctx));

        sql = "revoke impersonate on user root, test_user from test_user";
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
            Assert.assertTrue(e.getMessage().contains(
                    "Operation not permitted, 'GRANT' cannot be granted to user or role directly"));
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
            Assert.assertTrue(e.getMessage().contains("Invalid grant statement with error privilege object"));
        }

        try {
            UtFrameUtils.parseStmtWithNewParser(
                    "grant alter on resource db.resource to test_user", ctx);
            Assert.fail();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Invalid grant statement with error privilege object"));
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
    public void testViewException() {
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
            Assert.assertTrue(e.getMessage().contains("Invalid grant statement with error privilege object"));
        }

        sql = "revoke select on ALL views IN ALL views IN all databases from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Getting syntax error at line 1, column 34. " +
                    "Detail message: Unexpected input 'views', the most similar input is {'DATABASES'}."));
        }

        sql = "revoke select on ALL views IN ALL views from test_user";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Getting syntax error at line 1, column 34. " +
                    "Detail message: Unexpected input 'views', the most similar input is {'DATABASES'}"));
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
        Assert.assertEquals("GRANT SELECT ON TABLE test.t0 TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "GRANT SELECT on TABLE test.t0, test.t1 to role public";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT ON TABLE test.t0, test.t1 TO ROLE 'public'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));
    }

    @Test
    public void testGrantRevokePriv() {
        String sql = "grant SELECT, INSERT on table test.t0 to role public";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT, INSERT ON TABLE test.t0 TO ROLE 'public'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant ALL PRIVILEGES on table test.t0 to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT DELETE, DROP, INSERT, SELECT, ALTER, EXPORT, UPDATE " +
                "ON TABLE test.t0 TO USER 'test_user'@'%'", AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant SELECT on all tables in all databases to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant SELECT on all tables in all databases to user test_user with grant option";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'test_user'@'%' WITH GRANT OPTION",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant SELECT on all tables in database test to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT SELECT ON ALL TABLES IN DATABASE test TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant ALTER on database test to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT ALTER ON DATABASE test TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant ALTER on all databases to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT ALTER ON ALL DATABASES TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant GRANT on system to user test_user";
        analyzeFail(sql, "Operation not permitted, 'GRANT' cannot be granted to user or role directly");

        sql = "revoke GRANT on system from role root";
        analyzeFail(sql, "Operation not permitted, 'GRANT' cannot be granted to user or role directly");

        sql = "grant NODE on system to user test_user";
        analyzeFail(sql, "Operation not permitted, 'NODE' cannot be granted to user or role directly");

        sql = "revoke NODE on system from role root";
        analyzeFail(sql, "Operation not permitted, 'NODE' cannot be granted to user or role directly");

        sql = "grant CREATE RESOURCE on system to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT CREATE RESOURCE ON SYSTEM TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));

        sql = "grant NODE, CREATE RESOURCE on system to user test_user";
        analyzeFail(sql, "Operation not permitted, 'NODE' cannot be granted to user or role directly");

        sql = "grant IMPERSONATE on user test_user to user test_user";
        grantPrivilegeStmt = (GrantPrivilegeStmt) analyzeSuccess(sql);
        Assert.assertEquals("GRANT IMPERSONATE ON USER 'test_user'@'%' TO USER 'test_user'@'%'",
                AstToSQLBuilder.toSQL(grantPrivilegeStmt));
    }

    @Test
    public void testExistsCheck() throws Exception {
        ConnectContext context = AnalyzeTestUtil.getConnectContext();
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user user_exists", context);
        context.getGlobalStateMgr().getAuthenticationMgr().createUser(createUserStmt);

        analyzeSuccess("create user user_not_exists");
        analyzeSuccess("create user if not exists user_not_exists");
        analyzeFail("create user user_exists", "Operation CREATE USER failed for 'user_exists'@'%' : user already exists");
        analyzeSuccess("create user if not exists user_exists");

        analyzeSuccess("drop user user_exists");
        analyzeSuccess("drop user if exists user_exists");
        analyzeFail("drop user user_not_exists", "Operation DROP USER failed for 'user_not_exists'@'%' : user not exists");
        analyzeSuccess("drop user if exists user_not_exists");

        analyzeSuccess("alter user user_exists identified by 'xxx'");
        analyzeSuccess("alter user if exists user_exists identified by 'xxx'");
        analyzeFail("alter user user_not_exists identified by 'xxx'",
                "Operation ALTER USER failed for 'user_not_exists'@'%' : user not exists");
        analyzeSuccess("alter user if exists user_not_exists identified by 'xxx'");

        context = AnalyzeTestUtil.getConnectContext();
        CreateRoleStmt createRoleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "create role role_exists", context);
        context.getGlobalStateMgr().getAuthorizationMgr().createRole(createRoleStmt);

        analyzeSuccess("create role role_not_exists");
        analyzeSuccess("create role if not exists role_not_exists");
        analyzeFail("create role role_exists", "Operation CREATE ROLE failed for role_exists : role already exists");
        analyzeSuccess("create role if not exists role_exists");

        analyzeSuccess("drop role role_exists");
        analyzeSuccess("drop role if exists role_exists");
        analyzeFail("drop role role_not_exists", "Operation DROP ROLE failed for role_not_exists : role not exists");
        analyzeSuccess("drop role if exists role_not_exists");
    }

    @Test
    public void testGrantFunction() {
        String sql = "GRANT usage ON GLOBAL FUNCTION xxx to user test_user";
        analyzeFail(sql, "syntax error");

        sql = "GRANT usage ON FUNCTION db1.xxx to user test_user";
        analyzeFail(sql, "syntax error");

        sql = "GRANT usage ON GLOBAL FUNCTION TO USER test_user";
        analyzeFail(sql, "syntax error");
    }

    @Test
    public void testErrorParam() {
        analyzeFail("grant SELECT on ALL TABLES to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant SELECT on ALL DATABASES in all databases to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant IMPERSONATE on ALL USERS in all databases to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant USAGE on ALL RESOURCES in all databases to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant USAGE on ALL CATALOGS in all databases to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant USAGE on ALL RESOURCE GROUPS in all databases to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant USAGE on ALL FUNCTIONS to test_user",
                "Invalid grant statement with error privilege object");

        analyzeFail("grant USAGE on ALL GLOBAL FUNCTIONS in all databases to test_user",
                "Invalid grant statement with error privilege object");

    }
}
