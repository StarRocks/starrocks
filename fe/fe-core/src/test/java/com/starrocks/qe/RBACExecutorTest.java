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
package com.starrocks.qe;

import com.starrocks.analysis.InformationFunction;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

public class RBACExecutorTest {
    private ConnectContext ctx;
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME_0 = "tbl0";
    private static final String TABLE_NAME_1 = "tbl1";

    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.createMinStarRocksCluster();

        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        // create db.tbl0 ~ tbl3
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withDatabase(DB_NAME);
        starRocksAssert.withDatabase(DB_NAME + "1");
        for (int i = 0; i < 4; ++i) {
            starRocksAssert.withTable("create table db.tbl" + i + createTblStmtStr);
        }

        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();

        for (int i = 0; i < 5; i++) {
            String sql = "create user u" + i;
            CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            globalStateMgr.getAuthenticationManager().createUser(createUserStmt);
        }
        for (int i = 0; i < 5; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }
    }

    @Test
    public void testShowGrants() throws Exception {
        String sql = "grant all on CATALOG default_catalog to u1";
        GrantPrivilegeStmt grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        ShowGrantsStmt stmt = new ShowGrantsStmt(new UserIdentity("u1", "%"));
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("[['u1'@'%', default_catalog, GRANT USAGE, CREATE_DATABASE, DROP, ALTER " +
                "ON CATALOG default_catalog TO USER 'u1'@'%']]", resultSet.getResultRows().toString());

        sql = "grant all on CATALOG default_catalog to role r1";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        stmt = new ShowGrantsStmt("r1");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();
        Assert.assertEquals("[[r1, default_catalog, GRANT USAGE, CREATE_DATABASE, DROP, ALTER " +
                "ON CATALOG default_catalog TO ROLE 'r1']]", resultSet.getResultRows().toString());

        sql = "grant r1 to role r0";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantRoleStmt, ctx);

        sql = "grant r2 to role r0";
        grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantRoleStmt, ctx);

        sql = "grant SELECT on TABLE db.tbl0 to role r0";
        grantPrivilegeStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(grantPrivilegeStmt, ctx);

        stmt = new ShowGrantsStmt("r0");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();
        Assert.assertEquals("[[r0, null, GRANT 'r1', 'r2' TO  ROLE r0]," +
                " [r0, default_catalog, GRANT SELECT ON TABLE db.tbl0 TO ROLE 'r0']]",
                resultSet.getResultRows().toString());
    }

    @Test
    public void testShowRoles() throws Exception {
        ShowRolesStmt stmt = new ShowRolesStmt();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("[[root], [db_admin], [cluster_admin], [user_admin], [public], " +
                "[r0], [r1], [r2], [r3], [r4]]", resultSet.getResultRows().toString());
    }

    @Test
    public void testShowUsers() throws Exception {
        ShowUserStmt stmt = new ShowUserStmt(true);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("[['u3'@'%'], ['root'@'%'], ['u2'@'%'], ['u4'@'%'], ['u1'@'%'], ['u0'@'%']]",
                resultSet.getResultRows().toString());
    }

    @Test
    public void testCurrentRole() throws Exception {
        String sql = "create role drop_role1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        sql = "create role drop_role2";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        Long roleId1 = ctx.getGlobalStateMgr().getPrivilegeManager().getRoleIdByNameAllowNull("drop_role1");
        Long roleId2 = ctx.getGlobalStateMgr().getPrivilegeManager().getRoleIdByNameAllowNull("drop_role2");
        HashSet roleIds = new HashSet<>();
        roleIds.add(roleId1);
        roleIds.add(roleId2);
        ctx.setCurrentRoleIds(roleIds);

        sql = "select current_role()";
        QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        InformationFunction e = (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assert.assertTrue(e.getStrValue().contains("drop_role2") && e.getStrValue().contains("drop_role1"));

        sql = "drop role drop_role1";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        sql = "select current_role()";
        queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        e = (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assert.assertEquals("drop_role2", e.getStrValue());

        sql = "drop role drop_role2";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        sql = "select current_role()";
        queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        e = (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assert.assertEquals("NONE", e.getStrValue());
    }

    @Test
    public void testRevokeDefaultRole() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on db.tbl0 to role r1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant r1 to u1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on db.tbl1 to role r2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant r2 to role r1", ctx), ctx);

        SetDefaultRoleExecutor.execute((SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set default role r1 to u1", ctx), ctx);

        ctx.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        SetRoleExecutor.execute((SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role r1", ctx), ctx);
        Assert.assertTrue(PrivilegeManager.checkTableAction(ctx, "db", "tbl0", PrivilegeType.SELECT));
        Assert.assertTrue(PrivilegeManager.checkTableAction(ctx, "db", "tbl1", PrivilegeType.SELECT));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke r2 from role r1", ctx), ctx);
        Assert.assertTrue(PrivilegeManager.checkTableAction(ctx, "db", "tbl0", PrivilegeType.SELECT));
        Assert.assertFalse(PrivilegeManager.checkTableAction(ctx, "db", "tbl1", PrivilegeType.SELECT));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke r1 from u1", ctx), ctx);
        Assert.assertFalse(PrivilegeManager.checkTableAction(ctx, "db", "tbl0", PrivilegeType.SELECT));
        Assert.assertFalse(PrivilegeManager.checkTableAction(ctx, "db", "tbl1", PrivilegeType.SELECT));
    }
}
