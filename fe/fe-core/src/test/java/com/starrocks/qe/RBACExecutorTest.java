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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AuthorizationAnalyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.CreateFunctionAnalyzer;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

public class RBACExecutorTest {
    private ConnectContext ctx;
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME_0 = "tbl0";
    private static final String TABLE_NAME_1 = "tbl1";

    @BeforeEach
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

        GlobalStateMgr.getCurrentState()
                .setAuthorizationMgr(new AuthorizationMgr(new DefaultAuthorizationProvider()));
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(new AuthenticationMgr());

        for (int i = 0; i < 5; i++) {
            String sql = "create user u" + i;
            CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);
        }
        for (int i = 0; i < 5; i++) {
            String sql = "create role r" + i;
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            DDLStmtExecutor.execute(stmt, ctx);
        }
    }

    @Test
    public void testShowRoles() throws Exception {
        String sql = "create role test_role comment \"yi shan yi shan, liang jing jing\"";
        StatementBase createRoleStmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(createRoleStmt, ctx);

        ShowRolesStmt stmt = new ShowRolesStmt();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        String resultString = resultSet.getResultRows().toString();
        // sampling test a some of the result rows
        Assertions.assertTrue(
                resultString.contains("[root, true, built-in root role which has all privileges on all objects]"));
        Assertions.assertTrue(
                resultString.contains("[db_admin, true, built-in database administration role]"));
        Assertions.assertTrue(
                resultString.contains("[test_role, false, yi shan yi shan, liang jing jing]"));

        // clean
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role test_role", ctx), ctx);
    }

    @Test
    public void testShowUsers() throws Exception {
        ShowUserStmt stmt = new ShowUserStmt(true);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[['u3'@'%'], ['root'@'%'], ['u2'@'%'], ['u4'@'%'], ['u1'@'%'], ['u0'@'%']]",
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

        Long roleId1 = ctx.getGlobalStateMgr().getAuthorizationMgr().getRoleIdByNameAllowNull("drop_role1");
        Long roleId2 = ctx.getGlobalStateMgr().getAuthorizationMgr().getRoleIdByNameAllowNull("drop_role2");
        HashSet roleIds = new HashSet<>();
        roleIds.add(roleId1);
        roleIds.add(roleId2);
        ctx.setCurrentRoleIds(roleIds);

        sql = "select current_role()";
        QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        InformationFunction e = (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assertions.assertTrue(e.getStrValue().contains("drop_role2") && e.getStrValue().contains("drop_role1"));

        sql = "drop role drop_role1";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        sql = "select current_role()";
        queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        e = (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assertions.assertEquals("drop_role2", e.getStrValue());

        sql = "drop role drop_role2";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);

        sql = "select current_role()";
        queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        e = (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assertions.assertEquals("NONE", e.getStrValue());
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
        Authorizer.checkTableAction(ctx,
                "db", "tbl0", PrivilegeType.SELECT);
        Authorizer.checkTableAction(ctx,
                "db", "tbl1", PrivilegeType.SELECT);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke r2 from role r1", ctx), ctx);
        Authorizer.checkTableAction(ctx,
                "db", "tbl0", PrivilegeType.SELECT);
        Assertions.assertThrows(AccessDeniedException.class, () ->
                Authorizer.checkTableAction(ctx,
                        "db", "tbl1", PrivilegeType.SELECT));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke r1 from u1", ctx), ctx);
        Assertions.assertThrows(AccessDeniedException.class, () ->
                Authorizer.checkTableAction(ctx,
                        "db", "tbl0", PrivilegeType.SELECT));
        Assertions.assertThrows(AccessDeniedException.class, () ->
                Authorizer.checkTableAction(ctx,
                        "db", "tbl1", PrivilegeType.SELECT));
    }

    @Test
    public void testShowFunctionsWithPriv() throws Exception {
        new MockUp<AuthorizationAnalyzer>() {
            @Mock
            public void analyze(ConnectContext context) throws AnalysisException {
            }
        };

        new MockUp<CreateFunctionAnalyzer>() {
            @Mock
            public void analyze(CreateFunctionStmt stmt, ConnectContext context) {

            }
        };

        String createSql = "CREATE FUNCTION db.MY_UDF_JSON_GET(string, string) RETURNS string " +
                "properties ( " +
                "'symbol' = 'com.starrocks.udf.sample.UDFSplit', 'object_file' = 'test' " +
                ")";

        CreateFunctionStmt statement = (CreateFunctionStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);

        Type[] arg = new Type[1];
        arg[0] = Type.INT;
        Function function = ScalarFunction.createUdf(new FunctionName("db", "MY_UDF_JSON_GET"), arg, Type.INT,
                false, TFunctionBinaryType.SRJAR,
                "objectFile", "mainClass.getCanonicalName()", "", "");
        function.setChecksum("checksum");

        statement.setFunction(function);
        DDLStmtExecutor.execute(statement, ctx);

        ShowFunctionsStmt stmt = new ShowFunctionsStmt("db", false, false, false, null, null);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[my_udf_json_get]]", resultSet.getResultRows().toString());

        ctx.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        stmt = new ShowFunctionsStmt("db", false, false, false, null, null);
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[]", resultSet.getResultRows().toString());

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant usage on function db.my_udf_json_get(int) to u1", ctx), ctx);
        stmt = new ShowFunctionsStmt("db", false, false, false, null, null);
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("[[my_udf_json_get]]", resultSet.getResultRows().toString());

        stmt = new ShowFunctionsStmt("db", true, false, false, null, null);
        resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.getResultRows().size() > 0);
    }
}
