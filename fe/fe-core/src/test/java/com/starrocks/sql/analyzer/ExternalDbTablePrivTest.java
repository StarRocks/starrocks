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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class ExternalDbTablePrivTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;

    private void mockHiveMeta() {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(String catalogName, String databaseName) {
                return new Database(112233, databaseName);
            }
        };

        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(String catalogName, String databaseName, String tableName) {
                return new OlapTable(112244, tableName, new ArrayList<>(),
                        null, null, null);
            }
        };
    }


    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        AuthorizationMgr authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();
        ctxToRoot();
        createUsers();
        ConnectorPlanTestBase.mockHiveCatalog(starRocksAssert.getCtx());
    }


    private static void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getQualifiedUser());
    }

    private static void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
    }

    private void grantRevokeSqlAsRoot(String grantSql) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);
        ctxToTestUser();
    }

    private static void verifyGrantRevoke(String sql, String grantSql, String revokeSql,
                                          String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 1. before grant: access denied
        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, ctx);
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        ctxToTestUser();
        PrivilegeCheckerV2.check(statement, ctx);

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx), ctx);

        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private static void createUsers() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
        testUser = createUserStmt.getUserIdentity();
    }

    @Before
    public void setup() throws DdlException {
        mockHiveMeta();
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentCatalog("hive0");
        ctx.setDatabase("tpch");
    }

    @After
    public void teardown() {
        // restore some current infos in context
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        ctx.setDatabase(null);
    }

    public void verifySelect(String grantSql, String revokeSql, String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        // 1. before grant: access denied
        ctxToTestUser();
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx,
                "hive0", "tpch", "region", PrivilegeType.SELECT));

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        ctxToTestUser();
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx,
                "hive0", "tpch", "region", PrivilegeType.SELECT));

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx), ctx);

        ctxToTestUser();
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx,
                "hive0", "tpch", "region", PrivilegeType.SELECT));
    }

    public void verifyDrop(String grantSql, String revokeSql, String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        // 1. before grant: access denied
        ctxToTestUser();
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx,
                "hive0", "tpch", "region", PrivilegeType.DROP));

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        ctxToTestUser();
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx,
                "hive0", "tpch", "region", PrivilegeType.DROP));

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx), ctx);

        ctxToTestUser();
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx,
                "hive0", "tpch", "region", PrivilegeType.DROP));
    }

    @Test
    public void testPrivOnTable() throws Exception {
        // Test select on table
        verifySelect(
                "grant select on table tpch.region to test",
                "revoke select on table tpch.region from test",
                "SELECT command denied to user 'test'@'localhost' for table 'hive0.tpch.region'");

        // Test drop on table
        verifyDrop(
                "grant drop on tpch.region to test",
                "revoke drop on tpch.region from test",
                "DROP command denied to user 'test'@'localhost' for table 'hive0.tpch.region'");
    }

    @Test
    public void testPrivOnDatabase() throws Exception {
        // Test drop on database
        verifyGrantRevoke(
                "drop database tpch",
                "grant drop on database tpch to test",
                "revoke drop on database tpch from test",
                "Access denied for user 'test' to database 'tpch'");

        // Test create_table on database
        verifyGrantRevoke(
                "create table hive0.tpch.test1111 (id int) duplicate key (id) distributed by hash(id)" +
                        " buckets 10 properties(\"replication_num\"=\"1\");",
                "grant CREATE TABLE on database tpch to test",
                "revoke CREATE TABLE on database tpch from test",
                "Access denied for user 'test' to database 'tpch'");
    }

    @Test
    public void testPrivOnExternalCatalog() throws Exception {
        // set catalog xxx: check any action on or in catalog
        verifyGrantRevoke(
                "set catalog hive0",
                "grant select on tpch.region to test",
                "revoke select on tpch.region from test",
                "Access denied for user 'test' to catalog 'hive0'");
    }
}