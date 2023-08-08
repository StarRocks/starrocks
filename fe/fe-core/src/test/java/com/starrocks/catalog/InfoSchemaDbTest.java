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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.GrantsTo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.PrivilegeStmtAnalyzer;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TGetGrantsToRolesOrUserItem;
import com.starrocks.thrift.TGetGrantsToRolesOrUserRequest;
import com.starrocks.thrift.TGetGrantsToRolesOrUserResponse;
import com.starrocks.thrift.TGrantsToType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfoSchemaDbTest {
    ConnectContext ctx;
    GlobalStateMgr globalStateMgr;
    AuthorizationMgr authorizationManager;

    @Before
    public void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        globalStateMgr.getAuthorizationMgr().initBuiltinRolesAndUsers();

        authorizationManager = globalStateMgr.getAuthorizationMgr();

        starRocksAssert.withDatabase("db");
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable("create table db.tbl " + createTblStmtStr);
        starRocksAssert.withView("create view db.v as select * from db.tbl");

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user2", ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);

        CreateRoleStmt createRoleStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "create role test_role", ctx);
        globalStateMgr.getAuthorizationMgr().createRole(createRoleStmt);
    }

    @Test
    public void testNormal() throws IOException {
        Database db = new InfoSchemaDb();

        Assert.assertFalse(db.registerTableUnlocked(null));
        db.dropTable("authors");
        db.dropTableWithLock("authors");
        db.write(null);
        Assert.assertNull(db.getTable("authors"));
    }

    @Test
    public void testInitRole() throws Exception {
        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.ROLE);
        TGetGrantsToRolesOrUserResponse response = GrantsTo.getGrantsTo(request);
        System.out.println(response);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("root");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_type("DATABASE");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());

        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("root");
        item.setObject_type("SYSTEM");
        item.setPrivilege_type("CREATE GLOBAL FUNCTION");
        item.setIs_grantable(false);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());

        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("root");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("v");
        item.setObject_type("VIEW");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());
    }

    @Test
    public void testGrantUserOnDB() throws Exception {
        String sql = "grant DROP on database db to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_type("DATABASE");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke DROP on database db from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());

        sql = "grant drop on all databases to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);
        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_type("DATABASE");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke DROP on all databases from test_user";
        revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());
    }

    @Test
    public void testGrantUserOnTable() throws Exception {
        String sql = "grant select on Table db.tbl to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("tbl");
        item.setObject_type("TABLE");
        item.setPrivilege_type("SELECT");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke select on db.tbl from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());
    }

    @Test
    public void testGrantUserOnView() throws Exception {
        String sql = "grant select on view db.v to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("v");
        item.setObject_type("VIEW");
        item.setPrivilege_type("SELECT");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke select on view db.v from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());

        sql = "grant drop on all views in database db to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);
        item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("v");
        item.setObject_type("VIEW");
        item.setPrivilege_type("DROP");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke DROP on all views in database db from test_user";
        revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());
    }

    @Test
    public void testGrantUserOnUser() throws Exception {
        String sql = "grant impersonate on user test_user2 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_name("'test_user2'@'%'");
        item.setObject_type("USER");
        item.setPrivilege_type("IMPERSONATE");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));

        sql = "revoke impersonate on user test_user2 from test_user";
        RevokePrivilegeStmt revokePrivilegeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.revoke(revokePrivilegeStmt);
        Assert.assertFalse(GrantsTo.getGrantsTo(request).isSetGrants_to());
    }


    @Test
    public void testShowFunctionsWithPriv() throws Exception {
        new MockUp<CreateFunctionStmt>() {
            @Mock
            public void analyze(ConnectContext context) throws AnalysisException {
            }
        };

        new MockUp<PrivilegeStmtAnalyzer>() {
            @Mock
            public void analyze(ConnectContext context) throws AnalysisException {
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

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant usage on function db.my_udf_json_get(int) to test_user", ctx), ctx);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);
        TGetGrantsToRolesOrUserItem item = new TGetGrantsToRolesOrUserItem();
        item.setGrantee("'test_user'@'%'");
        item.setObject_catalog("default_catalog");
        item.setObject_database("db");
        item.setObject_name("my_udf_json_get(INT)");
        item.setObject_type("FUNCTION");
        item.setPrivilege_type("USAGE");
        item.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item));
    }

    @Test
    public void testShowExternalCatalogPrivilege(@Mocked HiveMetaStoreClient metaStoreThriftClient) throws Exception {

        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog_1 COMMENT \"hive_catalog\" PROPERTIES(\"type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\");";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(createCatalog, ctx);
        Assert.assertTrue(stmt instanceof CreateCatalogStmt);
        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        CreateCatalogStmt statement = (CreateCatalogStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);

        new Expectations() {
            {
                metaStoreThriftClient.getAllDatabases();
                result = Lists.newArrayList("db");
                minTimes = 0;

                metaStoreThriftClient.getAllTables("db");
                result = Lists.newArrayList("tbl");
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = ctx.getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb((String) any, (String) any);
                result = new com.starrocks.catalog.Database(0, "db");
                minTimes = 0;

                metadataMgr.getTable((String) any, (String) any, (String) any);
                result = HiveTable.builder().setHiveTableName("tbl")
                        .setFullSchema(Lists.newArrayList(new Column("v1", Type.INT))).build();
                minTimes = 0;
            }
        };

        ctx.getGlobalStateMgr().changeCatalog(ctx, "hive_catalog_1");

        String sql = "grant usage on catalog hive_catalog_1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        sql = "grant all on all databases to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        sql = "grant all on all tables in all databases to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grant(grantStmt);

        TGetGrantsToRolesOrUserRequest request = new TGetGrantsToRolesOrUserRequest();
        request.setType(TGrantsToType.USER);

        TGetGrantsToRolesOrUserItem item1 = new TGetGrantsToRolesOrUserItem();
        item1.setGrantee("'test_user'@'%'");
        item1.setObject_catalog("hive_catalog_1");
        item1.setObject_type("CATALOG");
        item1.setPrivilege_type("USAGE");
        item1.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item1));

        TGetGrantsToRolesOrUserItem item2 = new TGetGrantsToRolesOrUserItem();
        item2.setGrantee("'test_user'@'%'");
        item2.setObject_catalog("hive_catalog_1");
        item2.setObject_database("db");
        item2.setObject_name("tbl");
        item2.setObject_type("TABLE");
        item2.setPrivilege_type("DELETE, DROP, INSERT, SELECT, ALTER, EXPORT, UPDATE");
        item2.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item2));

        TGetGrantsToRolesOrUserItem item3 = new TGetGrantsToRolesOrUserItem();
        item3.setGrantee("'test_user'@'%'");
        item3.setObject_catalog("hive_catalog_1");
        item3.setObject_database("db");
        item3.setObject_type("DATABASE");
        item3.setPrivilege_type("CREATE TABLE, DROP, ALTER, CREATE VIEW, CREATE FUNCTION, CREATE MATERIALIZED VIEW");
        item3.setIs_grantable(false);
        Assert.assertTrue(GrantsTo.getGrantsTo(request).grants_to.contains(item3));

        Config.enable_show_external_catalog_privilege = false;
        if (GrantsTo.getGrantsTo(request).grants_to != null) {
            Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item1));
            Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item2));
            Assert.assertFalse(GrantsTo.getGrantsTo(request).grants_to.contains(item3));
        }
        Config.enable_show_external_catalog_privilege = true;
    }

    @Test
    public void testRoot() {
        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        Map<ObjectType, List<PrivilegeEntry>> privileges =
                authorizationManager.getTypeToPrivilegeEntryListByRole("root");
        Set<TGetGrantsToRolesOrUserItem> s = Deencapsulation.invoke(GrantsTo.class, "getGrantItems",
                authorizationMgr, "root", privileges);
    }
}