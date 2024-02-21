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
package com.starrocks.privilege;

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class InvalidateObjectTest {
    private ConnectContext ctx;
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME_0 = "tbl0";
    private static final String TABLE_NAME_1 = "tbl1";
    private UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
    private long publicRoleId;

    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        // create db.tbl0 ~ tbl3
        String createTblStmtStr = "(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withDatabase(DB_NAME);
        starRocksAssert.withDatabase(DB_NAME + "1");
        for (int i = 0; i < 4; ++i) {
            starRocksAssert.withTable("create table db.tbl" + i + createTblStmtStr);
        }
        // create view
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create view db.view1 as select * from db.tbl1", ctx), ctx);
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        globalStateMgr.getAuthorizationMgr().initBuiltinRolesAndUsers();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        String createResourceStmt = "create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")";
        starRocksAssert.withResource(createResourceStmt);

        // create mv
        starRocksAssert.withDatabase("db3");
        createMvForTest(starRocksAssert, starRocksAssert.getCtx());

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);
        publicRoleId = globalStateMgr.getAuthorizationMgr().getRoleIdByNameNoLock("public");

        GlobalVariable.setActivateAllRolesOnLogin(true);
    }

    @After
    public void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    private void setCurrentUserAndRoles(ConnectContext ctx, UserIdentity userIdentity) {
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(userIdentity);
    }

    private static void createMaterializedView(String sql, ConnectContext connectContext) throws Exception {
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
    }

    private static void createMvForTest(StarRocksAssert starRocksAssert,
                                        ConnectContext connectContext) throws Exception {
        starRocksAssert.withTable("CREATE TABLE db3.tbl1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view db3.mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from db3.tbl1;";
        createMaterializedView(sql, connectContext);
    }

    @Test
    public void testRemoveInvalidateObject() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        // 1. add validate entry: select on db.tbl1 to test_user
        String sql = "grant select on db.tbl1 to test_user";
        GrantPrivilegeStmt grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        Assert.assertEquals(1, grantTableStmt.getObjectList().size());
        TablePEntryObject goodTableObject = (TablePEntryObject) grantTableStmt.getObjectList().get(0);
        // 2. add validate entry: create table + drop on db to test_user
        sql = "grant create table, drop on DATABASE db to test_user";
        GrantPrivilegeStmt grantDbStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantDbStmt);
        List<PEntryObject> objects = Arrays.asList(goodTableObject);
        // 3. add invalidate entry: select on invalidatedb.table
        objects = Arrays.asList(new TablePEntryObject("-1", goodTableObject.tableUUID));
        manager.grantToUser(grantTableStmt.getObjectType(), grantTableStmt.getPrivilegeTypes(), objects, false, testUser);
        // 4. add invalidate entry: select on db.invalidatetable
        objects = Arrays.asList(new TablePEntryObject(goodTableObject.databaseUUID, "-1"));
        manager.grantToUser(grantTableStmt.getObjectType(), grantTableStmt.getPrivilegeTypes(), objects, false, testUser);
        // 5. add invalidate entry: create table, drop on invalidatedb
        objects = Arrays.asList(new DbPEntryObject("-1"));
        manager.grantToUser(grantDbStmt.getObjectType(), grantDbStmt.getPrivilegeTypes(), objects, false, testUser);
        // 6. add valid entry: ALL databases
        objects = Arrays.asList(new DbPEntryObject(PrivilegeBuiltinConstants.ALL_DATABASES_UUID));
        manager.grantToUser(grantDbStmt.getObjectType(), grantDbStmt.getPrivilegeTypes(), objects, false, testUser);
        // 7. add valid user
        sql = "grant impersonate on USER root to test_user";
        GrantPrivilegeStmt grantUserStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantUserStmt);
        // 8. add invalidate entry: bad impersonate user
        objects = Arrays.asList(new UserPEntryObject(UserIdentity.createAnalyzedUserIdentWithIp("bad", "%")));
        manager.grantToUser(grantUserStmt.getObjectType(), grantUserStmt.getPrivilegeTypes(), objects, false, testUser);
        // 9. add valid resource
        sql = "grant usage on resource 'hive0' to test_user";
        GrantPrivilegeStmt grantResourceStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantResourceStmt);
        // 10. add invalidate entry: bad resource name
        objects = Arrays.asList(new ResourcePEntryObject("bad_resource"));
        manager.grantToUser(grantResourceStmt.getObjectType(), grantResourceStmt.getPrivilegeTypes(), objects, false,
                testUser);

        // check before clean up:
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        int numTablePEntries = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getObjectType()).size();
        int numDbPEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getObjectType()).size();
        int numUserPEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantUserStmt.getObjectType()).size();
        int numResourcePEntires = manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantResourceStmt.getObjectType()).size();

        manager.removeInvalidObject();

        // check after clean up
        System.out.println(GsonUtils.GSON.toJson(manager.userToPrivilegeCollection));
        Assert.assertEquals(numTablePEntries - 2, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantTableStmt.getObjectType()).size());
        Assert.assertEquals(numDbPEntires - 1, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantDbStmt.getObjectType()).size());
        Assert.assertEquals(numUserPEntires - 1, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantUserStmt.getObjectType()).size());
        Assert.assertEquals(numResourcePEntires - 1, manager.userToPrivilegeCollection.get(testUser).
                typeToPrivilegeEntryList.get(grantResourceStmt.getObjectType()).size());
    }
}
