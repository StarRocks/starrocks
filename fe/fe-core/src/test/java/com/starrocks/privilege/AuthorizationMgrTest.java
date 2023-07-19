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

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SetRoleExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class AuthorizationMgrTest {
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

    private static void createMaterializedView(String sql, ConnectContext connectContext) throws Exception {
        Config.enable_experimental_mv = true;
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStatement);
    }

    @After
    public void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    private void setCurrentUserAndRoles(ConnectContext ctx, UserIdentity userIdentity) {
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setCurrentRoleIds(userIdentity);
    }

    @Test
    public void testTable() throws Exception {
        setCurrentUserAndRoles(ctx, testUser);
        ctx.setQualifiedUser(testUser.getQualifiedUser());

        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));

        String sql = "grant select on table db.tbl1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);
        sql = "grant ALTER on materialized view db3.mv1 to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));
        Assert.assertTrue(PrivilegeActions.checkActionInDb(ctx, DB_NAME, PrivilegeType.SELECT));
        Assert.assertFalse(PrivilegeActions.checkActionInDb(ctx, DB_NAME, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkActionInDb(ctx, "db3", PrivilegeType.ALTER));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);
        sql = "revoke ALTER on materialized view db3.mv1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));

        // grant many priveleges
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "grant select, insert, delete on table db.tbl1 to test_user with grant option";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertTrue(grantStmt.isWithGrantOption());
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.INSERT));
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DELETE));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));

        // revoke only select
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.INSERT));
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DELETE));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));

        // revoke all
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke ALL on table db.tbl1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.INSERT));
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DELETE));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnTable(ctx, DB_NAME, TABLE_NAME_1));

        // grant view
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "grant alter on view db.view1 to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
        Assert.assertTrue(PrivilegeActions.checkActionInDb(ctx, DB_NAME, PrivilegeType.ALTER));
        Assert.assertFalse(PrivilegeActions.checkActionInDb(ctx, DB_NAME, PrivilegeType.SELECT));

        // revoke view
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke ALL on view db.view1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));
    }

    @Test
    public void testPersist() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        AuthorizationMgr masterManager = masterGlobalStateMgr.getAuthorizationMgr();

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(emptyImage.getDataOutputStream());

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        String sql = "grant select on db.tbl1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

        UtFrameUtils.PseudoImage grantImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(grantImage.getDataOutputStream());

        sql = "revoke select on db.tbl1 from test_user";
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.revoke(revokeStmt);
        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        UtFrameUtils.PseudoImage revokeImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(revokeImage.getDataOutputStream());

        // start to replay
        masterGlobalStateMgr.loadRBACPrivilege(emptyImage.getDataInputStream());
        AuthorizationMgr followerManager = masterGlobalStateMgr.getAuthorizationMgr();

        UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

        info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

        // check image
        masterGlobalStateMgr.loadRBACPrivilege(grantImage.getDataInputStream());
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

        masterGlobalStateMgr.loadRBACPrivilege(revokeImage.getDataInputStream());
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
    }

    @Test
    public void testRole() throws Exception {
        String sql = "create role test_role";
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertFalse(manager.checkRoleExists("test_role"));

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertTrue(manager.checkRoleExists("test_role"));

        // not check create twice
        DDLStmtExecutor.execute(stmt, ctx);

        sql = "drop role test_role";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertFalse(manager.checkRoleExists("test_role"));

        // not check drop twice
        DDLStmtExecutor.execute(stmt, ctx);

        // test create role with comment
        sql = "create role test_role2 comment \"yi shan yi shan, liang jing jing\"";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        System.out.println(manager.getRoleComment("test_role2"));
        Assert.assertTrue(manager.getRoleComment("test_role2").contains("yi shan yi shan, liang jing jing"));

        // test alter role comment
        sql = "alter role test_role2 set comment=\"man tian dou shi xiao xing xing\"";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        Assert.assertTrue(manager.getRoleComment("test_role2").contains("man tian dou shi xiao xing xing"));

        // test alter immutable role comment, then throw exception
        sql = "alter role root set comment=\"gua zai tian shang fang guang ming\"";
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("root is immutable"));
        }

        // clean
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role test_role2", ctx), ctx);
    }

    // used in testPersistRole
    private void assertTableSelectOnTest(AuthorizationMgr manager, boolean canSelectTbl0, boolean canSelectTbl1)
            throws PrivilegeException {
        setCurrentUserAndRoles(ctx, testUser);
        ;
        if (canSelectTbl0) {
            Assert.assertTrue(PrivilegeActions.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_0, PrivilegeType.SELECT));
        } else {
            Assert.assertFalse(PrivilegeActions.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_0, PrivilegeType.SELECT));
        }

        if (canSelectTbl1) {
            Assert.assertTrue(PrivilegeActions.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        } else {
            Assert.assertFalse(PrivilegeActions.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        }
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
    }

    @Test
    public void testPersistRole() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        AuthorizationMgr masterManager = masterGlobalStateMgr.getAuthorizationMgr();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(emptyImage.getDataOutputStream());
        Assert.assertFalse(masterManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(masterManager.checkRoleExists("test_persist_role1"));

        // 1. create 2 roles
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        for (int i = 0; i != 2; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "create role test_persist_role" + i, ctx), ctx);
            Assert.assertTrue(masterManager.checkRoleExists("test_persist_role" + i));
        }
        UtFrameUtils.PseudoImage createRoleImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(createRoleImage.getDataOutputStream());

        // 2. grant select on db.tbl<i> to role
        for (int i = 0; i != 2; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_persist_role" + i, ctx), ctx);
        }
        UtFrameUtils.PseudoImage grantPrivsToRoleImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(grantPrivsToRoleImage.getDataOutputStream());
        assertTableSelectOnTest(masterManager, false, false);

        // 3. grant test_persist_role0 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_persist_role0 to test_user", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, false);
        UtFrameUtils.PseudoImage grantRoleToUserImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(grantRoleToUserImage.getDataOutputStream());

        // 4. grant test_persist_role1 to role test_persist_role0
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_persist_role1 to role test_persist_role0", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, true);
        UtFrameUtils.PseudoImage grantRoleToRoleImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(grantRoleToRoleImage.getDataOutputStream());

        // 5. revoke test_persist_role1 from role test_persist_role0
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke test_persist_role1 from role test_persist_role0", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, false);
        UtFrameUtils.PseudoImage revokeRoleFromRoleImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(revokeRoleFromRoleImage.getDataOutputStream());

        // 6. revoke test_persist_role0 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke test_persist_role0 from test_user", ctx), ctx);
        assertTableSelectOnTest(masterManager, false, false);
        UtFrameUtils.PseudoImage revokeRoleFromUserImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(revokeRoleFromUserImage.getDataOutputStream());

        // 7. drop 2 roles
        for (int i = 0; i != 2; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "drop role test_persist_role" + i, ctx), ctx);
            Assert.assertFalse(masterManager.checkRoleExists("test_persist_role" + i));
        }
        UtFrameUtils.PseudoImage dropRoleImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.saveRBACPrivilege(dropRoleImage.getDataOutputStream());

        //
        // start to replay
        //
        masterGlobalStateMgr.loadRBACPrivilege(emptyImage.getDataInputStream());
        AuthorizationMgr followerManager = masterGlobalStateMgr.getAuthorizationMgr();
        Assert.assertFalse(followerManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(followerManager.checkRoleExists("test_persist_role1"));

        // 1. replay create 2 roles
        for (int i = 0; i != 2; ++i) {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
            Assert.assertTrue(followerManager.checkRoleExists("test_persist_role" + i));
        }
        // 2. replay grant insert on db.tbl<i> to role
        for (int i = 0; i != 2; ++i) {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
        }
        // 3. replay grant test_persist_role0 to test_user
        assertTableSelectOnTest(followerManager, false, false);
        {
            UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
            followerManager.replayUpdateUserPrivilegeCollection(
                    info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        }
        assertTableSelectOnTest(followerManager, true, false);

        // 4. replay grant test_persist_role1 to role test_persist_role0
        {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
        }
        assertTableSelectOnTest(followerManager, true, true);

        // 5. replay revoke test_persist_role1 from role test_persist_role0
        {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
            followerManager.replayUpdateRolePrivilegeCollection(info);
        }
        assertTableSelectOnTest(followerManager, true, false);

        // 6. replay revoke test_persist_role0 from test_user
        {
            UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
            followerManager.replayUpdateUserPrivilegeCollection(
                    info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        }
        assertTableSelectOnTest(followerManager, false, false);

        // 7. replay drop 2 roles
        for (int i = 0; i != 2; ++i) {
            RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo)
                    UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_ROLE_V2);
            followerManager.replayDropRole(info);
            Assert.assertFalse(followerManager.checkRoleExists("test_persist_role" + i));
        }

        //
        // check image
        //
        // 1. check image after create role
        masterGlobalStateMgr.loadRBACPrivilege(createRoleImage.getDataInputStream());
        AuthorizationMgr imageManager = masterGlobalStateMgr.getAuthorizationMgr();

        Assert.assertTrue(imageManager.checkRoleExists("test_persist_role0"));
        Assert.assertTrue(imageManager.checkRoleExists("test_persist_role1"));

        // 2. check image after grant select on db.tbl<i> to role
        masterGlobalStateMgr.loadRBACPrivilege(grantPrivsToRoleImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, false, false);

        // 3. check image after grant test_persist_role0 to test_user
        masterGlobalStateMgr.loadRBACPrivilege(
                grantRoleToUserImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, true, false);

        // 4. check image after grant test_persist_role1 to role test_persist_role0
        masterGlobalStateMgr.loadRBACPrivilege(
                grantRoleToRoleImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, true, true);

        // 5. check image after revoke test_persist_role1 from role test_persist_role0
        masterGlobalStateMgr.loadRBACPrivilege(
                revokeRoleFromRoleImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, true, false);

        // 6. check image after revoke test_persist_role0 from test_user
        masterGlobalStateMgr.loadRBACPrivilege(
                revokeRoleFromUserImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, false, false);

        // 7. check image after drop 2 roles
        masterGlobalStateMgr.loadRBACPrivilege(
                dropRoleImage.getDataInputStream());
        imageManager = masterGlobalStateMgr.getAuthorizationMgr();
        Assert.assertFalse(imageManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(imageManager.checkRoleExists("test_persist_role1"));
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
        manager.grantToUser(grantResourceStmt.getObjectType(), grantResourceStmt.getPrivilegeTypes(), objects, false, testUser);

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

    @Test
    public void testGrantAll() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        setCurrentUserAndRoles(ctx, testUser);
        ;
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertFalse(PrivilegeActions.checkTableAction(
                ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

        List<List<String>> sqls = Arrays.asList(
                Arrays.asList(
                        "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO test_user",
                        "REVOKE SELECT ON ALL TABLES IN ALL DATABASES FROM test_user"),
                Arrays.asList(
                        "GRANT SELECT ON ALL TABLES IN DATABASE db TO test_user",
                        "REVOKE SELECT ON ALL TABLES IN DATABASE db FROM test_user")
        );
        for (List<String> sqlPair : sqls) {
            setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
            GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sqlPair.get(0), ctx);
            manager.grant(grantStmt);
            setCurrentUserAndRoles(ctx, testUser);
            ;
            Assert.assertTrue(PrivilegeActions.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

            setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
            RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sqlPair.get(1), ctx);
            manager.revoke(revokeStmt);
            setCurrentUserAndRoles(ctx, testUser);
            ;
            Assert.assertFalse(PrivilegeActions.checkTableAction(
                    ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        }

        // on all databases
        Assert.assertFalse(PrivilegeActions.checkDbAction(
                ctx, DB_NAME, PrivilegeType.CREATE_TABLE));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT create table ON ALL DATABASES TO test_user", ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(PrivilegeActions.checkDbAction(
                ctx, DB_NAME, PrivilegeType.CREATE_TABLE));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE create table ON ALL DATABASES FROM test_user", ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(PrivilegeActions.checkDbAction(
                ctx, DB_NAME, PrivilegeType.CREATE_TABLE));

        // on all users
        AuthorizationMgr authorizationManager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertFalse(authorizationManager.canExecuteAs(ctx, UserIdentity.ROOT));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON ALL USERS TO test_user", ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(authorizationManager.canExecuteAs(ctx, UserIdentity.ROOT));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE IMPERSONATE ON ALL USERS FROM test_user", ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(authorizationManager.canExecuteAs(ctx, UserIdentity.ROOT));
    }

    @Test
    public void testImpersonate() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(manager.canExecuteAs(ctx, UserIdentity.ROOT));

        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON USER root, test_user TO test_user", ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertTrue(manager.canExecuteAs(ctx, UserIdentity.ROOT));

        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE IMPERSONATE ON USER root FROM test_user", ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertFalse(manager.canExecuteAs(ctx, UserIdentity.ROOT));
    }

    @Test
    public void testShowDbsTables() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_table_priv", ctx);
        ctx.getGlobalStateMgr().getAuthenticationMgr().createUser(createUserStmt);
        String sql = "grant select on db.tbl1 to user_with_table_priv";
        GrantPrivilegeStmt grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        UserIdentity userWithTablePriv = UserIdentity.createAnalyzedUserIdentWithIp("user_with_table_priv", "%");

        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_db_priv", ctx);
        ctx.getGlobalStateMgr().getAuthenticationMgr().createUser(createUserStmt);
        sql = "grant drop on DATABASE db to user_with_db_priv";
        grantTableStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantTableStmt);
        UserIdentity userWithDbPriv = UserIdentity.createAnalyzedUserIdentWithIp("user_with_db_priv", "%");


        // show tables in db:
        // root can see two tables + 1 views
        ShowTableStmt showTableStmt = new ShowTableStmt("db", false, null);
        ShowExecutor executor = new ShowExecutor(ctx, showTableStmt);
        ShowResultSet resultSet = executor.execute();
        Set<String> allTables = resultSet.getResultRows().stream().map(k -> k.get(0)).collect(Collectors.toSet());
        Assert.assertEquals(new HashSet<>(Arrays.asList("tbl0", "tbl1", "tbl2", "tbl3", "view1")), allTables);

        // user with table priv can only see tbl1
        setCurrentUserAndRoles(ctx, userWithTablePriv);
        executor = new ShowExecutor(ctx, showTableStmt);
        resultSet = executor.execute();
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("tbl1", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // show databases
        ShowDbStmt showDbStmt = new ShowDbStmt(null);

        // root can see db && db1
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        executor = new ShowExecutor(ctx, showDbStmt);
        resultSet = executor.execute();
        Set<String> set = new HashSet<>();
        while (resultSet.next()) {
            set.add(resultSet.getString(0));
        }
        Assert.assertTrue(set.contains("db"));
        Assert.assertTrue(set.contains("db1"));

        // user with table priv can only see db
        setCurrentUserAndRoles(ctx, userWithTablePriv);
        executor = new ShowExecutor(ctx, showDbStmt);
        resultSet = executor.execute();
        set.clear();
        while (resultSet.next()) {
            set.add(resultSet.getString(0));
        }
        Assert.assertTrue(set.contains("db"));
        Assert.assertFalse(set.contains("db1"));

        // user with table priv can only see db
        setCurrentUserAndRoles(ctx, userWithDbPriv);
        executor = new ShowExecutor(ctx, showDbStmt);
        resultSet = executor.execute();
        set.clear();
        while (resultSet.next()) {
            set.add(resultSet.getString(0));
        }
        Assert.assertTrue(set.contains("db"));
        Assert.assertFalse(set.contains("db1"));
    }

    private void assertDbActionsOnTest(boolean canCreateTable, boolean canDrop, UserIdentity testUser) {
        setCurrentUserAndRoles(ctx, testUser);
        ;
        if (canCreateTable) {
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db", PrivilegeType.CREATE_TABLE));
        } else {
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db", PrivilegeType.CREATE_TABLE));
        }

        if (canDrop) {
            Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, "db", PrivilegeType.DROP));
        } else {
            Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, "db", PrivilegeType.DROP));
        }
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
    }

    @Test
    public void testGrantRoleToUser() throws Exception {
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_role_user", ctx);
        ctx.getGlobalStateMgr().getAuthenticationMgr().createUser(createUserStmt);
        UserIdentity testUser = createUserStmt.getUserIdentity();
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);

        // grant create table on database db to role1
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role role1;", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create table on database db to role role1;", ctx), ctx);

        // can't create table
        assertDbActionsOnTest(false, false, testUser);

        // grant role1 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to test_role_user", ctx), ctx);

        // can create table but can't drop
        assertDbActionsOnTest(true, false, testUser);

        // grant role2 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role role2;", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on database db to role role2;", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role2 to test_role_user;", ctx), ctx);

        // can create table & drop
        assertDbActionsOnTest(true, true, testUser);

        // grant drop to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on database db to test_role_user;", ctx), ctx);

        // still, can create table & drop
        assertDbActionsOnTest(true, true, testUser);

        // revoke role1 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role1 from test_role_user;", ctx), ctx);

        // can drop but can't create table
        assertDbActionsOnTest(false, true, testUser);

        // grant role1 to test_user; revoke create table from role1
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to test_role_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke create table on database db from role role1", ctx), ctx);

        // can drop but can't create table
        assertDbActionsOnTest(false, true, testUser);

        // revoke empty role role1
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role1 from test_role_user;", ctx), ctx);

        // can drop but can't create table
        assertDbActionsOnTest(false, true, testUser);

        // revoke role2 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role2 from test_role_user;", ctx), ctx);

        // can drop
        assertDbActionsOnTest(false, true, testUser);

        // grant role2 to test_user; revoke drop from role2
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role2 to test_role_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke drop on database db from role role2", ctx), ctx);

        // can drop
        assertDbActionsOnTest(false, true, testUser);

        // grant drop on role2; revoke drop from user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on database db to role role2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke drop on database db from test_role_user", ctx), ctx);

        assertDbActionsOnTest(false, true, testUser);

        // revoke role2 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role2 from test_role_user;", ctx), ctx);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1, role2 to test_role_user;", ctx), ctx);

        List<String> expected = Arrays.asList("role1", "role2");
        expected.sort(null);
        List<String> result = manager.getRoleNamesByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_role_user", "%"));
        result.sort(null);
        Assert.assertEquals(expected, result);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke role1, role2 from test_role_user;", ctx), ctx);

        Assert.assertEquals("[]", manager.getRoleNamesByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_role_user", "%")).toString());

        // can't drop
        assertDbActionsOnTest(false, false, testUser);
    }

    @Test
    public void testRoleInheritanceDepth() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        // create role0 ~ role4
        long[] roleIds = new long[5];
        for (int i = 0; i != 5; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role role%d;", i), ctx), ctx);
            roleIds[i] = manager.getRoleIdByNameNoLock("role" + i);
        }
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_role_inheritance"), ctx), ctx);

        // role0 -> role1 -> role2
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role0 to role role1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to role role2", ctx), ctx);

        // role inheritance depth
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[2]));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        Assert.assertEquals(2, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        // role predecessor
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])),
                manager.getAllPredecessorsUnlocked(roleIds[0]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1])),
                manager.getAllPredecessorsUnlocked(roleIds[1]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2])),
                manager.getAllPredecessorsUnlocked(roleIds[2]));
        // role descendants
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2])),
                manager.getAllDescendantsUnlocked(roleIds[0]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1], roleIds[2])),
                manager.getAllDescendantsUnlocked(roleIds[1]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[2])),
                manager.getAllDescendantsUnlocked(roleIds[2]));

        // role0 -> role1 -> role2
        //    \       ^
        //     \      |
        //      \-> role3
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role0 to role role3", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role3 to role role1", ctx), ctx);
        // role inheritance depth
        Assert.assertEquals(3, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[2]));
        Assert.assertEquals(2, manager.getMaxRoleInheritanceDepthInner(0, roleIds[3]));
        // role predecessor
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])),
                manager.getAllPredecessorsUnlocked(roleIds[0]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(roleIds[1]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3])),
                manager.getAllPredecessorsUnlocked(roleIds[2]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[3])),
                manager.getAllPredecessorsUnlocked(roleIds[3]));
        // role descendants
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3])),
                manager.getAllDescendantsUnlocked(roleIds[0]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1], roleIds[2])),
                manager.getAllDescendantsUnlocked(roleIds[1]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[2])),
                manager.getAllDescendantsUnlocked(roleIds[2]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1], roleIds[2], roleIds[3])),
                manager.getAllDescendantsUnlocked(roleIds[3]));

        // role0 -> role1 -> role2 -> role4
        //    \       ^
        //     \      |
        //      \-> role3
        int oldValue = Config.privilege_max_role_depth;
        // simulate exception
        Config.privilege_max_role_depth = 3;
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant role2 to role role4", ctx), ctx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("role inheritance depth for role0"));
            Assert.assertTrue(e.getMessage().contains("is 4 > 3"));
        }
        Assert.assertEquals(3, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Config.privilege_max_role_depth = oldValue;
        // normal cases
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role2 to role role4", ctx), ctx);
        Assert.assertEquals(4, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));

        // grant too many roles to user
        oldValue = Config.privilege_max_total_roles_per_user;
        Config.privilege_max_total_roles_per_user = 3;
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_role_inheritance", "%");
        UserPrivilegeCollectionV2 collection = manager.getUserPrivilegeCollectionUnlocked(user);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role1 to user_test_role_inheritance", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role0 to user_test_role_inheritance", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        // exception:
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant role4 to user_test_role_inheritance", ctx), ctx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("'user_test_role_inheritance'@'%' has total 5 predecessor roles > 3"));
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        // normal grant
        Config.privilege_max_total_roles_per_user = oldValue;
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant role4 to user_test_role_inheritance", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(
                        roleIds[0], roleIds[1], roleIds[2], roleIds[3], roleIds[4])),
                manager.getAllPredecessorsUnlocked(collection));


        // grant role with circle: bad case
        // grant role4 to role0
        //    ---------------------------
        //   |                           |
        //   V                           |
        // role0 -> role1 -> role2 -> role4
        //    \       ^
        //     \      |
        //      \-> role3
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3], roleIds[4])),
                manager.getAllPredecessorsUnlocked(roleIds[4]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), manager.getAllPredecessorsUnlocked(roleIds[0]));
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant role4 to role role0", ctx), ctx);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("role role0"));
            Assert.assertTrue(e.getMessage().contains("is already a predecessor role of role4"));
        }
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[2], roleIds[3], roleIds[4])),
                manager.getAllPredecessorsUnlocked(roleIds[4]));
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), manager.getAllPredecessorsUnlocked(roleIds[0]));
    }

    private void assertTableSelectOnTest(UserIdentity userIdentity, boolean... canSelectOnTbls) throws Exception {
        boolean[] args = canSelectOnTbls;
        Assert.assertEquals(4, args.length);
        setCurrentUserAndRoles(ctx, userIdentity);
        for (int i = 0; i != 4; ++i) {
            if (args[i]) {
                Assert.assertTrue("cannot select tbl" + i,
                        PrivilegeActions.checkTableAction(ctx, DB_NAME, "tbl" + i, PrivilegeType.SELECT));
            } else {
                Assert.assertFalse("can select tbl" + i,
                        PrivilegeActions.checkTableAction(ctx, DB_NAME, "tbl" + i, PrivilegeType.SELECT));
            }
        }
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
    }

    private void assertTableSelectOnTestWithoutSetRole(UserIdentity userIdentity, boolean... canSelectOnTbls) throws Exception {
        boolean[] args = canSelectOnTbls;
        Assert.assertEquals(4, args.length);
        ctx.setCurrentUserIdentity(userIdentity);
        for (int i = 0; i != 4; ++i) {
            if (args[i]) {
                Assert.assertTrue("cannot select tbl" + i,
                        PrivilegeActions.checkTableAction(ctx, DB_NAME, "tbl" + i, PrivilegeType.SELECT));
            } else {
                Assert.assertFalse("can select tbl" + i,
                        PrivilegeActions.checkTableAction(ctx, DB_NAME, "tbl" + i, PrivilegeType.SELECT));
            }
        }
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
    }

    @Test
    public void dropRoleWithInheritance() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        // create role0 ~ role3
        long[] roleIds = new long[4];
        for (int i = 0; i != 4; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role test_drop_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_drop_role_" + i, ctx), ctx);
            roleIds[i] = manager.getRoleIdByNameNoLock("test_drop_role_" + i);
        }
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_drop_role_inheritance"), ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_drop_role_inheritance", "%");
        UserPrivilegeCollectionV2 collection = manager.getUserPrivilegeCollectionUnlocked(user);

        // role0 -> role1[user] -> role2
        // role3[user]
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_0 to role test_drop_role_1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_1 to role test_drop_role_2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_1 to user_test_drop_role_inheritance", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_drop_role_3 to user_test_drop_role_inheritance", ctx), ctx);

        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1], roleIds[3])),
                manager.getAllPredecessorsUnlocked(collection));
        assertTableSelectOnTest(user, true, true, false, true);

        // role0 -> role1[user] -> role2
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_drop_role_3;", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        Assert.assertEquals(2, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        assertTableSelectOnTest(user, true, true, false, false);

        // role0 -> role1[user]
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_drop_role_2;", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        Assert.assertEquals(1, manager.getMaxRoleInheritanceDepthInner(0, roleIds[0]));
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        assertTableSelectOnTest(user, true, true, false, false);

        // role1[user]
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_drop_role_0;", ctx), ctx);
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        Assert.assertEquals(0, manager.getMaxRoleInheritanceDepthInner(0, roleIds[1]));
        assertTableSelectOnTest(user, false, true, false, false);

        // clean invalidate roles
        int userNumRoles = manager.userToPrivilegeCollection.get(user).getAllRoles().size();
        int role1NumParents = manager.roleIdToPrivilegeCollection.get(roleIds[1]).getParentRoleIds().size();
        int role1NumSubs = manager.roleIdToPrivilegeCollection.get(roleIds[1]).getSubRoleIds().size();
        manager.removeInvalidObject();
        Assert.assertEquals(userNumRoles - 1, manager.userToPrivilegeCollection.get(user).getAllRoles().size());
        Assert.assertEquals(role1NumParents - 1,
                manager.roleIdToPrivilegeCollection.get(roleIds[1]).getParentRoleIds().size());
        Assert.assertEquals(role1NumSubs - 1,
                manager.roleIdToPrivilegeCollection.get(roleIds[1]).getSubRoleIds().size());

        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1])),
                manager.getAllPredecessorsUnlocked(collection));
        assertTableSelectOnTest(user, false, true, false, false);
    }

    @Test
    public void testSetRole() throws Exception {
        GlobalVariable.setActivateAllRolesOnLogin(false);
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_set_role"), ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_set_role", "%");
        // create role0 ~ role3
        // grant select on tblx to rolex
        // grant role0, role1, role2 to user
        long[] roleIds = new long[4];
        for (int i = 0; i != 4; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role test_set_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_set_role_" + i, ctx), ctx);
            if (i != 3) {
                DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                        "grant test_set_role_" + i + " to user_test_set_role", ctx), ctx);
            }
            roleIds[i] = manager.getRoleIdByNameNoLock("test_set_role_" + i);
        }

        // default: user can select all tables
        assertTableSelectOnTestWithoutSetRole(user, false, false, false, false);

        // set one role
        ctx.setCurrentUserIdentity(user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role 'test_set_role_0'"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), ctx.getCurrentRoleIds());
        assertTableSelectOnTestWithoutSetRole(user, true, false, false, false);

        // set on other 3 roles
        setCurrentUserAndRoles(ctx, user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role 'test_set_role_1', 'test_set_role_2'"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[1], roleIds[2])), ctx.getCurrentRoleIds());
        assertTableSelectOnTestWithoutSetRole(user, false, true, true, false);

        // bad case: role not exists
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create role bad_role", ctx), ctx);
        SetRoleStmt stmt = (SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role 'test_set_role_1', 'test_set_role_2', 'bad_role'", ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role bad_role", ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        try {
            SetRoleExecutor.execute(stmt, ctx);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot find role bad_role"));
        }
        try {
            SetRoleExecutor.execute((SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "set role 'test_set_role_1', 'test_set_role_3'", ctx), ctx);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Role test_set_role_3 is not granted"));
        }

        try {
            SetRoleExecutor.execute((SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                    "set role all except 'test_set_role_1', 'test_set_role_3'", ctx), ctx);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Role test_set_role_3 is not granted"));
        }

        // drop role1
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "drop role test_set_role_1;", ctx), ctx);

        ctx.setCurrentUserIdentity(user);
        SetRoleExecutor.execute((SetRoleStmt) UtFrameUtils.parseStmtWithNewParser(
                "set role all", ctx), ctx);
        assertTableSelectOnTestWithoutSetRole(user, true, false, true, false);

        setCurrentUserAndRoles(ctx, user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role all"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0], roleIds[2])), ctx.getCurrentRoleIds());
        assertTableSelectOnTestWithoutSetRole(user, true, false, true, false);

        setCurrentUserAndRoles(ctx, user);
        new StmtExecutor(ctx, UtFrameUtils.parseStmtWithNewParser(
                String.format("set role all except 'test_set_role_2'"), ctx)).execute();
        Assert.assertEquals(new HashSet<>(Arrays.asList(roleIds[0])), ctx.getCurrentRoleIds());
        assertTableSelectOnTestWithoutSetRole(user, true, false, false, false);

        // predecessors
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_set_role_3 to role test_set_role_0;", ctx), ctx);
        assertTableSelectOnTestWithoutSetRole(user, true, false, false, true);
        GlobalVariable.setActivateAllRolesOnLogin(true);
    }

    @Test
    public void testBuiltinRoles() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        // create user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create user user_test_builtin_role"), ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("user_test_builtin_role", "%");

        // public role
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT select on db.tbl0 TO ROLE public"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_0, PrivilegeType.SELECT));

        // root role
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT root TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.GRANT));
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
        Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, DB_NAME, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkViewAction(ctx, DB_NAME, "view1", PrivilegeType.DROP));
        Assert.assertTrue(manager.canExecuteAs(ctx, UserIdentity.ROOT));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE root FROM user_test_builtin_role"), ctx), ctx);

        // db_admin role
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT db_admin TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertFalse(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.GRANT));
        Assert.assertFalse(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
        Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, DB_NAME, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkViewAction(ctx, DB_NAME, "view1", PrivilegeType.DROP));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE db_admin FROM user_test_builtin_role"), ctx), ctx);

        // cluster_admin
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT cluster_admin TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertFalse(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.GRANT));
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
        Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, DB_NAME, PrivilegeType.DROP));
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertFalse(PrivilegeActions.checkViewAction(ctx, DB_NAME, "view1", PrivilegeType.DROP));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE cluster_admin FROM user_test_builtin_role"), ctx), ctx);

        // user_admin
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT user_admin TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.GRANT));
        Assert.assertFalse(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
        Assert.assertFalse(PrivilegeActions.checkDbAction(ctx, DB_NAME, PrivilegeType.DROP));
        Assert.assertFalse(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertFalse(PrivilegeActions.checkViewAction(ctx, DB_NAME, "view1", PrivilegeType.DROP));
        Assert.assertTrue(manager.canExecuteAs(ctx, UserIdentity.ROOT));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE user_admin FROM user_test_builtin_role"), ctx), ctx);

        // user root
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.GRANT));
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.NODE));
        Assert.assertTrue(PrivilegeActions.checkDbAction(ctx, DB_NAME, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkViewAction(ctx, DB_NAME, "view1", PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkAnyActionOnOrInDb(ctx, DB_NAME));

        // grant to imutable role
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("create role role_test_builtin_role"), ctx), ctx);
        List<String> modifyImutableRoleSqls = Arrays.asList(
                "GRANT select on db.tbl0 TO ROLE root",
                "REVOKE select on db.tbl0 FROM ROLE root",
                "GRANT role_test_builtin_role TO ROLE cluster_admin"
        );
        for (String sql : modifyImutableRoleSqls) {
            try {
                DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
                System.err.println(sql);
                Assert.fail();
            } catch (DdlException e) {
                System.err.println(e.getMessage());
                Assert.assertTrue(e.getMessage().contains("is not mutable"));
            }
        }

        // drop builtin role
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("drop role public", ctx), ctx);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("role public cannot be dropped"));
        }

        // revoke public role
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "revoke public from user_test_builtin_role", ctx), ctx);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Every user and role has role PUBLIC implicitly granted"));
        }
    }

    @Test
    public void testGrantRoleSame() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user grant_same_user", ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("grant_same_user", "%");
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        long[] roleIds = new long[3];
        for (int i = 0; i != 3; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("create role grant_same_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("grant all on all users to role grant_same_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("grant select on all tables in all databases to role grant_same_role_%d;", i), ctx), ctx);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    String.format("grant grant_same_role_%d to grant_same_user;", i), ctx), ctx);

            roleIds[i] = manager.getRoleIdByNameNoLock("grant_same_role_" + i);
        }

        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(manager.canExecuteAs(ctx, UserIdentity.ROOT));
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
    }

    @Test
    public void testGrantAllResource() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user resource_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on resource 'hive0' to resource_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant usage on all resources to resource_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant alter on all resources to resource_user with grant option", ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("resource_user", "%");
        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.USAGE));
        Assert.assertTrue(PrivilegeActions.checkResourceAction(ctx, "hive0", PrivilegeType.ALTER));
    }

    @Test
    public void testGrantOnCatalog() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create external catalog test_catalog properties (\"type\"=\"iceberg\")", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user test_catalog_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on catalog 'test_catalog' to test_catalog_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant usage on all catalogs to test_catalog_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create database on all catalogs to test_catalog_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant alter on all catalogs to test_catalog_user with grant option", ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_catalog_user", "%");
        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(PrivilegeActions.checkCatalogAction(ctx, "test_catalog",
                PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkCatalogAction(ctx, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                PrivilegeType.CREATE_DATABASE));
        Assert.assertTrue(PrivilegeActions.checkCatalogAction(ctx, "test_catalog",
                PrivilegeType.USAGE));
        Assert.assertTrue(PrivilegeActions.checkCatalogAction(ctx, "test_catalog",
                PrivilegeType.ALTER));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user test_catalog_user2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create database on catalog " + InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME +
                        " to test_catalog_user2 with grant option", ctx), ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.createAnalyzedUserIdentWithIp("test_catalog_user2", "%"));
        Assert.assertTrue(PrivilegeActions.checkCatalogAction(ctx, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                PrivilegeType.CREATE_DATABASE));
    }

    @Test
    public void testGrantView() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user view_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on view db.view1 to view_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on all views in all databases to view_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant alter on all views in database db to view_user with grant option", ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("view_user", "%");
        setCurrentUserAndRoles(ctx, user);
        Assert.assertTrue(PrivilegeActions.checkViewAction(ctx, "db", "view1", PrivilegeType.DROP));
        Assert.assertTrue(PrivilegeActions.checkViewAction(ctx, "db", "view1", PrivilegeType.SELECT));
        Assert.assertTrue(PrivilegeActions.checkViewAction(ctx, "db", "view1", PrivilegeType.ALTER));
    }

    @Test
    public void testGrantBrief() throws Exception {
        ctx.setDatabase("db");
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user brief_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on tbl0 to brief_user", ctx), ctx);
        ctx.setCurrentUserIdentity(new UserIdentity("brief_user", "%"));
        Assert.assertTrue(PrivilegeActions.checkTableAction(ctx, "db", "tbl0", PrivilegeType.DROP));

        try {
            StatementBase statementBase =
                    UtFrameUtils.parseStmtWithNewParser("grant drop on db to brief_user", ctx);
            DDLStmtExecutor.execute(statementBase, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot find table db in db db"));
        }
    }

    @Test
    public void testPartialRevoke() throws Exception {
        String sql = "grant select on table db.tbl0 to test_user";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);

        sql = "grant select on table db.* to test_user";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);

        try {
            StatementBase statementBase =
                    UtFrameUtils.parseStmtWithNewParser("revoke select on table db.tbl1 from test_user", ctx);
            DDLStmtExecutor.execute(statementBase, ctx);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("There is no such grant defined on TABLE db.tbl1"));
        }

        StatementBase statementBase =
                UtFrameUtils.parseStmtWithNewParser("revoke select on table db.* from test_user", ctx);

        statementBase =
                UtFrameUtils.parseStmtWithNewParser("revoke insert on table db.* from test_user", ctx);
        statementBase =
                UtFrameUtils.parseStmtWithNewParser("revoke select on table db.tbl0 from test_user", ctx);
    }

    @Test
    public void testSystem() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u1", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u2", ctx), ctx);

        String sql = "grant OPERATE ON SYSTEM TO USER u1 with grant option";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);

        setCurrentUserAndRoles(ctx, new UserIdentity("u1", "%"));
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.OPERATE));

        sql = "grant OPERATE ON SYSTEM TO USER u2";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        setCurrentUserAndRoles(ctx, new UserIdentity("u2", "%"));
        Assert.assertTrue(PrivilegeActions.checkSystemAction(ctx, PrivilegeType.OPERATE));
    }

    @Test
    public void testGrantStorageVolumeUsageToPublicRole() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("create user u1", ctx), ctx);

        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String storageVolumeId = ctx.getGlobalStateMgr().getStorageVolumeMgr()
                .createStorageVolume("test", "S3", locations, storageParams, Optional.empty(), "");
        manager.grantStorageVolumeUsageToPublicRole(storageVolumeId);
        setCurrentUserAndRoles(ctx, new UserIdentity("u1", "%"));
        Assert.assertTrue(PrivilegeActions.checkStorageVolumeAction(ctx, "test", PrivilegeType.USAGE));
    }

    @Test
    public void testLoadV2() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        AuthorizationMgr masterManager = masterGlobalStateMgr.getAuthorizationMgr();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        for (int i = 0; i != 2; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "create role test_persist_role" + i, ctx), ctx);
            Assert.assertTrue(masterManager.checkRoleExists("test_persist_role" + i));
        }

        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterGlobalStateMgr.getAuthorizationMgr().saveV2(emptyImage.getDataOutputStream());

        AuthorizationMgr authorizationMgr = new AuthorizationMgr();
        SRMetaBlockReader reader = new SRMetaBlockReader(emptyImage.getDataInputStream());
        authorizationMgr.loadV2(reader);

        Assert.assertNotNull(authorizationMgr.getRolePrivilegeCollection("test_persist_role0"));
    }
}
