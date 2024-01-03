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

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.meta.MetaContext;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfoDeprecated;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SetRoleExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
        UtFrameUtils.setUpForPersistTest();

        String createResourceSql = "create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")";
        CreateResourceStmt createResourceStmt = (CreateResourceStmt) UtFrameUtils.parseStmtWithNewParser(createResourceSql, ctx);
        if (!GlobalStateMgr.getCurrentState().getResourceMgr().containsResource(createResourceStmt.getResourceName())) {
            GlobalStateMgr.getCurrentState().getResourceMgr().createResource(createResourceStmt);
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        RBACMockedMetadataMgr metadataMgr =
                new RBACMockedMetadataMgr(globalStateMgr.getLocalMetastore(), globalStateMgr.getConnectorMgr());
        metadataMgr.init();
        globalStateMgr.setMetadataMgr(metadataMgr);

        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "create user test_user", ctx);
        globalStateMgr.getAuthenticationMgr().createUser(createUserStmt);

        GlobalVariable.setActivateAllRolesOnLogin(true);
        publicRoleId = globalStateMgr.getAuthorizationMgr().getRoleIdByNameNoLock("public");
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
        ctx.setQualifiedUser(testUser.getUser());

        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnTable(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME, TABLE_NAME_1)));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT);
        Authorizer.checkAnyActionOnOrInDb(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME);
        Authorizer.checkAnyActionOnTable(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, TABLE_NAME_1));

        String sql = "grant select on table db.tbl1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);
        sql = "grant ALTER on materialized view db3.mv1 to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT);
        Authorizer.checkAnyActionOnOrInDb(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME);
        Authorizer.checkAnyActionOnTable(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, TABLE_NAME_1));
        Authorizer.checkActionInDb(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, PrivilegeType.SELECT);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkActionInDb(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), DB_NAME, PrivilegeType.DROP));
        Authorizer.checkActionInDb(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "db3", PrivilegeType.ALTER);

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);
        sql = "revoke ALTER on materialized view db3.mv1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnTable(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME, TABLE_NAME_1)));

        // grant many priveleges
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "grant select, insert, delete on table db.tbl1 to test_user with grant option";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assert.assertTrue(grantStmt.isWithGrantOption());
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT);
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.INSERT);
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.DELETE);
        Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME);
        Authorizer.checkAnyActionOnTable(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, TABLE_NAME_1));

        // revoke only select
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke select on db.tbl1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.INSERT);
        Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.DELETE);
        Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME);
        Authorizer.checkAnyActionOnTable(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, TABLE_NAME_1));

        // revoke all
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke ALL on table db.tbl1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.INSERT));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.DELETE));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnTable(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME, TABLE_NAME_1)));

        // grant view
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "grant alter on view db.view1 to test_user";
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME);
        Authorizer.checkActionInDb(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, PrivilegeType.ALTER);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkActionInDb(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), DB_NAME, PrivilegeType.SELECT));

        // revoke view
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        sql = "revoke ALL on view db.view1 from test_user";
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME));
    }

    void saveRBACPrivilege(GlobalStateMgr globalStateMgr, DataOutputStream dataOutputStream) throws IOException {
        AuthenticationMgr authenticationMgr = globalStateMgr.getAuthenticationMgr();
        AuthorizationMgr authorizationMgr = globalStateMgr.getAuthorizationMgr();
        authenticationMgr.saveV2(dataOutputStream);
        authorizationMgr.saveV2(dataOutputStream);
    }

    void loadRBACPrivilege(GlobalStateMgr globalStateMgr, DataInputStream dataInputStream)
            throws IOException, SRMetaBlockEOFException, SRMetaBlockException, PrivilegeException {
        AuthenticationMgr authenticationMgr = globalStateMgr.getAuthenticationMgr();
        AuthorizationMgr authorizationMgr = globalStateMgr.getAuthorizationMgr();

        SRMetaBlockReader srMetaBlockReader = new SRMetaBlockReader(dataInputStream);
        authenticationMgr.loadV2(srMetaBlockReader);
        srMetaBlockReader.close();
        srMetaBlockReader = new SRMetaBlockReader(dataInputStream);
        authorizationMgr.loadV2(srMetaBlockReader);
        srMetaBlockReader.close();

        for (UserIdentity userIdentity : authorizationMgr.getAllUserIdentities()) {
            authorizationMgr.invalidateUserInCache(userIdentity);
        }

        for (String roleName : authorizationMgr.getAllRoles()) {
            Long roleId = authorizationMgr.getRoleIdByNameAllowNull(roleName);
            authorizationMgr.invalidateRolesInCacheRoleUnlocked(roleId);
        }
    }

    @Test
    public void testPersist() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();

        AuthenticationMgr authenticationMgr = masterGlobalStateMgr.getAuthenticationMgr();
        AuthorizationMgr authorizationMgr = masterGlobalStateMgr.getAuthorizationMgr();

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataOutputStream());

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        String sql = "grant select on db.tbl1 to test_user";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationMgr.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1,
                PrivilegeType.SELECT);

        UtFrameUtils.PseudoImage grantImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, grantImage.getDataOutputStream());

        sql = "revoke select on db.tbl1 from test_user";
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationMgr.revoke(revokeStmt);
        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        UtFrameUtils.PseudoImage revokeImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, revokeImage.getDataOutputStream());

        // start to replay
        loadRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataInputStream());
        AuthorizationMgr followerManager = masterGlobalStateMgr.getAuthorizationMgr();

        UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1,
                PrivilegeType.SELECT);

        info = (UserPrivilegeCollectionInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerManager.replayUpdateUserPrivilegeCollection(
                info.getUserIdentity(), info.getPrivilegeCollection(), info.getPluginId(), info.getPluginVersion());
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

        // check image
        loadRBACPrivilege(masterGlobalStateMgr, grantImage.getDataInputStream());
        authorizationMgr.invalidateUserInCache(ctx.getCurrentUserIdentity());
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1,
                PrivilegeType.SELECT);

        loadRBACPrivilege(masterGlobalStateMgr, revokeImage.getDataInputStream());
        authorizationMgr.invalidateUserInCache(ctx.getCurrentUserIdentity());
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
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
            Authorizer.checkTableAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_0, PrivilegeType.SELECT);
        } else {
            Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_0, PrivilegeType.SELECT));
        }

        if (canSelectTbl1) {
            Authorizer.checkTableAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT);
        } else {
            Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        }
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
    }

    @Test
    public void testPersistRole() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        AuthorizationMgr masterManager = masterGlobalStateMgr.getAuthorizationMgr();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataOutputStream());
        saveRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataOutputStream());
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
        saveRBACPrivilege(masterGlobalStateMgr, createRoleImage.getDataOutputStream());

        // 2. grant select on db.tbl<i> to role
        for (int i = 0; i != 2; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "grant select on db.tbl" + i + " to role test_persist_role" + i, ctx), ctx);
        }
        UtFrameUtils.PseudoImage grantPrivsToRoleImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, grantPrivsToRoleImage.getDataOutputStream());
        assertTableSelectOnTest(masterManager, false, false);

        // 3. grant test_persist_role0 to test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_persist_role0 to test_user", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, false);
        UtFrameUtils.PseudoImage grantRoleToUserImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, grantRoleToUserImage.getDataOutputStream());

        // 4. grant test_persist_role1 to role test_persist_role0
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant test_persist_role1 to role test_persist_role0", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, true);
        UtFrameUtils.PseudoImage grantRoleToRoleImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, grantRoleToRoleImage.getDataOutputStream());

        // 5. revoke test_persist_role1 from role test_persist_role0
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke test_persist_role1 from role test_persist_role0", ctx), ctx);
        assertTableSelectOnTest(masterManager, true, false);
        UtFrameUtils.PseudoImage revokeRoleFromRoleImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, revokeRoleFromRoleImage.getDataOutputStream());

        // 6. revoke test_persist_role0 from test_user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke test_persist_role0 from test_user", ctx), ctx);
        assertTableSelectOnTest(masterManager, false, false);
        UtFrameUtils.PseudoImage revokeRoleFromUserImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, revokeRoleFromUserImage.getDataOutputStream());

        // 7. drop 2 roles
        for (int i = 0; i != 2; ++i) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "drop role test_persist_role" + i, ctx), ctx);
            Assert.assertFalse(masterManager.checkRoleExists("test_persist_role" + i));
        }
        UtFrameUtils.PseudoImage dropRoleImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, dropRoleImage.getDataOutputStream());

        //
        // start to replay
        //
        loadRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataInputStream());
        loadRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataInputStream());
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
        loadRBACPrivilege(masterGlobalStateMgr, createRoleImage.getDataInputStream());
        AuthorizationMgr imageManager = masterGlobalStateMgr.getAuthorizationMgr();

        Assert.assertTrue(imageManager.checkRoleExists("test_persist_role0"));
        Assert.assertTrue(imageManager.checkRoleExists("test_persist_role1"));

        // 2. check image after grant select on db.tbl<i> to role
        loadRBACPrivilege(masterGlobalStateMgr, grantPrivsToRoleImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, false, false);

        // 3. check image after grant test_persist_role0 to test_user
        loadRBACPrivilege(masterGlobalStateMgr,
                grantRoleToUserImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, true, false);

        // 4. check image after grant test_persist_role1 to role test_persist_role0
        loadRBACPrivilege(masterGlobalStateMgr,
                grantRoleToRoleImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, true, true);

        // 5. check image after revoke test_persist_role1 from role test_persist_role0
        loadRBACPrivilege(masterGlobalStateMgr,
                revokeRoleFromRoleImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, true, false);

        // 6. check image after revoke test_persist_role0 from test_user
        loadRBACPrivilege(masterGlobalStateMgr,
                revokeRoleFromUserImage.getDataInputStream());
        assertTableSelectOnTest(imageManager, false, false);

        // 7. check image after drop 2 roles
        loadRBACPrivilege(masterGlobalStateMgr,
                dropRoleImage.getDataInputStream());
        imageManager = masterGlobalStateMgr.getAuthorizationMgr();
        Assert.assertFalse(imageManager.checkRoleExists("test_persist_role0"));
        Assert.assertFalse(imageManager.checkRoleExists("test_persist_role1"));
    }

    @Test
    public void testGrantAll() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        setCurrentUserAndRoles(ctx, testUser);
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));

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
            Authorizer.checkTableAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT);

            setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
            RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sqlPair.get(1), ctx);
            manager.revoke(revokeStmt);
            setCurrentUserAndRoles(ctx, testUser);
            ;
            Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT));
        }

        // on all databases
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkDbAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, DB_NAME, PrivilegeType.CREATE_TABLE));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT create table ON ALL DATABASES TO test_user", ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        new NativeAccessControl().checkDbAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, DB_NAME, PrivilegeType.CREATE_TABLE);

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE create table ON ALL DATABASES FROM test_user", ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        ;
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkDbAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, DB_NAME, PrivilegeType.CREATE_TABLE));

        // on all users
        AuthorizationMgr authorizationManager = ctx.getGlobalStateMgr().getAuthorizationMgr();
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE));

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON ALL USERS TO test_user", ctx);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE);

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE IMPERSONATE ON ALL USERS FROM test_user", ctx);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE));
    }

    @Test
    public void testImpersonate() throws Exception {
        AuthorizationMgr manager = ctx.getGlobalStateMgr().getAuthorizationMgr();

        setCurrentUserAndRoles(ctx, testUser);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE));

        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON USER root, test_user TO test_user", ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        manager.grant(grantStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE);

        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE IMPERSONATE ON USER root FROM test_user", ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        manager.revoke(revokeStmt);

        setCurrentUserAndRoles(ctx, testUser);
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE));
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
        Assert.assertEquals(new HashSet<>(Arrays.asList("tbl0", "tbl1", "tbl2", "tbl3", "mv1", "view1")), allTables);

        // user with table priv can only see tbl1
        setCurrentUserAndRoles(ctx, userWithTablePriv);
        executor = new ShowExecutor(ctx, showTableStmt);
        resultSet = executor.execute();
        allTables = resultSet.getResultRows().stream().map(k -> k.get(0)).collect(Collectors.toSet());
        Assert.assertEquals(new HashSet<>(Lists.newArrayList("tbl1")), allTables);

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
            new NativeAccessControl().checkDbAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, "db", PrivilegeType.CREATE_TABLE);
        } else {
            Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkDbAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, "db", PrivilegeType.CREATE_TABLE));
        }

        if (canDrop) {
            new NativeAccessControl().checkDbAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, "db", PrivilegeType.DROP);
        } else {
            Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkDbAction(
                    ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, "db", PrivilegeType.DROP));
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
                Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME,
                        "tbl" + i, PrivilegeType.SELECT);
            } else {
                int finalI = i;
                Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl()
                        .checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME,
                                "tbl" + finalI), PrivilegeType.SELECT));
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
                new NativeAccessControl().checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                        new TableName(DB_NAME, "tbl" + i), PrivilegeType.SELECT);
            } else {
                int finalI = i;
                Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl()
                        .checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME,
                                "tbl" + finalI), PrivilegeType.SELECT));
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
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_0,
                PrivilegeType.SELECT);

        // root role
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT root TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.GRANT);
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.NODE);
        new NativeAccessControl().checkDbAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, DB_NAME,
                PrivilegeType.DROP);
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1,
                PrivilegeType.DROP);
        new NativeAccessControl().checkViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, "view1"), PrivilegeType.DROP);

        Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE root FROM user_test_builtin_role"), ctx), ctx);

        // db_admin role
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT db_admin TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkSystemAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.GRANT));
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkSystemAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.NODE));
        new NativeAccessControl().checkDbAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                null, DB_NAME, PrivilegeType.DROP);
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                DB_NAME, TABLE_NAME_1, PrivilegeType.DROP);
        new NativeAccessControl().checkViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, "view1"), PrivilegeType.DROP);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE db_admin FROM user_test_builtin_role"), ctx), ctx);

        // cluster_admin
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT cluster_admin TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkSystemAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.GRANT));
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.NODE);
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkDbAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, DB_NAME, PrivilegeType.DROP));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkViewAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME, "view1"), PrivilegeType.DROP));
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE cluster_admin FROM user_test_builtin_role"), ctx), ctx);

        // user_admin
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("GRANT user_admin TO user_test_builtin_role"), ctx), ctx);
        setCurrentUserAndRoles(ctx, user);
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.GRANT);
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkSystemAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.NODE));
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkDbAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), null, DB_NAME, PrivilegeType.DROP));
        Assert.assertThrows(AccessDeniedException.class, () -> Authorizer.checkTableAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), DB_NAME, TABLE_NAME_1, PrivilegeType.DROP));
        Assert.assertThrows(AccessDeniedException.class, () -> new NativeAccessControl().checkViewAction(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), new TableName(DB_NAME, "view1"), PrivilegeType.DROP));
        Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE);
        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                String.format("REVOKE user_admin FROM user_test_builtin_role"), ctx), ctx);

        // user root
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.GRANT);
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.NODE);
        new NativeAccessControl().checkDbAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                null, DB_NAME, PrivilegeType.DROP);
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                DB_NAME, TABLE_NAME_1, PrivilegeType.DROP);
        new NativeAccessControl().checkViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName(DB_NAME, "view1"), PrivilegeType.DROP);
        Authorizer.checkAnyActionOnOrInDb(
                ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME);

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
        Authorizer.checkUserAction(ctx.getCurrentUserIdentity(),
                ctx.getCurrentRoleIds(), UserIdentity.ROOT, PrivilegeType.IMPERSONATE);
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                DB_NAME, TABLE_NAME_1, PrivilegeType.SELECT);
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
        new NativeAccessControl().checkResourceAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "hive0",
                PrivilegeType.DROP);
        new NativeAccessControl().checkResourceAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "hive0",
                PrivilegeType.USAGE);
        new NativeAccessControl().checkResourceAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "hive0",
                PrivilegeType.ALTER);
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
        new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "test_catalog",
                PrivilegeType.DROP);
        new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                PrivilegeType.CREATE_DATABASE);
        new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "test_catalog",
                PrivilegeType.USAGE);
        new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "test_catalog",
                PrivilegeType.ALTER);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user test_catalog_user2", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create database on catalog " + InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME +
                        " to test_catalog_user2 with grant option", ctx), ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.createAnalyzedUserIdentWithIp("test_catalog_user2", "%"));
        new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                PrivilegeType.CREATE_DATABASE);
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
        new NativeAccessControl().checkViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName("db", "view1"), PrivilegeType.DROP);
        new NativeAccessControl().checkViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName("db", "view1"), PrivilegeType.SELECT);
        new NativeAccessControl().checkViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName("db", "view1"), PrivilegeType.ALTER);
    }

    @Test
    public void testGrantMaterializedView() throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user mv_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on materialized view db3.mv1 to mv_user", ctx), ctx);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("mv_user", "%");
        setCurrentUserAndRoles(ctx, user);
        new NativeAccessControl().checkMaterializedViewAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName("db3", "mv1"), PrivilegeType.SELECT);
        new NativeAccessControl().checkAnyActionOnMaterializedView(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                new TableName("db3", "mv1"));
        new NativeAccessControl().checkAnyActionOnAnyMaterializedView(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                "db3");
    }

    @Test
    public void testGrantBrief() throws Exception {
        ctx.setDatabase("db");
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user brief_user", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant drop on tbl0 to brief_user", ctx), ctx);
        ctx.setCurrentUserIdentity(new UserIdentity("brief_user", "%"));
        Authorizer.checkTableAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), "db", "tbl0",
                PrivilegeType.DROP);

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
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.OPERATE);

        sql = "grant OPERATE ON SYSTEM TO USER u2";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        setCurrentUserAndRoles(ctx, new UserIdentity("u2", "%"));
        new NativeAccessControl().checkSystemAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(), PrivilegeType.OPERATE);
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
        new NativeAccessControl().checkStorageVolumeAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                "test", PrivilegeType.USAGE);
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

    @Test
    public void testUpgradePrivilege(@Mocked HiveMetastore hiveMetastore) throws Exception {
        ConnectContext connectCtx = new ConnectContext();
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog_1 COMMENT \"hive_catalog\" PROPERTIES(\"type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\");";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(createCatalog, ctx);
        Assert.assertTrue(stmt instanceof CreateCatalogStmt);
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        connectCtx.setCurrentUserIdentity(UserIdentity.ROOT);
        connectCtx.setCurrentRoleIds(UserIdentity.ROOT);
        CreateCatalogStmt statement = (CreateCatalogStmt) stmt;
        DDLStmtExecutor.execute(statement, connectCtx);

        new Expectations() {
            {
                hiveMetastore.getAllDatabaseNames();
                result = Lists.newArrayList("db");
                minTimes = 0;

                hiveMetastore.getAllTableNames("db");
                result = Lists.newArrayList("tbl");
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = connectCtx.getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadataMgr) {
            {
                metadataMgr.getDb("hive_catalog_1", "db");
                result = new com.starrocks.catalog.Database(0, "db");
                minTimes = 0;

                metadataMgr.getTable("hive_catalog_1", "db", "tbl");
                result = HiveTable.builder().setHiveTableName("tbl")
                        .setFullSchema(Lists.newArrayList(new Column("v1", Type.INT))).build();
                minTimes = 0;
            }
        };

        connectCtx.getGlobalStateMgr().changeCatalog(connectCtx, "hive_catalog_1");

        MetaContext.get().setStarRocksMetaVersion(3);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user test_upgrade_priv", connectCtx), connectCtx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create database on catalog hive_catalog_1 to test_upgrade_priv", ctx), ctx);
        setCurrentUserAndRoles(ctx, UserIdentity.createAnalyzedUserIdentWithIp("test_upgrade_priv", "%"));
        new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                "hive_catalog_1", PrivilegeType.CREATE_DATABASE);
        Assert.assertThrows(AccessDeniedException.class, () ->
                new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                        "hive_catalog_1", PrivilegeType.USAGE));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user test_upgrade_priv_2", connectCtx), connectCtx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant create table on database db to test_upgrade_priv_2", connectCtx), connectCtx);
        setCurrentUserAndRoles(connectCtx, UserIdentity.createAnalyzedUserIdentWithIp("test_upgrade_priv_2", "%"));
        Assert.assertThrows(AccessDeniedException.class, () ->
                new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                        "hive_catalog_1", PrivilegeType.USAGE));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user test_upgrade_priv_3", connectCtx), connectCtx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on table db.tbl to test_upgrade_priv_3", connectCtx), connectCtx);
        setCurrentUserAndRoles(connectCtx, UserIdentity.createAnalyzedUserIdentWithIp("test_upgrade_priv_3", "%"));
        Assert.assertThrows(AccessDeniedException.class, () ->
                new NativeAccessControl().checkCatalogAction(ctx.getCurrentUserIdentity(), ctx.getCurrentRoleIds(),
                        "hive_catalog_1", PrivilegeType.USAGE));

        GlobalStateMgr masterGlobalStateMgr = connectCtx.getGlobalStateMgr();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, image.getDataOutputStream());
        loadRBACPrivilege(masterGlobalStateMgr, image.getDataInputStream());

        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        UserPrivilegeCollectionV2 up1 = authorizationMgr.getUserPrivilegeCollectionUnlockedAllowNull(
                UserIdentity.createAnalyzedUserIdentWithIp("test_upgrade_priv", "%"));
        UserPrivilegeCollectionV2 up2 = authorizationMgr.getUserPrivilegeCollectionUnlockedAllowNull(
                UserIdentity.createAnalyzedUserIdentWithIp("test_upgrade_priv_2", "%"));
        UserPrivilegeCollectionV2 up3 = authorizationMgr.getUserPrivilegeCollectionUnlockedAllowNull(
                UserIdentity.createAnalyzedUserIdentWithIp("test_upgrade_priv_3", "%"));

        authorizationMgr.checkAction(up1, ObjectType.CATALOG, PrivilegeType.USAGE,
                Lists.newArrayList("hive_catalog_1"));
        authorizationMgr.checkAction(up2, ObjectType.CATALOG, PrivilegeType.USAGE,
                Lists.newArrayList("hive_catalog_1"));
        authorizationMgr.checkAction(up3, ObjectType.CATALOG, PrivilegeType.USAGE,
                Lists.newArrayList("hive_catalog_1"));

        saveRBACPrivilege(masterGlobalStateMgr, image.getDataOutputStream());
        loadRBACPrivilege(masterGlobalStateMgr, image.getDataInputStream());
    }

    @Test
    public void testSysTypeError() throws Exception {
        GlobalStateMgr masterGlobalStateMgr = ctx.getGlobalStateMgr();
        AuthorizationMgr masterManager = masterGlobalStateMgr.getAuthorizationMgr();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        setCurrentUserAndRoles(ctx, UserIdentity.ROOT);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user user_for_system", ctx), ctx);

        String sql = "GRANT OPERATE ON SYSTEM TO USER user_for_system";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.grant(grantStmt);

        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        saveRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataOutputStream());
        loadRBACPrivilege(masterGlobalStateMgr, emptyImage.getDataInputStream());

        sql = "show grants for user_for_system";
        ShowGrantsStmt showStreamLoadStmt = (ShowGrantsStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, showStreamLoadStmt);
        ShowResultSet resultSet = executor.execute();
    }

    @Test
    public void testUserPriUpgrade() throws IOException, InterruptedException, PrivilegeException {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();

        UserPrivilegeCollection collection = new UserPrivilegeCollection();
        HashSet<Long> roleIds = new HashSet<>();
        roleIds.add(1L);
        collection.grantRoles(roleIds);
        collection.setDefaultRoleIds(roleIds);
        collection.grant(ObjectTypeDeprecate.SYSTEM, Lists.newArrayList(PrivilegeType.OPERATE),
                Lists.newArrayList(new PEntryObject[] {null}), false);

        UserPrivilegeCollectionInfoDeprecated deprecated = new UserPrivilegeCollectionInfoDeprecated(
                new UserIdentity("u1", "%"),
                collection,
                (short) 1, (short) 1);

        deprecated.write(emptyImage.getDataOutputStream());

        UserPrivilegeCollectionInfo userPrivilegeCollectionInfo =
                UserPrivilegeCollectionInfo.read(emptyImage.getDataInputStream());

        Assert.assertEquals(new UserIdentity("u1", "%"), userPrivilegeCollectionInfo.getUserIdentity());
        Assert.assertEquals(roleIds, userPrivilegeCollectionInfo.getPrivilegeCollection().getAllRoles());
        Assert.assertEquals(roleIds, userPrivilegeCollectionInfo.getPrivilegeCollection().getDefaultRoleIds());
        Assert.assertTrue(userPrivilegeCollectionInfo.getPrivilegeCollection()
                .check(ObjectType.SYSTEM, PrivilegeType.OPERATE, null));
    }
}
