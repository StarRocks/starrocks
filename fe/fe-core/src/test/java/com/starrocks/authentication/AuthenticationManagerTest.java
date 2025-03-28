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

package com.starrocks.authentication;

import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SetDefaultRoleExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthenticationManagerTest {
    static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testInitDefault() throws Exception {
        AuthenticationMgr manager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        Assert.assertTrue(manager.doesUserExist(UserIdentity.ROOT));
        Assert.assertFalse(manager.doesUserExist(UserIdentity.createAnalyzedUserIdentWithIp("fake", "%")));
        Assert.assertEquals(new UserProperty().getMaxConn(), manager.getMaxConn(AuthenticationMgr.ROOT_USER));
    }

    @Test
    public void testCreateUserPersist() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        UserIdentity testUserWithIp = UserIdentity.createAnalyzedUserIdentWithIp("test", "10.1.1.1");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");

        AuthenticationMgr masterManager = new AuthenticationMgr();
        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, testUserWithIp.getUser(), testUserWithIp.getHost(), scramble, seed));

        Assert.assertFalse(masterManager.doesUserExist(testUser));
        Assert.assertFalse(masterManager.doesUserExist(testUserWithIp));
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getImageWriter());

        // master create test@%; no password
        String sql = "create user test";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        Assert.assertTrue(masterManager.doesUserExist(testUser));
        Assert.assertFalse(masterManager.doesUserExist(testUserWithIp));
        UserIdentity user = masterManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1").getKey();
        Assert.assertEquals(user, testUser);

        // create twice fail
        masterManager.createUser(stmt);

        // master create test@10.1.1.1
        sql = "create user 'test'@'10.1.1.1' identified by 'abc'";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        Assert.assertTrue(masterManager.doesUserExist(testUser));
        Assert.assertTrue(masterManager.doesUserExist(testUserWithIp));
        user = masterManager.getBestMatchedUserIdentity(testUser.getUser(), testUserWithIp.getHost()).getKey();
        Assert.assertEquals(user, testUserWithIp);

        // make final snapshot
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getImageWriter());

        // login from 10.1.1.2 with password will fail
        Map.Entry<UserIdentity, UserAuthenticationInfo> entry =
                masterManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.2");
        PlainPasswordAuthenticationProvider provider = new PlainPasswordAuthenticationProvider();
        Assert.assertThrows(AuthenticationException.class, () ->
                provider.authenticate(ctx, entry.getKey().getUser(), entry.getKey().getHost(), scramble, seed, entry.getValue()));

        // start to replay
        AuthenticationMgr followerManager = new AuthenticationMgr();
        followerManager.loadV2(emptyImage.getMetaBlockReader());

        Assert.assertFalse(followerManager.doesUserExist(testUser));
        Assert.assertFalse(followerManager.doesUserExist(testUserWithIp));

        // replay create test@%; no password
        CreateUserInfo info = (CreateUserInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_USER_V2);
        followerManager.replayCreateUser(
                info.getUserIdentity(),
                info.getAuthenticationInfo(),
                info.getUserProperty(),
                info.getUserPrivilegeCollection(),
                info.getPluginId(),
                info.getPluginVersion());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertFalse(followerManager.doesUserExist(testUserWithIp));

        Map.Entry<UserIdentity, UserAuthenticationInfo> bestUser =
                followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1");
        Assert.assertEquals(bestUser.getKey(), testUser);

        // replay create test@10.1.1.1
        info = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_USER_V2);
        followerManager.replayCreateUser(
                info.getUserIdentity(),
                info.getAuthenticationInfo(),
                info.getUserProperty(),
                info.getUserPrivilegeCollection(),
                info.getPluginId(),
                info.getPluginVersion());
        Assert.assertTrue(followerManager.doesUserExist(testUser));
        Assert.assertTrue(followerManager.doesUserExist(testUserWithIp));
        bestUser = followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1");
        Assert.assertEquals(bestUser.getKey(), testUserWithIp);

        // login from 10.1.1.2 with password will fail
        Map.Entry<UserIdentity, UserAuthenticationInfo> entry1 =
                followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.2");
        Assert.assertThrows(AuthenticationException.class, () ->
                provider.authenticate(ctx, entry1.getKey().getUser(), entry1.getKey().getHost(), scramble, seed,
                        entry1.getValue()));

        // purely loaded from image
        AuthenticationMgr imageManager = new AuthenticationMgr();
        imageManager.loadV2(finalImage.getMetaBlockReader());

        Assert.assertTrue(imageManager.doesUserExist(testUser));
        Assert.assertTrue(imageManager.doesUserExist(testUserWithIp));
        bestUser = followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1");
        Assert.assertEquals(bestUser.getKey(), testUserWithIp);
        Map.Entry<UserIdentity, UserAuthenticationInfo> entry2 =
                followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.2");
        Assert.assertThrows(AuthenticationException.class, () ->
                provider.authenticate(ctx, entry2.getKey().getUser(), entry2.getKey().getHost(), scramble, seed,
                        entry2.getValue()));
    }

    @Test
    public void testCreateUserWithDefaultRole() throws Exception {
        AuthenticationMgr masterManager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        AuthorizationMgr authorizationManager = ctx.getGlobalStateMgr().getAuthorizationMgr();

        String sql = "create role test_r1";
        CreateRoleStmt createStmt =
                (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "create role test_r2";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "create role test_r3";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "create user test2 default role test_r1";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        Set<Long> s = authorizationManager.getDefaultRoleIdsByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test2", "%"));
        Long roleId = authorizationManager.getRoleIdByNameAllowNull("test_r1");
        Assert.assertEquals(1, s.size());
        Assert.assertTrue(s.contains(roleId));

        sql = "create user test_u3 default role test_r1, test_r2";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        s = authorizationManager.getDefaultRoleIdsByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_u3", "%"));
        Assert.assertEquals(2, s.size());

        roleId = authorizationManager.getRoleIdByNameAllowNull("test_r1");
        Assert.assertTrue(s.contains(roleId));
        roleId = authorizationManager.getRoleIdByNameAllowNull("test_r2");
        Assert.assertTrue(s.contains(roleId));

        sql = "alter user test_u3 default role test_r1";
        SetDefaultRoleStmt setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        s = authorizationManager.getDefaultRoleIdsByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_u3", "%"));
        Assert.assertEquals(1, s.size());
        roleId = authorizationManager.getRoleIdByNameAllowNull("test_r1");
        Assert.assertTrue(s.contains(roleId));

        sql = "alter user test_u3 default role test_r1, test_r2";
        setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        s = authorizationManager.getDefaultRoleIdsByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_u3", "%"));
        Assert.assertEquals(2, s.size());

        sql = "alter user test_u3 default role test_r3";
        try {
            setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Role test_r3 is not granted to 'test_u3'@'%'.",
                    e.getMessage());
        }

        sql = "alter user test_u3 default role NONE";
        setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        s = authorizationManager.getDefaultRoleIdsByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_u3", "%"));
        Assert.assertEquals(0, s.size());

        sql = "alter user test_u3 default role ALL";
        setDefaultRoleStmt = (SetDefaultRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetDefaultRoleExecutor.execute(setDefaultRoleStmt, ctx);
        s = authorizationManager.getDefaultRoleIdsByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("test_u3", "%"));
        Assert.assertEquals(2, s.size());
    }

    @Test
    public void testCreateUserPersistWithProperties() throws Exception {
        AuthenticationMgr masterManager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        String user = "user123";

        // 1. create empty image
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getImageWriter());

        // 2. create user with properties
        String sql = "create user user123 properties (\"session.tx_visible_wait_timeout\" = \"100\", " +
                "\"session.metadata_collect_query_timeout\" = \"200\")";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);
        UserProperty userProperty = masterManager.getUserProperty(user);
        Assert.assertEquals(2, userProperty.getSessionVariables().size());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("tx_visible_wait_timeout"));
        Assert.assertEquals("200", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));

        // 2.1. create user with default catalog or database, we expect it will be failed
        sql = "create user user2 properties (\"default_session_catalog\" = \"my_catalog\")";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        try {
            masterManager.createUser(stmt);
            Assert.assertEquals(1, 2);
        } catch (Exception e) {
        }

        // 3. save final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getImageWriter());

        // 4 verify replay...

        // 4.1 load empty image
        AuthenticationMgr followerManager = new AuthenticationMgr();
        SRMetaBlockReader srMetaBlockReader = new SRMetaBlockReaderV2(emptyImage.getJsonReader());
        followerManager.loadV2(srMetaBlockReader);

        // 4.2 replay update user property
        CreateUserInfo createUserInfo = (CreateUserInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_USER_V2);
        followerManager.replayCreateUser(
                createUserInfo.getUserIdentity(),
                createUserInfo.getAuthenticationInfo(),
                createUserInfo.getUserProperty(),
                createUserInfo.getUserPrivilegeCollection(),
                createUserInfo.getPluginId(),
                createUserInfo.getPluginVersion());
        userProperty = followerManager.getUserProperty(user);
        Assert.assertEquals(2, userProperty.getSessionVariables().size());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("tx_visible_wait_timeout"));
        Assert.assertEquals("200", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));

        // 4.3 verify final image
        AuthenticationMgr finalManager = new AuthenticationMgr();
        srMetaBlockReader = new SRMetaBlockReaderV2(finalImage.getJsonReader());
        finalManager.loadV2(srMetaBlockReader);
        userProperty = finalManager.getUserProperty(user);
        Assert.assertEquals(2, userProperty.getSessionVariables().size());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("tx_visible_wait_timeout"));
        Assert.assertEquals("200", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));
    }

    @Test
    public void testDropAlterUser() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        UserIdentity testUserWithIp = UserIdentity.createAnalyzedUserIdentWithIp("test", "10.1.1.1");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");

        AuthenticationMgr manager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        Assert.assertThrows(AuthenticationException.class, () -> AuthenticationHandler.authenticate(ctx,
                testUser.getUser(), testUser.getHost(), scramble, seed));
        Assert.assertFalse(manager.doesUserExist(testUser));
        Assert.assertFalse(manager.doesUserExist(testUserWithIp));

        String sql = "create user test;";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertTrue(manager.doesUserExist(testUser));
        sql = "create user 'test'@'10.1.1.1' identified by 'abc'";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertThrows(AuthenticationException.class, () -> AuthenticationHandler.authenticate(ctx,
                testUser.getUser(), testUser.getHost(), scramble, seed));
        Assert.assertTrue(manager.doesUserExist(testUserWithIp));

        sql = "alter user test identified by 'abc'";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
        Assert.assertEquals(testUser,
                AuthenticationHandler.authenticate(ctx, testUser.getUser(), testUser.getHost(), scramble, seed));
        Assert.assertTrue(manager.doesUserExist(testUser));

        StatementBase dropStmt = UtFrameUtils.parseStmtWithNewParser("drop user test", ctx);
        DDLStmtExecutor.execute(dropStmt, ctx);
        Assert.assertThrows(AuthenticationException.class, () -> AuthenticationHandler.authenticate(ctx,
                testUser.getUser(), testUser.getHost(), scramble, seed));
        Assert.assertFalse(manager.doesUserExist(testUser));

        // can drop twice
        DDLStmtExecutor.execute(dropStmt, ctx);

        // can alter twice
        DDLStmtExecutor.execute(stmt, ctx);

        // still has max connection
        Assert.assertNotEquals(0, manager.getMaxConn("test"));

        sql = "drop user 'test'@'10.1.1.1' ";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, ctx), ctx);
        DDLStmtExecutor.execute(dropStmt, ctx);
        Assert.assertFalse(manager.doesUserExist(testUserWithIp));

        // can't get max connection after all test user are dropped
        Assert.assertThrows(SemanticException.class, () -> manager.getMaxConn("test"));
    }

    @Test
    public void testAlterPersistWithProperties() throws Exception {
        AuthenticationMgr masterManager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        CatalogMgr catalogMgr = ctx.getGlobalStateMgr().getCatalogMgr();

        // 1. create empty image
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getImageWriter());

        // create two catalogs
        String catalogName = "catalog";
        String createExternalCatalog = "CREATE EXTERNAL CATALOG catalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
        CreateCatalogStmt createCatalogStmt = (CreateCatalogStmt) UtFrameUtils.parseStmtWithNewParser(createExternalCatalog, ctx);
        catalogMgr.createCatalog(createCatalogStmt);

        String newCatalogName = "new_catalog";
        createExternalCatalog = "CREATE EXTERNAL CATALOG new_catalog " + "PROPERTIES( " + "   \"type\"=\"hive\", " +
                "   \"hive.metastore.uris\"=\"thrift://xx.xx.xx.xx:9083\" " + ");";
        createCatalogStmt = (CreateCatalogStmt) UtFrameUtils.parseStmtWithNewParser(createExternalCatalog, ctx);
        catalogMgr.createCatalog(createCatalogStmt);

        // 2. create user with properties
        String sql = "create user user1 default role root properties (\"max_user_connections\" = \"100\", " +
                "\"session.metadata_collect_query_timeout\" = \"100\", \"session.catalog\" = \"catalog\")";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(createUserStmt);
        UserProperty userProperty = masterManager.getUserProperty("user1");
        Assert.assertEquals(1, userProperty.getSessionVariables().size());
        Assert.assertEquals(100, userProperty.getMaxConn());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));
        Assert.assertEquals(catalogName, userProperty.getCatalog());

        // 3. alter user with properties
        sql = "alter user user1 set properties (\"max_user_connections\" = \"200\", \"catalog\" = \"new_catalog\")";
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.updateUserProperty(setUserPropertyStmt.getUser(), setUserPropertyStmt.getPropertyPairList());
        Assert.assertEquals(1, userProperty.getSessionVariables().size());
        Assert.assertEquals(200, userProperty.getMaxConn());
        Assert.assertEquals(newCatalogName, userProperty.getCatalog());
        Assert.assertTrue(userProperty.getSessionVariables().get("metadata_collect_query_timeout").equals("100"));

        // 4. save final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getImageWriter());

        // 5 verify replay...

        // 5.1 load empty image
        AuthenticationMgr followerManager = new AuthenticationMgr();
        SRMetaBlockReader srMetaBlockReader = new SRMetaBlockReaderV2(emptyImage.getJsonReader());
        followerManager.loadV2(srMetaBlockReader);

        // 5.2 replay create user
        CreateUserInfo createUserInfo = (CreateUserInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_USER_V2);
        followerManager.replayCreateUser(
                createUserInfo.getUserIdentity(),
                createUserInfo.getAuthenticationInfo(),
                createUserInfo.getUserProperty(),
                createUserInfo.getUserPrivilegeCollection(),
                createUserInfo.getPluginId(),
                createUserInfo.getPluginVersion());
        userProperty = followerManager.getUserProperty("user1");
        Assert.assertEquals(1, userProperty.getSessionVariables().size());
        Assert.assertEquals(100, userProperty.getMaxConn());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));
        Assert.assertEquals(catalogName, userProperty.getCatalog());

        // 5.2 replay alter user
        UserPropertyInfo propertyInfo =
                (UserPropertyInfo) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PROP_V3);
        followerManager.replayUpdateUserProperty(propertyInfo);
        userProperty = followerManager.getUserProperty("user1");
        Assert.assertEquals(1, userProperty.getSessionVariables().size());
        Assert.assertEquals(200, userProperty.getMaxConn());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));
        Assert.assertEquals(newCatalogName, userProperty.getCatalog());

        // 4.3 verify final image
        AuthenticationMgr finalManager = new AuthenticationMgr();
        srMetaBlockReader = new SRMetaBlockReaderV2(finalImage.getJsonReader());
        finalManager.loadV2(srMetaBlockReader);
        userProperty = finalManager.getUserProperty("user1");
        Assert.assertEquals(1, userProperty.getSessionVariables().size());
        Assert.assertEquals(200, userProperty.getMaxConn());
        Assert.assertEquals("100", userProperty.getSessionVariables().get("metadata_collect_query_timeout"));
        Assert.assertEquals(newCatalogName, userProperty.getCatalog());
    }

    @Test
    public void testDropAlterPersist() throws Exception {
        UserIdentity testUser = UserIdentity.createAnalyzedUserIdentWithIp("test", "%");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");

        AuthenticationMgr masterManager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        Assert.assertFalse(masterManager.doesUserExist(testUser));
        Assert.assertTrue(masterManager.doesUserExist(UserIdentity.ROOT));

        // 1. create empty image
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getImageWriter());

        // 2. create user
        String sql = "create user test";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(createStmt);
        Assert.assertTrue(masterManager.doesUserExist(testUser));
        Assert.assertEquals(testUser, AuthenticationHandler.authenticate(ctx,
                testUser.getUser(), "10.1.1.1", new byte[0], null));

        // 3. alter user
        sql = "alter user test identified by 'abc'";
        AlterUserStmt alterUserStmt = (AlterUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.alterUser(alterUserStmt.getUserIdentity(), alterUserStmt.getAuthenticationInfo(), null);
        Assert.assertEquals(testUser, AuthenticationHandler.authenticate(ctx,
                testUser.getUser(), "10.1.1.1", scramble, seed));

        // 3.1 update user property
        sql = "set property for 'test' 'max_user_connections' = '555'";
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.updateUserProperty("test", setUserPropertyStmt.getPropertyPairList());
        Assert.assertEquals(555, masterManager.getMaxConn("test"));

        // 4. save image after alter
        UtFrameUtils.PseudoImage alterImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(alterImage.getImageWriter());

        // 5. drop user
        sql = "drop user test";
        DropUserStmt dropStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.dropUser(dropStmt);
        Assert.assertFalse(masterManager.doesUserExist(testUser));

        // 6. save final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getImageWriter());

        // 7. verify replay...
        AuthenticationMgr followerManager = new AuthenticationMgr();
        followerManager.loadV2(emptyImage.getMetaBlockReader());

        Assert.assertFalse(followerManager.doesUserExist(testUser));
        // 7.1 replay create user
        CreateUserInfo createInfo = (CreateUserInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_USER_V2);
        followerManager.replayCreateUser(
                createInfo.getUserIdentity(), createInfo.getAuthenticationInfo(), createInfo.getUserProperty(),
                createInfo.getUserPrivilegeCollection(), createInfo.getPluginId(), createInfo.getPluginVersion());
        Assert.assertTrue(followerManager.doesUserExist(testUser));

        Assert.assertEquals(testUser, followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1").getKey());
        // 7.2 replay alter user
        AlterUserInfo alterInfo = (AlterUserInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_USER_V2);
        followerManager.replayAlterUser(alterInfo.getUserIdentity(), alterInfo.getAuthenticationInfo(), null);
        Assert.assertEquals(testUser, followerManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1").getKey());
        // 7.2.1 replay update user property
        UserPropertyInfo userPropertyInfo = (UserPropertyInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PROP_V3);
        followerManager.replayUpdateUserProperty(userPropertyInfo);
        Assert.assertEquals(555, followerManager.getMaxConn("test"));
        // 7.3 replay drop user
        UserIdentity dropInfo = (UserIdentity)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_USER_V3);
        followerManager.replayDropUser(dropInfo);
        Assert.assertFalse(followerManager.doesUserExist(testUser));
        Assert.assertTrue(followerManager.doesUserExist(UserIdentity.ROOT));

        // 8. verify alter image
        AuthenticationMgr alterManager = new AuthenticationMgr();
        alterManager.loadV2(alterImage.getMetaBlockReader());

        Assert.assertTrue(alterManager.doesUserExist(testUser));
        Assert.assertEquals(testUser, alterManager.getBestMatchedUserIdentity(testUser.getUser(), "10.1.1.1").getKey());
        Assert.assertTrue(alterManager.doesUserExist(UserIdentity.ROOT));

        // 9. verify final image
        AuthenticationMgr finalManager = new AuthenticationMgr();
        finalManager.loadV2(finalImage.getMetaBlockReader());
        Assert.assertFalse(finalManager.doesUserExist(testUser));
        Assert.assertTrue(finalManager.doesUserExist(UserIdentity.ROOT));
    }

    @Test
    public void testUserWithHost() throws Exception {
        UserIdentity testUserWithHost = UserIdentity.createAnalyzedUserIdentWithDomain("user_with_host", "host01");
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");
        AuthenticationMgr manager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        Assert.assertFalse(manager.doesUserExist(testUserWithHost));

        // create a user with host name
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_host@['host01'] identified by 'abc'", ctx), ctx);
        Assert.assertTrue(manager.doesUserExist(testUserWithHost));
        Assert.assertEquals(new HashSet<String>(Arrays.asList("host01")), manager.getAllHostnames());
        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.1", scramble, seed));

        // update host -> ip list
        Map<String, Set<String>> hostToIpList = new HashMap<>();
        hostToIpList.put("host01", new HashSet<>(Arrays.asList("10.1.1.2")));
        manager.setHostnameToIpSet(hostToIpList);

        // check login
        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.1", scramble, seed));
        Assert.assertEquals(testUserWithHost,
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.2", scramble, seed));
        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.3", scramble, seed));

        // update host -> ip list
        hostToIpList = new HashMap<>();
        hostToIpList.put("host01", new HashSet<>(Arrays.asList("10.1.1.1", "10.1.1.2")));
        hostToIpList.put("host02", new HashSet<>(Arrays.asList("10.1.1.1")));
        manager.setHostnameToIpSet(hostToIpList);

        // check login
        Assert.assertEquals(testUserWithHost,
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.1", scramble, seed));
        Assert.assertEquals(testUserWithHost,
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.2", scramble, seed));
        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.3", scramble, seed));

        // create a user with ip
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_host@'10.1.1.1' identified by 'abc'", ctx), ctx);
        UserIdentity testUserWithIp = UserIdentity.createAnalyzedUserIdentWithIp("user_with_host", "10.1.1.1");
        Assert.assertTrue(manager.doesUserExist(testUserWithHost));
        Assert.assertTrue(manager.doesUserExist(testUserWithIp));

        // login matches ip
        Assert.assertEquals(testUserWithIp,
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.1", scramble, seed));

        // create a user with %
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user user_with_host@'%' identified by 'def'", ctx), ctx);
        UserIdentity testUserWithAll = UserIdentity.createAnalyzedUserIdentWithIp("user_with_host", "%");
        byte[] scramble2 = MysqlPassword.scramble(seed, "def");
        Assert.assertTrue(manager.doesUserExist(testUserWithHost));
        Assert.assertTrue(manager.doesUserExist(testUserWithIp));
        Assert.assertTrue(manager.doesUserExist(testUserWithAll));

        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.1", scramble2, seed));
        Assert.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.2", scramble2, seed));

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter user user_with_host@'%' identified by 'abc'", ctx), ctx);
        Assert.assertEquals(testUserWithIp,
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.1", scramble, seed));
        Assert.assertEquals(testUserWithHost,
                AuthenticationHandler.authenticate(ctx, "user_with_host", "10.1.1.2", scramble, seed));
    }

    @Test
    public void testSortUserIdentity() throws Exception {
        AuthenticationMgr manager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user sort_user@['host01'] identified by 'abc'", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user sort_user@'10.1.1.2' identified by 'abc'", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user sort_user@'10.1.1.1' identified by 'abc'", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create user sort_user@'%' identified by 'abc'", ctx), ctx);
        List<String> l = new ArrayList<>();
        Iterator<Map.Entry<UserIdentity, UserAuthenticationInfo>> it = manager.userToAuthenticationInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<UserIdentity, UserAuthenticationInfo> entry = it.next();
            UserIdentity userIdentity = entry.getKey();
            if (userIdentity.getUser().equals("sort_user")) {
                l.add(userIdentity.toString());
            }
        }
        Assert.assertEquals(Arrays.asList(
                "'sort_user'@'10.1.1.1'", "'sort_user'@'10.1.1.2'", "'sort_user'@['host01']", "'sort_user'@'%'"), l);
    }

    @Test
    public void testIsRoleInSession() throws Exception {
        AuthenticationMgr masterManager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        AuthorizationMgr authorizationManager = ctx.getGlobalStateMgr().getAuthorizationMgr();

        String sql = "create role test_in_role_r1";
        CreateRoleStmt createStmt =
                (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "create role test_in_role_r2";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "create role test_in_role_r3";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "create role test_in_role_r4";
        createStmt = (CreateRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.createRole(createStmt);

        sql = "grant test_in_role_r3 to role test_in_role_r2";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grantRole(grantRoleStmt);

        sql = "grant test_in_role_r2 to role test_in_role_r1";
        grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        authorizationManager.grantRole(grantRoleStmt);

        sql = "create user test_in_role_u1 default role test_in_role_r1";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);

        ctx.setCurrentUserIdentity(new UserIdentity("test_in_role_u1", "%"));
        ctx.setCurrentRoleIds(new UserIdentity("test_in_role_u1", "%"));

        Assert.assertTrue(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r1")).getBoolean());
        Assert.assertTrue(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r2")).getBoolean());
        Assert.assertTrue(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r3")).getBoolean());

        Assert.assertFalse(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r4")).getBoolean());

        sql = "create user test_in_role_u2 default role test_in_role_r2";
        stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.createUser(stmt);

        ctx.setCurrentUserIdentity(new UserIdentity("test_in_role_u2", "%"));
        ctx.setCurrentRoleIds(new UserIdentity("test_in_role_u2", "%"));

        Assert.assertFalse(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r1")).getBoolean());
        Assert.assertTrue(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r2")).getBoolean());
        Assert.assertTrue(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r3")).getBoolean());

        ctx.setCurrentRoleIds(new HashSet<>());

        Assert.assertFalse(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r1")).getBoolean());
        Assert.assertFalse(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r2")).getBoolean());
        Assert.assertFalse(ScalarOperatorFunctions.isRoleInSession(
                ConstantOperator.createVarchar("test_in_role_r3")).getBoolean());

        sql = "select is_role_in_session(v1) from (select 1 as v1) t";
        try {
            stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("IS_ROLE_IN_SESSION currently only supports constant parameters"));
        }

        sql = "select is_role_in_session(\"a\", \"b\") from (select 1 as v1) t";
        try {
            stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            Assert.fail();
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("IS_ROLE_IN_SESSION currently only supports a single parameter"));
        }
    }

    @Test
    public void testSetUserPropertyPersist() throws Exception {
        AuthenticationMgr masterManager = ctx.getGlobalStateMgr().getAuthenticationMgr();
        Assert.assertTrue(masterManager.doesUserExist(UserIdentity.ROOT));

        // 1. create empty image
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getImageWriter());

        // 2. update user property
        String sql = "set property for 'root' 'max_user_connections' = '555'";
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        masterManager.updateUserProperty("root", setUserPropertyStmt.getPropertyPairList());
        Assert.assertEquals(555, masterManager.getMaxConn("root"));

        // 3. save final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getImageWriter());

        // 4 verify replay...

        // 4.1 load empty image
        AuthenticationMgr followerManager = new AuthenticationMgr();
        SRMetaBlockReader srMetaBlockReader = new SRMetaBlockReaderV2(emptyImage.getJsonReader());
        followerManager.loadV2(srMetaBlockReader);

        // 4.2 replay update user property
        UserPropertyInfo userPropertyInfo = (UserPropertyInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_USER_PROP_V3);
        followerManager.replayUpdateUserProperty(userPropertyInfo);
        Assert.assertEquals(555, followerManager.getMaxConn("root"));

        // 4.3 verify final image
        AuthenticationMgr finalManager = new AuthenticationMgr();
        srMetaBlockReader = new SRMetaBlockReaderV2(finalImage.getJsonReader());
        finalManager.loadV2(srMetaBlockReader);
        Assert.assertTrue(finalManager.doesUserExist(UserIdentity.ROOT));
        Assert.assertEquals(555, finalManager.getMaxConn("root"));
    }
}
