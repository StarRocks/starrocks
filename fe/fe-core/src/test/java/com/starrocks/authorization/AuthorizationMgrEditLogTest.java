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

package com.starrocks.authorization;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterRoleStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AuthorizationMgrEditLogTest {
    private AuthorizationMgr authorizationMgr;
    private AuthenticationMgr authenticationMgr;
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Grant Privilege Tests ====================

    @Test
    public void testGrantToUserNormalCase() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_grant";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(),
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());

        // 2. Prepare grant statement
        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO '" + userName + "'@'%'";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);

        // 3. Execute grantToUser operation (master side)
        authorizationMgr.grant(grantStmt);

        // 4. Verify master state
        UserPrivilegeCollectionV2 userCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(userCollection);
        
        // Verify the privilege content is correct
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        Assertions.assertTrue(userCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "User should have SELECT privilege on ALL TABLES IN ALL DATABASES");

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        AuthenticationMgr followerAuthMgr2 = new AuthenticationMgr();

        // First create the user in follower
        followerAuthMgr2.replayCreateUser(userIdentity,
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userIdentity.getUser()),
                new UserPrivilegeCollectionV2(), (short) 0, (short) 0);

        // Replay the grant operation
        UserPrivilegeCollectionInfo replayInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                replayInfo.getUserIdentity(),
                replayInfo.getPrivilegeCollection(),
                replayInfo.getPluginId(),
                replayInfo.getPluginVersion());

        // 6. Verify follower state is consistent with master
        UserPrivilegeCollectionV2 followerCollection = followerAuthMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(followerCollection);
        
        // Verify the privilege content is correct in follower
        Assertions.assertTrue(followerCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "Follower user should have SELECT privilege on ALL TABLES IN ALL DATABASES");
    }

    @Test
    public void testGrantToUserEditLogException() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_grant_exception";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(),
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());

        // 2. Prepare grant statement
        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO '" + userName + "'@'%'";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);

        // 3. Mock EditLog.logUpdateUserPrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateUserPrivilege(any(UserPrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserPrivilegeCollectionV2 initialCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Set<Long> initialRoles = new HashSet<>(initialCollection.getAllRoles());
        Set<Long> initialDefaultRoles = new HashSet<>(initialCollection.getDefaultRoleIds());
        
        // Verify initial state has no SELECT privilege
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        boolean initialHasSelectPrivilege = initialCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);

        // 4. Execute grant operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.grant(grantStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserPrivilegeCollectionV2 unchangedCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(unchangedCollection);
        
        // Verify roles have not changed
        Assertions.assertEquals(initialRoles, unchangedCollection.getAllRoles(),
                "User roles should remain unchanged after EditLog exception");
        
        // Verify default roles have not changed
        Assertions.assertEquals(initialDefaultRoles, unchangedCollection.getDefaultRoleIds(),
                "User default roles should remain unchanged after EditLog exception");
        
        // Verify privileges have not changed (should still not have SELECT privilege)
        boolean unchangedHasSelectPrivilege = unchangedCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);
        Assertions.assertEquals(initialHasSelectPrivilege, unchangedHasSelectPrivilege,
                "User privileges should remain unchanged after EditLog exception");
    }

    @Test
    public void testGrantToRoleNormalCase() throws Exception {
        // 1. Prepare test data - create a role first
        String roleName = "test_role_grant";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare grant statement
        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE " + roleName;
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);

        // 3. Execute grantToRole operation (master side)
        authorizationMgr.grant(grantStmt);

        // 4. Verify master state
        RolePrivilegeCollectionV2 roleCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(roleCollection);
        
        // Verify the privilege content is correct
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        Assertions.assertTrue(roleCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "Role should have SELECT privilege on ALL TABLES IN ALL DATABASES");

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // First create the role in follower
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);

        // Replay the grant operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(replayInfo);

        // 6. Verify follower state is consistent with master
        RolePrivilegeCollectionV2 followerCollection = followerAuthMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(followerCollection);
        
        // Verify the privilege content is correct in follower
        Assertions.assertTrue(followerCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "Follower role should have SELECT privilege on ALL TABLES IN ALL DATABASES");
    }

    @Test
    public void testGrantToRoleEditLogException() throws Exception {
        // 1. Prepare test data - create a role first
        String roleName = "test_role_grant_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare grant statement
        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE " + roleName;
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);

        // 3. Mock EditLog.logUpdateRolePrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateRolePrivilege(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        RolePrivilegeCollectionV2 initialCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        
        // Verify initial state has no SELECT privilege
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        boolean initialHasSelectPrivilege = initialCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);

        // 4. Execute grant operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.grant(grantStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        RolePrivilegeCollectionV2 unchangedCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(unchangedCollection);
        
        // Verify privileges have not changed
        boolean unchangedHasSelectPrivilege = unchangedCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);
        Assertions.assertEquals(initialHasSelectPrivilege, unchangedHasSelectPrivilege,
                "Role privileges should remain unchanged after EditLog exception");
    }

    // ==================== Revoke Privilege Tests ====================

    @Test
    public void testRevokeFromUserNormalCase() throws Exception {
        // 1. Prepare test data - create a user and grant privilege first
        String userName = "test_user_revoke";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(),
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());

        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO '" + userName + "'@'%'";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);
        authorizationMgr.grant(grantStmt);

        // 2. Prepare revoke statement
        String revokeSql = "REVOKE SELECT ON ALL TABLES IN ALL DATABASES FROM '" + userName + "'@'%'";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx);

        // 3. Execute revokeFromUser operation (master side)
        authorizationMgr.revoke(revokeStmt);

        // 4. Verify master state
        UserPrivilegeCollectionV2 userCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(userCollection);
        
        // Verify the privilege has been revoked (should not have SELECT privilege)
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        Assertions.assertFalse(userCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "User should not have SELECT privilege on ALL TABLES IN ALL DATABASES after revoke");

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        AuthenticationMgr followerAuthMgr2 = new AuthenticationMgr();

        // First create the user and grant in follower
        followerAuthMgr2.replayCreateUser(userIdentity,
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userIdentity.getUser()),
                new UserPrivilegeCollectionV2(), (short) 0, (short) 0);

        // Replay grant
        UserPrivilegeCollectionInfo grantInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                grantInfo.getUserIdentity(),
                grantInfo.getPrivilegeCollection(),
                grantInfo.getPluginId(),
                grantInfo.getPluginVersion());

        // Replay the revoke operation
        UserPrivilegeCollectionInfo replayInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                replayInfo.getUserIdentity(),
                replayInfo.getPrivilegeCollection(),
                replayInfo.getPluginId(),
                replayInfo.getPluginVersion());

        // 6. Verify follower state is consistent with master
        UserPrivilegeCollectionV2 followerCollection = followerAuthMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(followerCollection);
        
        // Verify the privilege has been revoked in follower
        Assertions.assertFalse(followerCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "Follower user should not have SELECT privilege on ALL TABLES IN ALL DATABASES after revoke");
    }

    @Test
    public void testRevokeFromUserEditLogException() throws Exception {
        // 1. Prepare test data - create a user and grant privilege first
        String userName = "test_user_revoke_exception";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(),
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());

        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO '" + userName + "'@'%'";
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);
        authorizationMgr.grant(grantStmt);

        // 2. Prepare revoke statement
        String revokeSql = "REVOKE SELECT ON ALL TABLES IN ALL DATABASES FROM '" + userName + "'@'%'";
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx);

        // 3. Mock EditLog.logUpdateUserPrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateUserPrivilege(any(UserPrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserPrivilegeCollectionV2 initialCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Set<Long> initialRoles = new HashSet<>(initialCollection.getAllRoles());
        Set<Long> initialDefaultRoles = new HashSet<>(initialCollection.getDefaultRoleIds());
        
        // Verify initial state has SELECT privilege (before revoke)
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        boolean initialHasSelectPrivilege = initialCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);
        Assertions.assertTrue(initialHasSelectPrivilege, "Initial state should have SELECT privilege");

        // 4. Execute revoke operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.revoke(revokeStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserPrivilegeCollectionV2 unchangedCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(unchangedCollection);
        
        // Verify roles have not changed
        Assertions.assertEquals(initialRoles, unchangedCollection.getAllRoles(),
                "User roles should remain unchanged after EditLog exception");
        
        // Verify default roles have not changed
        Assertions.assertEquals(initialDefaultRoles, unchangedCollection.getDefaultRoleIds(),
                "User default roles should remain unchanged after EditLog exception");
        
        // Verify privileges have not changed (should still have SELECT privilege)
        boolean unchangedHasSelectPrivilege = unchangedCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);
        Assertions.assertEquals(initialHasSelectPrivilege, unchangedHasSelectPrivilege,
                "User privileges should remain unchanged after EditLog exception");
    }

    @Test
    public void testRevokeFromRoleNormalCase() throws Exception {
        // 1. Prepare test data - create a role and grant privilege first
        String roleName = "test_role_revoke";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE " + roleName;
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);
        authorizationMgr.grant(grantStmt);

        // 2. Prepare revoke statement
        String revokeSql = "REVOKE SELECT ON ALL TABLES IN ALL DATABASES FROM ROLE " + roleName;
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx);

        // 3. Execute revokeFromRole operation (master side)
        authorizationMgr.revoke(revokeStmt);

        // 4. Verify master state
        RolePrivilegeCollectionV2 roleCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(roleCollection);
        
        // Verify the privilege has been revoked (should not have SELECT privilege)
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        Assertions.assertFalse(roleCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "Role should not have SELECT privilege on ALL TABLES IN ALL DATABASES after revoke");

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // First create the role and grant in follower
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);

        // Replay grant
        RolePrivilegeCollectionInfo grantInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(grantInfo);

        // Replay the revoke operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(replayInfo);

        // 6. Verify follower state is consistent with master
        RolePrivilegeCollectionV2 followerCollection = followerAuthMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(followerCollection);
        
        // Verify the privilege has been revoked in follower
        Assertions.assertFalse(followerCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb),
                "Follower role should not have SELECT privilege on ALL TABLES IN ALL DATABASES after revoke");
    }

    @Test
    public void testRevokeFromRoleEditLogException() throws Exception {
        // 1. Prepare test data - create a role and grant privilege first
        String roleName = "test_role_revoke_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        String grantSql = "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE " + roleName;
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(grantSql, ctx);
        authorizationMgr.grant(grantStmt);

        // 2. Prepare revoke statement
        String revokeSql = "REVOKE SELECT ON ALL TABLES IN ALL DATABASES FROM ROLE " + roleName;
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx);

        // 3. Mock EditLog.logUpdateRolePrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateRolePrivilege(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        RolePrivilegeCollectionV2 initialCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        
        // Verify initial state has SELECT privilege (before revoke)
        TablePEntryObject allTablesInAllDb = new TablePEntryObject(
                PrivilegeBuiltinConstants.ALL_DATABASES_UUID, PrivilegeBuiltinConstants.ALL_TABLES_UUID);
        boolean initialHasSelectPrivilege = initialCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);
        Assertions.assertTrue(initialHasSelectPrivilege, "Initial state should have SELECT privilege");

        // 4. Execute revoke operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.revoke(revokeStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        RolePrivilegeCollectionV2 unchangedCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(unchangedCollection);
        
        // Verify privileges have not changed (should still have SELECT privilege)
        boolean unchangedHasSelectPrivilege = unchangedCollection.check(ObjectType.TABLE, PrivilegeType.SELECT, allTablesInAllDb);
        Assertions.assertEquals(initialHasSelectPrivilege, unchangedHasSelectPrivilege,
                "Role privileges should remain unchanged after EditLog exception");
    }

    // ==================== Grant Role Tests ====================

    @Test
    public void testGrantRoleToUserNormalCase() throws Exception {
        // 1. Prepare test data - create a user and role first
        String userName = "test_user_role";
        String createUserSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        authenticationMgr.createUser(createUserStmt);
        UserIdentity userIdentity = new UserIdentity(createUserStmt.getUser().getUser(),
                createUserStmt.getUser().getHost(), createUserStmt.getUser().isDomain());

        String roleName = "test_role_user";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare grant role statement
        String grantRoleSql = "GRANT " + roleName + " TO '" + userName + "'@'%'";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);

        // 3. Execute grantRoleToUser operation (master side)
        authorizationMgr.grantRole(grantRoleStmt);

        // 4. Verify master state
        UserPrivilegeCollectionV2 userCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(userCollection);
        Long roleId = authorizationMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(roleId);
        Assertions.assertTrue(userCollection.getAllRoles().contains(roleId));

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        AuthenticationMgr followerAuthMgr2 = new AuthenticationMgr();

        // First create the user and role in follower
        followerAuthMgr2.replayCreateUser(userIdentity,
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userIdentity.getUser()),
                new UserPrivilegeCollectionV2(), (short) 0, (short) 0);

        // Replay role creation
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);

        // Replay the grant role operation
        UserPrivilegeCollectionInfo replayInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                replayInfo.getUserIdentity(),
                replayInfo.getPrivilegeCollection(),
                replayInfo.getPluginId(),
                replayInfo.getPluginVersion());

        // 6. Verify follower state is consistent with master
        UserPrivilegeCollectionV2 followerCollection = followerAuthMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(followerCollection);
        Long followerRoleId = followerAuthMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(followerRoleId);
        Assertions.assertTrue(followerCollection.getAllRoles().contains(followerRoleId));
    }

    @Test
    public void testGrantRoleToUserEditLogException() throws Exception {
        // 1. Prepare test data - create a user and role first
        String userName = "test_user_role_exception";
        String createUserSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        authenticationMgr.createUser(createUserStmt);
        UserIdentity userIdentity = new UserIdentity(createUserStmt.getUser().getUser(),
                createUserStmt.getUser().getHost(), createUserStmt.getUser().isDomain());

        String roleName = "test_role_user_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare grant role statement
        String grantRoleSql = "GRANT " + roleName + " TO '" + userName + "'@'%'";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);

        // 3. Mock EditLog.logUpdateUserPrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateUserPrivilege(any(UserPrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserPrivilegeCollectionV2 initialCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Set<Long> initialRoles = new HashSet<>(initialCollection.getAllRoles());

        // 4. Execute grantRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.grantRole(grantRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserPrivilegeCollectionV2 unchangedCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(unchangedCollection);
        Assertions.assertEquals(initialRoles, unchangedCollection.getAllRoles());
    }

    @Test
    public void testGrantRoleToRoleNormalCase() throws Exception {
        // 1. Prepare test data - create two roles first
        String parentRoleName = "test_parent_role";
        CreateRoleStmt createParentRoleStmt = new CreateRoleStmt(Collections.singletonList(parentRoleName), false, null);
        authorizationMgr.createRole(createParentRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(parentRoleName));

        String childRoleName = "test_child_role";
        CreateRoleStmt createChildRoleStmt = new CreateRoleStmt(Collections.singletonList(childRoleName), false, null);
        authorizationMgr.createRole(createChildRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(childRoleName));

        // 2. Prepare grant role statement
        String grantRoleSql = "GRANT " + parentRoleName + " TO ROLE " + childRoleName;
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);

        // 3. Execute grantRoleToRole operation (master side)
        authorizationMgr.grantRole(grantRoleStmt);

        // 4. Verify master state
        RolePrivilegeCollectionV2 childRoleCollection = authorizationMgr.getRolePrivilegeCollection(childRoleName);
        Assertions.assertNotNull(childRoleCollection);
        Long parentRoleId = authorizationMgr.getRoleIdByNameAllowNull(parentRoleName);
        Assertions.assertNotNull(parentRoleId);
        Assertions.assertTrue(childRoleCollection.getParentRoleIds().contains(parentRoleId));

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // First create the roles in follower
        RolePrivilegeCollectionInfo createParentInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createParentInfo);

        RolePrivilegeCollectionInfo createChildInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createChildInfo);

        // Replay the grant role operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(replayInfo);

        // 6. Verify follower state is consistent with master
        RolePrivilegeCollectionV2 followerChildCollection = followerAuthMgr.getRolePrivilegeCollection(childRoleName);
        Assertions.assertNotNull(followerChildCollection);
        Long followerParentRoleId = followerAuthMgr.getRoleIdByNameAllowNull(parentRoleName);
        Assertions.assertNotNull(followerParentRoleId);
        Assertions.assertTrue(followerChildCollection.getParentRoleIds().contains(followerParentRoleId));
    }

    @Test
    public void testGrantRoleToRoleEditLogException() throws Exception {
        // 1. Prepare test data - create two roles first
        String parentRoleName = "test_parent_role_exception";
        CreateRoleStmt createParentRoleStmt = new CreateRoleStmt(Collections.singletonList(parentRoleName), false, null);
        authorizationMgr.createRole(createParentRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(parentRoleName));

        String childRoleName = "test_child_role_exception";
        CreateRoleStmt createChildRoleStmt = new CreateRoleStmt(Collections.singletonList(childRoleName), false, null);
        authorizationMgr.createRole(createChildRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(childRoleName));

        // 2. Prepare grant role statement
        String grantRoleSql = "GRANT " + parentRoleName + " TO ROLE " + childRoleName;
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);

        // 3. Mock EditLog.logUpdateRolePrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateRolePrivilege(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        RolePrivilegeCollectionV2 initialChildCollection = authorizationMgr.getRolePrivilegeCollection(childRoleName);
        Set<Long> initialParentRoles = new HashSet<>(initialChildCollection.getParentRoleIds());

        // 4. Execute grantRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.grantRole(grantRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        RolePrivilegeCollectionV2 unchangedCollection = authorizationMgr.getRolePrivilegeCollection(childRoleName);
        Assertions.assertNotNull(unchangedCollection);
        Assertions.assertEquals(initialParentRoles, unchangedCollection.getParentRoleIds());
    }

    // ==================== Revoke Role Tests ====================

    @Test
    public void testRevokeRoleFromUserNormalCase() throws Exception {
        // 1. Prepare test data - create a user and role, then grant role first
        String userName = "test_user_revoke_role";
        String createUserSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        authenticationMgr.createUser(createUserStmt);
        UserIdentity userIdentity = new UserIdentity(createUserStmt.getUser().getUser(),
                createUserStmt.getUser().getHost(), createUserStmt.getUser().isDomain());

        String roleName = "test_role_revoke_user";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        String grantRoleSql = "GRANT " + roleName + " TO '" + userName + "'@'%'";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // 2. Prepare revoke role statement
        String revokeRoleSql = "REVOKE " + roleName + " FROM '" + userName + "'@'%'";
        RevokeRoleStmt revokeRoleStmt = (RevokeRoleStmt) UtFrameUtils.parseStmtWithNewParser(revokeRoleSql, ctx);

        // 3. Execute revokeRoleFromUser operation (master side)
        authorizationMgr.revokeRole(revokeRoleStmt);

        // 4. Verify master state
        UserPrivilegeCollectionV2 userCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(userCollection);
        Long roleId = authorizationMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(roleId);
        Assertions.assertFalse(userCollection.getAllRoles().contains(roleId));

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        AuthenticationMgr followerAuthMgr2 = new AuthenticationMgr();

        // First create the user and role, then grant in follower
        followerAuthMgr2.replayCreateUser(userIdentity,
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userIdentity.getUser()),
                new UserPrivilegeCollectionV2(), (short) 0, (short) 0);

        // Replay role creation
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);

        // Replay grant role
        UserPrivilegeCollectionInfo grantInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                grantInfo.getUserIdentity(),
                grantInfo.getPrivilegeCollection(),
                grantInfo.getPluginId(),
                grantInfo.getPluginVersion());

        // Replay the revoke role operation
        UserPrivilegeCollectionInfo replayInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                replayInfo.getUserIdentity(),
                replayInfo.getPrivilegeCollection(),
                replayInfo.getPluginId(),
                replayInfo.getPluginVersion());

        // 6. Verify follower state is consistent with master
        UserPrivilegeCollectionV2 followerCollection = followerAuthMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(followerCollection);
        Long followerRoleId = followerAuthMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(followerRoleId);
        Assertions.assertFalse(followerCollection.getAllRoles().contains(followerRoleId));
    }

    @Test
    public void testRevokeRoleFromUserEditLogException() throws Exception {
        // 1. Prepare test data - create a user and role, then grant role first
        String userName = "test_user_revoke_role_exception";
        String createUserSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        authenticationMgr.createUser(createUserStmt);
        UserIdentity userIdentity = new UserIdentity(createUserStmt.getUser().getUser(),
                createUserStmt.getUser().getHost(), createUserStmt.getUser().isDomain());

        String roleName = "test_role_revoke_user_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        String grantRoleSql = "GRANT " + roleName + " TO '" + userName + "'@'%'";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // 2. Prepare revoke role statement
        String revokeRoleSql = "REVOKE " + roleName + " FROM '" + userName + "'@'%'";
        RevokeRoleStmt revokeRoleStmt = (RevokeRoleStmt) UtFrameUtils.parseStmtWithNewParser(revokeRoleSql, ctx);

        // 3. Mock EditLog.logUpdateUserPrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateUserPrivilege(any(UserPrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserPrivilegeCollectionV2 initialCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Set<Long> initialRoles = new HashSet<>(initialCollection.getAllRoles());

        // 4. Execute revokeRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.revokeRole(revokeRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserPrivilegeCollectionV2 unchangedCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(unchangedCollection);
        Assertions.assertEquals(initialRoles, unchangedCollection.getAllRoles());
    }

    @Test
    public void testRevokeRoleFromRoleNormalCase() throws Exception {
        // 1. Prepare test data - create two roles and grant role first
        String parentRoleName = "test_parent_role_revoke";
        CreateRoleStmt createParentRoleStmt = new CreateRoleStmt(Collections.singletonList(parentRoleName), false, null);
        authorizationMgr.createRole(createParentRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(parentRoleName));

        String childRoleName = "test_child_role_revoke";
        CreateRoleStmt createChildRoleStmt = new CreateRoleStmt(Collections.singletonList(childRoleName), false, null);
        authorizationMgr.createRole(createChildRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(childRoleName));

        String grantRoleSql = "GRANT " + parentRoleName + " TO ROLE " + childRoleName;
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // 2. Prepare revoke role statement
        String revokeRoleSql = "REVOKE " + parentRoleName + " FROM ROLE " + childRoleName;
        RevokeRoleStmt revokeRoleStmt = (RevokeRoleStmt) UtFrameUtils.parseStmtWithNewParser(revokeRoleSql, ctx);

        // 3. Execute revokeRoleFromRole operation (master side)
        authorizationMgr.revokeRole(revokeRoleStmt);

        // 4. Verify master state
        RolePrivilegeCollectionV2 childRoleCollection = authorizationMgr.getRolePrivilegeCollection(childRoleName);
        Assertions.assertNotNull(childRoleCollection);
        Long parentRoleId = authorizationMgr.getRoleIdByNameAllowNull(parentRoleName);
        Assertions.assertNotNull(parentRoleId);
        Assertions.assertFalse(childRoleCollection.getParentRoleIds().contains(parentRoleId));

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // First create the roles and grant in follower
        RolePrivilegeCollectionInfo createParentInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createParentInfo);

        RolePrivilegeCollectionInfo createChildInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createChildInfo);

        // Replay grant role
        RolePrivilegeCollectionInfo grantInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(grantInfo);

        // Replay the revoke role operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(replayInfo);

        // 6. Verify follower state is consistent with master
        RolePrivilegeCollectionV2 followerChildCollection = followerAuthMgr.getRolePrivilegeCollection(childRoleName);
        Assertions.assertNotNull(followerChildCollection);
        Long followerParentRoleId = followerAuthMgr.getRoleIdByNameAllowNull(parentRoleName);
        Assertions.assertNotNull(followerParentRoleId);
        Assertions.assertFalse(followerChildCollection.getParentRoleIds().contains(followerParentRoleId));
    }

    @Test
    public void testRevokeRoleFromRoleEditLogException() throws Exception {
        // 1. Prepare test data - create two roles and grant role first
        String parentRoleName = "test_parent_role_revoke_exception";
        CreateRoleStmt createParentRoleStmt = new CreateRoleStmt(Collections.singletonList(parentRoleName), false, null);
        authorizationMgr.createRole(createParentRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(parentRoleName));

        String childRoleName = "test_child_role_revoke_exception";
        CreateRoleStmt createChildRoleStmt = new CreateRoleStmt(Collections.singletonList(childRoleName), false, null);
        authorizationMgr.createRole(createChildRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(childRoleName));

        String grantRoleSql = "GRANT " + parentRoleName + " TO ROLE " + childRoleName;
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // 2. Prepare revoke role statement
        String revokeRoleSql = "REVOKE " + parentRoleName + " FROM ROLE " + childRoleName;
        RevokeRoleStmt revokeRoleStmt = (RevokeRoleStmt) UtFrameUtils.parseStmtWithNewParser(revokeRoleSql, ctx);

        // 3. Mock EditLog.logUpdateRolePrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateRolePrivilege(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        RolePrivilegeCollectionV2 initialChildCollection = authorizationMgr.getRolePrivilegeCollection(childRoleName);
        Set<Long> initialParentRoles = new HashSet<>(initialChildCollection.getParentRoleIds());

        // 4. Execute revokeRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.revokeRole(revokeRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        RolePrivilegeCollectionV2 unchangedCollection = authorizationMgr.getRolePrivilegeCollection(childRoleName);
        Assertions.assertNotNull(unchangedCollection);
        Assertions.assertEquals(initialParentRoles, unchangedCollection.getParentRoleIds());
    }

    // ==================== Role Management Tests ====================

    @Test
    public void testCreateRoleNormalCase() throws Exception {
        // 1. Prepare test data
        String roleName = "test_role_create";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);

        // 2. Verify initial state
        Assertions.assertFalse(authorizationMgr.checkRoleExists(roleName));

        // 3. Execute createRole operation (master side)
        authorizationMgr.createRole(createRoleStmt);

        // 4. Verify master state
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));
        RolePrivilegeCollectionV2 roleCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(roleCollection);
        Assertions.assertEquals(roleName, roleCollection.getName());

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // Verify follower initial state
        Assertions.assertFalse(followerAuthMgr.checkRoleExists(roleName));

        // Replay the operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(replayInfo);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerAuthMgr.checkRoleExists(roleName));
        RolePrivilegeCollectionV2 followerCollection = followerAuthMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(followerCollection);
        Assertions.assertEquals(roleName, followerCollection.getName());
    }

    @Test
    public void testCreateRoleEditLogException() throws Exception {
        // 1. Prepare test data
        String roleName = "test_role_create_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);

        // 2. Mock EditLog.logUpdateRolePrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateRolePrivilege(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(authorizationMgr.checkRoleExists(roleName));

        // Save initial state snapshot
        int initialRoleCount = authorizationMgr.getAllRoles().size();

        // 3. Execute createRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.createRole(createRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(authorizationMgr.checkRoleExists(roleName));
        Assertions.assertEquals(initialRoleCount, authorizationMgr.getAllRoles().size());
    }

    @Test
    public void testAlterRoleNormalCase() throws Exception {
        // 1. Prepare test data - create a role first
        String roleName = "test_role_alter";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare alter role statement
        String newComment = "Updated comment";
        AlterRoleStmt alterRoleStmt = new AlterRoleStmt(Collections.singletonList(roleName), false, newComment);

        // 3. Execute alterRole operation (master side)
        authorizationMgr.alterRole(alterRoleStmt);

        // 4. Verify master state
        RolePrivilegeCollectionV2 roleCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(roleCollection);
        Assertions.assertEquals(newComment, roleCollection.getComment());

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // First create the role in follower
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);

        // Replay the alter operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(replayInfo);

        // 6. Verify follower state is consistent with master
        RolePrivilegeCollectionV2 followerCollection = followerAuthMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(followerCollection);
        Assertions.assertEquals(newComment, followerCollection.getComment());
    }

    @Test
    public void testAlterRoleEditLogException() throws Exception {
        // 1. Prepare test data - create a role first
        String roleName = "test_role_alter_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare alter role statement
        String newComment = "Updated comment";
        AlterRoleStmt alterRoleStmt = new AlterRoleStmt(Collections.singletonList(roleName), false, newComment);

        // 3. Mock EditLog.logUpdateRolePrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateRolePrivilege(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        RolePrivilegeCollectionV2 initialCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        String initialComment = initialCollection.getComment();

        // 4. Execute alterRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.alterRole(alterRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        RolePrivilegeCollectionV2 unchangedCollection = authorizationMgr.getRolePrivilegeCollection(roleName);
        Assertions.assertNotNull(unchangedCollection);
        Assertions.assertEquals(initialComment, unchangedCollection.getComment());
    }

    @Test
    public void testDropRoleNormalCase() throws Exception {
        // 1. Prepare test data - create a role first
        String roleName = "test_role_drop";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare drop role statement
        DropRoleStmt dropRoleStmt = new DropRoleStmt(Collections.singletonList(roleName), false);

        // 3. Execute dropRole operation (master side)
        authorizationMgr.dropRole(dropRoleStmt);

        // 4. Verify master state
        Assertions.assertFalse(authorizationMgr.checkRoleExists(roleName));

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());

        // First create the role in follower
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);
        Assertions.assertTrue(followerAuthMgr.checkRoleExists(roleName));

        // Replay the drop operation
        RolePrivilegeCollectionInfo replayInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_ROLE_V2);
        followerAuthMgr.replayDropRole(replayInfo);

        // 6. Verify follower state is consistent with master
        Assertions.assertFalse(followerAuthMgr.checkRoleExists(roleName));
    }

    @Test
    public void testDropRoleEditLogException() throws Exception {
        // 1. Prepare test data - create a role first
        String roleName = "test_role_drop_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 2. Prepare drop role statement
        DropRoleStmt dropRoleStmt = new DropRoleStmt(Collections.singletonList(roleName), false);

        // 3. Mock EditLog.logDropRole to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropRole(any(RolePrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        // 4. Execute dropRole operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.dropRole(dropRoleStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));
    }

    // ==================== Set User Default Role Tests ====================

    @Test
    public void testSetUserDefaultRoleNormalCase() throws Exception {
        // 1. Prepare test data - create a user and role, then grant role first
        String userName = "test_user_default_role";
        String createUserSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        authenticationMgr.createUser(createUserStmt);
        UserIdentity userIdentity = new UserIdentity(createUserStmt.getUser().getUser(),
                createUserStmt.getUser().getHost(), createUserStmt.getUser().isDomain());

        String roleName = "test_role_default";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        String grantRoleSql = "GRANT " + roleName + " TO '" + userName + "'@'%'";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // 2. Get role ID
        Long roleId = authorizationMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(roleId);

        // 3. Execute setUserDefaultRole operation (master side)
        Set<Long> defaultRoleIds = new HashSet<>();
        defaultRoleIds.add(roleId);
        authorizationMgr.setUserDefaultRole(defaultRoleIds, userIdentity);

        // 4. Verify master state
        UserPrivilegeCollectionV2 userCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(userCollection);
        Assertions.assertTrue(userCollection.getDefaultRoleIds().contains(roleId));

        // 5. Test follower replay functionality
        AuthorizationMgr followerAuthMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        AuthenticationMgr followerAuthMgr2 = new AuthenticationMgr();

        // First create the user and role, then grant in follower
        followerAuthMgr2.replayCreateUser(userIdentity,
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userIdentity.getUser()),
                new UserPrivilegeCollectionV2(), (short) 0, (short) 0);

        // Replay role creation
        RolePrivilegeCollectionInfo createInfo = (RolePrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateRolePrivilegeCollection(createInfo);

        // Replay grant role
        UserPrivilegeCollectionInfo grantInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                grantInfo.getUserIdentity(),
                grantInfo.getPrivilegeCollection(),
                grantInfo.getPluginId(),
                grantInfo.getPluginVersion());

        // Replay the set default role operation
        UserPrivilegeCollectionInfo replayInfo = (UserPrivilegeCollectionInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PRIVILEGE_V2);
        followerAuthMgr.replayUpdateUserPrivilegeCollection(
                replayInfo.getUserIdentity(),
                replayInfo.getPrivilegeCollection(),
                replayInfo.getPluginId(),
                replayInfo.getPluginVersion());

        // 6. Verify follower state is consistent with master
        UserPrivilegeCollectionV2 followerCollection = followerAuthMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(followerCollection);
        Long followerRoleId = followerAuthMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(followerRoleId);
        Assertions.assertTrue(followerCollection.getDefaultRoleIds().contains(followerRoleId));
    }

    @Test
    public void testSetUserDefaultRoleEditLogException() throws Exception {
        // 1. Prepare test data - create a user and role, then grant role first
        String userName = "test_user_default_role_exception";
        String createUserSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx);
        authenticationMgr.createUser(createUserStmt);
        UserIdentity userIdentity = new UserIdentity(createUserStmt.getUser().getUser(),
                createUserStmt.getUser().getHost(), createUserStmt.getUser().isDomain());

        String roleName = "test_role_default_exception";
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(Collections.singletonList(roleName), false, null);
        authorizationMgr.createRole(createRoleStmt);
        Assertions.assertTrue(authorizationMgr.checkRoleExists(roleName));

        String grantRoleSql = "GRANT " + roleName + " TO '" + userName + "'@'%'";
        GrantRoleStmt grantRoleStmt = (GrantRoleStmt) UtFrameUtils.parseStmtWithNewParser(grantRoleSql, ctx);
        authorizationMgr.grantRole(grantRoleStmt);

        // 2. Get role ID
        Long roleId = authorizationMgr.getRoleIdByNameAllowNull(roleName);
        Assertions.assertNotNull(roleId);

        // 3. Mock EditLog.logUpdateUserPrivilege to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateUserPrivilege(any(UserPrivilegeCollectionInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserPrivilegeCollectionV2 initialCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Set<Long> initialDefaultRoles = new HashSet<>(initialCollection.getDefaultRoleIds());

        // 4. Execute setUserDefaultRole operation and expect exception
        Set<Long> defaultRoleIds = new HashSet<>();
        defaultRoleIds.add(roleId);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authorizationMgr.setUserDefaultRole(defaultRoleIds, userIdentity);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserPrivilegeCollectionV2 unchangedCollection = authorizationMgr.getUserPrivilegeCollection(userIdentity);
        Assertions.assertNotNull(unchangedCollection);
        Assertions.assertEquals(initialDefaultRoles, unchangedCollection.getDefaultRoleIds());
    }
}

