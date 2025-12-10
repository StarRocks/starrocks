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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Pair;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GroupProviderLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AuthenticationMgrEditLogTest {
    private AuthenticationMgr authenticationMgr;
    private ConnectContext ctx;
    private static final String TEST_PROVIDER_NAME = "test_provider_editlog";

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        // Clean up any existing test providers
        cleanupTestProviders();
    }

    @AfterEach
    public void tearDown() {
        cleanupTestProviders();
        UtFrameUtils.tearDownForPersisTest();
    }

    private void cleanupTestProviders() {
        try {
            DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    private Map<String, String> createUnixGroupProviderProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "unix");
        return properties;
    }

    @Test
    public void testCreateGroupProviderStatementNormalCase() throws Exception {
        // 1. Prepare test data
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // 2. Verify initial state
        Assertions.assertNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // 3. Execute createGroupProviderStatement operation (master side)
        authenticationMgr.createGroupProviderStatement(stmt, ctx);

        // 4. Verify master state
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider);
        Assertions.assertEquals(TEST_PROVIDER_NAME, provider.getName());
        Assertions.assertEquals("unix", provider.getType());

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthMgr = new AuthenticationMgr();
        
        // Verify follower initial state
        Assertions.assertNull(followerAuthMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // Replay the operation
        GroupProviderLog replayLog = (GroupProviderLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_GROUP_PROVIDER);
        followerAuthMgr.replayCreateGroupProvider(replayLog.getName(), replayLog.getPropertyMap());

        // 6. Verify follower state is consistent with master
        GroupProvider followerProvider = followerAuthMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(followerProvider);
        Assertions.assertEquals(TEST_PROVIDER_NAME, followerProvider.getName());
        Assertions.assertEquals("unix", followerProvider.getType());
    }

    @Test
    public void testCreateGroupProviderStatementEditLogException() throws Exception {
        // 1. Prepare test data
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // 2. Mock EditLog.logCreateGroupProvider to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateGroupProvider(any(GroupProviderLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // Save initial state snapshot
        int initialProviderCount = authenticationMgr.getAllGroupProviders().size();

        // 3. Execute createGroupProviderStatement operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authenticationMgr.createGroupProviderStatement(stmt, ctx);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));
        Assertions.assertEquals(initialProviderCount, authenticationMgr.getAllGroupProviders().size());
    }

    @Test
    public void testDropGroupProviderStatementNormalCase() throws Exception {
        // 1. Prepare test data - create a group provider first
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt createStmt = new CreateGroupProviderStmt(
                TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);
        
        Assertions.assertNotNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // 2. Prepare drop statement
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, false, NodePosition.ZERO);

        // 3. Execute dropGroupProviderStatement operation (master side)
        authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);

        // 4. Verify master state
        Assertions.assertNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthMgr = new AuthenticationMgr();
        
        // First create the provider in follower
        followerAuthMgr.replayCreateGroupProvider(TEST_PROVIDER_NAME, properties);
        Assertions.assertNotNull(followerAuthMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // Replay the drop operation
        GroupProviderLog replayLog = (GroupProviderLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_GROUP_PROVIDER);
        followerAuthMgr.replayDropGroupProvider(replayLog.getName());

        // 6. Verify follower state is consistent with master
        Assertions.assertNull(followerAuthMgr.getGroupProvider(TEST_PROVIDER_NAME));
    }

    @Test
    public void testDropGroupProviderStatementEditLogException() throws Exception {
        // 1. Prepare test data - create a group provider first
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt createStmt = new CreateGroupProviderStmt(
                TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);
        
        Assertions.assertNotNull(authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME));

        // 2. Prepare drop statement
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, false, NodePosition.ZERO);

        // 3. Mock EditLog.logDropGroupProvider to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropGroupProvider(any(GroupProviderLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        GroupProvider initialProvider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(initialProvider);

        // 4. Execute dropGroupProviderStatement operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        GroupProvider unchangedProvider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(unchangedProvider);
        Assertions.assertEquals(TEST_PROVIDER_NAME, unchangedProvider.getName());
    }

    // ==================== User Management Tests ====================

    @Test
    public void testCreateUserNormalCase() throws Exception {
        // 1. Prepare test data
        String userName = "test_user_create";
        String sql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password123'";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        UserIdentity userIdentity = new UserIdentity(stmt.getUser().getUser(), stmt.getUser().getHost(), 
                stmt.getUser().isDomain());

        // 2. Verify initial state
        Assertions.assertFalse(authenticationMgr.doesUserExist(userIdentity));

        // 3. Execute createUser operation (master side)
        authenticationMgr.createUser(stmt);

        // 4. Verify master state
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));
        UserAuthenticationInfo authInfo = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertNotNull(authInfo);

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthMgr = new AuthenticationMgr();
        
        // Verify follower initial state
        Assertions.assertFalse(followerAuthMgr.doesUserExist(userIdentity));

        // Replay the operation
        CreateUserInfo replayInfo = (CreateUserInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_USER_V2);
        followerAuthMgr.replayCreateUser(
                replayInfo.getUserIdentity(),
                replayInfo.getAuthenticationInfo(),
                replayInfo.getUserProperty(),
                replayInfo.getUserPrivilegeCollection(),
                replayInfo.getPluginId(),
                replayInfo.getPluginVersion());

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerAuthMgr.doesUserExist(userIdentity));
        UserAuthenticationInfo followerAuthInfo = followerAuthMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertNotNull(followerAuthInfo);
    }

    @Test
    public void testCreateUserEditLogException() throws Exception {
        // 1. Prepare test data
        String userName = "test_user_exception";
        String sql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password123'";
        CreateUserStmt stmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        UserIdentity userIdentity = new UserIdentity(stmt.getUser().getUser(), stmt.getUser().getHost(), 
                stmt.getUser().isDomain());

        // 2. Mock EditLog.logCreateUser to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateUser(any(CreateUserInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(authenticationMgr.doesUserExist(userIdentity));

        // Save initial state snapshot
        int initialUserCount = authenticationMgr.getUserToAuthenticationInfo().size();

        // 3. Execute createUser operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authenticationMgr.createUser(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(authenticationMgr.doesUserExist(userIdentity));
        Assertions.assertEquals(initialUserCount, authenticationMgr.getUserToAuthenticationInfo().size());
    }

    @Test
    public void testAlterUserNormalCase() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_alter";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'oldpassword'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(), 
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));

        // 2. Prepare alter user data
        UserAuthenticationInfo newAuthInfo = new UserAuthenticationInfo(createStmt.getUser(), 
                new UserAuthOption(null, "newpassword", true, NodePosition.ZERO));
        Map<String, String> properties = new HashMap<>();
        properties.put("max_user_connections", "100");

        // 3. Execute alterUser operation (master side)
        authenticationMgr.alterUser(userIdentity, newAuthInfo, properties);

        // 4. Verify master state
        UserAuthenticationInfo updatedAuthInfo = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertNotNull(updatedAuthInfo);

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthMgr = new AuthenticationMgr();
        
        // First create the user in follower
        followerAuthMgr.replayCreateUser(userIdentity, 
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userIdentity.getUser()),
                null, (short) 0, (short) 0);
        Assertions.assertTrue(followerAuthMgr.doesUserExist(userIdentity));

        // Replay the alter operation
        AlterUserInfo replayInfo = (AlterUserInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_USER_V2);
        followerAuthMgr.replayAlterUser(replayInfo.getUserIdentity(), 
                replayInfo.getAuthenticationInfo(), replayInfo.getProperties());

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerAuthMgr.doesUserExist(userIdentity));
        UserAuthenticationInfo followerAuthInfo = followerAuthMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertNotNull(followerAuthInfo);
    }

    @Test
    public void testAlterUserEditLogException() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_alter_exception";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'oldpassword'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(), 
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));

        // 2. Prepare alter user data
        UserAuthenticationInfo newAuthInfo = new UserAuthenticationInfo(createStmt.getUser(), 
                new UserAuthOption(null, "newpassword", true, NodePosition.ZERO));
        Map<String, String> properties = new HashMap<>();
        properties.put("max_user_connections", "100");

        // 3. Mock EditLog.logAlterUser to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterUser(any(AlterUserInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserAuthenticationInfo initialAuthInfo = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);

        // 4. Execute alterUser operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authenticationMgr.alterUser(userIdentity, newAuthInfo, properties);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserAuthenticationInfo unchangedAuthInfo = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        Assertions.assertNotNull(unchangedAuthInfo);
        // Note: We can't easily compare auth info, but we verify the user still exists
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));
    }

    @Test
    public void testUpdateUserPropertyNormalCase() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_property";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        Assertions.assertTrue(authenticationMgr.doesUserExist(
                new UserIdentity(createStmt.getUser().getUser(), createStmt.getUser().getHost(), 
                        createStmt.getUser().isDomain())));

        // 2. Prepare property update
        List<Pair<String, String>> properties = new ArrayList<>();
        properties.add(new Pair<>("max_user_connections", "200"));

        // 3. Execute updateUserProperty operation (master side)
        authenticationMgr.updateUserProperty(userName, properties);

        // 4. Verify master state
        UserProperty userProperty = authenticationMgr.getUserProperty(userName);
        Assertions.assertNotNull(userProperty);
        Assertions.assertEquals(200, userProperty.getMaxConn());

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthMgr = new AuthenticationMgr();
        
        // First create the user in follower
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(), 
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());
        followerAuthMgr.replayCreateUser(userIdentity, 
                authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity),
                authenticationMgr.getUserProperty(userName),
                null, (short) 0, (short) 0);

        // Replay the property update operation
        UserPropertyInfo replayInfo = (UserPropertyInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_USER_PROP_V3);
        followerAuthMgr.replayUpdateUserProperty(replayInfo);

        // 6. Verify follower state is consistent with master
        UserProperty followerUserProperty = followerAuthMgr.getUserProperty(userName);
        Assertions.assertNotNull(followerUserProperty);
        Assertions.assertEquals(200, followerUserProperty.getMaxConn());
    }

    @Test
    public void testUpdateUserPropertyEditLogException() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_property_exception";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        Assertions.assertTrue(authenticationMgr.doesUserExist(
                new UserIdentity(createStmt.getUser().getUser(), createStmt.getUser().getHost(), 
                        createStmt.getUser().isDomain())));

        // 2. Prepare property update
        List<Pair<String, String>> properties = new ArrayList<>();
        properties.add(new Pair<>("max_user_connections", "200"));

        // 3. Mock EditLog.logUpdateUserPropertyV2 to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logUpdateUserPropertyV2(any(UserPropertyInfo.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        UserProperty initialProperty = authenticationMgr.getUserProperty(userName);
        long initialMaxConn = initialProperty.getMaxConn();

        // 4. Execute updateUserProperty operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authenticationMgr.updateUserProperty(userName, properties);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        UserProperty unchangedProperty = authenticationMgr.getUserProperty(userName);
        Assertions.assertNotNull(unchangedProperty);
        Assertions.assertEquals(initialMaxConn, unchangedProperty.getMaxConn());
    }

    @Test
    public void testDropUserNormalCase() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_drop";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(), 
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));

        // 2. Prepare drop statement
        String dropSql = "DROP USER '" + userName + "'";
        DropUserStmt dropStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);

        // 3. Save user property before dropping (needed for follower replay)
        UserProperty userProperty = authenticationMgr.getUserProperty(userName);
        UserAuthenticationInfo userAuthInfo = authenticationMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        
        // 4. Execute dropUser operation (master side)
        authenticationMgr.dropUser(dropStmt);

        // 5. Verify master state
        Assertions.assertFalse(authenticationMgr.doesUserExist(userIdentity));

        // 6. Test follower replay functionality
        AuthenticationMgr followerAuthMgr = new AuthenticationMgr();
        
        // First create the user in follower
        followerAuthMgr.replayCreateUser(userIdentity, 
                userAuthInfo,
                userProperty,
                null, (short) 0, (short) 0);
        Assertions.assertTrue(followerAuthMgr.doesUserExist(userIdentity));

        // Replay the drop operation
        UserIdentity replayUserIdentity = (UserIdentity) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_USER_V3);
        followerAuthMgr.replayDropUser(replayUserIdentity);

        // 7. Verify follower state is consistent with master
        Assertions.assertFalse(followerAuthMgr.doesUserExist(userIdentity));
    }

    @Test
    public void testDropUserEditLogException() throws Exception {
        // 1. Prepare test data - create a user first
        String userName = "test_user_drop_exception";
        String createSql = "CREATE USER '" + userName + "' IDENTIFIED BY 'password'";
        CreateUserStmt createStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        authenticationMgr.createUser(createStmt);
        UserIdentity userIdentity = new UserIdentity(createStmt.getUser().getUser(), 
                createStmt.getUser().getHost(), createStmt.getUser().isDomain());
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));

        // 2. Prepare drop statement
        String dropSql = "DROP USER '" + userName + "'";
        DropUserStmt dropStmt = (DropUserStmt) UtFrameUtils.parseStmtWithNewParser(dropSql, ctx);

        // 3. Mock EditLog.logDropUser to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropUser(any(UserIdentity.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));

        // 4. Execute dropUser operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            authenticationMgr.dropUser(dropStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(authenticationMgr.doesUserExist(userIdentity));
    }
}


