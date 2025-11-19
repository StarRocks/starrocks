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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GroupProviderLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
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
}


