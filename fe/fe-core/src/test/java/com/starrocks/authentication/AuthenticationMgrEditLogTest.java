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
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GroupProviderLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.SecurityIntegrationPersistInfo;
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
    private AuthenticationMgr masterAuthenticationMgr;
    private ConnectContext ctx;
    private static final String TEST_PROVIDER_NAME = "test_provider_editlog";

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        masterAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

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

    // ========== GroupProvider Tests ==========

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

    // ========== SecurityIntegration Tests ==========

    private Map<String, String> createTestSecurityIntegrationProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "authentication_jwt");
        properties.put("jwks_url", "https://example.com/.well-known/jwks.json");
        properties.put("principal_field", "sub");
        return properties;
    }

    @Test
    public void testCreateSecurityIntegrationNormalCase() throws Exception {
        // 1. Prepare test data
        String name = "test_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();

        // 2. Verify initial state
        Assertions.assertNull(masterAuthenticationMgr.getSecurityIntegration(name));

        // 3. Execute createSecurityIntegration operation (master side)
        masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        // 4. Verify master state
        SecurityIntegration securityIntegration = masterAuthenticationMgr.getSecurityIntegration(name);
        Assertions.assertNotNull(securityIntegration);
        Assertions.assertEquals(name, securityIntegration.getName());
        Map<String, String> actualProps = securityIntegration.getPropertyMap();
        Assertions.assertEquals("authentication_jwt", actualProps.get("type"));
        Assertions.assertEquals("https://example.com/.well-known/jwks.json", actualProps.get("jwks_url"));

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthenticationMgr = new AuthenticationMgr();

        SecurityIntegrationPersistInfo replayInfo = (SecurityIntegrationPersistInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_SECURITY_INTEGRATION);

        // Execute follower replay
        followerAuthenticationMgr.replayCreateSecurityIntegration(replayInfo.name, replayInfo.propertyMap);

        // 6. Verify follower state is consistent with master
        SecurityIntegration followerSecurityIntegration = followerAuthenticationMgr.getSecurityIntegration(name);
        Assertions.assertNotNull(followerSecurityIntegration);
        Assertions.assertEquals(name, followerSecurityIntegration.getName());
        Map<String, String> followerProps = followerSecurityIntegration.getPropertyMap();
        Assertions.assertEquals("authentication_jwt", followerProps.get("type"));
    }

    @Test
    public void testCreateSecurityIntegrationEditLogException() throws Exception {
        // 1. Prepare test data
        String name = "exception_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();

        // 2. Create a separate AuthenticationMgr for exception testing
        AuthenticationMgr exceptionAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logCreateSecurityIntegration to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logCreateSecurityIntegration(any(SecurityIntegrationPersistInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNull(exceptionAuthenticationMgr.getSecurityIntegration(name));

        // 4. Execute createSecurityIntegration operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAuthenticationMgr.createSecurityIntegration(name, propertyMap);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNull(exceptionAuthenticationMgr.getSecurityIntegration(name));
    }

    @Test
    public void testCreateSecurityIntegrationDuplicate() throws Exception {
        // 1. Create a security integration first
        String name = "duplicate_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();
        masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        // 2. Verify initial state
        Assertions.assertNotNull(masterAuthenticationMgr.getSecurityIntegration(name));

        // 3. Try to create duplicate security integration and expect DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);
        });
        Assertions.assertTrue(exception.getMessage().contains("security integration '" + name + "' already exists"));
    }

    @Test
    public void testAlterSecurityIntegrationNormalCase() throws Exception {
        // 1. Create a security integration first
        String name = "test_alter_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();
        masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        // 2. Prepare alter properties
        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("issuer", "https://new-issuer.com");
        alterProps.put("audience", "new-audience");

        // 3. Execute alterSecurityIntegration operation (master side)
        masterAuthenticationMgr.alterSecurityIntegration(name, alterProps);

        // 4. Verify master state
        SecurityIntegration securityIntegration = masterAuthenticationMgr.getSecurityIntegration(name);
        Assertions.assertNotNull(securityIntegration);
        Map<String, String> actualProps = securityIntegration.getPropertyMap();
        Assertions.assertEquals("https://new-issuer.com", actualProps.get("issuer"));
        Assertions.assertEquals("new-audience", actualProps.get("audience"));
        // Original properties should still exist
        Assertions.assertEquals("authentication_jwt", actualProps.get("type"));

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthenticationMgr = new AuthenticationMgr();
        followerAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        SecurityIntegrationPersistInfo replayInfo = (SecurityIntegrationPersistInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_SECURITY_INTEGRATION);

        // Execute follower replay
        followerAuthenticationMgr.replayAlterSecurityIntegration(replayInfo.name, replayInfo.propertyMap);

        // 6. Verify follower state is consistent with master
        SecurityIntegration followerSecurityIntegration = followerAuthenticationMgr.getSecurityIntegration(name);
        Assertions.assertNotNull(followerSecurityIntegration);
        Map<String, String> followerProps = followerSecurityIntegration.getPropertyMap();
        Assertions.assertEquals("https://new-issuer.com", followerProps.get("issuer"));
        Assertions.assertEquals("new-audience", followerProps.get("audience"));
    }

    @Test
    public void testAlterSecurityIntegrationEditLogException() throws Exception {
        // 1. Create a security integration first
        String name = "exception_alter_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();
        masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        // 2. Create a separate AuthenticationMgr for exception testing
        AuthenticationMgr exceptionAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        
        // Ensure security integration exists for exception testing
        if (exceptionAuthenticationMgr.getSecurityIntegration(name) == null) {
            exceptionAuthenticationMgr.createSecurityIntegration(name, propertyMap);
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAlterSecurityIntegration to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterSecurityIntegration(any(SecurityIntegrationPersistInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Prepare alter properties
        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("issuer", "https://new-issuer.com");

        // Save initial state
        SecurityIntegration initialIntegration = exceptionAuthenticationMgr.getSecurityIntegration(name);
        String initialIssuer = initialIntegration.getPropertyMap().get("issuer");

        // 4. Execute alterSecurityIntegration operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAuthenticationMgr.alterSecurityIntegration(name, alterProps);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        SecurityIntegration currentIntegration = exceptionAuthenticationMgr.getSecurityIntegration(name);
        Assertions.assertEquals(initialIssuer, currentIntegration.getPropertyMap().get("issuer"));
    }

    @Test
    public void testAlterSecurityIntegrationNonExistent() throws Exception {
        // 1. Test altering non-existent security integration
        String nonExistentName = "non_existent_security_integration";
        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("issuer", "https://new-issuer.com");

        // 2. Verify initial state
        Assertions.assertNull(masterAuthenticationMgr.getSecurityIntegration(nonExistentName));

        // 3. Execute alterSecurityIntegration operation and expect DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterAuthenticationMgr.alterSecurityIntegration(nonExistentName, alterProps);
        });
        Assertions.assertTrue(exception.getMessage().contains("security integration '" + nonExistentName + "' not found"));
    }

    @Test
    public void testDropSecurityIntegrationNormalCase() throws Exception {
        // 1. Create a security integration first
        String name = "test_drop_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();
        masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        // 2. Verify initial state
        Assertions.assertNotNull(masterAuthenticationMgr.getSecurityIntegration(name));

        // 3. Execute dropSecurityIntegration operation (master side)
        masterAuthenticationMgr.dropSecurityIntegration(name);

        // 4. Verify master state
        Assertions.assertNull(masterAuthenticationMgr.getSecurityIntegration(name));

        // 5. Test follower replay functionality
        AuthenticationMgr followerAuthenticationMgr = new AuthenticationMgr();
        followerAuthenticationMgr.replayCreateSecurityIntegration(name, propertyMap);
        Assertions.assertNotNull(followerAuthenticationMgr.getSecurityIntegration(name));

        SecurityIntegrationPersistInfo replayInfo = (SecurityIntegrationPersistInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_SECURITY_INTEGRATION);

        // Execute follower replay
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(name, replayInfo.name);
        followerAuthenticationMgr.replayDropSecurityIntegration(replayInfo.name);

        // 6. Verify follower state is consistent with master
        Assertions.assertNull(followerAuthenticationMgr.getSecurityIntegration(name));
    }

    @Test
    public void testDropSecurityIntegrationEditLogException() throws Exception {
        // 1. Create a security integration first
        String name = "exception_drop_security_integration";
        Map<String, String> propertyMap = createTestSecurityIntegrationProperties();
        masterAuthenticationMgr.createSecurityIntegration(name, propertyMap);

        // 2. Create a separate AuthenticationMgr for exception testing
        AuthenticationMgr exceptionAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        
        // Ensure security integration exists for exception testing
        if (exceptionAuthenticationMgr.getSecurityIntegration(name) == null) {
            exceptionAuthenticationMgr.createSecurityIntegration(name, propertyMap);
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logDropSecurityIntegration to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropSecurityIntegration(any(SecurityIntegrationPersistInfo.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNotNull(exceptionAuthenticationMgr.getSecurityIntegration(name));

        // 4. Execute dropSecurityIntegration operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionAuthenticationMgr.dropSecurityIntegration(name);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNotNull(exceptionAuthenticationMgr.getSecurityIntegration(name));
    }

    @Test
    public void testDropSecurityIntegrationNonExistent() throws Exception {
        // 1. Test dropping non-existent security integration
        String nonExistentName = "non_existent_security_integration";

        // 2. Verify initial state
        Assertions.assertNull(masterAuthenticationMgr.getSecurityIntegration(nonExistentName));

        // 3. Execute dropSecurityIntegration operation and expect DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            masterAuthenticationMgr.dropSecurityIntegration(nonExistentName);
        });
        Assertions.assertTrue(exception.getMessage().contains("security integration '" + nonExistentName + "' not found"));
    }
}
