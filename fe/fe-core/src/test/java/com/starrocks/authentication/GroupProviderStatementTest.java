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
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for Group Provider statements with IF NOT EXISTS and IF EXISTS functionality
 */
public class GroupProviderStatementTest {
    private ConnectContext ctx;
    private AuthenticationMgr authenticationMgr;
    private static final String TEST_PROVIDER_NAME = "test_provider";
    private static final String NON_EXISTENT_PROVIDER_NAME = "non_existent_provider";

    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        // Clean up any existing test providers
        cleanupTestProviders();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanupTestProviders();
    }

    /**
     * Test case: Create Group Provider with IF NOT EXISTS when provider does not exist
     * Test point: Should successfully create the provider without error
     */
    @Test
    public void testCreateGroupProviderIfNotExistsWhenNotExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.createGroupProviderStatement(stmt, ctx);

        // Verify provider was created
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Group provider should be created successfully");
        Assertions.assertEquals(TEST_PROVIDER_NAME, provider.getName());
        Assertions.assertEquals("unix", provider.getType());
    }

    /**
     * Test case: Create Group Provider with IF NOT EXISTS when provider already exists
     * Test point: Should silently return without error and not modify existing provider
     */
    @Test
    public void testCreateGroupProviderIfNotExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt firstStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(firstStmt, ctx);

        // Get the original provider for comparison
        GroupProvider originalProvider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(originalProvider, "Original provider should exist");

        // Try to create again with IF NOT EXISTS
        CreateGroupProviderStmt secondStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.createGroupProviderStatement(secondStmt, ctx);

        // Verify provider still exists and is unchanged
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Group provider should still exist");
        Assertions.assertEquals(originalProvider.getName(), provider.getName());
        Assertions.assertEquals(originalProvider.getType(), provider.getType());
    }

    /**
     * Test case: Create Group Provider without IF NOT EXISTS when provider already exists
     * Test point: Should throw DdlException with appropriate error message
     */
    @Test
    public void testCreateGroupProviderWithoutIfNotExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt firstStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(firstStmt, ctx);

        // Try to create again without IF NOT EXISTS
        CreateGroupProviderStmt secondStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should throw DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.createGroupProviderStatement(secondStmt, ctx);
        });

        Assertions.assertTrue(exception.getMessage().contains("Group provider '" + TEST_PROVIDER_NAME + "' already exists"),
                "Error message should indicate provider already exists: " + exception.getMessage());
    }

    /**
     * Test case: Drop Group Provider with IF EXISTS when provider does not exist
     * Test point: Should silently return without error
     */
    @Test
    public void testDropGroupProviderIfExistsWhenNotExists() throws Exception {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.dropGroupProviderStatement(stmt, ctx);

        // Verify provider still does not exist
        GroupProvider provider = authenticationMgr.getGroupProvider(NON_EXISTENT_PROVIDER_NAME);
        Assertions.assertNull(provider, "Group provider should not exist");
    }

    /**
     * Test case: Drop Group Provider with IF EXISTS when provider exists
     * Test point: Should successfully drop the provider
     */
    @Test
    public void testDropGroupProviderIfExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        // Verify provider exists
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Group provider should exist before drop");

        // Drop with IF EXISTS
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);

        // Should not throw exception
        authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);

        // Verify provider was dropped
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNull(provider, "Group provider should be dropped successfully");
    }

    /**
     * Test case: Drop Group Provider without IF EXISTS when provider does not exist
     * Test point: Should throw DdlException with appropriate error message
     */
    @Test
    public void testDropGroupProviderWithoutIfExistsWhenNotExists() throws Exception {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, false, NodePosition.ZERO);

        // Should throw DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.dropGroupProviderStatement(stmt, ctx);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("Group provider '" + NON_EXISTENT_PROVIDER_NAME + "' does not exist"),
                "Error message should indicate provider does not exist: " + exception.getMessage());
    }

    /**
     * Test case: Test CreateGroupProviderStmt constructor and getters
     * Test point: Verify proper initialization of statement properties
     */
    @Test
    public void testCreateGroupProviderStmtProperties() {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertEquals(properties, stmt.getPropertyMap(), "Properties should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");
    }

    /**
     * Test case: Test DropGroupProviderStmt constructor and getters
     * Test point: Verify proper initialization of statement properties
     */
    @Test
    public void testDropGroupProviderStmtProperties() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfExists(), "IF EXISTS should be true");
    }

    /**
     * Test case: Test CreateGroupProviderStmt without IF NOT EXISTS
     * Test point: Verify default behavior when IF NOT EXISTS is false
     */
    @Test
    public void testCreateGroupProviderStmtWithoutIfNotExists() {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertEquals(properties, stmt.getPropertyMap(), "Properties should match");
        Assertions.assertFalse(stmt.isIfNotExists(), "IF NOT EXISTS should be false");
    }

    /**
     * Test case: Test DropGroupProviderStmt without IF EXISTS
     * Test point: Verify default behavior when IF EXISTS is false
     */
    @Test
    public void testDropGroupProviderStmtWithoutIfExists() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, false, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertFalse(stmt.isIfExists(), "IF EXISTS should be false");
    }

    /**
     * Test case: Test CreateGroupProviderStmt with legacy constructor
     * Test point: Verify backward compatibility with existing constructor
     */
    @Test
    public void testCreateGroupProviderStmtLegacyConstructor() {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertEquals(properties, stmt.getPropertyMap(), "Properties should match");
        Assertions.assertFalse(stmt.isIfNotExists(), "IF NOT EXISTS should default to false");
    }

    /**
     * Test case: Test DropGroupProviderStmt with legacy constructor
     * Test point: Verify backward compatibility with existing constructor
     */
    @Test
    public void testDropGroupProviderStmtLegacyConstructor() {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, NodePosition.ZERO);

        Assertions.assertEquals(TEST_PROVIDER_NAME, stmt.getName(), "Provider name should match");
        Assertions.assertFalse(stmt.isIfExists(), "IF EXISTS should default to false");
    }

    /**
     * Test case: Test multiple Group Provider operations in sequence
     * Test point: Verify proper handling of multiple create/drop operations
     */
    @Test
    public void testMultipleGroupProviderOperations() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // Create provider
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        // Verify exists
        GroupProvider provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Provider should exist after creation");

        // Try to create again with IF NOT EXISTS (should not error)
        CreateGroupProviderStmt createAgainStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createAgainStmt, ctx);

        // Verify still exists
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNotNull(provider, "Provider should still exist after IF NOT EXISTS create");

        // Drop with IF EXISTS
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
        authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);

        // Verify dropped
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNull(provider, "Provider should be dropped");

        // Try to drop again with IF EXISTS (should not error)
        DropGroupProviderStmt dropAgainStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
        authenticationMgr.dropGroupProviderStatement(dropAgainStmt, ctx);

        // Verify still does not exist
        provider = authenticationMgr.getGroupProvider(TEST_PROVIDER_NAME);
        Assertions.assertNull(provider, "Provider should still not exist after IF EXISTS drop");
    }

    /**
     * Helper method to create Unix Group Provider properties
     */
    private Map<String, String> createUnixGroupProviderProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "unix");
        return properties;
    }

    /**
     * Helper method to clean up test providers
     */
    private void cleanupTestProviders() {
        try {
            // Try to drop test providers if they exist
            DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }

        try {
            DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, true, NodePosition.ZERO);
            authenticationMgr.dropGroupProviderStatement(dropStmt, ctx);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}
