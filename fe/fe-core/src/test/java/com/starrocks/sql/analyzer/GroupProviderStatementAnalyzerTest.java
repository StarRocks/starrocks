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

package com.starrocks.sql.analyzer;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.catalog.UserIdentity;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Unit tests for GroupProviderStatementAnalyzer with IF NOT EXISTS and IF EXISTS functionality
 */
public class GroupProviderStatementAnalyzerTest {
    private ConnectContext ctx;
    private AuthenticationMgr authenticationMgr;
    private static final String TEST_PROVIDER_NAME = "test_provider";
    private static final String NON_EXISTENT_PROVIDER_NAME = "non_existent_provider";

    @Mock
    private GroupProvider mockGroupProvider;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        UtFrameUtils.setUpForPersistTest();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        // Setup mock group provider
        when(mockGroupProvider.getName()).thenReturn(TEST_PROVIDER_NAME);
        when(mockGroupProvider.getType()).thenReturn("unix");
        // Mock checkProperty method (void method)
        // when(mockGroupProvider.checkProperty()).thenReturn(null);

        // Clean up any existing test providers
        cleanupTestProviders();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanupTestProviders();
        UtFrameUtils.tearDownForPersisTest();
    }

    /**
     * Test case: Analyze Create Group Provider with IF NOT EXISTS when provider does not exist
     * Test point: Should pass analysis without error
     */
    @Test
    public void testAnalyzeCreateGroupProviderIfNotExistsWhenNotExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(stmt, ctx);
    }

    /**
     * Test case: Analyze Create Group Provider with IF NOT EXISTS when provider already exists
     * Test point: Should pass analysis without error due to IF NOT EXISTS
     */
    @Test
    public void testAnalyzeCreateGroupProviderIfNotExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt firstStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(firstStmt, ctx);

        // Analyze create again with IF NOT EXISTS
        CreateGroupProviderStmt secondStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, true, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(secondStmt, ctx);
    }

    /**
     * Test case: Analyze Create Group Provider without IF NOT EXISTS when provider already exists
     * Test point: Should throw SemanticException with appropriate error message
     */
    @Test
    public void testAnalyzeCreateGroupProviderWithoutIfNotExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt firstStmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(firstStmt, ctx);

        // Analyze create again without IF NOT EXISTS
        CreateGroupProviderStmt secondStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should throw SemanticException
        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            GroupProviderStatementAnalyzer.analyze(secondStmt, ctx);
        });

        Assertions.assertTrue(exception.getMessage().contains("Group Provider '" + TEST_PROVIDER_NAME + "' already exists"),
                "Error message should indicate provider already exists: " + exception.getMessage());
    }

    /**
     * Test case: Analyze Drop Group Provider with IF EXISTS when provider does not exist
     * Test point: Should pass analysis without error
     */
    @Test
    public void testAnalyzeDropGroupProviderIfExistsWhenNotExists() throws Exception {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, true, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(stmt, ctx);
    }

    /**
     * Test case: Analyze Drop Group Provider with IF EXISTS when provider exists
     * Test point: Should pass analysis without error
     */
    @Test
    public void testAnalyzeDropGroupProviderIfExistsWhenExists() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();

        // First create the provider
        CreateGroupProviderStmt createStmt =
                new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);
        authenticationMgr.createGroupProviderStatement(createStmt, ctx);

        // Analyze drop with IF EXISTS
        DropGroupProviderStmt dropStmt = new DropGroupProviderStmt(TEST_PROVIDER_NAME, true, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(dropStmt, ctx);
    }

    /**
     * Test case: Analyze Drop Group Provider without IF EXISTS when provider does not exist
     * Test point: Should throw SemanticException with appropriate error message
     */
    @Test
    public void testAnalyzeDropGroupProviderWithoutIfExistsWhenNotExists() throws Exception {
        DropGroupProviderStmt stmt = new DropGroupProviderStmt(NON_EXISTENT_PROVIDER_NAME, false, NodePosition.ZERO);

        // Should throw SemanticException
        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            GroupProviderStatementAnalyzer.analyze(stmt, ctx);
        });

        Assertions.assertTrue(exception.getMessage().contains("Group Provider '" + NON_EXISTENT_PROVIDER_NAME + "' not found"),
                "Error message should indicate provider not found: " + exception.getMessage());
    }

    /**
     * Test case: Analyze Create Group Provider with missing type property
     * Test point: Should throw SemanticException for missing required property
     */
    @Test
    public void testAnalyzeCreateGroupProviderMissingType() throws Exception {
        Map<String, String> properties = new HashMap<>();
        // Missing "type" property
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should throw SemanticException
        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            GroupProviderStatementAnalyzer.analyze(stmt, ctx);
        });

        Assertions.assertTrue(exception.getMessage().contains("missing required property: type"),
                "Error message should indicate missing type property: " + exception.getMessage());
    }

    /**
     * Test case: Analyze Create Group Provider with unsupported type
     * Test point: Should throw SemanticException for unsupported group provider type
     */
    @Test
    public void testAnalyzeCreateGroupProviderUnsupportedType() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "unsupported_type");
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should throw SemanticException
        SemanticException exception = Assertions.assertThrows(SemanticException.class, () -> {
            GroupProviderStatementAnalyzer.analyze(stmt, ctx);
        });

        Assertions.assertTrue(exception.getMessage().contains("unsupported group provider type"),
                "Error message should indicate unsupported type: " + exception.getMessage());
    }

    /**
     * Test case: Analyze Create Group Provider with valid Unix type
     * Test point: Should pass analysis for valid Unix group provider
     */
    @Test
    public void testAnalyzeCreateGroupProviderValidUnixType() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(stmt, ctx);
    }

    /**
     * Test case: Analyze Create Group Provider with valid File type
     * Test point: Should pass analysis for valid File group provider
     */
    @Test
    public void testAnalyzeCreateGroupProviderValidFileType() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "file");
        properties.put("group_file_url", "test_file");
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(stmt, ctx);
    }

    /**
     * Test case: Analyze Create Group Provider with valid LDAP type
     * Test point: Should pass analysis for valid LDAP group provider
     */
    @Test
    public void testAnalyzeCreateGroupProviderValidLDAPType() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ldap");
        properties.put("ldap_conn_url", "ldap://localhost:389");
        properties.put("ldap_bind_root_dn", "cn=admin,dc=example,dc=com");
        properties.put("ldap_bind_root_pwd", "password");
        properties.put("ldap_bind_base_dn", "dc=example,dc=com");
        properties.put("ldap_group_dn", "ou=groups,dc=example,dc=com");
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should not throw exception
        GroupProviderStatementAnalyzer.analyze(stmt, ctx);
    }

    /**
     * Test case: Test analysis with null context
     * Test point: Should handle null context gracefully
     */
    @Test
    public void testAnalyzeWithNullContext() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt(TEST_PROVIDER_NAME, properties, false, NodePosition.ZERO);

        // Should not throw exception even with null context
        GroupProviderStatementAnalyzer.analyze(stmt, null);
    }

    /**
     * Test case: Test analysis with empty provider name
     * Test point: Should handle empty provider name appropriately
     */
    @Test
    public void testAnalyzeWithEmptyProviderName() throws Exception {
        Map<String, String> properties = createUnixGroupProviderProperties();
        CreateGroupProviderStmt stmt = new CreateGroupProviderStmt("", properties, false, NodePosition.ZERO);

        // Should not throw exception during analysis (name validation happens elsewhere)
        GroupProviderStatementAnalyzer.analyze(stmt, ctx);
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
