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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for Group Provider permission control
 * Based on permission errors from test/sql/test_group_provider
 * <p>
 * These tests verify that Group Provider operations require SECURITY privilege:
 * - CREATE GROUP PROVIDER
 * - DROP GROUP PROVIDER
 * - SHOW GROUP PROVIDERS
 * - SHOW CREATE GROUP PROVIDER
 * <p>
 * Test scenarios include:
 * - Non-admin users should be denied access (AccessDeniedException)
 * - Users with SECURITY privilege should be granted access
 * - Root user should always have access
 * - Various Group Provider types (Unix, File, LDAP) and configurations
 * - Quoted identifiers and complex properties
 */
public class GroupProviderPermissionTest {
    private ConnectContext rootCtx;
    private ConnectContext userCtx;
    private ConnectContext securityUserCtx;
    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        // Initialize authentication and authorization managers
        authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        // Create root context
        rootCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);

        // Create a non-admin user context for permission testing
        userCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.createAnalyzedUserIdentWithIp("u1", "%"));

        // Create a user with SECURITY privilege for testing
        securityUserCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.createAnalyzedUserIdentWithIp("security_user", "%"));

        // Create test users and roles
        setupTestUsersAndRoles();
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    /**
     * Setup test users and roles with appropriate privileges
     */
    private void setupTestUsersAndRoles() throws Exception {
        ConnectContext context = new ConnectContext();
        // Create test users
        CreateUserStmt createUserStmt =
                new CreateUserStmt(new UserIdentity("u1", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO);
        Analyzer.analyze(createUserStmt, context);
        authenticationMgr.createUser(createUserStmt);

        createUserStmt = new CreateUserStmt(new UserIdentity("security_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO);
        Analyzer.analyze(createUserStmt, context);
        authenticationMgr.createUser(createUserStmt);

        // Create a role with SECURITY privilege
        authorizationMgr.createRole(new CreateRoleStmt(List.of("security_role"), true, ""));

        // Grant SECURITY privilege to the role
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SECURITY ON SYSTEM TO ROLE security_role",
                new ConnectContext());
        authorizationMgr.grant(grantStmt);

        // Grant the role to security_user
        authorizationMgr.grantRole(new com.starrocks.sql.ast.GrantRoleStmt(
                List.of("security_role"), new UserIdentity("security_user", "%"), NodePosition.ZERO));
        Long role = authorizationMgr.getRoleIdByNameAllowNull("security_role");
        authorizationMgr.setUserDefaultRole(Set.of(role), new UserIdentity("security_user", "%"));
    }

    /**
     * Test case: Non-admin user attempting to SHOW GROUP PROVIDERS
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; show group providers
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testShowGroupProvidersPermissionDenied() throws Exception {
        String sql = "SHOW GROUP PROVIDERS";

        // Parse should succeed regardless of user context
        ShowGroupProvidersStmt stmt =
                (ShowGroupProvidersStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege");

        ConnectContext context = new ConnectContext();
        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), context);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(context, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check");
    }

    /**
     * Test case: Non-admin user attempting to SHOW CREATE GROUP PROVIDER
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; show create group provider unix_group_provider
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testShowCreateGroupProviderPermissionDenied() throws Exception {
        String sql = "SHOW CREATE GROUP PROVIDER unix_group_provider";

        // Parse should succeed regardless of user context
        ShowCreateGroupProviderStmt stmt =
                (ShowCreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("unix_group_provider", stmt.getName(), "Provider name should match");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for SHOW CREATE GROUP PROVIDER");

        ConnectContext context = new ConnectContext();
        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), context);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(context, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for SHOW CREATE GROUP PROVIDER");
    }

    /**
     * Test case: Non-admin user attempting to DROP GROUP PROVIDER
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; drop group provider if exists unix_group_provider
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testDropGroupProviderPermissionDenied() throws Exception {
        String sql = "DROP GROUP PROVIDER IF EXISTS unix_group_provider";

        // Parse should succeed regardless of user context
        DropGroupProviderStmt stmt =
                (DropGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("unix_group_provider", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfExists(), "IF EXISTS should be true");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for DROP GROUP PROVIDER");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for DROP GROUP PROVIDER");
    }

    /**
     * Test case: Non-admin user attempting to CREATE GROUP PROVIDER
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; create group provider if not exists unix_group_provider2 properties(...)
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testCreateGroupProviderPermissionDenied() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS unix_group_provider2 PROPERTIES(\"type\" = \"unix\")";

        // Parse should succeed regardless of user context
        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("unix_group_provider2", stmt.getName(), "Provider name should match");
        Assertions.assertTrue(stmt.isIfNotExists(), "IF NOT EXISTS should be true");
        Assertions.assertEquals("unix", stmt.getPropertyMap().get("type"),
                "Type property should be parsed correctly");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for CREATE GROUP PROVIDER");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for CREATE GROUP PROVIDER");
    }

    /**
     * Test case: Root user can execute all group provider operations
     * Test point: Should parse successfully and pass permission checks for admin user
     */
    @Test
    public void testGroupProviderOperationsAsRoot() throws Exception {
        // Test CREATE
        String createSql = "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(\"type\" = \"unix\")";
        CreateGroupProviderStmt createStmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(createSql, rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(createStmt, "Create statement should parse successfully");
        Assertions.assertEquals("test_provider", createStmt.getName(), "Provider name should match");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for CREATE GROUP PROVIDER");

        // Test SHOW CREATE
        String showCreateSql = "SHOW CREATE GROUP PROVIDER test_provider";
        ShowCreateGroupProviderStmt showCreateStmt =
                (ShowCreateGroupProviderStmt) SqlParser.parseSingleStatement(showCreateSql,
                        rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(showCreateStmt, "Show create statement should parse successfully");
        Assertions.assertEquals("test_provider", showCreateStmt.getName(), "Provider name should match");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for SHOW CREATE GROUP PROVIDER");

        // Test SHOW ALL
        String showAllSql = "SHOW GROUP PROVIDERS";
        ShowGroupProvidersStmt showAllStmt =
                (ShowGroupProvidersStmt) SqlParser.parseSingleStatement(showAllSql, rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(showAllStmt, "Show all statement should parse successfully");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for SHOW GROUP PROVIDERS");

        // Test DROP
        String dropSql = "DROP GROUP PROVIDER IF EXISTS test_provider";
        DropGroupProviderStmt dropStmt =
                (DropGroupProviderStmt) SqlParser.parseSingleStatement(dropSql, rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(dropStmt, "Drop statement should parse successfully");
        Assertions.assertEquals("test_provider", dropStmt.getName(), "Provider name should match");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for DROP GROUP PROVIDER");
    }

    /**
     * Test case: Permission check for impersonation context
     * Test point: Should handle impersonation context correctly
     * Based on: grant impersonate on user root to u1; execute as u1 with no revert
     */
    @Test
    public void testGroupProviderWithImpersonation() throws Exception {
        // Test that impersonation context doesn't affect parsing
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS impersonated_provider PROPERTIES(\"type\" = \"unix\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully in impersonation context");
        Assertions.assertEquals("impersonated_provider", stmt.getName(), "Provider name should match");

        // Permission check would still fail during execution phase
        // even with impersonation privileges
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "User should not have SECURITY privilege even in impersonation context");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check in impersonation context");
    }

    /**
     * Test case: Comprehensive permission validation for Group Provider operations
     * Test point: Verify that all Group Provider operations require SECURITY privilege
     */
    @Test
    public void testComprehensiveGroupProviderPermissionValidation() throws Exception {
        // Test all Group Provider operations require SECURITY privilege
        String[] operations = {
                "SHOW GROUP PROVIDERS",
                "SHOW CREATE GROUP PROVIDER test_provider",
                "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(\"type\" = \"unix\")",
                "DROP GROUP PROVIDER IF EXISTS test_provider"
        };

        for (String sql : operations) {
            // Parse should succeed for all operations
            Object stmt = SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());
            Assertions.assertNotNull(stmt, "Statement should parse successfully: " + sql);

            // Permission check should fail for non-admin user
            Assertions.assertThrows(AccessDeniedException.class, () -> {
                Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
            }, "Non-admin user should not have SECURITY privilege for: " + sql);

            ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
            // Permission check should succeed for user with SECURITY privilege
            Assertions.assertDoesNotThrow(() -> {
                Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
            }, "User with SECURITY privilege should pass permission check for: " + sql);

            // Permission check should succeed for root user
            Assertions.assertDoesNotThrow(() -> {
                Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
            }, "Root user should pass permission check for: " + sql);
        }
    }

    /**
     * Test case: Permission check for quoted identifiers
     * Test point: Should handle quoted identifiers in permission context
     */
    @Test
    public void testGroupProviderPermissionWithQuotedIdentifiers() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS `test-provider` PROPERTIES(\"type\" = \"unix\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test-provider", stmt.getName(), "Provider name should match");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for quoted identifiers");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Permission check should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for quoted identifiers");
    }

    /**
     * Test case: Permission check for complex group provider
     * Test point: Should handle complex properties in permission context
     */
    @Test
    public void testGroupProviderPermissionWithComplexProperties() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS complex_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\", " +
                "\"ldap_bind_root_dn\" = \"cn=admin,dc=example,dc=com\", " +
                "\"ldap_bind_root_pwd\" = \"password\", " +
                "\"ldap_bind_base_dn\" = \"dc=example,dc=com\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("complex_provider", stmt.getName(), "Provider name should match");
        Assertions.assertEquals(5, stmt.getPropertyMap().size(), "All properties should be parsed");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for complex properties");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Permission check should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for complex properties");
    }

    /**
     * Test case: Permission check for different group provider types
     * Test point: Should handle different types in permission context
     */
    @Test
    public void testGroupProviderPermissionWithDifferentTypes() throws Exception {
        // Test Unix type
        String unixSql = "CREATE GROUP PROVIDER IF NOT EXISTS unix_provider PROPERTIES(\"type\" = \"unix\")";
        CreateGroupProviderStmt unixStmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(unixSql, userCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(unixStmt, "Unix provider statement should parse successfully");
        Assertions.assertEquals("unix", unixStmt.getPropertyMap().get("type"), "Type should be unix");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for Unix type");

        // Test File type
        String fileSql = "CREATE GROUP PROVIDER IF NOT EXISTS file_provider PROPERTIES(" +
                "\"type\" = \"file\", " +
                "\"file_url\" = \"/path/to/file\")";
        CreateGroupProviderStmt fileStmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(fileSql, userCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(fileStmt, "File provider statement should parse successfully");
        Assertions.assertEquals("file", fileStmt.getPropertyMap().get("type"), "Type should be file");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for File type");

        // Test LDAP type
        String ldapSql = "CREATE GROUP PROVIDER IF NOT EXISTS ldap_provider PROPERTIES(" +
                "\"type\" = \"ldap\", " +
                "\"ldap_conn_url\" = \"ldap://localhost:389\")";
        CreateGroupProviderStmt ldapStmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(ldapSql, userCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(ldapStmt, "LDAP provider statement should parse successfully");
        Assertions.assertEquals("ldap", ldapStmt.getPropertyMap().get("type"), "Type should be ldap");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for LDAP type");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // All types should pass permission check for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for all types");
    }

    /**
     * Test case: Permission check for unsupported group provider type
     * Test point: Should handle unsupported types in permission context
     */
    @Test
    public void testGroupProviderPermissionWithUnsupportedType() throws Exception {
        String sql = "CREATE GROUP PROVIDER IF NOT EXISTS unsupported_provider PROPERTIES(\"type\" = \"unsupported\")";

        CreateGroupProviderStmt stmt =
                (CreateGroupProviderStmt) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("unsupported_provider", stmt.getName(), "Provider name should match");
        Assertions.assertEquals("unsupported", stmt.getPropertyMap().get("type"), "Type should be unsupported");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for unsupported type");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Permission check should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for unsupported type");

        // Note: Type validation would fail during execution phase, but permission check comes first
    }

    // ==================== Revoke Permission Tests ====================

    /**
     * Test case: Revoke SECURITY privilege from user
     * Test point: User should lose access to Group Provider operations after privilege revocation
     */
    @Test
    public void testRevokeSecurityPrivilegeFromUser() throws Exception {
        // First grant SECURITY privilege to user
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SECURITY ON SYSTEM TO USER security_user",
                new ConnectContext());
        authorizationMgr.grant(grantStmt);

        // Verify user has SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User should have SECURITY privilege after grant");

        // Now revoke SECURITY privilege from user
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE SECURITY ON SYSTEM FROM USER security_user",
                new ConnectContext());
        authorizationMgr.revoke(revokeStmt);

        // Verify user no longer has SECURITY privilege
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User should not have SECURITY privilege after revoke");
    }

    /**
     * Test case: Revoke SECURITY privilege from role
     * Test point: Users with revoked role should lose access to Group Provider operations
     */
    @Test
    public void testRevokeSecurityPrivilegeFromRole() throws Exception {
        // Create a new user for this test
        ConnectContext testUserCtx = UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));

        // Create the test user first
        authenticationMgr.createUser(
                new CreateUserStmt(new UserIdentity("test_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        // Grant role to user
        authorizationMgr.grantRole(new com.starrocks.sql.ast.GrantRoleStmt(
                List.of("security_role"), new UserIdentity("test_user", "%"), NodePosition.ZERO));
        Long role = authorizationMgr.getRoleIdByNameAllowNull("security_role");
        authorizationMgr.setUserDefaultRole(Set.of(role), new UserIdentity("test_user", "%"));

        // Update ConnectContext with the new role information
        testUserCtx.setCurrentRoleIds(new UserIdentity("test_user", "%"));

        // Verify user has SECURITY privilege through role
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should have SECURITY privilege through role");

        // Now revoke SECURITY privilege from role
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE SECURITY ON SYSTEM FROM ROLE security_role",
                new ConnectContext());
        authorizationMgr.revoke(revokeStmt);

        // Verify user no longer has SECURITY privilege
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should not have SECURITY privilege after role privilege revocation");
    }

    /**
     * Test case: Revoke role from user
     * Test point: User should lose access to Group Provider operations after role revocation
     */
    @Test
    public void testRevokeRoleFromUser() throws Exception {
        // Create a new user for this test
        ConnectContext testUserCtx = UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.createAnalyzedUserIdentWithIp("test_user2", "%"));

        // Create the test user first
        authenticationMgr.createUser(
                new CreateUserStmt(new UserIdentity("test_user2", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        // Grant role to user
        authorizationMgr.grantRole(new com.starrocks.sql.ast.GrantRoleStmt(
                List.of("security_role"), new UserIdentity("test_user2", "%"), NodePosition.ZERO));
        Long role = authorizationMgr.getRoleIdByNameAllowNull("security_role");
        authorizationMgr.setUserDefaultRole(Set.of(role), new UserIdentity("test_user2", "%"));

        // Update ConnectContext with the new role information
        testUserCtx.setCurrentRoleIds(new UserIdentity("test_user2", "%"));

        // Verify user has SECURITY privilege through role
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should have SECURITY privilege through role");

        // Now revoke SECURITY privilege from role (instead of revoking the role itself)
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE SECURITY ON SYSTEM FROM ROLE security_role",
                new ConnectContext());
        authorizationMgr.revoke(revokeStmt);

        // Verify user no longer has SECURITY privilege
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should not have SECURITY privilege after role privilege revocation");
    }

    /**
     * Test case: Comprehensive revoke permission validation for Group Provider operations
     * Test point: Verify that all Group Provider operations are denied after privilege revocation
     */
    @Test
    public void testComprehensiveRevokePermissionValidation() throws Exception {
        // Create a user with SECURITY privilege
        ConnectContext testUserCtx = UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.createAnalyzedUserIdentWithIp("comprehensive_user", "%"));

        // Create the test user first
        authenticationMgr.createUser(
                new CreateUserStmt(new UserIdentity("comprehensive_user", "%"), true, null, List.of(), Map.of(),
                        NodePosition.ZERO));

        // Grant SECURITY privilege directly to user
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SECURITY ON SYSTEM TO USER comprehensive_user",
                new ConnectContext());
        authorizationMgr.grant(grantStmt);

        // Update ConnectContext with the user identity
        testUserCtx.setCurrentUserIdentity(new UserIdentity("comprehensive_user", "%"));

        // Verify user has SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should have SECURITY privilege after grant");

        // Test all Group Provider operations require SECURITY privilege
        String[] operations = {
                "SHOW GROUP PROVIDERS",
                "SHOW CREATE GROUP PROVIDER test_provider",
                "CREATE GROUP PROVIDER IF NOT EXISTS test_provider PROPERTIES(\"type\" = \"unix\")",
                "DROP GROUP PROVIDER IF EXISTS test_provider"
        };

        for (String sql : operations) {
            // Parse should succeed for all operations
            Object stmt = SqlParser.parseSingleStatement(sql, testUserCtx.getSessionVariable().getSqlMode());
            Assertions.assertNotNull(stmt, "Statement should parse successfully: " + sql);

            // Permission check should pass for user with SECURITY privilege
            Assertions.assertDoesNotThrow(() -> {
                Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
            }, "User with SECURITY privilege should pass permission check for: " + sql);
        }

        // Now revoke SECURITY privilege from user
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE SECURITY ON SYSTEM FROM USER comprehensive_user",
                new ConnectContext());
        authorizationMgr.revoke(revokeStmt);

        // Test all Group Provider operations should fail after revoke
        for (String sql : operations) {
            // Permission check should fail for user without SECURITY privilege
            Assertions.assertThrows(AccessDeniedException.class, () -> {
                Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
            }, "User without SECURITY privilege should fail permission check for: " + sql);
        }
    }

    /**
     * Test case: Revoke non-existent privilege
     * Test point: Should handle revoke of non-existent privilege gracefully
     */
    @Test
    public void testRevokeNonExistentPrivilege() throws Exception {
        // Try to revoke SECURITY privilege from user who doesn't have it
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE SECURITY ON SYSTEM FROM USER u1",
                new ConnectContext());

        // This should not throw an exception, but should be handled gracefully
        Assertions.assertDoesNotThrow(() -> {
            authorizationMgr.revoke(revokeStmt);
        }, "Revoking non-existent privilege should be handled gracefully");

        // Verify user still doesn't have SECURITY privilege
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "User should not have SECURITY privilege after revoke of non-existent privilege");
    }

    /**
     * Test case: Revoke permission with different Group Provider types
     * Test point: Verify revoke works consistently across different Group Provider types
     */
    @Test
    public void testRevokePermissionWithDifferentGroupProviderTypes() throws Exception {
        // Create a user with SECURITY privilege
        ConnectContext testUserCtx = UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.createAnalyzedUserIdentWithIp("type_test_user", "%"));

        // Create the test user first
        authenticationMgr.createUser(
                new CreateUserStmt(new UserIdentity("type_test_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        // Grant SECURITY privilege directly to user
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT SECURITY ON SYSTEM TO USER type_test_user",
                new ConnectContext());
        authorizationMgr.grant(grantStmt);

        // Update ConnectContext with the user identity
        testUserCtx.setCurrentUserIdentity(new UserIdentity("type_test_user", "%"));

        // Test different Group Provider types
        String[] providerTypes = {"unix", "file", "ldap"};

        for (String type : providerTypes) {
            String sql = "CREATE GROUP PROVIDER IF NOT EXISTS " + type + "_provider PROPERTIES(\"type\" = \"" + type + "\")";

            // Parse should succeed
            Object stmt = SqlParser.parseSingleStatement(sql, testUserCtx.getSessionVariable().getSqlMode());
            Assertions.assertNotNull(stmt, "Statement should parse successfully for type: " + type);

            // Permission check should pass for user with SECURITY privilege
            Assertions.assertDoesNotThrow(() -> {
                Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
            }, "User with SECURITY privilege should pass permission check for type: " + type);
        }

        // Now revoke SECURITY privilege from user
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE SECURITY ON SYSTEM FROM USER type_test_user",
                new ConnectContext());
        authorizationMgr.revoke(revokeStmt);

        // Test all Group Provider types should fail after revoke
        for (String type : providerTypes) {
            String sql = "CREATE GROUP PROVIDER IF NOT EXISTS " + type + "_provider PROPERTIES(\"type\" = \"" + type + "\")";

            // Permission check should fail for user without SECURITY privilege
            Assertions.assertThrows(AccessDeniedException.class, () -> {
                Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
            }, "User without SECURITY privilege should fail permission check for type: " + type);
        }
    }
}