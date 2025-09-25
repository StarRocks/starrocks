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
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for Security Integration permission control
 * Based on permission errors from test/sql/test_security_integration
 * <p>
 * These tests verify that Security Integration operations require SECURITY privilege:
 * - CREATE SECURITY INTEGRATION
 * - DROP SECURITY INTEGRATION
 * - SHOW SECURITY INTEGRATIONS
 * - SHOW CREATE SECURITY INTEGRATION
 * - ALTER SECURITY INTEGRATION
 * <p>
 * Test scenarios include:
 * - Non-admin users should be denied access (AccessDeniedException)
 * - Users with SECURITY privilege should be granted access
 * - Root user should always have access
 * - Various Security Integration types (JWT, SAML) and configurations
 * - Quoted identifiers and complex properties
 */
public class SecurityIntegrationPermissionTest {
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

        createUserStmt =
                new CreateUserStmt(new UserIdentity("security_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO);
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
     * Test case: Non-admin user attempting to CREATE SECURITY INTEGRATION
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; create security integration oidc2 properties(...)
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testCreateSecurityIntegrationPermissionDenied() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION oidc2 PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\")";

        // Parse should succeed regardless of user context
        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc2", stmt.getName(), "Integration name should match");
        Assertions.assertEquals("authentication_jwt", stmt.getPropertyMap().get("type"),
                "Type property should be parsed correctly");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for CREATE SECURITY INTEGRATION");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for CREATE SECURITY INTEGRATION");
    }

    /**
     * Test case: Non-admin user attempting to SHOW SECURITY INTEGRATIONS
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; show security integrations
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testShowSecurityIntegrationsPermissionDenied() throws Exception {
        String sql = "SHOW SECURITY INTEGRATIONS";

        // Parse should succeed regardless of user context
        ShowSecurityIntegrationStatement stmt =
                (ShowSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for SHOW SECURITY INTEGRATIONS");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for SHOW SECURITY INTEGRATIONS");
    }

    /**
     * Test case: Non-admin user attempting to SHOW CREATE SECURITY INTEGRATION
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; show create security integration oidc
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testShowCreateSecurityIntegrationPermissionDenied() throws Exception {
        String sql = "SHOW CREATE SECURITY INTEGRATION oidc";

        // Parse should succeed regardless of user context
        ShowCreateSecurityIntegrationStatement stmt =
                (ShowCreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for SHOW CREATE SECURITY INTEGRATION");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for SHOW CREATE SECURITY INTEGRATION");
    }

    /**
     * Test case: Non-admin user attempting to DROP SECURITY INTEGRATION
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; drop security integration oidc
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testDropSecurityIntegrationPermissionDenied() throws Exception {
        String sql = "DROP SECURITY INTEGRATION oidc";

        // Parse should succeed regardless of user context
        DropSecurityIntegrationStatement stmt =
                (DropSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql, userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for DROP SECURITY INTEGRATION");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for DROP SECURITY INTEGRATION");
    }

    /**
     * Test case: Non-admin user attempting to ALTER SECURITY INTEGRATION
     * Test point: Should parse successfully but fail during permission check
     * Based on: execute as u1 with no revert; alter security integration oidc set (...)
     * Expected error: (5203, 'Access denied; you need (at least one of) the SECURITY privilege(s) on SYSTEM for this operation.')
     */
    @Test
    public void testAlterSecurityIntegrationPermissionDenied() throws Exception {
        String sql = "ALTER SECURITY INTEGRATION oidc SET (\"principal_field\" = \"preferred_name\")";

        // Parse should succeed regardless of user context
        AlterSecurityIntegrationStatement stmt =
                (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("oidc", stmt.getName(), "Integration name should match");
        Assertions.assertEquals("preferred_name", stmt.getProperties().get("principal_field"),
                "Property should be parsed correctly");

        // Test permission check - should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for ALTER SECURITY INTEGRATION");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // Test permission check - should succeed for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for ALTER SECURITY INTEGRATION");
    }

    /**
     * Test case: Root user can execute all security integration operations
     * Test point: Should parse successfully for admin user
     */
    @Test
    public void testSecurityIntegrationOperationsAsRoot() throws Exception {
        // Test CREATE
        String createSql = "CREATE SECURITY INTEGRATION test_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\")";
        CreateSecurityIntegrationStatement createStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(createSql,
                        rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(createStmt, "Create statement should parse successfully");
        Assertions.assertEquals("test_oidc", createStmt.getName(), "Integration name should match");

        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for CREATE SECURITY INTEGRATION");

        // Test SHOW CREATE
        String showCreateSql = "SHOW CREATE SECURITY INTEGRATION test_oidc";
        ShowCreateSecurityIntegrationStatement showCreateStmt =
                (ShowCreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(showCreateSql,
                        rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(showCreateStmt, "Show create statement should parse successfully");
        Assertions.assertEquals("test_oidc", showCreateStmt.getName(), "Integration name should match");

        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for SHOW CREATE SECURITY INTEGRATION");

        // Test SHOW ALL
        String showAllSql = "SHOW SECURITY INTEGRATIONS";
        ShowSecurityIntegrationStatement showAllStmt =
                (ShowSecurityIntegrationStatement) SqlParser.parseSingleStatement(showAllSql,
                        rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(showAllStmt, "Show all statement should parse successfully");

        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for SHOW SECURITY INTEGRATIONS");

        // Test ALTER
        String alterSql = "ALTER SECURITY INTEGRATION test_oidc SET (\"principal_field\" = \"sub\")";
        AlterSecurityIntegrationStatement alterStmt =
                (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(alterSql,
                        rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(alterStmt, "Alter statement should parse successfully");
        Assertions.assertEquals("test_oidc", alterStmt.getName(), "Integration name should match");

        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for ALTER SECURITY INTEGRATION");

        // Test DROP
        String dropSql = "DROP SECURITY INTEGRATION test_oidc";
        DropSecurityIntegrationStatement dropStmt =
                (DropSecurityIntegrationStatement) SqlParser.parseSingleStatement(dropSql,
                        rootCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(dropStmt, "Drop statement should parse successfully");
        Assertions.assertEquals("test_oidc", dropStmt.getName(), "Integration name should match");

        // Test permission check - root user should have SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(rootCtx, PrivilegeType.SECURITY);
        }, "Root user should have SECURITY privilege for DROP SECURITY INTEGRATION");
    }

    /**
     * Test case: Permission check for impersonation context
     * Test point: Should handle impersonation context correctly
     * Based on: grant impersonate on user root to u1; execute as u1 with no revert
     */
    @Test
    public void testSecurityIntegrationWithImpersonation() throws Exception {
        // Test that impersonation context doesn't affect parsing
        String sql = "CREATE SECURITY INTEGRATION impersonated_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully in impersonation context");
        Assertions.assertEquals("impersonated_oidc", stmt.getName(), "Integration name should match");

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
     * Test case: Permission check for quoted identifiers
     * Test point: Should handle quoted identifiers in permission context
     */
    @Test
    public void testSecurityIntegrationPermissionWithQuotedIdentifiers() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION `test-integration` PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("test-integration", stmt.getName(), "Integration name should match");

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
     * Test case: Permission check for complex security integration
     * Test point: Should handle complex properties in permission context
     */
    @Test
    public void testSecurityIntegrationPermissionWithComplexProperties() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION complex_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"https://example.com/.well-known/jwks.json\", " +
                "\"principal_field\" = \"sub\", " +
                "\"issuer\" = \"https://example.com\", " +
                "\"audience\" = \"starrocks-client\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("complex_oidc", stmt.getName(), "Integration name should match");
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
     * Test case: Comprehensive permission validation for Security Integration operations
     * Test point: Verify that all Security Integration operations require SECURITY privilege
     */
    @Test
    public void testComprehensiveSecurityIntegrationPermissionValidation() throws Exception {
        // Test all Security Integration operations require SECURITY privilege
        String[] operations = {
                "SHOW SECURITY INTEGRATIONS",
                "SHOW CREATE SECURITY INTEGRATION test_integration",
                "CREATE SECURITY INTEGRATION test_integration PROPERTIES(\"type\" = \"authentication_jwt\")",
                "DROP SECURITY INTEGRATION test_integration",
                "ALTER SECURITY INTEGRATION test_integration SET (\"principal_field\" = \"sub\")"
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
     * Test case: Permission check for different security integration types
     * Test point: Should handle different types in permission context
     */
    @Test
    public void testSecurityIntegrationPermissionWithDifferentTypes() throws Exception {
        // Test authentication_jwt type
        String jwtSql = "CREATE SECURITY INTEGRATION jwt_integration PROPERTIES(\"type\" = \"authentication_jwt\")";
        CreateSecurityIntegrationStatement jwtStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(jwtSql,
                        userCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(jwtStmt, "JWT integration statement should parse successfully");
        Assertions.assertEquals("authentication_jwt", jwtStmt.getPropertyMap().get("type"), "Type should be authentication_jwt");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for JWT type");

        // Test authentication_saml type
        String samlSql = "CREATE SECURITY INTEGRATION saml_integration PROPERTIES(" +
                "\"type\" = \"authentication_saml\", " +
                "\"saml_idp_url\" = \"https://example.com/saml\")";
        CreateSecurityIntegrationStatement samlStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(samlSql,
                        userCtx.getSessionVariable().getSqlMode());
        Assertions.assertNotNull(samlStmt, "SAML integration statement should parse successfully");
        Assertions.assertEquals("authentication_saml", samlStmt.getPropertyMap().get("type"),
                "Type should be authentication_saml");

        // Permission check should fail for non-admin user
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(userCtx, PrivilegeType.SECURITY);
        }, "Non-admin user should not have SECURITY privilege for SAML type");

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("security_user", "%"), false), securityUserCtx);
        // All types should pass permission check for user with SECURITY privilege
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(securityUserCtx, PrivilegeType.SECURITY);
        }, "User with SECURITY privilege should pass permission check for all types");
    }

    /**
     * Test case: Permission check for unsupported security integration type
     * Test point: Should handle unsupported types in permission context
     */
    @Test
    public void testSecurityIntegrationPermissionWithUnsupportedType() throws Exception {
        String sql = "CREATE SECURITY INTEGRATION unsupported_integration PROPERTIES(\"type\" = \"unsupported\")";

        CreateSecurityIntegrationStatement stmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                        userCtx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt, "Statement should parse successfully");
        Assertions.assertEquals("unsupported_integration", stmt.getName(), "Integration name should match");
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
     * Test point: User should lose access to Security Integration operations after privilege revocation
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
     * Test point: Users with revoked role should lose access to Security Integration operations
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
     * Test point: User should lose access to Security Integration operations after role revocation
     */
    @Test
    public void testRevokeRoleFromUser() throws Exception {
        // Create the test user first
        authenticationMgr.createUser(
                new CreateUserStmt(new UserIdentity("test_user2", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        // Grant role to user
        authorizationMgr.grantRole(new com.starrocks.sql.ast.GrantRoleStmt(
                List.of("security_role"), new UserIdentity("test_user2", "%"), NodePosition.ZERO));
        Long role = authorizationMgr.getRoleIdByNameAllowNull("security_role");
        Set<Long> defaultRoles = new HashSet<>();
        defaultRoles.add(role);
        authorizationMgr.setUserDefaultRole(defaultRoles, new UserIdentity("test_user2", "%"));

        ConnectContext testUserCtx = new ConnectContext();
        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserIdentity("test_user2", "%"), false), testUserCtx);

        // Update ConnectContext with the new role information
        testUserCtx.setCurrentRoleIds(new UserIdentity("test_user2", "%"));

        // Verify user has SECURITY privilege through role
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should have SECURITY privilege through role");

        // Now revoke role from user
        authorizationMgr.revokeRole(new com.starrocks.sql.ast.RevokeRoleStmt(
                List.of("security_role"), new UserIdentity("test_user2", "%"), NodePosition.ZERO));

        // Verify user no longer has SECURITY privilege
        Assertions.assertThrows(AccessDeniedException.class, () -> {
            Authorizer.checkSystemAction(testUserCtx, PrivilegeType.SECURITY);
        }, "User should not have SECURITY privilege after role revocation");
    }

    /**
     * Test case: Comprehensive revoke permission validation
     * Test point: Verify that all Security Integration operations are denied after privilege revocation
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

        // Test all Security Integration operations require SECURITY privilege
        String[] operations = {
                "SHOW SECURITY INTEGRATIONS",
                "SHOW CREATE SECURITY INTEGRATION test_integration",
                "CREATE SECURITY INTEGRATION test_integration PROPERTIES(\"type\" = \"authentication_jwt\")",
                "DROP SECURITY INTEGRATION test_integration",
                "ALTER SECURITY INTEGRATION test_integration SET (\"principal_field\" = \"sub\")"
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

        // Test all Security Integration operations should fail after revoke
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
}