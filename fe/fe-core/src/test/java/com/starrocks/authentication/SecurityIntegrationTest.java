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

import com.google.common.base.Joiner;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class SecurityIntegrationTest {
    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();
    private ConnectContext ctx;
    private AuthenticationMgr authenticationMgr;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        // Initialize authentication manager
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    @AfterEach
    public void tearDown() throws Exception {
    }

    /**
     * Test case: JWT Security Integration property validation
     * Test point: Validate JWT-specific properties and their processing
     */
    @Test
    public void testJWTSecurityIntegrationProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("group_provider", "A, B, C");
        properties.put("permitted_groups", "B");

        JWTSecurityIntegration oidcSecurityIntegration =
                new JWTSecurityIntegration("oidc", properties);

        List<String> groupProviderNameList = oidcSecurityIntegration.getGroupProviderName();
        Assertions.assertEquals("A,B,C", Joiner.on(",").join(groupProviderNameList),
                "Group provider names should be parsed correctly");

        List<String> permittedGroups = oidcSecurityIntegration.getGroupAllowedLoginList();
        Assertions.assertEquals("B", Joiner.on(",").join(permittedGroups),
                "Permitted groups should be parsed correctly");

        // Test with empty properties
        oidcSecurityIntegration = new JWTSecurityIntegration("oidc", new HashMap<>());
        Assertions.assertTrue(oidcSecurityIntegration.getGroupProviderName().isEmpty(),
                "Empty group provider list should be handled correctly");
        Assertions.assertTrue(oidcSecurityIntegration.getGroupAllowedLoginList().isEmpty(),
                "Empty permitted groups list should be handled correctly");

        // Test with empty string properties
        properties = new HashMap<>();
        properties.put("group_provider", "");
        properties.put("permitted_groups", "");
        oidcSecurityIntegration = new JWTSecurityIntegration("oidc", properties);
        Assertions.assertTrue(oidcSecurityIntegration.getGroupProviderName().isEmpty(),
                "Empty string group provider should be handled correctly");
        Assertions.assertTrue(oidcSecurityIntegration.getGroupAllowedLoginList().isEmpty(),
                "Empty string permitted groups should be handled correctly");
    }

    @Test
    public void testAuthentication() throws Exception {
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());

        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        authenticationMgr.createSecurityIntegration("oidc2", properties, true);

        Config.authentication_chain = new String[] {"native", "oidc2"};

        String idToken = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MysqlCodec.writeInt1(outputStream, 1);
        MysqlCodec.writeLenEncodedString(outputStream, idToken);

        ConnectContext connectContext = new ConnectContext();
        connectContext.setAuthPlugin(AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString());
        AuthenticationHandler.authenticate(
                connectContext, "harbor", "127.0.0.1", outputStream.toByteArray());
    }

    private String getOpenIdConnect(String fileName) throws IOException {
        String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
        File file = new File(path + "/" + fileName);
        BufferedReader reader = new BufferedReader(new FileReader(file));

        StringBuilder sb = new StringBuilder();
        String tempStr;
        while ((tempStr = reader.readLine()) != null) {
            sb.append(tempStr);
        }

        return sb.toString();
    }

    @Test
    public void testGroupProvider() throws Exception {
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());

        Map<String, String> properties = new HashMap<>();
        properties.put(JWTSecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, "file_group_provider");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "group1");

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        authenticationMgr.createSecurityIntegration("oidc", properties, true);

        new MockUp<FileGroupProvider>() {
            @Mock
            public InputStream getPath(String groupFileUrl) throws IOException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath() + "/" + "file_group";
                return new FileInputStream(path);
            }
        };
        Map<String, String> groupProvider = new HashMap<>();
        groupProvider.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file");
        groupProvider.put(FileGroupProvider.GROUP_FILE_URL, "file_group");
        authenticationMgr.replayCreateGroupProvider("file_group_provider", groupProvider);

        String idToken = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MysqlCodec.writeInt1(outputStream, 1);
        MysqlCodec.writeLenEncodedString(outputStream, idToken);

        Config.group_provider = new String[] {"file_group_provider"};
        Config.authentication_chain = new String[] {"native", "oidc"};

        try {
            ConnectContext connectContext = new ConnectContext();
            connectContext.setAuthPlugin(AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString());
            AuthenticationHandler.authenticate(
                    connectContext, "harbor", "127.0.0.1", outputStream.toByteArray());
            StatementBase statementBase = SqlParser.parse("select current_group()", connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);

            QueryStatement queryStatement = (QueryStatement) statementBase;
            InformationFunction informationFunction =
                    (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
            Assertions.assertEquals("group2, group1", informationFunction.getStrValue());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "group_5");
        authenticationMgr.alterSecurityIntegration("oidc", alterProperties, true);
        Assertions.assertThrows(AuthenticationException.class, () -> AuthenticationHandler.authenticate(
                new ConnectContext(), "harbor", "127.0.0.1", outputStream.toByteArray()));
    }

    @Test
    public void testLDAPSecurityIntegration() throws DdlException, AuthenticationException, IOException {
        Map<String, String> properties = new HashMap<>();

        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN, "cn=admin,dc=example,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD, "");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN, "");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR, "");
        SimpleLDAPSecurityIntegration ldapSecurityIntegration = new SimpleLDAPSecurityIntegration("ldap", properties);

        SimpleLDAPSecurityIntegration finalLdapSecurityIntegration = ldapSecurityIntegration;
        Assertions.assertThrows(SemanticException.class, finalLdapSecurityIntegration::checkProperty);

        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        ldapSecurityIntegration = new SimpleLDAPSecurityIntegration("ldap", properties);
        Assertions.assertNotNull(ldapSecurityIntegration.getAuthenticationProvider());
        Assertions.assertNotNull(SecurityIntegrationFactory.createSecurityIntegration("ldap", properties));

        LDAPAuthProvider ldapAuthProviderForNative =
                (LDAPAuthProvider) ldapSecurityIntegration.getAuthenticationProvider();

        ConnectContext context = new ConnectContext();
        context.setAuthPlugin(AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString());

        Assertions.assertThrows(AuthenticationException.class, () ->
                ldapAuthProviderForNative.authenticate(
                        context.getAccessControlContext(),
                        new UserIdentity("admin", "%"),
                        "x".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testLDAPSecurityIntegrationPassword() throws DdlException, AuthenticationException, IOException {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN, "cn=admin,dc=example,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD, "12345");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN, "");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR, "");
        authenticationMgr.createSecurityIntegration("ldap", properties, true);

        ShowResultSet resultSet =
                ShowExecutor.execute(new ShowCreateSecurityIntegrationStatement("ldap", NodePosition.ZERO), null);
        Assert.assertTrue(
                resultSet.getResultRows().get(0).get(1).contains("\"authentication_ldap_simple_bind_root_pwd\" = \"***\""));

        properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_oauth2");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_CLIENT_SECRET, "123");
        authenticationMgr.createSecurityIntegration("oauth2", properties, true);
        resultSet =
                ShowExecutor.execute(new ShowCreateSecurityIntegrationStatement("oauth2", NodePosition.ZERO), null);
        Assert.assertTrue(
                resultSet.getResultRows().get(0).get(1).contains("\"client_secret\" = \"***\""));
    }

    @Test
    public void testShowCreateGroupProviderPassword() throws DdlException {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put("ldap_bind_root_dn", "cn=admin,dc=example,dc=com");
        properties.put("ldap_bind_root_pwd", "12345");
        properties.put("ldap_search_base_dn", "dc=example,dc=com");
        authenticationMgr.replayCreateGroupProvider("ldap_group", properties);

        ShowResultSet resultSet =
                ShowExecutor.execute(new ShowCreateGroupProviderStmt("ldap_group", NodePosition.ZERO), null);
        Assert.assertTrue(
                resultSet.getResultRows().get(0).get(1).contains("\"ldap_bind_root_pwd\" = \"***\""));
    }

    /**
     * Test case: Complete Security Integration lifecycle with actual execution
     * Test point: End-to-end testing of all security integration operations
     */
    @Test
    public void testCompleteSecurityIntegrationLifecycle() throws Exception {
        // Step 1: Create Security Integration with OIDC properties
        String createSql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement createStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(createSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(createStmt, "Create statement should parse successfully");
        Assertions.assertEquals("oidc", createStmt.getName(), "Integration name should match");

        Map<String, String> properties = createStmt.getPropertyMap();
        Assertions.assertEquals("authentication_jwt", properties.get("type"), "Type should be authentication_jwt");
        Assertions.assertEquals("jwks.json", properties.get("jwks_url"), "JWKS URL should match");
        Assertions.assertEquals("sub", properties.get("principal_field"), "Principal field should match");

        // Actually create the security integration
        authenticationMgr.createSecurityIntegration("oidc", properties, true);
        Assertions.assertNotNull(authenticationMgr.getSecurityIntegration("oidc"),
                "Security integration should be created successfully");

        // Step 2: Show CREATE Security Integration
        String showCreateSql = "SHOW CREATE SECURITY INTEGRATION oidc";
        ShowCreateSecurityIntegrationStatement showCreateStmt =
                (ShowCreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(showCreateSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(showCreateStmt, "Show create statement should parse successfully");
        Assertions.assertEquals("oidc", showCreateStmt.getName(), "Integration name should match");

        // Actually execute show create
        ShowResultSet showResult = ShowExecutor.execute(showCreateStmt, ctx);
        Assertions.assertNotNull(showResult, "Show create should execute successfully");
        Assertions.assertFalse(showResult.getResultRows().isEmpty(), "Show create should return results");

        // Step 3: Alter Security Integration
        String alterSql = "ALTER SECURITY INTEGRATION oidc SET (\"principal_field\" = \"preferred_name\")";
        AlterSecurityIntegrationStatement alterStmt =
                (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(alterSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(alterStmt, "Alter statement should parse successfully");
        Assertions.assertEquals("oidc", alterStmt.getName(), "Integration name should match");
        Assertions.assertEquals("preferred_name", alterStmt.getProperties().get("principal_field"),
                "Principal field should be updated");

        // Actually alter the security integration
        authenticationMgr.alterSecurityIntegration("oidc", alterStmt.getProperties(), true);
        SecurityIntegration alteredIntegration = authenticationMgr.getSecurityIntegration("oidc");
        Assertions.assertNotNull(alteredIntegration, "Altered security integration should exist");

        // Step 4: Show all Security Integrations
        String showAllSql = "SHOW SECURITY INTEGRATIONS";
        ShowSecurityIntegrationStatement showAllStmt =
                (ShowSecurityIntegrationStatement) SqlParser.parseSingleStatement(showAllSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(showAllStmt, "Show all statement should parse successfully");

        // Actually execute show all
        ShowResultSet showAllResult = ShowExecutor.execute(showAllStmt, ctx);
        Assertions.assertNotNull(showAllResult, "Show all should execute successfully");
        Assertions.assertFalse(showAllResult.getResultRows().isEmpty(), "Show all should return results");

        // Step 5: Drop Security Integration
        String dropSql = "DROP SECURITY INTEGRATION oidc";
        DropSecurityIntegrationStatement dropStmt =
                (DropSecurityIntegrationStatement) SqlParser.parseSingleStatement(dropSql, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(dropStmt, "Drop statement should parse successfully");
        Assertions.assertEquals("oidc", dropStmt.getName(), "Integration name should match");

        // Actually drop the security integration
        authenticationMgr.dropSecurityIntegration("oidc", true);
        Assertions.assertNull(authenticationMgr.getSecurityIntegration("oidc"),
                "Security integration should be dropped successfully");
    }

    /**
     * Test case: Error scenarios and exception handling
     * Test point: Validation of error conditions and exception handling
     */
    @Test
    public void testSecurityIntegrationErrorScenarios() throws Exception {
        // Test 1: Missing required 'type' property - should fail during creation
        String missingTypeSql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement missingTypeStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(missingTypeSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(missingTypeStmt, "Statement should parse successfully");
        Assertions.assertFalse(missingTypeStmt.getPropertyMap().containsKey("type"),
                "Missing type property should be detected");

        // Actually try to create with missing type - should throw exception
        Assertions.assertThrows(SemanticException.class, () -> {
            authenticationMgr.createSecurityIntegration("oidc", missingTypeStmt.getPropertyMap(), true);
        }, "Creating security integration without type should fail");

        // Test 2: Valid statement with all required properties
        String validSql = "CREATE SECURITY INTEGRATION oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement validStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(validSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(validStmt, "Valid statement should parse successfully");
        Assertions.assertTrue(validStmt.getPropertyMap().containsKey("type"),
                "Valid statement should have type property");

        // Actually create with valid properties - should succeed
        authenticationMgr.createSecurityIntegration("oidc", validStmt.getPropertyMap(), true);
        Assertions.assertNotNull(authenticationMgr.getSecurityIntegration("oidc"),
                "Valid security integration should be created successfully");

        // Test 3: Duplicate creation - should fail
        Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.createSecurityIntegration("oidc", validStmt.getPropertyMap(), false);
        }, "Creating duplicate security integration should fail");

        // Test 4: Alter non-existent integration - should fail
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put("principal_field", "preferred_name");
        Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.alterSecurityIntegration("non_existent", alterProperties, false);
        }, "Altering non-existent security integration should fail");

        // Test 5: Drop non-existent integration - should fail
        Assertions.assertThrows(DdlException.class, () -> {
            authenticationMgr.dropSecurityIntegration("non_existent", false);
        }, "Dropping non-existent security integration should fail");

        // Clean up
        authenticationMgr.dropSecurityIntegration("oidc", true);
    }

    /**
     * Test case: Multiple Security Integrations
     * Test point: Handling multiple integrations with different configurations
     */
    @Test
    public void testMultipleSecurityIntegrations() throws Exception {
        // Create first integration
        String sql1 = "CREATE SECURITY INTEGRATION oidc1 PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks1.json\", " +
                "\"principal_field\" = \"sub\")";

        CreateSecurityIntegrationStatement stmt1 =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql1, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt1, "First integration should parse successfully");
        Assertions.assertEquals("oidc1", stmt1.getName(), "First integration name should match");

        // Actually create first integration
        authenticationMgr.createSecurityIntegration("oidc1", stmt1.getPropertyMap(), true);
        SecurityIntegration integration1 = authenticationMgr.getSecurityIntegration("oidc1");
        Assertions.assertNotNull(integration1, "First integration should be created successfully");

        // Create second integration
        String sql2 = "CREATE SECURITY INTEGRATION oidc2 PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks2.json\", " +
                "\"principal_field\" = \"preferred_name\")";

        CreateSecurityIntegrationStatement stmt2 =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql2, ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(stmt2, "Second integration should parse successfully");
        Assertions.assertEquals("oidc2", stmt2.getName(), "Second integration name should match");

        // Actually create second integration
        authenticationMgr.createSecurityIntegration("oidc2", stmt2.getPropertyMap(), true);
        SecurityIntegration integration2 = authenticationMgr.getSecurityIntegration("oidc2");
        Assertions.assertNotNull(integration2, "Second integration should be created successfully");

        // Verify different configurations
        Assertions.assertNotEquals(stmt1.getPropertyMap().get("jwks_url"),
                stmt2.getPropertyMap().get("jwks_url"),
                "Different integrations should have different JWKS URLs");
        Assertions.assertNotEquals(stmt1.getPropertyMap().get("principal_field"),
                stmt2.getPropertyMap().get("principal_field"),
                "Different integrations should have different principal fields");

        // Test that both integrations exist
        Assertions.assertNotNull(authenticationMgr.getSecurityIntegration("oidc1"),
                "First integration should still exist");
        Assertions.assertNotNull(authenticationMgr.getSecurityIntegration("oidc2"),
                "Second integration should still exist");

        // Test show all integrations
        String showAllSql = "SHOW SECURITY INTEGRATIONS";
        ShowSecurityIntegrationStatement showAllStmt =
                (ShowSecurityIntegrationStatement) SqlParser.parseSingleStatement(showAllSql,
                        ctx.getSessionVariable().getSqlMode());
        ShowResultSet showAllResult = ShowExecutor.execute(showAllStmt, ctx);
        Assertions.assertNotNull(showAllResult, "Show all should execute successfully");
        Assertions.assertTrue(showAllResult.getResultRows().size() >= 2,
                "Show all should return at least 2 integrations");

        // Clean up
        authenticationMgr.dropSecurityIntegration("oidc1", true);
        authenticationMgr.dropSecurityIntegration("oidc2", true);
    }

    /**
     * Test case: Security Integration Factory creation
     * Test point: Test SecurityIntegrationFactory for different types
     */
    @Test
    public void testSecurityIntegrationFactory() throws Exception {
        // Test JWT type creation
        Map<String, String> jwtProperties = new HashMap<>();
        jwtProperties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        jwtProperties.put("jwks_url", "jwks.json");
        jwtProperties.put("principal_field", "sub");

        SecurityIntegration jwtIntegration = SecurityIntegrationFactory.createSecurityIntegration("jwt_test", jwtProperties);
        Assertions.assertNotNull(jwtIntegration, "JWT security integration should be created successfully");
        Assertions.assertEquals("jwt_test", jwtIntegration.getName(), "JWT integration name should match");

        // Test unsupported type - should throw exception
        Map<String, String> unsupportedProperties = new HashMap<>();
        unsupportedProperties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "unsupported_type");

        Assertions.assertThrows(SemanticException.class, () -> {
            SecurityIntegrationFactory.createSecurityIntegration("unsupported_test", unsupportedProperties);
        }, "Creating unsupported security integration type should fail");
    }

    /**
     * Test case: Security Integration with Group Provider integration
     * Test point: Test integration with group providers
     */
    @Test
    public void testSecurityIntegrationWithGroupProvider() throws Exception {
        // Create security integration with group provider
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        properties.put("jwks_url", "jwks.json");
        properties.put("principal_field", "preferred_username");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, "file_group_provider");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "group1");

        authenticationMgr.createSecurityIntegration("oidc_with_groups", properties, true);
        SecurityIntegration integration = authenticationMgr.getSecurityIntegration("oidc_with_groups");
        Assertions.assertNotNull(integration, "Security integration with group provider should be created successfully");

        // Verify it's a JWT integration
        Assertions.assertTrue(integration instanceof JWTSecurityIntegration,
                "Integration should be JWT type");

        JWTSecurityIntegration jwtIntegration = (JWTSecurityIntegration) integration;
        List<String> groupProviders = jwtIntegration.getGroupProviderName();
        Assertions.assertEquals(1, groupProviders.size(), "Should have one group provider");
        Assertions.assertEquals("file_group_provider", groupProviders.get(0),
                "Group provider name should match");

        List<String> permittedGroups = jwtIntegration.getGroupAllowedLoginList();
        Assertions.assertEquals(1, permittedGroups.size(), "Should have one permitted group");
        Assertions.assertEquals("group1", permittedGroups.get(0),
                "Permitted group should match");

        // Clean up
        authenticationMgr.dropSecurityIntegration("oidc_with_groups", true);
    }

    /**
     * Test case: Password masking in show create
     * Test point: Test that sensitive properties are masked in show create output
     */
    @Test
    public void testPasswordMaskingInShowCreate() throws Exception {
        // Create security integration with sensitive properties
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_oauth2");
        properties.put("client_secret", "secret123");

        authenticationMgr.createSecurityIntegration("oauth2_test", properties, true);

        // Test show create - sensitive properties should be masked
        ShowCreateSecurityIntegrationStatement showCreateStmt =
                new ShowCreateSecurityIntegrationStatement("oauth2_test", NodePosition.ZERO);
        ShowResultSet resultSet = ShowExecutor.execute(showCreateStmt, ctx);

        Assertions.assertNotNull(resultSet, "Show create should execute successfully");
        Assertions.assertFalse(resultSet.getResultRows().isEmpty(), "Show create should return results");

        String createStatement = resultSet.getResultRows().get(0).get(1).toString();
        Assertions.assertTrue(createStatement.contains("\"client_secret\" = \"***\""),
                "Sensitive properties should be masked in show create output");

        // Clean up
        authenticationMgr.dropSecurityIntegration("oauth2_test", true);
    }

    /**
     * Test case: Edge cases and special characters
     * Test point: Handling edge cases and special characters in properties
     */
    @Test
    public void testSecurityIntegrationEdgeCases() throws Exception {
        // Test with special characters in integration name
        String specialNameSql = "CREATE SECURITY INTEGRATION `test-integration-123` PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"https://example.com/.well-known/jwks.json\")";

        CreateSecurityIntegrationStatement specialNameStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(specialNameSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(specialNameStmt, "Statement with special name should parse successfully");
        Assertions.assertEquals("test-integration-123", specialNameStmt.getName(),
                "Special name should be parsed correctly");

        // Test with complex JWKS URL
        String complexUrlSql = "CREATE SECURITY INTEGRATION complex_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"https://example.com/.well-known/jwks.json?version=1&format=json\", " +
                "\"principal_field\" = \"sub\", " +
                "\"issuer\" = \"https://example.com\", " +
                "\"audience\" = \"starrocks-client\")";

        CreateSecurityIntegrationStatement complexUrlStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(complexUrlSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(complexUrlStmt, "Statement with complex URL should parse successfully");
        Assertions.assertEquals("complex_oidc", complexUrlStmt.getName(), "Complex integration name should match");

        Map<String, String> complexProperties = complexUrlStmt.getPropertyMap();
        Assertions.assertTrue(complexProperties.get("jwks_url").contains("?"),
                "Complex URL should contain query parameters");
        Assertions.assertEquals("https://example.com", complexProperties.get("issuer"),
                "Issuer should be parsed correctly");
        Assertions.assertEquals("starrocks-client", complexProperties.get("audience"),
                "Audience should be parsed correctly");
    }

    /**
     * Test case: Case sensitivity and keyword variations
     * Test point: Handling case variations in SQL keywords
     */
    @Test
    public void testSecurityIntegrationCaseVariations() throws Exception {
        // Test with different case variations
        String[] caseVariations = {
                "CREATE SECURITY INTEGRATION test_oidc PROPERTIES(\"type\" = \"authentication_jwt\"," +
                        " \"jwks_url\" = \"jwks.json\")",
                "create security integration test_oidc properties(\"type\" = \"authentication_jwt\"," +
                        " \"jwks_url\" = \"jwks.json\")",
                "Create Security Integration test_oidc Properties(\"type\" = \"authentication_jwt\"," +
                        " \"jwks_url\" = \"jwks.json\")",
                "CREATE security INTEGRATION test_oidc PROPERTIES(\"type\" = \"authentication_jwt\"," +
                        " \"jwks_url\" = \"jwks.json\")"
        };

        for (int i = 0; i < caseVariations.length; i++) {
            String sql = caseVariations[i];
            CreateSecurityIntegrationStatement stmt =
                    (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(sql,
                            ctx.getSessionVariable().getSqlMode());

            Assertions.assertNotNull(stmt, "Case variation " + i + " should parse successfully");
            Assertions.assertEquals("test_oidc", stmt.getName(),
                    "Case variation " + i + " should have correct name");
            Assertions.assertEquals("authentication_jwt", stmt.getPropertyMap().get("type"),
                    "Case variation " + i + " should have correct type");
        }
    }

    /**
     * Test case: Property validation scenarios
     * Test point: Various property validation scenarios
     */
    @Test
    public void testSecurityIntegrationPropertyValidation() throws Exception {
        // Test single property
        String singlePropSql = "CREATE SECURITY INTEGRATION single_oidc PROPERTIES(\"type\" = \"authentication_jwt\")";
        CreateSecurityIntegrationStatement singlePropStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(singlePropSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(singlePropStmt, "Single property statement should parse successfully");
        Assertions.assertEquals(1, singlePropStmt.getPropertyMap().size(),
                "Single property should be handled correctly");

        // Test many properties
        String manyPropsSql = "CREATE SECURITY INTEGRATION many_oidc PROPERTIES(" +
                "\"type\" = \"authentication_jwt\", " +
                "\"jwks_url\" = \"jwks.json\", " +
                "\"principal_field\" = \"sub\", " +
                "\"issuer\" = \"https://example.com\", " +
                "\"audience\" = \"starrocks-client\", " +
                "\"algorithm\" = \"RS256\", " +
                "\"clock_skew\" = \"300\")";

        CreateSecurityIntegrationStatement manyPropsStmt =
                (CreateSecurityIntegrationStatement) SqlParser.parseSingleStatement(manyPropsSql,
                        ctx.getSessionVariable().getSqlMode());

        Assertions.assertNotNull(manyPropsStmt, "Many properties statement should parse successfully");
        Assertions.assertEquals(7, manyPropsStmt.getPropertyMap().size(),
                "Many properties should be handled correctly");
    }

    /**
     * Test case: Comprehensive validation scenarios
     * Test point: Test various validation scenarios for security integrations
     */
    @Test
    public void testComprehensiveValidationScenarios() throws Exception {
        // Test 1: Valid JWT integration
        Map<String, String> validJwtProperties = new HashMap<>();
        validJwtProperties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        validJwtProperties.put("jwks_url", "https://example.com/.well-known/jwks.json");
        validJwtProperties.put("principal_field", "sub");
        validJwtProperties.put("issuer", "https://example.com");
        validJwtProperties.put("audience", "starrocks-client");

        authenticationMgr.createSecurityIntegration("valid_jwt", validJwtProperties, true);
        SecurityIntegration validIntegration = authenticationMgr.getSecurityIntegration("valid_jwt");
        Assertions.assertNotNull(validIntegration, "Valid JWT integration should be created successfully");

        // Test 2: Integration with all optional properties
        Map<String, String> fullProperties = new HashMap<>();
        fullProperties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        fullProperties.put("jwks_url", "jwks.json");
        fullProperties.put("principal_field", "sub");
        fullProperties.put("issuer", "https://example.com");
        fullProperties.put("audience", "starrocks-client");
        fullProperties.put("algorithm", "RS256");
        fullProperties.put("clock_skew", "300");
        fullProperties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, "group1,group2");
        fullProperties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "allowed1,allowed2");

        authenticationMgr.createSecurityIntegration("full_jwt", fullProperties, true);
        SecurityIntegration fullIntegration = authenticationMgr.getSecurityIntegration("full_jwt");
        Assertions.assertNotNull(fullIntegration, "Full JWT integration should be created successfully");

        // Verify it's a JWT integration with all properties
        Assertions.assertTrue(fullIntegration instanceof JWTSecurityIntegration,
                "Full integration should be JWT type");

        // Clean up
        authenticationMgr.dropSecurityIntegration("valid_jwt", true);
        authenticationMgr.dropSecurityIntegration("full_jwt", true);
    }
}
