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
import com.starrocks.analysis.InformationFunction;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
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

    @Test
    public void testProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("group_provider", "A, B, C");
        properties.put("permitted_groups", "B");

        JWTSecurityIntegration oidcSecurityIntegration =
                new JWTSecurityIntegration("oidc", properties);

        List<String> groupProviderNameList = oidcSecurityIntegration.getGroupProviderName();
        Assertions.assertEquals("A,B,C", Joiner.on(",").join(groupProviderNameList));

        List<String> permittedGroups = oidcSecurityIntegration.getGroupAllowedLoginList();
        Assertions.assertEquals("B", Joiner.on(",").join(permittedGroups));

        oidcSecurityIntegration = new JWTSecurityIntegration("oidc", new HashMap<>());
        Assertions.assertTrue(oidcSecurityIntegration.getGroupProviderName().isEmpty());
        Assertions.assertTrue(oidcSecurityIntegration.getGroupAllowedLoginList().isEmpty());

        properties = new HashMap<>();
        properties.put("group_provider", "");
        properties.put("permitted_groups", "");
        oidcSecurityIntegration = new JWTSecurityIntegration("oidc", properties);
        Assertions.assertTrue(oidcSecurityIntegration.getGroupProviderName().isEmpty());
        Assertions.assertTrue(oidcSecurityIntegration.getGroupAllowedLoginList().isEmpty());
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
                        context,
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
}
