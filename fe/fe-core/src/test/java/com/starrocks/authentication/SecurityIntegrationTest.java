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
import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SecurityIntegrationTest {
    @Test
    public void testProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("group_provider", "A, B, C");
        properties.put("permitted_groups", "B");

        OIDCSecurityIntegration oidcSecurityIntegration =
                new OIDCSecurityIntegration("oidc", properties);

        List<String> groupProviderNameList = oidcSecurityIntegration.getGroupProviderName();
        Assert.assertEquals("A,B,C", Joiner.on(",").join(groupProviderNameList));

        List<String> permittedGroups = oidcSecurityIntegration.getGroupAllowedLoginList();
        Assert.assertEquals("B", Joiner.on(",").join(permittedGroups));

        oidcSecurityIntegration = new OIDCSecurityIntegration("oidc", new HashMap<>());
        Assert.assertTrue(oidcSecurityIntegration.getGroupProviderName().isEmpty());
        Assert.assertTrue(oidcSecurityIntegration.getGroupAllowedLoginList().isEmpty());

        properties = new HashMap<>();
        properties.put("group_provider", "");
        properties.put("permitted_groups", "");
        oidcSecurityIntegration = new OIDCSecurityIntegration("oidc", properties);
        Assert.assertTrue(oidcSecurityIntegration.getGroupProviderName().isEmpty());
        Assert.assertTrue(oidcSecurityIntegration.getGroupAllowedLoginList().isEmpty());
    }

    @Test
    public void testAuthentication() throws DdlException, AuthenticationException, IOException {
        GlobalStateMgr.getCurrentState().setJwkMgr(new JwkMgr() {
            @Override
            public JWKSet getJwkSet(String jwksUrl) throws IOException, ParseException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
                InputStream jwksInputStream = new FileInputStream(path + "/" + jwksUrl);
                return JWKSet.load(jwksInputStream);
            }
        });

        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_openid_connect");
        properties.put(OIDCSecurityIntegration.OIDC_JWKS_URL, "jwks.json");
        properties.put(OIDCSecurityIntegration.OIDC_PRINCIPAL_FIELD, "preferred_username");

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        authenticationMgr.createSecurityIntegration("oidc2", properties, true);


        Config.authentication_chain = new String[] {"native", "oidc2"};

        String idToken = getOpenIdConnect("oidc.json");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MysqlCodec.writeInt1(outputStream, 1);
        MysqlCodec.writeLenEncodedString(outputStream, idToken);

        AuthenticationHandler.authenticate(
                new ConnectContext(), "harbor", "127.0.0.1", outputStream.toByteArray(), null);
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
    public void testFileGroupProvider() throws DdlException, AuthenticationException, IOException, NoSuchMethodException {
        new MockUp<FileGroupProvider>() {
            @Mock
            public InputStream getPath(String groupFileUrl) throws IOException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath() + "/" + "file_group";
                return new FileInputStream(path);
            }
        };

        String groupName = "g1";
        Map<String, String> properties = new HashMap<>();
        properties.put(FileGroupProvider.GROUP_FILE_URL, "file_group");
        FileGroupProvider fileGroupProvider = new FileGroupProvider(groupName, properties);
        fileGroupProvider.init();

        Set<String> groups = fileGroupProvider.getGroup(new UserIdentity("harbor", "127.0.0.1"));
        Assert.assertTrue(groups.contains("group1"));
        Assert.assertTrue(groups.contains("group2"));
    }

    @Test
    public void testGroupProvider() throws DdlException, AuthenticationException, IOException {
        GlobalStateMgr.getCurrentState().setJwkMgr(new JwkMgr() {
            @Override
            public JWKSet getJwkSet(String jwksUrl) throws IOException, ParseException {
                String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
                InputStream jwksInputStream = new FileInputStream(path + "/" + jwksUrl);
                return JWKSet.load(jwksInputStream);
            }
        });

        Map<String, String> properties = new HashMap<>();
        properties.put(OIDCSecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_openid_connect");
        properties.put(OIDCSecurityIntegration.OIDC_JWKS_URL, "jwks.json");
        properties.put(OIDCSecurityIntegration.OIDC_PRINCIPAL_FIELD, "preferred_username");
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

        String idToken = getOpenIdConnect("oidc.json");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MysqlCodec.writeInt1(outputStream, 1);
        MysqlCodec.writeLenEncodedString(outputStream, idToken);

        Config.group_provider = new String[] {"file_group_provider"};
        Config.authentication_chain = new String[] {"native", "oidc"};

        try {
            ConnectContext connectContext = new ConnectContext();
            AuthenticationHandler.authenticate(
                    connectContext, "harbor", "127.0.0.1", outputStream.toByteArray(), null);
            StatementBase statementBase = SqlParser.parse("select current_group()", connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);

            QueryStatement queryStatement = (QueryStatement) statementBase;
            InformationFunction informationFunction =
                    (InformationFunction) queryStatement.getQueryRelation().getOutputExpression().get(0);
            Assert.assertEquals("group2, group1", informationFunction.getStrValue());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "group_5");
        authenticationMgr.alterSecurityIntegration("oidc", alterProperties, true);
        Assert.assertThrows(AuthenticationException.class, () -> AuthenticationHandler.authenticate(
                new ConnectContext(), "harbor", "127.0.0.1", outputStream.toByteArray(), null));
    }
}
