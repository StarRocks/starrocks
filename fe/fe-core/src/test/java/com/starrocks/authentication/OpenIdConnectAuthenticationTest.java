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

import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

public class OpenIdConnectAuthenticationTest {

    static class MockJwkMgr extends JwkMgr {
        @Override
        public JWKSet getJwkSet(String jwksUrl) throws IOException, ParseException {
            String path = ClassLoader.getSystemClassLoader().getResource("auth").getPath();
            InputStream jwksInputStream = new FileInputStream(path + "/" + jwksUrl);
            return JWKSet.load(jwksInputStream);
        }
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
    public void testAuthentication() throws AuthenticationException, IOException {
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockJwkMgr());

        OpenIdConnectAuthenticationProvider provider =
                new OpenIdConnectAuthenticationProvider("jwks.json", "preferred_username", "", "");
        provider.analyzeAuthOption(new UserIdentity("harbor", "%"),
                new UserAuthOption("", null, null, true, NodePosition.ZERO));
        String openIdConnectJson = getOpenIdConnect("oidc.json");

        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(openIdConnectJson);
        try {
            provider.authenticate("harbor", "%", serializer.toArray(), null, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFake() throws Exception {
        String openIdConnectJson = getOpenIdConnect("fake-oidc.json");
        MockJwkMgr mockJwkMgr = new MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor", jwkSet, "preferred_username", "", "");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("JWT with kid JKA9Gjuzv--loolRTY_pBN19sUF1Mf8naxOwvb0mgKQ is invalid"));
        }
    }

    @Test
    public void testErrorJwks() throws Exception {
        String openIdConnectJson = getOpenIdConnect("oidc.json");
        MockJwkMgr mockJwkMgr = new MockJwkMgr();

        try {
            JWKSet jwkSet = mockJwkMgr.getJwkSet("error-jwks.json");
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor", jwkSet, "preferred_username", "", "");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot find public key for kid"));
        }
    }

    @Test
    public void testIssuerAndAudience() throws Exception {
        String openIdConnectJson = getOpenIdConnect("oidc.json");
        MockJwkMgr mockJwkMgr = new MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "preferred_username", "http://localhost:38080/realms/master", "12345");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "xxx", "http://localhost:38080/realms/master", "12345");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Can not get specified principal xxx"));
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "sub", "http://localhost:38080/realms/master", "12345");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(
                    e.getMessage().contains("Login name harbor is not matched to user 8f7f0fa5-e1eb-45d0-8e82-8c89c1a45663"));
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "preferred_username", "foo", "12345");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Issuer (iss) field http://localhost:38080/realms/master is invalid"));
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "preferred_username", "http://localhost:38080/realms/master", "foo");
            Assert.fail();
        } catch (AuthenticationException e) {
            Assert.assertTrue(e.getMessage().contains("Audience (aud) field [12345] is invalid"));
        }
    }
}
