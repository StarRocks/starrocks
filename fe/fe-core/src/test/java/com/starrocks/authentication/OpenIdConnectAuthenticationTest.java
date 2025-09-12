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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.UserAuthOptionAnalyzer;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OpenIdConnectAuthenticationTest {

    private final String[] emptyAudience = {};
    private final String[] emptyIssuer = {};
    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();

    @Test
    public void testAuthentication() throws Exception {
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());

        JWTAuthenticationProvider provider =
                new JWTAuthenticationProvider("jwks.json", "preferred_username", emptyIssuer, emptyAudience);
        UserAuthOptionAnalyzer.analyzeAuthOption(new UserRef("harbor", "%"),
                new UserAuthOption(null, "", true, NodePosition.ZERO));
        String openIdConnectJson = mockTokenUtils.generateTestOIDCToken(3600 * 1000);

        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(openIdConnectJson);
        try {
            provider.authenticate(new ConnectContext().getAccessControlContext(), new UserIdentity("harbor", "%"),
                    serializer.toArray());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testFake() throws Exception {
        String openIdConnectJson = mockTokenUtils.getOpenIdConnect("fake-oidc.json");
        MockTokenUtils.MockJwkMgr mockJwkMgr = new MockTokenUtils.MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor", jwkSet, "preferred_username", emptyIssuer, emptyAudience);
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().contains("JWT with kid JKA9Gjuzv--loolRTY_pBN19sUF1Mf8naxOwvb0mgKQ is invalid"));
        }
    }

    @Test
    public void testErrorJwks() throws Exception {
        String openIdConnectJson = mockTokenUtils.getOpenIdConnect("oidc.json");
        MockTokenUtils.MockJwkMgr mockJwkMgr = new MockTokenUtils.MockJwkMgr();

        try {
            JWKSet jwkSet = mockJwkMgr.getJwkSet("error-jwks.json");
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor", jwkSet, "preferred_username", emptyIssuer, emptyAudience);
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().contains("Cannot find public key for kid"));
        }
    }

    @Test
    public void testIssuerAndAudience() throws Exception {
        String openIdConnectJson = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        MockTokenUtils.MockJwkMgr mockJwkMgr = new MockTokenUtils.MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor", jwkSet, "preferred_username",
                    new String[] {"http://localhost:38080/realms/master", "foo"}, new String[] {"12345", "56789"});
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "xxx", new String[] {"http://localhost:38080/realms/master"}, new String[] {"12345"});
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().contains("Can not get specified principal xxx"));
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "sub", new String[] {"http://localhost:38080/realms/master"}, new String[] {"12345"});
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Login name harbor is not matched to user 8f7f0fa5-e1eb-45d0-8e82-8c89c1a45663"));
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "preferred_username", new String[] {"foo", "foo1"}, new String[] {"12345"});
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().contains("Issuer (iss) field http://localhost:38080/realms/master is invalid"));
        }

        try {
            OpenIdConnectVerifier.verify(openIdConnectJson, "harbor",
                    jwkSet, "preferred_username", new String[] {"http://localhost:38080/realms/master"},
                    new String[] {"foo", "56789"});
            Assertions.fail();
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().contains("Audience (aud) field [12345] is invalid"));
        }
    }

    @Test
    public void testExpiry() throws Exception {

        MockTokenUtils.MockJwkMgr mockJwkMgr = new MockTokenUtils.MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");

        try {
            String unexpiredToken = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
            OpenIdConnectVerifier.verify(unexpiredToken, "harbor",
                    jwkSet, "preferred_username", new String[] {"http://localhost:38080/realms/master"}, new String[] {"12345"});
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        try {
            String unexpiredToken = mockTokenUtils.generateTestOIDCToken(-60L);
            OpenIdConnectVerifier.verify(unexpiredToken, "harbor",
                    jwkSet, "preferred_username", new String[] {"http://localhost:38080/realms/master"}, new String[] {"12345"});
        } catch (AuthenticationException e) {
            Assertions.assertTrue(e.getMessage().contains("JWT expired at"));
        }
    }
}
