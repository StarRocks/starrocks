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
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link AuthenticationHandler#authenticateWithIdToken} lets channels without a MySQL plugin exchange (Arrow Flight
 * Basic auth) log in JWT users: the token must reach the JWT security integration in the OpenID Connect form the
 * JWT provider reads, and must not be offered to password-based integrations. The JWKS fetch and the token
 * signature check are stubbed, so these tests need no identity provider.
 */
public class IdTokenAuthenticationTest {
    private static final String JWT_SI = "jwt_corp";
    private static final String LDAP_SI = "ldap_corp";
    private static final String TOKEN = "header.payload.signature";

    private AuthenticationMgr savedAuthenticationMgr;
    private EditLog savedEditLog;
    private String[] savedAuthChain;
    private AuthenticationMgr authenticationMgr;

    // what the JWT provider handed to the verifier
    private final AtomicReference<String> verifiedToken = new AtomicReference<>();
    private final AtomicReference<String> verifiedUser = new AtomicReference<>();
    private final AtomicInteger ldapBindCount = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Process-wide singletons, restored in tearDown() so later classes in this fork are not affected.
        savedEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        savedAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        savedAuthChain = Config.authentication_chain;
        AuthenticationHandler.invalidateRejectedCredentialCache();

        Map<String, String> jwt = new HashMap<>();
        jwt.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, AuthPlugin.Server.AUTHENTICATION_JWT.name());
        jwt.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        jwt.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        jwt.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, "https://issuer.example.com");
        authenticationMgr.replayCreateSecurityIntegration(JWT_SI, jwt);

        Map<String, String> ldap = new HashMap<>();
        ldap.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name());
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=People,dc=example,dc=com");
        authenticationMgr.replayCreateSecurityIntegration(LDAP_SI, ldap);

        new MockUp<JwkMgr>() {
            @Mock
            public JWKSet getJwkSet(String jwksUrl) {
                return new JWKSet();
            }
        };
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) {
                ldapBindCount.incrementAndGet();
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedAuthChain;
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(savedAuthenticationMgr);
        GlobalStateMgr.getCurrentState().setEditLog(savedEditLog);
    }

    private void acceptTokens() {
        new MockUp<OpenIdConnectVerifier>() {
            @Mock
            public void verify(String idToken, String userName, JWKSet jwkSet, String principalField,
                               String[] requiredIssuer, String[] requiredAudience) {
                verifiedToken.set(idToken);
                verifiedUser.set(userName);
            }
        };
    }

    @Test
    public void testIdTokenReachesTheJwtSecurityIntegration() throws Exception {
        acceptTokens();
        Config.authentication_chain = new String[] {"native", LDAP_SI, JWT_SI};
        ConnectContext context = new ConnectContext();

        UserIdentity user = AuthenticationHandler.authenticateWithIdToken(context, "alice", "10.1.2.3", TOKEN);

        Assertions.assertEquals("alice", user.getUser());
        // The provider decoded the framed response back to exactly the token the client sent.
        Assertions.assertEquals(TOKEN, verifiedToken.get());
        Assertions.assertEquals("alice", verifiedUser.get());
        Assertions.assertEquals(AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(), context.getAuthPlugin());
        Assertions.assertEquals(JWT_SI, context.getSecurityIntegration());
        // A token is not a password: the LDAP integration earlier in the chain was not tried with it.
        Assertions.assertEquals(0, ldapBindCount.get());
    }

    @Test
    public void testRejectedTokenFails() {
        new MockUp<OpenIdConnectVerifier>() {
            @Mock
            public void verify(String idToken, String userName, JWKSet jwkSet, String principalField,
                               String[] requiredIssuer, String[] requiredAudience) throws AuthenticationException {
                throw new AuthenticationException("bad signature");
            }
        };
        Config.authentication_chain = new String[] {"native", JWT_SI};

        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticateWithIdToken(new ConnectContext(), "alice", "10.1.2.3", TOKEN));
    }

    @Test
    public void testClearPasswordIsStillNotOfferedToTheJwtIntegration() {
        acceptTokens();
        Config.authentication_chain = new String[] {"native", JWT_SI};

        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticateWithClearPassword(new ConnectContext(), "alice", "10.1.2.3", TOKEN));
        Assertions.assertNull(verifiedToken.get());
    }
}
