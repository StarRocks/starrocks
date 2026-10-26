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
import com.starrocks.common.Config;
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

/**
 * Component-level tests for the SI-LDAP-over-HTTP-Basic path (the B1 fix, design §5.3.1) through
 * {@link AuthenticationHandler} with a {@code PLAIN_PASSWORD} credential. The LDAP bind is stubbed
 * with a {@code MockUp<LDAPAuthProvider>} so no real LDAP server is needed.
 *
 * Covers TC-L2-45-06 (native -> SI fallback), TC-L2-45-07 (PLAIN_PASSWORD skips a JWT SI) and the
 * key TC-L2-45-08 (B1 root-cause regression: a fresh ConnectContext has authPlugin==null, which
 * used to make the SI chain skip every SI; kind-based dispatch must match SI-LDAP regardless).
 */
public class SiLdapHttpBasicTest {
    private AuthenticationMgr authenticationMgr;
    private String[] savedChain;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        savedChain = Config.authentication_chain;
        registerLdapSi("ldap_corp");
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedChain;
    }

    private void registerLdapSi(String name) {
        Map<String, String> props = new HashMap<>();
        props.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        props.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        props.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        props.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=People,dc=example,dc=com");
        authenticationMgr.replayCreateSecurityIntegration(name, props);
    }

    private void registerJwtSi(String name) {
        Map<String, String> props = new HashMap<>();
        props.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        props.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        props.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        props.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, "https://issuer.example.com");
        authenticationMgr.replayCreateSecurityIntegration(name, props);
    }

    /** Stub the LDAP bind so it succeeds without a real directory server. */
    private void stubLdapBindSuccess() {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) {
                // bind succeeds
            }
        };
    }

    @Test
    public void testSiLdapHttpBasicAuthenticates() throws Exception {
        // TC-L2-45-06 / TC-L2-45-08: a fresh ConnectContext (authPlugin == null, the B1 condition)
        // with an HTTP-Basic (PLAIN_PASSWORD) credential must still match the SI-LDAP in the chain
        // and log in.
        stubLdapBindSuccess();
        Config.authentication_chain = new String[] {"native", "ldap_corp"};

        ConnectContext ctx = new ConnectContext();
        Assertions.assertNull(ctx.getAuthPlugin(), "fresh ctx must have authPlugin == null (the B1 condition)");

        UserIdentity uid = AuthenticationHandler.authenticate(
                ctx, AuthCredentials.ofPassword("alice", "ldap_pwd", AuthChannel.HTTP, "127.0.0.1"));

        Assertions.assertNotNull(uid, "SI-LDAP must authenticate even when authPlugin == null");
        Assertions.assertEquals("alice", uid.getUser());
        Assertions.assertTrue(uid.isEphemeral());
    }

    @Test
    public void testSiLdapWrongPasswordRejected() {
        // Negative: kind dispatch must not loosen authentication -- a failed LDAP bind still
        // yields 401.
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                throw new AuthenticationException("invalid credentials");
            }
        };
        Config.authentication_chain = new String[] {"native", "ldap_corp"};

        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(
                        new ConnectContext(), AuthCredentials.ofPassword("alice", "wrong", AuthChannel.HTTP, "127.0.0.1")));
    }

    @Test
    public void testPlainPasswordSkipsJwtSi() throws Exception {
        // TC-L2-45-07: a PLAIN_PASSWORD credential must skip a JWT SI (which only supports Bearer)
        // without erroring, and continue to the LDAP SI that does accept it.
        stubLdapBindSuccess();
        registerJwtSi("jwt_si");
        Config.authentication_chain = new String[] {"native", "jwt_si", "ldap_corp"};

        UserIdentity uid = AuthenticationHandler.authenticate(
                new ConnectContext(), AuthCredentials.ofPassword("alice", "ldap_pwd", AuthChannel.HTTP, "127.0.0.1"));

        Assertions.assertNotNull(uid);
        Assertions.assertEquals("alice", uid.getUser());
    }
}
