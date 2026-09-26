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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class AuthenticationHandlerTest {

    // field initializers run before each test, like @BeforeEach
    private final RSAKey jwtRsaKey = generateRsaKey();
    private final String[] originalAuthChain = Config.authentication_chain;

    private static RSAKey generateRsaKey() {
        try {
            return new RSAKeyGenerator(2048).keyID("test-kid").generate();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        // Real EditLog on an auto-committing pseudo journal (shields BDB): journal writes complete so the
        // WALApplier.apply() inside logJsonObject() still runs and the DDL takes effect in memory.
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() throws Exception {

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }
        };
    }

    @Test
    public void testLdapDNMappingGroup() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef("ldap_user", "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                        "uid=ldap_user,ou=company,dc=example,dc=com",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        new MockUp<LDAPAuthProvider>() {
            @Mock
            private void checkPassword(String dn, String password) throws Exception {
                // mock: always success
            }

            @Mock
            private String findUserDNByRoot(String user, AccessControlContext ctx) throws Exception {
                return "uid=test,ou=People,dc=starrocks,dc=com";
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");

        String groupName = "ldap_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("ldap_user", Set.of("group1", "group2"));
        groups.put("uid=ldap_user,ou=company,dc=example,dc=com", Set.of("group3", "group4"));
        groups.put("u1", Set.of("group5"));
        groups.put("u2", Set.of("group6"));
        ldapGroupProvider.setUserToGroupCache(groups);

        ConnectContext context = new ConnectContext();
        AccessControlContext authCtx = context.getAccessControlContext();
        AuthenticationHandler.authenticate(context, "ldap_user", "%", "\0".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("ldap_user", authCtx.getQualifiedUser());
        Assertions.assertEquals("uid=ldap_user,ou=company,dc=example,dc=com", authCtx.getDistinguishedName());

        Assertions.assertEquals(Set.of("group1", "group2"),
                ldapGroupProvider.getGroup(authCtx.getCurrentUserIdentity(), authCtx.getDistinguishedName()));

        properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");

        groupName = "ldap_group_provider2";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider2 = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Map<String, Set<String>> groups2 = new HashMap<>();
        groups2.put("ldap_user", Set.of("group1", "group2"));
        groups2.put("uid=ldap_user,ou=company,dc=example,dc=com", Set.of("group3", "group4"));
        groups2.put("u1", Set.of("group5"));
        groups2.put("u2", Set.of("group6"));
        ldapGroupProvider2.setUserToGroupCache(groups2);
        Assertions.assertEquals(Set.of("group3", "group4"),
                ldapGroupProvider2.getGroup(authCtx.getCurrentUserIdentity(), authCtx.getDistinguishedName()));
    }

    private void setUpJwtSecurityIntegration(AuthenticationMgr authenticationMgr, String name, String issuer) {
        Map<String, String> props = new HashMap<>();
        props.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, AuthPlugin.Server.AUTHENTICATION_JWT.name());
        props.put(JWTAuthenticationProvider.JWT_JWKS_URL, name + "-jwks.json");
        props.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "sub");
        props.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, issuer);
        authenticationMgr.replayCreateSecurityIntegration(name, props);
    }

    private String buildJwt(String subject, String issuer, Date expiration) throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject(subject)
                .issuer(issuer)
                .expirationTime(expiration)
                .build();
        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).keyID("test-kid").build(), claims);
        signedJWT.sign(new RSASSASigner(jwtRsaKey));
        return signedJWT.serialize();
    }

    @AfterEach
    public void tearDownJwt() {
        Config.authentication_chain = originalAuthChain;
    }

    @Test
    public void testAuthenticateWithToken_success() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        setUpJwtSecurityIntegration(authenticationMgr, "jwt_integration", "https://issuer.example.com");
        Config.authentication_chain = new String[] {"jwt_integration"};

        JWKSet jwkSet = new JWKSet(jwtRsaKey.toPublicJWK());
        new MockUp<JwkMgr>() {
            @Mock
            public JWKSet getJwkSet(String jwksUrl) {
                return jwkSet;
            }
        };

        String token = buildJwt("jwt_user", "https://issuer.example.com",
                new Date(System.currentTimeMillis() + 60_000));

        ConnectContext context = new ConnectContext();
        UserIdentity authenticatedUser = AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", token);

        Assertions.assertEquals("jwt_user", authenticatedUser.getUser());
        Assertions.assertEquals("jwt_user", context.getQualifiedUser());
        Assertions.assertEquals(token, context.getAuthToken());
    }

    @Test
    public void testAuthenticateWithToken_multipleIntegrations_selectsMatchingIssuerWithSingleJwksFetch()
            throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        setUpJwtSecurityIntegration(authenticationMgr, "jwt_integration_one", "https://issuer-one.example.com");
        setUpJwtSecurityIntegration(authenticationMgr, "jwt_integration_two", "https://issuer-two.example.com");
        Config.authentication_chain = new String[] {"jwt_integration_one", "jwt_integration_two"};

        JWKSet jwkSet = new JWKSet(jwtRsaKey.toPublicJWK());
        AtomicInteger jwksFetchCount = new AtomicInteger(0);
        new MockUp<JwkMgr>() {
            @Mock
            public JWKSet getJwkSet(String jwksUrl) {
                jwksFetchCount.incrementAndGet();
                return jwkSet;
            }
        };

        String token = buildJwt("jwt_user", "https://issuer-two.example.com",
                new Date(System.currentTimeMillis() + 60_000));

        ConnectContext context = new ConnectContext();
        UserIdentity authenticatedUser = AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", token);

        Assertions.assertEquals("jwt_user", authenticatedUser.getUser());
        Assertions.assertEquals(1, jwksFetchCount.get(), "only the matching issuer's JWKS should be fetched");
    }

    @Test
    public void testAuthenticateWithToken_noJwtIntegrationConfigured() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        Config.authentication_chain = new String[] {SecurityIntegration.AUTHENTICATION_CHAIN_MECHANISM_NATIVE};

        String token = buildJwt("jwt_user", "https://issuer.example.com",
                new Date(System.currentTimeMillis() + 60_000));

        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", token));
    }

    @Test
    public void testAuthenticateWithToken_issuerNotMatched() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        setUpJwtSecurityIntegration(authenticationMgr, "jwt_integration", "https://trusted.example.com");
        Config.authentication_chain = new String[] {"jwt_integration"};

        String token = buildJwt("jwt_user", "https://untrusted.example.com",
                new Date(System.currentTimeMillis() + 60_000));

        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", token));
    }

    @Test
    public void testAuthenticateWithToken_expiredToken() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        setUpJwtSecurityIntegration(authenticationMgr, "jwt_integration", "https://issuer.example.com");
        Config.authentication_chain = new String[] {"jwt_integration"};

        JWKSet jwkSet = new JWKSet(jwtRsaKey.toPublicJWK());
        new MockUp<JwkMgr>() {
            @Mock
            public JWKSet getJwkSet(String jwksUrl) {
                return jwkSet;
            }
        };

        String token = buildJwt("jwt_user", "https://issuer.example.com",
                new Date(System.currentTimeMillis() - 60_000));

        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", token));
    }

    @Test
    public void testAuthenticateWithToken_malformedToken() {
        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", "not-a-jwt"));
    }

    @Test
    public void testAuthenticateWithToken_emptyUserOrToken() {
        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "", "%", "some-token"));
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", ""));
    }

    @Test
    public void testAuthenticateWithToken_nativeJwtUser() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        Config.authentication_chain = new String[] {SecurityIntegration.AUTHENTICATION_CHAIN_MECHANISM_NATIVE};

        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef("native_jwt_user", "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption(AuthPlugin.Server.AUTHENTICATION_JWT.name(),
                        "{\"jwks_url\":\"native-jwks.json\",\"principal_field\":\"sub\","
                                + "\"required_issuer\":\"https://issuer.example.com\"}",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        JWKSet jwkSet = new JWKSet(jwtRsaKey.toPublicJWK());
        new MockUp<JwkMgr>() {
            @Mock
            public JWKSet getJwkSet(String jwksUrl) {
                return jwkSet;
            }
        };

        String token = buildJwt("native_jwt_user", "https://issuer.example.com",
                new Date(System.currentTimeMillis() + 60_000));

        ConnectContext context = new ConnectContext();
        UserIdentity authenticatedUser =
                AuthenticationHandler.authenticateWithToken(context, "native_jwt_user", "127.0.0.1", token);

        Assertions.assertEquals("native_jwt_user", authenticatedUser.getUser());
        Assertions.assertEquals(token, context.getAuthToken());
    }

    @Test
    public void testAuthenticateWithToken_nativeNonJwtUserRejected() throws Exception {
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        setUpJwtSecurityIntegration(authenticationMgr, "jwt_integration", "https://issuer.example.com");
        Config.authentication_chain = new String[] {"jwt_integration"};

        String token = buildJwt("root", "https://issuer.example.com", new Date(System.currentTimeMillis() + 60_000));

        // root is a native password user, so a token must not log it in through a JWT integration
        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithToken(context, "root", "127.0.0.1", token));
    }

    @Test
    public void testAuthenticateWithToken_authCheckDisabled() {
        boolean originalAuthCheck = Config.enable_auth_check;
        Config.enable_auth_check = false;
        try {
            ConnectContext context = new ConnectContext();
            Assertions.assertThrows(AuthenticationException.class,
                    () -> AuthenticationHandler.authenticateWithToken(context, "jwt_user", "%", "a.b.c"));
        } finally {
            Config.enable_auth_check = originalAuthCheck;
        }
    }
}
