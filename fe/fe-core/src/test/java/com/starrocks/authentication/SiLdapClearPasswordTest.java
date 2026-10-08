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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.http.BaseAction;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Authentication through a security integration on the channels that send a cleartext password
 * (HTTP Basic, BE-&gt;FE load RPCs, Arrow Flight Basic) rather than a MySQL handshake.
 *
 * <p>Those channels build a fresh {@link ConnectContext} whose {@code authPlugin} is unset, while the
 * security integration chain filters candidates by that field: every integration used to be skipped, so
 * an LDAP user defined by a security integration could log in over MySQL but got a 401 everywhere else.
 * {@code AuthenticationHandler.authenticateWithClearPassword} declares the credential shape instead.
 *
 * <p>The LDAP bind is stubbed, so these tests need no directory server.
 */
public class SiLdapClearPasswordTest {
    private static final String LDAP_SI = "ldap_corp";
    private static final String JWT_SI = "jwt_corp";

    private AuthenticationMgr authenticationMgr;
    private String[] savedAuthChain;
    private AuthenticationMgr savedAuthenticationMgr;
    private EditLog savedEditLog;

    /** Counts LDAP binds so a test can assert the LDAP integration was (or was not) tried. */
    private final AtomicInteger ldapBindCount = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Both of these are process-wide singletons: a fresh AuthenticationMgr knows only root, and a
        // NoOpEditLog swallows journal entries. Left installed, they turn into order-dependent failures in
        // whatever class runs next in this JVM fork, so tearDown() puts the originals back.
        savedEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        savedAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);

        savedAuthChain = Config.authentication_chain;
        ldapBindCount.set(0);
        lastBoundPassword.set(null);
        // The rejection cache is process-wide: without this, one case's wrong password short-circuits the next.
        AuthenticationHandler.invalidateRejectedCredentialCache();
        registerLdapSecurityIntegration(LDAP_SI);
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedAuthChain;
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(savedAuthenticationMgr);
        GlobalStateMgr.getCurrentState().setEditLog(savedEditLog);
    }

    private void registerLdapSecurityIntegration(String name) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name());
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=People,dc=example,dc=com");
        authenticationMgr.replayCreateSecurityIntegration(name, properties);
    }

    private void registerJwtSecurityIntegration(String name) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                AuthPlugin.Server.AUTHENTICATION_JWT.name());
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        properties.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, "https://issuer.example.com");
        authenticationMgr.replayCreateSecurityIntegration(name, properties);
    }

    /** The password the directory actually received, so a test can assert what was sent, not just that it was. */
    private final AtomicReference<String> lastBoundPassword = new AtomicReference<>();

    /** Accept any non-empty password, reject an empty one the way a directory server would. */
    private void stubLdapBind() {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                ldapBindCount.incrementAndGet();
                lastBoundPassword.set(password);
                if (password == null || password.isEmpty()) {
                    throw new AuthenticationException("invalid credentials: empty password");
                }
            }
        };
    }

    private void stubLdapBindRejectingEverything() {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                ldapBindCount.incrementAndGet();
                throw new AuthenticationException("invalid credentials");
            }
        };
    }

    @Test
    public void testClearPasswordMatchesLdapSecurityIntegration() throws Exception {
        stubLdapBind();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        ConnectContext context = new ConnectContext();
        // The condition that used to break every non-MySQL channel: nobody declared a plugin.
        Assertions.assertNull(context.getAuthPlugin());

        UserIdentity user = AuthenticationHandler.authenticateWithClearPassword(
                context, "alice", "10.1.2.3", "ldap_password");

        Assertions.assertNotNull(user);
        Assertions.assertEquals("alice", user.getUser());
        Assertions.assertTrue(user.isEphemeral());
        Assertions.assertEquals(1, ldapBindCount.get());
        // The declared plugin is the same one the MySQL protocol switches an LDAP user to.
        Assertions.assertEquals(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(), context.getAuthPlugin());
        // Identity and the security integration that authenticated it land on the passed context, which is
        // what callers that authorize afterwards rely on.
        Assertions.assertEquals(user, context.getCurrentUserIdentity());
        Assertions.assertEquals("alice", context.getQualifiedUser());
        Assertions.assertEquals(LDAP_SI, context.getSecurityIntegration());
    }

    @Test
    public void testWrongPasswordStillRejected() {
        stubLdapBindRejectingEverything();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticateWithClearPassword(
                        new ConnectContext(), "alice", "10.1.2.3", "wrong_password"));
        Assertions.assertEquals(1, ldapBindCount.get());
    }

    @Test
    public void testEmptyPasswordFailsWithoutCrashing() {
        stubLdapBind();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        // HTTP Basic may send "user:" with no password at all. The provider used to read the last byte of
        // the credential unconditionally, turning this into an ArrayIndexOutOfBoundsException (HTTP 500)
        // instead of an authentication failure.
        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticateWithClearPassword(
                        new ConnectContext(), "alice", "10.1.2.3", ""));
    }

    @Test
    public void testClearPasswordSkipsTokenBasedSecurityIntegration() throws Exception {
        stubLdapBind();
        AtomicInteger jwtAttempts = stubJwtCountingAttempts();
        registerJwtSecurityIntegration(JWT_SI);
        Config.authentication_chain = new String[] {"native", JWT_SI, LDAP_SI};

        // A password must not be handed to a JWT integration for signature verification; the chain has to
        // skip it and go on to the integration that does consume passwords.
        UserIdentity user = AuthenticationHandler.authenticateWithClearPassword(
                new ConnectContext(), "alice", "10.1.2.3", "ldap_password");

        Assertions.assertNotNull(user);
        Assertions.assertEquals("alice", user.getUser());
        // Without this assertion the test would also pass with the filter removed: the JWT provider would
        // reject the password, the chain would record that failure, carry on, and the LDAP integration would
        // still authenticate. What is under test is that the token integration is never even tried.
        Assertions.assertEquals(0, jwtAttempts.get());
    }

    /** Counts attempts on the JWT integration; it must stay at zero when a cleartext password is presented. */
    private AtomicInteger stubJwtCountingAttempts() {
        AtomicInteger attempts = new AtomicInteger();
        new MockUp<JWTAuthenticationProvider>() {
            @Mock
            public void authenticate(AccessControlContext context, UserIdentity userIdentity, byte[] authResponse)
                    throws AuthenticationException {
                attempts.incrementAndGet();
                throw new AuthenticationException("JWT provider cannot verify a password");
            }
        };
        return attempts;
    }

    @Test
    public void testNativeUserStillWinsOverSecurityIntegration() throws Exception {
        stubLdapBindRejectingEverything();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        ConnectContext rootCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        CreateUserStmt createUser = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER native_user IDENTIFIED BY ''", rootCtx);
        authenticationMgr.createUser(createUser);

        ConnectContext context = new ConnectContext();
        UserIdentity user = AuthenticationHandler.authenticateWithClearPassword(
                context, "native_user", "10.1.2.3", "");

        Assertions.assertNotNull(user);
        Assertions.assertFalse(user.isEphemeral());
        // The native table answered, so the LDAP integration was never tried.
        Assertions.assertEquals(0, ldapBindCount.get());
    }

    @Test
    public void testIntegrationTypeWithoutClientPluginIsSkipped() throws Exception {
        // Whether an integration can serve a request is decided by mapping its type to a client-side plugin.
        // A type with no such mapping simply cannot match, so the chain must skip it -- the mapping used to be
        // wrapped in Objects.requireNonNull, which turned "cannot match" into an NPE out of the auth path.
        // Nothing registers such a type today (the factory rejects unknown ones); adding one, e.g. a future
        // non-password integration, is what would walk into this branch.
        SecurityIntegration unmappableIntegration =
                new SecurityIntegration("future_si", Map.of(
                        SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_from_the_future")) {
                    @Override
                    public AuthenticationProvider getAuthenticationProvider() {
                        return new PlainPasswordAuthenticationProvider(new byte[0]);
                    }
                };
        new MockUp<AuthenticationMgr>() {
            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                return "future_si".equals(name) ? unmappableIntegration : null;
            }
        };
        Config.authentication_chain = new String[] {"native", "future_si"};

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticateWithClearPassword(
                        new ConnectContext(), "alice", "10.1.2.3", "any_password"));
        Assertions.assertNotNull(e.getMessage());
    }

    @Test
    public void testHttpBasicEntryAuthenticatesThroughTheChain() throws Exception {
        stubLdapBind();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        // The HTTP Basic entry point itself, not just the handler underneath it: this is the line the
        // coverage bot flags, and reverting it (as a backport merge conflict easily could) silently brings
        // the 401 back for every action that authenticates through BaseAction.
        BaseAction.ActionAuthorizationInfo authInfo = new BaseAction.ActionAuthorizationInfo();
        authInfo.fullUserName = "alice";
        authInfo.remoteIp = "10.1.2.3";
        authInfo.password = "ldap_password";

        UserIdentity user = BaseAction.checkPassword(authInfo);

        Assertions.assertNotNull(user);
        Assertions.assertEquals("alice", user.getUser());
        Assertions.assertTrue(user.isEphemeral());
        Assertions.assertEquals(1, ldapBindCount.get());
    }

    @Test
    public void testHttpBasicEntryStillRefusesAWrongPassword() {
        stubLdapBindRejectingEverything();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        BaseAction.ActionAuthorizationInfo authInfo = new BaseAction.ActionAuthorizationInfo();
        authInfo.fullUserName = "alice";
        authInfo.remoteIp = "10.1.2.3";
        authInfo.password = "wrong_password";

        // AccessDeniedException is what the HTTP layer turns into a 401
        Assertions.assertThrows(AccessDeniedException.class, () ->
                BaseAction.checkPassword(authInfo));
        Assertions.assertEquals(1, ldapBindCount.get());
    }

    @Test
    public void testOauth2NativeUserCannotAuthenticateWithAPassword() throws Exception {
        stubLdapBind();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        ConnectContext rootCtx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        CreateUserStmt createUser = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER oauth_user IDENTIFIED WITH authentication_oauth2 AS "
                        + "'{\"jwks_url\": \"https://example.com/jwks\", \"principal_field\": \"email\"}'",
                rootCtx);
        authenticationMgr.createUser(createUser);

        // OAuth2AuthenticationProvider.authenticate() returns *successfully* without checking anything when the
        // client did not declare the OAuth2 plugin -- it expects the protocol to finish the handshake later. On
        // the MySQL channel ConnectProcessor closes that hole on the first command; these channels serve the
        // request immediately, so authenticateWithClearPassword has to run the same check itself. Without it,
        // any password would get an OAuth2 user through Stream Load, the load RPCs and Arrow Flight.
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticateWithClearPassword(
                        new ConnectContext(), "oauth_user", "10.1.2.3", "any_password_at_all"));
        Assertions.assertEquals(ErrorCode.ERR_OAUTH2_NOT_AUTHENTICATED, e.getErrorCode());
    }

    @Test
    public void testSameWrongCredentialIsNotBoundTwice() throws Exception {
        stubLdapBindRejectingEverything();
        Config.authentication_chain = new String[] {"native", LDAP_SI};
        int savedTtl = Config.authentication_failure_cache_ttl_second;
        Config.authentication_failure_cache_ttl_second = 60;

        try {
            // A client stuck on a wrong password (a connector holding a rotated credential, a scraper with a
            // stale one) must not produce one LDAP bind per attempt: that is what drives AD badPwdCount toward
            // locking the account out.
            for (int attempt = 0; attempt < 5; attempt++) {
                Assertions.assertThrows(AuthenticationException.class, () ->
                        AuthenticationHandler.authenticateWithClearPassword(
                                new ConnectContext(), "alice", "10.1.2.3", "wrong_password"));
            }
            Assertions.assertEquals(1, ldapBindCount.get(), "only the first attempt may reach the directory");
        } finally {
            Config.authentication_failure_cache_ttl_second = savedTtl;
        }
    }

    @Test
    public void testRejectionCacheIgnoresUsernameCase() {
        // The LDAP paths lowercase the username before binding, so Alice / ALICE / alice are one account.
        // The cache must use the same canonical form, or varying the case walks straight past it and keeps
        // pushing that account's badPwdCount up.
        stubLdapBindRejectingEverything();
        Config.authentication_chain = new String[] {"native", LDAP_SI};
        int savedTtl = Config.authentication_failure_cache_ttl_second;
        Config.authentication_failure_cache_ttl_second = 60;

        try {
            for (String spelling : new String[] {"alice", "ALICE", "Alice", "aLiCe"}) {
                Assertions.assertThrows(AuthenticationException.class, () ->
                        AuthenticationHandler.authenticateWithClearPassword(
                                new ConnectContext(), spelling, "10.1.2.3", "wrong_password"));
            }
            Assertions.assertEquals(1, ldapBindCount.get(), "case variants must share one cache entry");
        } finally {
            Config.authentication_failure_cache_ttl_second = savedTtl;
        }
    }

    @Test
    public void testUnreachableDirectoryIsNotRemembered() {
        // The directory being down is not a credential rejection: the provider reports it as transient, so the
        // next attempt must reach the directory again instead of being answered from the cache. Otherwise a
        // brief LDAP outage would keep rejecting the *correct* password for the whole TTL.
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                ldapBindCount.incrementAndGet();
                throw new javax.naming.CommunicationException("localhost:389 connection refused");
            }
        };
        Config.authentication_chain = new String[] {"native", LDAP_SI};
        int savedTtl = Config.authentication_failure_cache_ttl_second;
        Config.authentication_failure_cache_ttl_second = 60;

        try {
            for (int attempt = 0; attempt < 3; attempt++) {
                Assertions.assertThrows(AuthenticationException.class, () ->
                        AuthenticationHandler.authenticateWithClearPassword(
                                new ConnectContext(), "alice", "10.1.2.3", "ldap_password"));
            }
            Assertions.assertEquals(3, ldapBindCount.get(), "a transient failure must not be cached");
        } finally {
            Config.authentication_failure_cache_ttl_second = savedTtl;
        }
    }

    @Test
    public void testFixingThePasswordTakesEffectImmediately() throws Exception {
        // Accept "ldap_password", reject anything else -- the shape of a directory after the client is fixed.
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                ldapBindCount.incrementAndGet();
                if (!"ldap_password".equals(password)) {
                    throw new AuthenticationException("invalid credentials");
                }
            }
        };
        Config.authentication_chain = new String[] {"native", LDAP_SI};
        int savedTtl = Config.authentication_failure_cache_ttl_second;
        Config.authentication_failure_cache_ttl_second = 60;

        try {
            Assertions.assertThrows(AuthenticationException.class, () ->
                    AuthenticationHandler.authenticateWithClearPassword(
                            new ConnectContext(), "alice", "10.1.2.3", "wrong_password"));

            // The cache key includes a hash of the credential, so the corrected password is a different key and
            // must not be held back by the rejection that was just cached. No TTL to wait out.
            UserIdentity user = AuthenticationHandler.authenticateWithClearPassword(
                    new ConnectContext(), "alice", "10.1.2.3", "ldap_password");

            Assertions.assertNotNull(user);
            Assertions.assertEquals(2, ldapBindCount.get());
        } finally {
            Config.authentication_failure_cache_ttl_second = savedTtl;
        }
    }

    @Test
    public void testRejectionCacheCanBeTurnedOff() {
        stubLdapBindRejectingEverything();
        Config.authentication_chain = new String[] {"native", LDAP_SI};
        int savedTtl = Config.authentication_failure_cache_ttl_second;
        Config.authentication_failure_cache_ttl_second = 0;

        try {
            for (int attempt = 0; attempt < 3; attempt++) {
                Assertions.assertThrows(AuthenticationException.class, () ->
                        AuthenticationHandler.authenticateWithClearPassword(
                                new ConnectContext(), "alice", "10.1.2.3", "wrong_password"));
            }
            Assertions.assertEquals(3, ldapBindCount.get(), "ttl=0 must disable the cache entirely");
        } finally {
            Config.authentication_failure_cache_ttl_second = savedTtl;
        }
    }

    @Test
    public void testMysqlHandshakePathUnchanged() throws Exception {
        stubLdapBind();
        Config.authentication_chain = new String[] {"native", LDAP_SI};

        // What the MySQL protocol does: switch the client to mysql_clear_password, then deliver the password
        // as a NUL-terminated frame. This must keep working exactly as before.
        ConnectContext context = new ConnectContext();
        context.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
        byte[] mysqlClearPasswordFrame = "ldap_password\0".getBytes(StandardCharsets.UTF_8);

        UserIdentity user = AuthenticationHandler.authenticate(context, "alice", "10.1.2.3", mysqlClearPasswordFrame);

        Assertions.assertNotNull(user);
        Assertions.assertEquals("alice", user.getUser());
        Assertions.assertTrue(user.isEphemeral());
        // The point of the test: the directory must receive the password itself, not the frame. Counting binds
        // is not enough -- drop the trailing-NUL trim and the bind still succeeds with a stray \0 appended,
        // which every real directory would reject.
        Assertions.assertEquals("ldap_password", lastBoundPassword.get());
    }
}
