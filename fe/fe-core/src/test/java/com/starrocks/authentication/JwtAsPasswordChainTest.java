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
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.privilege.AuthPlugin;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code authentication_chain_accept_jwt_as_password}: a MySQL clear-text password with the shape
 * of a JWT is offered to the token integrations before it is tried as a password. This is what
 * lets a MySQL client that cannot speak the OpenID Connect plugin log in through a JWT integration
 * that shares the chain with an LDAP one, by passing the token where the password goes. HTTP and
 * Arrow Flight keep Basic and Bearer apart and are not affected.
 */
public class JwtAsPasswordChainTest {
    private static final String LDAP_SI = "ldap_si";
    private static final String JWT_SI = "jwt_si";
    private static final String LDAP_PASSWORD = "ldap_pwd";
    /** Three base64url segments starting with {@code eyJ}: shaped like a JWT, signed by nobody. */
    private static final String JWT_SHAPED_PASSWORD =
            "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJub2JvZHkifQ.bm90LWEtc2lnbmF0dXJl";

    /** Every password the stubbed LDAP bind was asked to check, in order. */
    private static final List<String> BINDS = new ArrayList<>();
    /** Distinguishes the tokens of one run: the rejected-credential cache is keyed by the bytes. */
    private static final AtomicInteger NONCE = new AtomicInteger();

    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();
    private AuthenticationMgr authenticationMgr;
    private String[] savedChain;
    private boolean savedAcceptJwt;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        // The directory: everyone's password is LDAP_PASSWORD, except bob, whose real password
        // happens to be shaped like a JWT.
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                BINDS.add(password);
                boolean ok = LDAP_PASSWORD.equals(password)
                        || (dn.startsWith("uid=bob,") && JWT_SHAPED_PASSWORD.equals(password));
                if (!ok) {
                    throw new AuthenticationException("invalid credentials");
                }
            }
        };
    }

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        savedChain = Config.authentication_chain;
        savedAcceptJwt = Config.authentication_chain_accept_jwt_as_password;
        BINDS.clear();
        // Process-wide: a rejection one case remembers must not refuse another case's login.
        AuthenticationHandler.invalidateRejectedCredentialCache();

        Map<String, String> ldap = new HashMap<>();
        ldap.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=People,dc=example,dc=com");
        authenticationMgr.replayCreateSecurityIntegration(LDAP_SI, ldap);

        Map<String, String> jwt = new HashMap<>();
        jwt.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        jwt.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        jwt.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        jwt.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, "http://localhost:38080/realms/master");
        jwt.put(JWTAuthenticationProvider.JWT_REQUIRED_AUDIENCE, "12345");
        authenticationMgr.replayCreateSecurityIntegration(JWT_SI, jwt);

        Config.authentication_chain = new String[] {"native", LDAP_SI, JWT_SI};
        Config.authentication_chain_accept_jwt_as_password = true;
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedChain;
        Config.authentication_chain_accept_jwt_as_password = savedAcceptJwt;
    }

    /** A token the mock JWKS verifies, naming the user {@code harbor}; never the same bytes twice. */
    private String validToken() throws Exception {
        return mockTokenUtils.generateTokenWithClaims(Map.of("nonce", NONCE.incrementAndGet()), "12345", 3600 * 1000);
    }

    private static UserIdentity login(String user, String password, AuthChannel channel) throws AuthenticationException {
        return AuthenticationHandler.authenticate(new ConnectContext(),
                AuthCredentials.ofPassword(user, password, channel, "127.0.0.1"));
    }

    @Test
    public void testLooksLikeJwt() {
        Assertions.assertTrue(AuthCredentials.ofPassword("u", JWT_SHAPED_PASSWORD, AuthChannel.HTTP, "h").looksLikeJwt());
        Assertions.assertFalse(AuthCredentials.ofPassword("u", LDAP_PASSWORD, AuthChannel.HTTP, "h").looksLikeJwt());
        // The prefix alone is not enough: no dots, or a character outside base64url.
        Assertions.assertFalse(AuthCredentials.ofPassword("u", "eyJsomething-long-enough", AuthChannel.HTTP, "h")
                .looksLikeJwt());
        Assertions.assertFalse(AuthCredentials.ofPassword("u", "eyJa.b$c.dddddddddddddd", AuthChannel.HTTP, "h")
                .looksLikeJwt());
        Assertions.assertFalse(AuthCredentials.ofPassword("u", "eyJ.a.b", AuthChannel.HTTP, "h").looksLikeJwt());
        // JSON allows whitespace before the first key, so a header need not start with eyJ ...
        String spacedHeader = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\n  \"alg\": \"RS256\"\n}".getBytes(StandardCharsets.UTF_8));
        Assertions.assertFalse(spacedHeader.startsWith("eyJ"));
        Assertions.assertTrue(AuthCredentials.ofPassword("u", spacedHeader + ".e30.c2lnbmF0dXJl", AuthChannel.HTTP, "h")
                .looksLikeJwt());
        // ... but it must decode to a JSON object.
        Assertions.assertFalse(AuthCredentials.ofPassword("u", "abcdefgh.ijklmnop.qrstuvwx", AuthChannel.HTTP, "h")
                .looksLikeJwt());
    }

    @Test
    public void testTokenInThePasswordLogsInThroughTheJwtIntegrationWithoutTouchingLdap() throws Exception {
        UserIdentity uid = login("harbor", validToken(), AuthChannel.MYSQL);
        Assertions.assertEquals("harbor", uid.getUser());
        Assertions.assertTrue(BINDS.isEmpty(), "a verified token must never be bound against the directory");
    }

    @Test
    public void testMysqlClearPasswordFrameCarryingAToken() throws Exception {
        // The MySQL clear-password frame is the token plus a trailing NUL; the entry point strips it.
        byte[] frame = (validToken() + "\0").getBytes(StandardCharsets.UTF_8);
        AuthCredentials cred = MysqlProto.handshakeCredentials("harbor", frame,
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(), "127.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD, cred.kind());
        Assertions.assertEquals("harbor", AuthenticationHandler.authenticate(new ConnectContext(), cred).getUser());
        Assertions.assertTrue(BINDS.isEmpty());
    }

    @Test
    public void testSwitchOffKeepsTheTokenAPassword() throws Exception {
        Config.authentication_chain_accept_jwt_as_password = false;
        String token = validToken();
        Assertions.assertThrows(AuthenticationException.class, () -> login("harbor", token, AuthChannel.MYSQL));
        Assertions.assertEquals(List.of(token), BINDS, "with the switch off the token goes to LDAP as a password");
    }

    @Test
    public void testOrdinaryPasswordStillGoesToLdap() throws Exception {
        Assertions.assertEquals("alice", login("alice", LDAP_PASSWORD, AuthChannel.MYSQL).getUser());
        Assertions.assertEquals(List.of(LDAP_PASSWORD), BINDS);
    }

    @Test
    public void testJwtShapedPasswordFallsBackToLdap() throws Exception {
        // Nobody signed this, so the JWT integration refuses it and the password path runs. For
        // bob it is the real directory password, so the bind succeeds - which also proves the
        // refused token attempt did not land in the rejected-credential cache under these bytes,
        // or the password attempt would have been refused without a bind. Twice, to be sure.
        Assertions.assertEquals("bob", login("bob", JWT_SHAPED_PASSWORD, AuthChannel.MYSQL).getUser());
        Assertions.assertEquals("bob", login("bob", JWT_SHAPED_PASSWORD, AuthChannel.MYSQL).getUser());
        Assertions.assertEquals(List.of(JWT_SHAPED_PASSWORD, JWT_SHAPED_PASSWORD), BINDS);

        // For anyone else the directory says no, and the login fails as a wrong password.
        BINDS.clear();
        Assertions.assertThrows(AuthenticationException.class,
                () -> login("carol", JWT_SHAPED_PASSWORD, AuthChannel.MYSQL));
        Assertions.assertEquals(List.of(JWT_SHAPED_PASSWORD), BINDS);
    }

    @Test
    public void testRefusedLoginKeepsTheSessionProvider() throws Exception {
        // COM_CHANGE_USER keeps the old session when the new login is refused, so the refused
        // login must leave that session's provider alone - including when the token attempt ran
        // first and the password attempt was then refused straight from the rejection cache.
        Assertions.assertThrows(AuthenticationException.class,
                () -> login("carol", JWT_SHAPED_PASSWORD, AuthChannel.MYSQL));
        ConnectContext session = new ConnectContext();
        AuthenticationProvider sessionProvider = new PlainPasswordAuthenticationProvider(new byte[0]);
        session.getAccessControlContext().setAuthenticationProvider(sessionProvider);
        Assertions.assertThrows(AuthenticationException.class, () -> AuthenticationHandler.authenticate(session,
                AuthCredentials.ofPassword("carol", JWT_SHAPED_PASSWORD, AuthChannel.MYSQL, "127.0.0.1")));
        Assertions.assertSame(sessionProvider, session.getAuthenticationProvider());
    }

    @Test
    public void testChainWithoutTokenIntegrationTreatsTheTokenAsAPassword() throws Exception {
        Config.authentication_chain = new String[] {"native", LDAP_SI};
        String token = validToken();
        Assertions.assertThrows(AuthenticationException.class, () -> login("harbor", token, AuthChannel.MYSQL));
        Assertions.assertEquals(List.of(token), BINDS);
    }

    @Test
    public void testBasicPasswordOnHttpAndArrowIsNeverReadAsAToken() throws Exception {
        // HTTP and Arrow Flight say which credential they carry: a token belongs in a Bearer
        // header, so a Basic password stays a password even with the switch on, and the
        // directory is the one that refuses it.
        // Two tokens: the rejected-credential cache is keyed by user and bytes, not by channel.
        String token = validToken();
        String arrowToken = validToken();
        Assertions.assertThrows(AuthenticationException.class, () -> login("harbor", token, AuthChannel.HTTP));
        Assertions.assertThrows(AuthenticationException.class,
                () -> login("harbor", arrowToken, AuthChannel.ARROW_FLIGHT));
        Assertions.assertEquals(List.of(token, arrowToken), BINDS);

        // The same token in a Bearer credential is what those channels accept.
        UserIdentity uid = AuthenticationHandler.authenticate(new ConnectContext(),
                AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1"));
        Assertions.assertEquals("harbor", uid.getUser());
    }
}
