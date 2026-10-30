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
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The capability matrix of the design (§9.2), one test per cell, run through
 * {@link AuthenticationHandler} with the credential each entry point would build.
 *
 * <p>Rows are the seven ways a user can be defined - native password / LDAP / JWT / OAuth2, and
 * security integration LDAP / JWT / OAuth2. Columns are the channels: MySQL (one cell per handshake
 * frame the channel can produce), HTTP Basic, HTTP Bearer, Arrow Flight Basic, Arrow Flight
 * Bearer, and the BE-to-FE Thrift RPCs with either auth_type. Every cell asserts something: a
 * supported one logs in as the expected user through the expected path, a refused one fails with
 * the reason the code gives, and a cell the matrix marks not-applicable proves it is unreachable
 * (a Bearer request cannot resolve to a password user; a password cannot satisfy a token
 * integration).
 *
 * <p>Each row runs with an authentication chain containing only its own integration, so a cell
 * cannot pass by accident through a different row's provider. The LDAP bind is stubbed to accept
 * exactly the two LDAP users with exactly one password, for the same reason.
 *
 * <p>What this does not cover: the entry-point parsers themselves (HTTP header, Thrift auth_type,
 * MySQL frame) and the Arrow Flight internal session token, which have their own tests, and the
 * OAuth2 browser callback, which needs a running IdP and is an end-to-end case.
 */
public class AuthCapabilityMatrixTest {

    private static final String HOST = "127.0.0.1";
    private static final String LDAP_PASSWORD = "ldap_pw";
    // MockTokenUtils signs tokens with iss=http://localhost:38080/realms/master, aud=12345 and
    // preferred_username=harbor; the JWT-flavoured users are therefore all named harbor.
    private static final String ISSUER = "http://localhost:38080/realms/master";
    private static final String AUDIENCE = "12345";
    private static final String TOKEN_USER = "harbor";

    private static final MockTokenUtils TOKENS = new MockTokenUtils();
    private static String[] savedChain;
    private static String token;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        savedChain = Config.authentication_chain;
        token = TOKENS.generateTestOIDCToken(3600 * 1000);

        // The LDAP bind accepts exactly the two LDAP users of the matrix with exactly one password.
        // Anything else fails, so a cell cannot pass through LDAP by accident.
        new MockUp<LDAPAuthProvider>() {
            @Mock
            public void checkPassword(String dn, String password) throws Exception {
                boolean knownUser = dn.startsWith("uid=ldap_native,") || dn.startsWith("uid=ldap_si_user,");
                if (!knownUser || !LDAP_PASSWORD.equals(password)) {
                    throw new Exception("bind failed for " + dn);
                }
            }

            @Mock
            protected String findUserDNByRoot(String user) {
                return "uid=" + user + ",ou=People,dc=example,dc=com";
            }
        };
    }

    @AfterAll
    public static void afterClass() {
        Config.authentication_chain = savedChain;
    }

    // ------------------------------------------------------------------ fixture

    /** A fresh authentication manager with the four native users and the three integrations. */
    private static void fresh(String... chain) throws Exception {
        fresh(true, chain);
    }

    /**
     * The security-integration rows run without the native JWT / OAuth2 users. Native is tried
     * first, and the token user is named after the claim in every case, so with the native row
     * present it would answer before the integration ever ran and the cell would test the wrong path.
     */
    private static void freshWithoutNativeTokenUsers(String... chain) throws Exception {
        fresh(false, chain);
    }

    private static void fresh(boolean nativeTokenUsers, String... chain) throws Exception {
        // The rejection cache is process-wide and keyed by user + secret; a cell's failure must not
        // leak into the next cell that uses the same pair.
        AuthenticationHandler.invalidateRejectedCredentialCache();
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        AuthenticationMgr mgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(mgr);
        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);

        createUser(mgr, root, "CREATE USER pw_user IDENTIFIED BY 'pw'");
        createUser(mgr, root, "CREATE USER ldap_native IDENTIFIED WITH authentication_ldap_simple");
        if (nativeTokenUsers) {
            createUser(mgr, root, "CREATE USER " + TOKEN_USER + " IDENTIFIED WITH authentication_jwt AS '"
                    + "{\"jwks_url\":\"jwks.json\",\"principal_field\":\"preferred_username\","
                    + "\"required_issuer\":\"" + ISSUER + "\",\"required_audience\":\"" + AUDIENCE + "\"}'");
            createUser(mgr, root, "CREATE USER oauth_native IDENTIFIED WITH authentication_oauth2 AS '"
                    + "{\"jwks_url\":\"jwks.json\",\"principal_field\":\"preferred_username\","
                    + "\"required_issuer\":\"" + ISSUER + "\",\"required_audience\":\"" + AUDIENCE + "\","
                    + "\"connect_wait_timeout\":1}'");
        }

        Map<String, String> ldap = new HashMap<>();
        ldap.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=People,dc=example,dc=com");
        mgr.replayCreateSecurityIntegration("ldap_si", ldap);

        Map<String, String> jwt = new HashMap<>();
        jwt.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        jwt.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        jwt.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        jwt.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, ISSUER);
        jwt.put(JWTAuthenticationProvider.JWT_REQUIRED_AUDIENCE, AUDIENCE);
        mgr.replayCreateSecurityIntegration("jwt_si", jwt);

        Map<String, String> oauth2 = new HashMap<>();
        oauth2.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_oauth2");
        oauth2.put(OAuth2AuthenticationProvider.OAUTH2_JWKS_URL, "jwks.json");
        oauth2.put(OAuth2AuthenticationProvider.OAUTH2_PRINCIPAL_FIELD, "preferred_username");
        oauth2.put(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_ISSUER, ISSUER);
        oauth2.put(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_AUDIENCE, AUDIENCE);
        oauth2.put(OAuth2AuthenticationProvider.OAUTH2_CONNECT_WAIT_TIMEOUT, "1");
        mgr.replayCreateSecurityIntegration("oauth2_si", oauth2);

        Config.authentication_chain = chain;
    }

    private static void createUser(AuthenticationMgr mgr, ConnectContext root, String sql) throws Exception {
        mgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, root));
    }

    // ------------------------------------------------------------------ credential builders

    /** The mysql_native_password frame: a scramble of {@code password} against a salt on {@code ctx}. */
    private static AuthCredentials mysqlScramble(ConnectContext ctx, String user, String password) throws Exception {
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        ctx.setAuthDataSalt(seed);
        return MysqlProto.handshakeCredentials(user, MysqlPassword.scramble(seed, password),
                AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), HOST);
    }

    /** An ordinary client that could not speak the plugin the server wanted: 20 bytes of scramble. */
    private static AuthCredentials mysqlStockClient(String user) throws Exception {
        return MysqlProto.handshakeCredentials(user, new byte[20],
                AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), HOST);
    }

    private static AuthCredentials mysqlClearPassword(String user, String password) throws Exception {
        return MysqlProto.handshakeCredentials(user, (password + "\0").getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(), HOST);
    }

    private static AuthCredentials mysqlOpenIdConnect(String user, String jwt) throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(jwt);
        return MysqlProto.handshakeCredentials(user, serializer.toArray(),
                AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(), HOST);
    }

    private static AuthCredentials mysqlOAuth2Client(String user) throws Exception {
        return MysqlProto.handshakeCredentials(user, new byte[0],
                AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString(), HOST);
    }

    private static AuthCredentials basic(String user, String password, AuthChannel channel) {
        return AuthCredentials.ofPassword(user, password, channel, HOST);
    }

    private static AuthCredentials bearer(AuthChannel channel) {
        return AuthCredentials.ofBearer(token, channel, HOST);
    }

    // ------------------------------------------------------------------ assertions

    private static ConnectContext loginAs(String expectedUser, String expectedPath, AuthCredentials cred)
            throws Exception {
        return loginAs(new ConnectContext(), expectedUser, expectedPath, cred);
    }

    private static ConnectContext loginAs(ConnectContext ctx, String expectedUser, String expectedPath,
                                          AuthCredentials cred) throws Exception {
        UserIdentity uid = AuthenticationHandler.authenticate(ctx, cred);
        Assertions.assertEquals(expectedUser, uid.getUser());
        Assertions.assertEquals(expectedPath, ctx.getAccessControlContext().getSecurityIntegration(),
                "which path authenticated the user");
        return ctx;
    }

    private static void refused(String reason, AuthCredentials cred) {
        refused(new ConnectContext(), reason, cred);
    }

    private static void refused(ConnectContext ctx, String reason, AuthCredentials cred) {
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(ctx, cred));
        Assertions.assertTrue(e.getMessage().contains(reason),
                "expected the refusal to say '" + reason + "', was: " + e.getMessage());
    }

    /** The OAuth2 pass-through: the handshake succeeded, and the first statement gets the URL. */
    private static void firstStatementCarriesAuthorizationUrl(ConnectContext ctx) {
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                ctx.getAuthenticationProvider().checkLoginSuccess(ctx.getConnectionId(), ctx.getAccessControlContext()));
        Assertions.assertTrue(e.getMessage().contains("response_type=code"),
                "the first statement must carry the authorization URL, was: " + e.getMessage());
    }

    // ------------------------------------------------------------------ the matrix

    private static final List<DynamicTest> CELLS = new ArrayList<>();

    private static void cell(String name, Executable body) {
        CELLS.add(DynamicTest.dynamicTest(name, body));
    }

    @TestFactory
    public Stream<DynamicTest> capabilityMatrix() {
        CELLS.clear();
        nativePasswordRow();
        nativeLdapRow();
        nativeJwtRow();
        nativeOAuth2Row();
        siLdapRow();
        siJwtRow();
        siOAuth2Row();
        return CELLS.stream();
    }

    // ---- Native User / password: every password column works; Bearer cannot reach this user.

    private void nativePasswordRow() {
        cell("Native password × MySQL scramble: logs in", () -> {
            fresh("native");
            ConnectContext ctx = new ConnectContext();
            loginAs(ctx, "pw_user", "native", mysqlScramble(ctx, "pw_user", "pw"));
        });
        cell("Native password × MySQL scramble, wrong password: refused", () -> {
            fresh("native");
            ConnectContext ctx = new ConnectContext();
            refused(ctx, "Access denied", mysqlScramble(ctx, "pw_user", "not-pw"));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("Native password × " + ch + " Basic: logs in", () -> {
                fresh("native");
                loginAs("pw_user", "native", basic("pw_user", "pw", ch));
            });
            cell("Native password × " + ch + " Basic, wrong password: refused", () -> {
                fresh("native");
                refused("Access denied", basic("pw_user", "not-pw", ch));
            });
            cell("Native password × " + ch + " Bearer: not applicable - a token names nobody", () -> {
                fresh("native");
                refused("Access denied", bearer(ch));
            });
        }
    }

    // ---- Native User / LDAP plugin: password columns bind against the directory.

    private void nativeLdapRow() {
        cell("Native LDAP × MySQL clear-password frame: logs in", () -> {
            fresh("native");
            loginAs("ldap_native", "native", mysqlClearPassword("ldap_native", LDAP_PASSWORD));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("Native LDAP × " + ch + " Basic: logs in", () -> {
                fresh("native");
                loginAs("ldap_native", "native", basic("ldap_native", LDAP_PASSWORD, ch));
            });
            cell("Native LDAP × " + ch + " Basic, wrong password: refused", () -> {
                fresh("native");
                refused("bind failed", basic("ldap_native", "wrong", ch));
            });
            cell("Native LDAP × " + ch + " Bearer: not applicable - unreachable by a token", () -> {
                fresh("native");
                refused("Access denied", bearer(ch));
            });
        }
    }

    // ---- Native User / JWT plugin: MySQL works as it always did; Basic is refused by the provider;
    // ---- Bearer cannot find the user (no name to look up) - the documented non-goal.

    private void nativeJwtRow() {
        cell("Native JWT × MySQL OpenID Connect frame: logs in (no regression)", () -> {
            fresh("native");
            loginAs(TOKEN_USER, "native", mysqlOpenIdConnect(TOKEN_USER, token));
        });
        cell("Native JWT × MySQL OpenID Connect frame declaring someone else: refused (claim must match)", () -> {
            fresh("native");
            refused("Access denied", mysqlOpenIdConnect("mallory", token));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("Native JWT × " + ch + " Basic: refused in code - a JWT does not go in the password slot", () -> {
                fresh("native");
                refused("JWT user must authenticate via Bearer token", basic(TOKEN_USER, token, ch));
            });
            cell("Native JWT × " + ch + " Bearer: not supported - nothing to look the user up by (§2.3)", () -> {
                fresh("native");
                refused("Access denied", bearer(ch));
            });
        }
        cell("Native JWT × HTTP Bearer with a JWT integration present: the integration answers, not the native row (O3)", () -> {
            fresh("native", "jwt_si");
            loginAs(TOKEN_USER, "jwt_si", bearer(AuthChannel.HTTP));
        });
    }

    // ---- Native User / OAuth2 plugin: two MySQL clients, both through the browser callback.

    private void nativeOAuth2Row() {
        cell("Native OAuth2 × MySQL stock client: handshake passes, first statement carries the URL", () -> {
            fresh("native");
            ConnectContext ctx = loginAs("oauth_native", "native", mysqlStockClient("oauth_native"));
            firstStatementCarriesAuthorizationUrl(ctx);
        });
        cell("Native OAuth2 × MySQL OAuth2-aware client, callback not yet arrived: waits, then times out", () -> {
            fresh("native");
            refused("wait callback timeout", mysqlOAuth2Client("oauth_native"));
        });
        cell("Native OAuth2 × MySQL OAuth2-aware client, callback already deposited the token: logs in", () -> {
            fresh("native");
            ConnectContext ctx = new ConnectContext();
            ctx.getAccessControlContext().setAuthToken(token);
            loginAs(ctx, "oauth_native", "native", mysqlOAuth2Client("oauth_native"));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("Native OAuth2 × " + ch + " Basic: refused in code", () -> {
                fresh("native");
                refused("OAuth2 requires a Bearer token or the MySQL handshake", basic("oauth_native", "x", ch));
            });
            cell("Native OAuth2 × " + ch + " Bearer: not supported - nothing to look the user up by (§2.3)", () -> {
                fresh("native");
                refused("Access denied", bearer(ch));
            });
        }
    }

    // ---- SI / LDAP: every password column, MySQL included, binds through the integration.

    private void siLdapRow() {
        cell("SI LDAP × MySQL clear-password frame: logs in through the integration", () -> {
            freshWithoutNativeTokenUsers("native", "ldap_si");
            loginAs("ldap_si_user", "ldap_si", mysqlClearPassword("ldap_si_user", LDAP_PASSWORD));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("SI LDAP × " + ch + " Basic: logs in through the integration (the B1 fix)", () -> {
                freshWithoutNativeTokenUsers("native", "ldap_si");
                loginAs("ldap_si_user", "ldap_si", basic("ldap_si_user", LDAP_PASSWORD, ch));
            });
            cell("SI LDAP × " + ch + " Basic, wrong password: refused", () -> {
                freshWithoutNativeTokenUsers("native", "ldap_si");
                refused("bind failed", basic("ldap_si_user", "wrong", ch));
            });
            cell("SI LDAP × " + ch + " Bearer: not applicable - an LDAP integration does not read tokens", () -> {
                freshWithoutNativeTokenUsers("native", "ldap_si");
                refused("Access denied", bearer(ch));
            });
        }
    }

    // ---- SI / JWT: MySQL (declared user must match the claim) and every Bearer column.

    private void siJwtRow() {
        cell("SI JWT × MySQL OpenID Connect frame: logs in through the integration", () -> {
            freshWithoutNativeTokenUsers("native", "jwt_si");
            loginAs(TOKEN_USER, "jwt_si", mysqlOpenIdConnect(TOKEN_USER, token));
        });
        cell("SI JWT × MySQL OpenID Connect frame declaring someone else: refused (claim must match)", () -> {
            freshWithoutNativeTokenUsers("native", "jwt_si");
            refused("Access denied", mysqlOpenIdConnect("mallory", token));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("SI JWT × " + ch + " Bearer: logs in through the integration", () -> {
                freshWithoutNativeTokenUsers("native", "jwt_si");
                loginAs(TOKEN_USER, "jwt_si", bearer(ch));
            });
            cell("SI JWT × " + ch + " Basic: not applicable - a JWT integration does not read passwords", () -> {
                freshWithoutNativeTokenUsers("native", "jwt_si");
                refused("Access denied", basic(TOKEN_USER, "x", ch));
            });
        }
        cell("SI JWT × HTTP Bearer with the switch off: refused; MySQL OpenID Connect still logs in", () -> {
            freshWithoutNativeTokenUsers("native", "jwt_si");
            boolean saved = Config.enable_rest_bearer_authentication;
            Config.enable_rest_bearer_authentication = false;
            try {
                refused("disabled", bearer(AuthChannel.HTTP));
                loginAs(TOKEN_USER, "jwt_si", mysqlOpenIdConnect(TOKEN_USER, token));
            } finally {
                Config.enable_rest_bearer_authentication = saved;
            }
        });
    }

    // ---- SI / OAuth2: the two MySQL clients behave exactly as for a native OAuth2 user;
    // ---- Bearer columns act as a Resource Server.

    private void siOAuth2Row() {
        cell("SI OAuth2 × MySQL stock client: handshake passes through the integration, first statement carries the URL", () -> {
            freshWithoutNativeTokenUsers("native", "oauth2_si");
            ConnectContext ctx = loginAs("alice", "oauth2_si", mysqlStockClient("alice"));
            firstStatementCarriesAuthorizationUrl(ctx);
        });
        cell("SI OAuth2 × MySQL OAuth2-aware client, callback not yet arrived: waits, then times out", () -> {
            freshWithoutNativeTokenUsers("native", "oauth2_si");
            refused("wait callback timeout", mysqlOAuth2Client("alice"));
        });
        cell("SI OAuth2 × MySQL OAuth2-aware client, callback already deposited the token: logs in", () -> {
            freshWithoutNativeTokenUsers("native", "oauth2_si");
            ConnectContext ctx = new ConnectContext();
            ctx.getAccessControlContext().setAuthToken(token);
            loginAs(ctx, "alice", "oauth2_si", mysqlOAuth2Client("alice"));
        });
        cell("SI OAuth2 × MySQL OAuth2-aware client: a timed-out handshake must not poison the retry after authorizing", () -> {
            freshWithoutNativeTokenUsers("native", "oauth2_si");
            refused("wait callback timeout", mysqlOAuth2Client("alice"));
            // The user finishes the browser flow and the client reconnects. Nothing about the
            // credential changed - there is none - so a rejection cache keyed on it would refuse this.
            ConnectContext retry = new ConnectContext();
            retry.getAccessControlContext().setAuthToken(token);
            loginAs(retry, "alice", "oauth2_si", mysqlOAuth2Client("alice"));
        });
        for (AuthChannel ch : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            cell("SI OAuth2 × " + ch + " Bearer: verified as a Resource Server, logs in", () -> {
                freshWithoutNativeTokenUsers("native", "oauth2_si");
                loginAs(TOKEN_USER, "oauth2_si", bearer(ch));
            });
            cell("SI OAuth2 × " + ch + " Basic: not applicable - an OAuth2 integration does not read passwords", () -> {
                freshWithoutNativeTokenUsers("native", "oauth2_si");
                refused("Access denied", basic("alice", "x", ch));
            });
        }
    }
}
