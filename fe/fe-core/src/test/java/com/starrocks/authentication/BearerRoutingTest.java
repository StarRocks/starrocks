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

import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.starrocks.authentication.AccessControlContext;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Component-level tests for the Bearer path through {@link AuthenticationHandler}: issuer-based
 * routing across a multi-SI authentication chain, the enable_rest_bearer_authentication switch, and the
 * required_issuer requirement. The mock token (see {@link MockTokenUtils}) has
 * iss=http://localhost:38080/realms/master, aud=12345 and preferred_username=harbor.
 */
public class BearerRoutingTest {
    private static final String MASTER_ISSUER = "http://localhost:38080/realms/master";

    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();
    private AuthenticationMgr authenticationMgr;
    private String[] savedChain;
    private boolean savedBearerFlag;

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
        savedBearerFlag = Config.enable_rest_bearer_authentication;
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedChain;
        Config.enable_rest_bearer_authentication = savedBearerFlag;
    }

    private void registerJwtSi(String name, String requiredIssuer) {
        registerJwtSi(name, "jwks.json", requiredIssuer);
    }

    private void registerJwtSi(String name, String jwksUrl, String requiredIssuer) {
        registerJwtSi(name, jwksUrl, requiredIssuer, null);
    }

    private void registerJwtSi(String name, String jwksUrl, String requiredIssuer, String requiredAudience) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, jwksUrl);
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        if (requiredIssuer != null) {
            properties.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, requiredIssuer);
        }
        if (requiredAudience != null) {
            properties.put(JWTAuthenticationProvider.JWT_REQUIRED_AUDIENCE, requiredAudience);
        }
        authenticationMgr.replayCreateSecurityIntegration(name, properties);
    }

    private void createNativeUser(String sql) throws Exception {
        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(sql, root));
    }

    /**
     * The MySQL handshake frames a clear password, an OpenID Connect token and the OAuth2 browser
     * flow differently; the entry point is the one place that knows which is which. Everything
     * downstream sees a protocol-neutral kind, which is what lets the security integration chain
     * filter on "can this provider read it" alone.
     */
    @Test
    public void testMysqlHandshakeIsNormalisedByNegotiatedPlugin() throws Exception {
        AuthCredentials clear = MysqlProto.handshakeCredentials(
                "alice", "secret\0".getBytes(StandardCharsets.UTF_8),
                AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(), "127.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.PLAIN_PASSWORD, clear.kind());
        Assertions.assertEquals("secret", new String(clear.rawSecret(), StandardCharsets.UTF_8),
                "the trailing NUL belongs to the MySQL frame, not to the password");

        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(token);
        AuthCredentials jwt = MysqlProto.handshakeCredentials(
                "alice", serializer.toArray(),
                AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(), "127.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.BEARER_TOKEN, jwt.kind());
        Assertions.assertEquals(token, jwt.tokenString());

        AuthCredentials browser = MysqlProto.handshakeCredentials(
                "alice", new byte[0], AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString(), "127.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.MYSQL_OAUTH2_CLIENT, browser.kind());

        // The native-password scramble is the one frame that stays MySQL-specific: it answers the
        // salt from the server greeting. An old client that sends no plugin name is speaking it too.
        for (String plugin : new String[] {AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), null}) {
            Assertions.assertEquals(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE,
                    MysqlProto.handshakeCredentials("alice", new byte[] {1, 2, 3}, plugin, "127.0.0.1")
                            .kind(), "plugin: " + plugin);
        }

        // Every MySQL frame stays on the MySQL channel whatever it decodes to; the channel is what
        // guards the switches that must not reach back into the MySQL protocol.
        Assertions.assertEquals(AuthChannel.MYSQL, clear.channel());
        Assertions.assertEquals(AuthChannel.MYSQL, jwt.channel());
        Assertions.assertEquals(AuthChannel.MYSQL, browser.channel());
    }

    /**
     * A MySQL JWT user declares a name in the handshake, and the principal claim has to match it.
     * The binding is made by handing the provider the declared identity, so it survives only if
     * that decision is taken on "was a user declared" rather than on the credential kind - after
     * normalisation this credential is a BEARER_TOKEN like any HTTP one.
     */
    @Test
    public void testMysqlTokenMustMatchTheDeclaredUser() throws Exception {
        registerJwtSi("jwt_iss", "http://localhost:38080/realms/master");
        Config.authentication_chain = new String[] {"native", "jwt_iss"};

        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);   // preferred_username=harbor
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(token);

        Assertions.assertThrows(AuthenticationException.class, () ->
                        AuthenticationHandler.authenticate(new ConnectContext(),
                                MysqlProto.handshakeCredentials("mallory", serializer.toArray(),
                                        AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(), "127.0.0.1")),
                "a token naming someone else must not log in as the declared user");
    }

    /**
     * An OAuth2 security integration and an ordinary MySQL client. The client could not speak the
     * OAuth2 plugin, so it sent a native-password scramble; the integration takes it, answers the
     * handshake, and the first statement is where the user is told to authorize. Same shape as a
     * native OAuth2 user on the same client. What keeps this safe for a name nobody has verified:
     * every command but ping / quit is refused until the callback binds a token to the name
     * (ConnectProcessorTest), and the connection is closed by connect_wait_timeout if that never
     * happens (ConnectContextTest).
     */
    @Test
    public void testOAuth2IntegrationLetsAStockClientThroughToTheFirstStatement() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_oauth2");
        authenticationMgr.replayCreateSecurityIntegration("oauth2_si", properties);
        Config.authentication_chain = new String[] {"native", "oauth2_si"};

        ConnectContext ctx = new ConnectContext();
        UserIdentity uid = AuthenticationHandler.authenticate(ctx, MysqlProto.handshakeCredentials(
                "alice", new byte[20], AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), "127.0.0.1"));
        Assertions.assertEquals("alice", uid.getUser(), "the handshake is answered for the declared user");
        Assertions.assertTrue(ctx.getAuthenticationProvider() instanceof OAuth2AuthenticationProvider,
                "the integration that will gate the first statement is remembered on the connection");

        // No token has arrived, so the first statement must not run: it gets the authorization URL.
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                ctx.getAuthenticationProvider().checkLoginSuccess(ctx.getConnectionId(), ctx.getAccessControlContext()));
        Assertions.assertTrue(e.getMessage().contains("response_type=code"), e.getMessage());
    }

    /**
     * A native user whose row names the OAuth2 plugin, and a client that hands a token over in the
     * handshake (the OpenID Connect client plugin). The token is valid, but it names harbor: it
     * must not open a session as oauth_native. The same frame for a row named harbor logs in.
     */
    @Test
    public void testAnOAuth2UsersSessionCannotBeOpenedWithSomeoneElsesToken() throws Exception {
        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        String oauth2Row = " IDENTIFIED WITH authentication_oauth2 AS '{\"jwks_url\":\"jwks.json\","
                + "\"principal_field\":\"preferred_username\",\"required_issuer\":\"" + MASTER_ISSUER + "\","
                + "\"connect_wait_timeout\":1}'";
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER oauth_native" + oauth2Row, root));
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER harbor" + oauth2Row, root));
        Config.authentication_chain = new String[] {"native"};

        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(mockTokenUtils.generateTestOIDCToken(3600 * 1000));
        byte[] frame = serializer.toArray();
        String openIdClient = AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString();

        ConnectContext asSomebodyElse = new ConnectContext();
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(asSomebodyElse,
                        MysqlProto.handshakeCredentials("oauth_native", frame, openIdClient, "127.0.0.1")));
        Assertions.assertNotEquals(AuthFailureReason.DISABLED, e.getReason());
        Assertions.assertNull(asSomebodyElse.getAuthToken(), "the token must not be deposited on a refused connection");
        Assertions.assertNull(asSomebodyElse.getCurrentUserIdentity());

        ConnectContext asHarbor = new ConnectContext();
        UserIdentity uid = AuthenticationHandler.authenticate(asHarbor,
                MysqlProto.handshakeCredentials("harbor", frame, openIdClient, "127.0.0.1"));
        Assertions.assertEquals("harbor", uid.getUser());
        Assertions.assertFalse(uid.isEphemeral(), "the native row is the identity");
        Assertions.assertNotNull(asHarbor.getAuthToken(), "the first statement must find the connection logged in");
    }

    @Test
    public void testBearerRoutesToSiMatchingIssuer() throws Exception {
        // Two JWT SIs on the chain; the token's iss must route it to the matching one and log in as
        // the principal.
        registerJwtSi("jwt_other", "https://other.example.com");
        registerJwtSi("jwt_master", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_other", "jwt_master"};

        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        UserIdentity uid = AuthenticationHandler.authenticate(
                new ConnectContext(), AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1"));

        Assertions.assertEquals("harbor", uid.getUser());
        Assertions.assertTrue(uid.isEphemeral());
    }

    @Test
    public void testBearerNoMatchingIssuerRejected() throws Exception {
        // No SI accepts the token's issuer -> rejected.
        registerJwtSi("jwt_other", "https://other.example.com");
        Config.authentication_chain = new String[] {"native", "jwt_other"};

        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1")));
    }

    @Test
    public void testRequiredIssuerUnsetIsStillTried() throws Exception {
        // required_issuer only prunes: an integration that declares none is tried anyway and the
        // signature check decides. Reading "unset" as "never match" would be the old routing-key
        // model, and it would lock out MySQL JWT users whose integration leaves the property unset
        // - it has always been optional there.
        registerJwtSi("jwt_noiss", null);
        Config.authentication_chain = new String[] {"native", "jwt_noiss"};

        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        UserIdentity uid = AuthenticationHandler.authenticate(new ConnectContext(),
                AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1"));
        Assertions.assertEquals("harbor", uid.getUser());
    }

    @Test
    public void testBearerRejectedWhenSwitchDisabled() throws Exception {
        registerJwtSi("jwt_master", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_master"};
        Config.enable_rest_bearer_authentication = false;

        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1")));
        Assertions.assertTrue(e.getMessage().contains("enable_rest_bearer_authentication"));
    }

    @Test
    public void testForgedIssuerStillFailsSignature() throws Exception {
        // Threat T1: forging the iss to match a configured SI only routes the token there;
        // verification with that SI's JWKS must still fail because the token is signed by a key not
        // present in the JWKS.
        registerJwtSi("jwt_master", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_master"};

        String forged = mockTokenUtils.generateTokenSignedByUnknownKey(MASTER_ISSUER, 3600 * 1000);
        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        AuthCredentials.ofBearer(forged, AuthChannel.HTTP, "127.0.0.1")));
    }

    private static AuthCredentials httpBearer(String token) {
        return AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1");
    }

    /**
     * Ported from the test of #64833, which this PR supersedes: a token with no {@code iss} claim can
     * never satisfy an integration's {@code required_issuer}, so that integration is pruned without a
     * key fetch, while an integration that declares no issuer still verifies the token.
     */
    @Test
    public void testATokenWithoutAnIssuerSkipsTheIssuerBoundIntegrationWithoutAKeyFetch() throws Exception {
        AtomicInteger keyFetches = new AtomicInteger();
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr() {
            @Override
            public JWKSource<SecurityContext> getJwkSource(String jwksUrl) {
                keyFetches.incrementAndGet();
                return super.getJwkSource(jwksUrl);
            }
        });
        String noIssuer = mockTokenUtils.generateTestOIDCTokenWithoutIssuer(3600 * 1000);
        registerJwtSi("jwt_master", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_master"};

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(new ConnectContext(), httpBearer(noIssuer)));
        Assertions.assertEquals(AuthFailureReason.NO_MATCHING_SI, e.getReason());
        Assertions.assertEquals(0, keyFetches.get(), "an integration the token cannot satisfy costs no key fetch");

        registerJwtSi("jwt_any", null);
        Config.authentication_chain = new String[] {"native", "jwt_master", "jwt_any"};
        Assertions.assertEquals("harbor",
                AuthenticationHandler.authenticate(new ConnectContext(), httpBearer(noIssuer)).getUser());
        Assertions.assertEquals(1, keyFetches.get(), "only the issuer-less integration fetched its keys");
    }

    @Test
    public void testAnIntegrationThatCannotVerifyTheTokenDoesNotEndTheChain() throws Exception {
        // TC-L2-45-15 / INV-15: two integrations for the same issuer; the first one's key set does not
        // contain the token's kid. It fails, the chain moves on, the second one verifies. Before the
        // catch-and-continue, the first failure ended the login.
        registerJwtSi("si_bad", "error-jwks.json", MASTER_ISSUER);
        registerJwtSi("si_good", "jwks.json", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "si_bad", "si_good"};

        ConnectContext ctx = new ConnectContext();
        UserIdentity uid = AuthenticationHandler.authenticate(ctx, httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000)));
        Assertions.assertEquals("harbor", uid.getUser());
        Assertions.assertEquals("si_good", ctx.getAccessControlContext().getSecurityIntegration(),
                "the integration that verified the token is the one recorded on the connection");
    }

    @Test
    public void testAuthenticationNeverReadsTheClientPluginName() throws Exception {
        // TC-L2-45-17 / INV-16: ctx.authPlugin is what the MySQL client announced in its handshake.
        // The old chain compared it against each integration's plugin, which is why nothing but MySQL
        // could reach an integration. Now the credential says everything: the same request gives the
        // same answer whatever the field holds.
        registerJwtSi("jwt_si", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_si"};
        String good = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        String forged = mockTokenUtils.generateTokenSignedByUnknownKey(MASTER_ISSUER, 3600 * 1000);

        for (String announced : new String[] {null, AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(), "no_such_plugin"}) {
            ConnectContext ok = new ConnectContext();
            ok.setAuthPlugin(announced);
            Assertions.assertEquals("harbor", AuthenticationHandler.authenticate(ok, httpBearer(good)).getUser(),
                    "authPlugin=" + announced);

            ConnectContext bad = new ConnectContext();
            bad.setAuthPlugin(announced);
            Assertions.assertThrows(AuthenticationException.class,
                    () -> AuthenticationHandler.authenticate(bad, httpBearer(forged)), "authPlugin=" + announced);
        }
    }

    @Test
    public void testMalformedTokenIsRefusedBeforeAnyIntegrationIsAsked() throws Exception {
        // TC-L2-45-16 (1): the issuer is read once, before the loop. A token that does not parse is
        // refused right there; no provider ever sees it.
        registerJwtSi("jwt_si", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_si"};
        AtomicInteger providerCalls = new AtomicInteger();
        new MockUp<JWTAuthenticationProvider>() {
            @Mock
            public UserIdentity authenticate(AccessControlContext context, UserIdentity userIdentity,
                                             AuthCredentials cred) {
                providerCalls.incrementAndGet();
                return userIdentity;
            }
        };

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(new ConnectContext(), httpBearer("not.a.jwt")));
        Assertions.assertEquals(AuthFailureReason.MALFORMED, e.getReason());
        Assertions.assertEquals(0, providerCalls.get(), "no integration is consulted for a token that does not parse");
    }

    @Test
    public void testBearerSwitchLeavesMysqlTokenLoginsAlone() throws Exception {
        // TC-L2-45-11 (1)(3) / INV-9: enable_rest_bearer_authentication is about the non-MySQL entry points.
        // A native JWT user over MySQL, and a native OAuth2 user's browser flow, predate the switch
        // and behave exactly as with it on.
        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER harbor IDENTIFIED WITH authentication_jwt AS '{\"jwks_url\":\"jwks.json\","
                        + "\"principal_field\":\"preferred_username\",\"required_issuer\":\"" + MASTER_ISSUER + "\"}'",
                root));
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER oauth_native IDENTIFIED WITH authentication_oauth2 AS '{\"jwks_url\":\"jwks.json\","
                        + "\"principal_field\":\"preferred_username\",\"required_issuer\":\"" + MASTER_ISSUER + "\","
                        + "\"connect_wait_timeout\":1}'", root));
        Config.authentication_chain = new String[] {"native"};
        Config.enable_rest_bearer_authentication = false;

        // (1) the MySQL OpenID Connect frame still logs the native JWT user in.
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(0);
        serializer.writeLenEncodedString(mockTokenUtils.generateTestOIDCToken(3600 * 1000));
        UserIdentity uid = AuthenticationHandler.authenticate(new ConnectContext(), MysqlProto.handshakeCredentials(
                "harbor", serializer.toArray(), AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(),
                "127.0.0.1"));
        Assertions.assertEquals("harbor", uid.getUser());

        // (3) the OAuth2 browser flow is untouched: it waits for the callback and times out as it
        // always did, rather than being turned away as "disabled".
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(), MysqlProto.handshakeCredentials(
                        "oauth_native", new byte[0], AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString(),
                        "127.0.0.1")));
        Assertions.assertFalse(e.getMessage().contains("disabled"), e.getMessage());
        Assertions.assertNotEquals(AuthFailureReason.DISABLED, e.getReason());
    }

    @Test
    public void testBearerSkipsTheNativeTableEvenWhenTheNameExistsThere() throws Exception {
        // TC-L2-45-12: a native password user named like the token's principal must not be consulted
        // for a Bearer login - a token declares no user, so there is nothing to look up. The login
        // lands on the integration, and the native row's password is never involved.
        createNativeUser("CREATE USER harbor IDENTIFIED BY 'a-password-the-token-does-not-know'");
        registerJwtSi("jwt_si", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_si"};

        ConnectContext ctx = new ConnectContext();
        UserIdentity uid = AuthenticationHandler.authenticate(ctx, httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000)));
        Assertions.assertEquals("harbor", uid.getUser());
        Assertions.assertTrue(uid.isEphemeral(), "the identity comes from the integration, not the native row");
        Assertions.assertEquals("jwt_si", ctx.getAccessControlContext().getSecurityIntegration());

        // ... and turning Bearer off leaves the native password login untouched.
        Config.enable_rest_bearer_authentication = false;
        Assertions.assertEquals("harbor", AuthenticationHandler.authenticate(new ConnectContext(),
                AuthCredentials.ofPassword("harbor", "a-password-the-token-does-not-know", AuthChannel.HTTP, "127.0.0.1"))
                .getUser());
    }

    @Test
    public void testTwoIntegrationsOnOneIssuerAreToldApartByAudience() throws Exception {
        // TC-L2-45-14 / §10 O8: the same IdP serves two tenants. Both integrations pass the issuer
        // filter; the first one refuses the token on its audience claim and the chain moves on to the
        // one that declared this audience - which is then the integration recorded on the connection,
        // so its group provider and permitted groups apply, not the first one's.
        registerJwtSi("tenant_a", "jwks.json", MASTER_ISSUER, "aud-a");
        registerJwtSi("tenant_b", "jwks.json", MASTER_ISSUER, "aud-b");
        Config.authentication_chain = new String[] {"native", "tenant_a", "tenant_b"};

        ConnectContext ctxB = new ConnectContext();
        AuthenticationHandler.authenticate(ctxB, httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000, "aud-b")));
        Assertions.assertEquals("tenant_b", ctxB.getAccessControlContext().getSecurityIntegration());

        ConnectContext ctxA = new ConnectContext();
        AuthenticationHandler.authenticate(ctxA, httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000, "aud-a")));
        Assertions.assertEquals("tenant_a", ctxA.getAccessControlContext().getSecurityIntegration());

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000, "aud-nobody"))));
        Assertions.assertEquals(AuthFailureReason.CLAIM_MISMATCH, e.getReason(),
                "both were tried and both refused the audience");
    }

    @Test
    public void testAuthCheckBypassKeepsItsOldMeaningOnEveryChannel() throws Exception {
        // TC-L2-45-19 / TC-L2-49-14: enable_auth_check=false is not "let everyone in". It skips the
        // credential check for a user the native table knows, and refuses one it does not know - the
        // chain is not tried. The normalised kinds must not change either half, on MySQL or HTTP.
        createNativeUser("CREATE USER known IDENTIFIED BY 'right'");
        registerJwtSi("jwt_si", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_si"};
        boolean saved = Config.enable_auth_check;
        Config.enable_auth_check = false;
        try {
            // known user, wrong password: accepted on both channels
            Assertions.assertEquals("known", AuthenticationHandler.authenticate(new ConnectContext(),
                    AuthCredentials.ofPassword("known", "wrong", AuthChannel.HTTP, "127.0.0.1")).getUser());
            Assertions.assertEquals("known", AuthenticationHandler.authenticate(new ConnectContext(),
                    MysqlProto.handshakeCredentials("known", new byte[20],
                            AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), "127.0.0.1")).getUser());
            // unknown user: refused, and the integration chain is not a fallback
            for (AuthCredentials cred : new AuthCredentials[] {
                    AuthCredentials.ofPassword("nobody", "x", AuthChannel.HTTP, "127.0.0.1"),
                    MysqlProto.handshakeCredentials("nobody", new byte[20],
                            AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), "127.0.0.1")}) {
                AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                        () -> AuthenticationHandler.authenticate(new ConnectContext(), cred));
                Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, e.getErrorCode(), cred.kindLabel());
            }
        } finally {
            Config.enable_auth_check = saved;
        }
    }

    @Test
    public void testAnIssuerlessIntegrationStillVerifiesTheSignature() throws Exception {
        // TC-L2-45-03 / threat T1b: leaving required_issuer unset relaxes the pruning, not the
        // verification. The integration is tried, and a token signed by a key outside its JWKS is
        // still refused. Without this, "no issuer constraint" could later be read as "nothing to
        // check" and no test would notice.
        registerJwtSi("jwt_noiss", null);
        Config.authentication_chain = new String[] {"native", "jwt_noiss"};

        String forged = mockTokenUtils.generateTokenSignedByUnknownKey(MASTER_ISSUER, 3600 * 1000);
        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(), httpBearer(forged)));

        // ... and the same integration accepts a properly signed one, so the refusal above is the
        // signature check talking and not the integration being skipped.
        Assertions.assertEquals("harbor", AuthenticationHandler.authenticate(new ConnectContext(),
                httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000))).getUser());
    }

    @Test
    public void testAnOldClientWithNoPluginNameLogsInWithItsNativePassword() throws Exception {
        // TC-L2-49-13 / INV-13: a MySQL 5.1-era client sends no plugin name in the handshake. Its
        // frame is the native scramble, and the login has to work end to end with enable_auth_check
        // on - asserting only the credential kind would not catch a break further down the path.
        createNativeUser("CREATE USER old_client IDENTIFIED BY 'right'");
        registerJwtSi("jwt_si", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_si"};

        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        ConnectContext ctx = new ConnectContext();
        ctx.setAuthDataSalt(seed);
        AuthCredentials cred = MysqlProto.handshakeCredentials(
                "old_client", MysqlPassword.scramble(seed, "right"), null, "127.0.0.1");
        Assertions.assertEquals(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE, cred.kind());
        Assertions.assertEquals("old_client", AuthenticationHandler.authenticate(ctx, cred).getUser());

        // The wrong password from the same client is still refused, so the login above proves the
        // scramble was actually checked.
        ConnectContext wrongCtx = new ConnectContext();
        wrongCtx.setAuthDataSalt(seed);
        Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(wrongCtx, MysqlProto.handshakeCredentials(
                        "old_client", MysqlPassword.scramble(seed, "wrong"), null, "127.0.0.1")));
    }

    @Test
    public void testACredentialThatIsNotATokenMustNameItsUser() throws Exception {
        // TC-L2-45-13 (2): the "no declared user" case is handled in two layers. A password with no
        // user name has nothing to authenticate and is refused as input validation, before the
        // chain; a token declares its user inside itself and goes on. The error code is what tells
        // the two apart: ERR_AUTHENTICATION_FAIL comes from the top, the chain reports
        // ERR_AUTHENTICATION_FAIL_IN_AUTH_CHAIN.
        registerJwtSi("jwt_si", MASTER_ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_si"};

        for (AuthCredentials nameless : new AuthCredentials[] {
                AuthCredentials.ofPassword("", "pw", AuthChannel.HTTP, "127.0.0.1"),
                AuthCredentials.ofPassword(null, "pw", AuthChannel.ARROW_FLIGHT, "127.0.0.1"),
                MysqlProto.handshakeCredentials("", "pw\0".getBytes(StandardCharsets.UTF_8),
                        AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString(), "127.0.0.1")}) {
            AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                    () -> AuthenticationHandler.authenticate(new ConnectContext(), nameless));
            Assertions.assertEquals(ErrorCode.ERR_AUTHENTICATION_FAIL, e.getErrorCode(),
                    nameless.kindLabel() + " must be refused before the chain");
        }

        // A Bearer token declares no user either, and that is fine: the name is in the token.
        Assertions.assertEquals("harbor", AuthenticationHandler.authenticate(new ConnectContext(),
                httpBearer(mockTokenUtils.generateTestOIDCToken(3600 * 1000))).getUser());
    }
}
