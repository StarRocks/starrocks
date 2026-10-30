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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The authentication metrics as seen through {@link AuthenticationHandler}: success and failure
 * by channel and kind, and the failure reason, which is the only place a Bearer failure says what
 * went wrong (the HTTP response is a flat 401 on purpose). The mock token (see
 * {@link MockTokenUtils}) has iss=http://localhost:38080/realms/master and
 * preferred_username=harbor.
 */
public class AuthMetricsTest {
    private static final String ISSUER = "http://localhost:38080/realms/master";
    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();
    private AuthenticationMgr authenticationMgr;
    private String[] savedChain;
    private boolean savedBearerFlag;
    private boolean savedInit;

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
        savedInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        AuthenticationHandler.invalidateRejectedCredentialCache();
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedChain;
        Config.enable_rest_bearer_authentication = savedBearerFlag;
        MetricRepo.hasInit = savedInit;
    }

    private void registerJwtSi(String name, String jwksUrl, String requiredIssuer) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, jwksUrl);
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        properties.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, requiredIssuer);
        authenticationMgr.replayCreateSecurityIntegration(name, properties);
        Config.authentication_chain = new String[] {"native", name};
    }

    private static long reasons(AuthFailureReason reason) {
        return MetricRepo.COUNTER_AUTH_FAILURE_REASON.getMetric(reason.label()).getValue();
    }

    // Label values are the documented contract (alerts are written against them), so they are
    // spelled out here rather than derived.
    private static long failures(String channel, String kind) {
        return MetricRepo.COUNTER_AUTH_FAILURE.getMetric(channel, kind).getValue();
    }

    private static long successes(String channel, String kind) {
        return MetricRepo.COUNTER_AUTH_SUCCESS.getMetric(channel, kind).getValue();
    }

    /** Authenticate an HTTP Bearer token, expecting a failure, and return the reason it carried. */
    private AuthFailureReason failWith(String token) {
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1")));
        return e.getReason();
    }

    @Test
    public void testSuccessIsCountedByChannelAndKind() throws Exception {
        // TC-L1-411-01
        registerJwtSi("jwt_si", "jwks.json", ISSUER);
        long before = successes("http", "bearer_token");
        AuthenticationHandler.authenticate(new ConnectContext(), AuthCredentials.ofBearer(
                mockTokenUtils.generateTestOIDCToken(3600 * 1000), AuthChannel.HTTP, "127.0.0.1"));
        Assertions.assertEquals(before + 1, successes("http", "bearer_token"));
    }

    @Test
    public void testFailureIsCountedByChannelKindAndReason() throws Exception {
        // TC-L1-411-02 / TC-L1-411-04 (expired)
        registerJwtSi("jwt_si", "jwks.json", ISSUER);
        long failuresBefore = failures("http", "bearer_token");
        long expiredBefore = reasons(AuthFailureReason.EXPIRED);

        Assertions.assertEquals(AuthFailureReason.EXPIRED, failWith(mockTokenUtils.generateTestOIDCToken(-60_000)));

        Assertions.assertEquals(failuresBefore + 1, failures("http", "bearer_token"));
        Assertions.assertEquals(expiredBefore + 1, reasons(AuthFailureReason.EXPIRED));
    }

    /**
     * TC-L1-411-04: each way a token can be refused files under its own reason, and the reason
     * survives the trip through the security integration chain, which wraps the provider's
     * exception in its own.
     */
    @Test
    public void testEachRefusalHasItsOwnReason() throws Exception {
        registerJwtSi("jwt_si", "jwks.json", ISSUER);

        // Signed by a key the JWKS does not have: the kid is unknown.
        Assertions.assertEquals(AuthFailureReason.SIGNATURE_FAIL,
                failWith(mockTokenUtils.generateTokenSignedByUnknownKey(ISSUER, 3600 * 1000)));
        // HS256 is outside the algorithm allow-list (key confusion, T7).
        Assertions.assertEquals(AuthFailureReason.SIGNATURE_FAIL,
                failWith(mockTokenUtils.generateHmacConfusionToken(ISSUER)));
        // Well-formed and signed by our key, but the principal is not the user the MySQL client
        // declared. This is the check that stops `mysql -u bob` from logging in with alice's token.
        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        AuthenticationException mismatch = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(), new AuthCredentials("bob",
                        token.getBytes(StandardCharsets.UTF_8), AuthCredentialKind.BEARER_TOKEN,
                        AuthChannel.MYSQL, "127.0.0.1")));
        Assertions.assertEquals(AuthFailureReason.CLAIM_MISMATCH, mismatch.getReason());
        // Not a JWT at all.
        Assertions.assertEquals(AuthFailureReason.MALFORMED, failWith("this-is-not-a-token"));
    }

    @Test
    public void testNoIntegrationForTheIssuerIsItsOwnReason() throws Exception {
        // TC-L1-411-04 (no_matching_si): the only integration is for another issuer, so the token
        // is never tried anywhere. A configuration gap, not a bad credential.
        registerJwtSi("other_idp", "jwks.json", "https://some-other-idp.example");
        Assertions.assertEquals(AuthFailureReason.NO_MATCHING_SI,
                failWith(mockTokenUtils.generateTestOIDCToken(3600 * 1000)));
    }

    @Test
    public void testDisabledSwitchIsItsOwnReason() throws Exception {
        // TC-L1-411-04 (disabled)
        registerJwtSi("jwt_si", "jwks.json", ISSUER);
        Config.enable_rest_bearer_authentication = false;
        Assertions.assertEquals(AuthFailureReason.DISABLED,
                failWith(mockTokenUtils.generateTestOIDCToken(3600 * 1000)));
    }

    @Test
    public void testUnreachableJwksIsInternalNotBadCredential() throws Exception {
        // The IdP cannot be reached and there are no cached keys to fall back on: the token holder
        // did nothing wrong, and an alert on bad_credential must not fire for an IdP outage.
        registerJwtSi("jwt_si", "http://127.0.0.1:1/jwks", ISSUER);
        Assertions.assertEquals(AuthFailureReason.INTERNAL,
                failWith(mockTokenUtils.generateTestOIDCToken(3600 * 1000)));
    }

    @Test
    public void testIntegrationsThatDisagreeOnTheReasonAreFiledAsBadCredential() throws Exception {
        // Two integrations for the issuer refuse the same expired token for different reasons: one cannot
        // reach its IdP (internal), the other sees the expiry. Neither reason describes the whole refusal,
        // so the chain files it under the default instead of promoting one of them.
        registerJwtSi("jwt_unreachable", "http://127.0.0.1:1/jwks", ISSUER);
        registerJwtSi("jwt_expiry", "jwks.json", ISSUER);
        Config.authentication_chain = new String[] {"native", "jwt_unreachable", "jwt_expiry"};

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(), AuthCredentials.ofBearer(
                        mockTokenUtils.generateTestOIDCToken(-60_000), AuthChannel.HTTP, "127.0.0.1")));
        Assertions.assertEquals(AuthFailureReason.BAD_CREDENTIAL, e.getReason());
        Assertions.assertTrue(e.getMessage().contains("jwt_unreachable") && e.getMessage().contains("jwt_expiry"),
                e.getMessage());
    }

    @Test
    public void testAnIntegrationThatThrowsIsInternalNotBadCredential() throws Exception {
        // A misconfigured integration (an LDAP port that is not a number) throws while its provider is being
        // built. The chain turns that into an authentication failure filed as internal - the user typed
        // nothing wrong - instead of letting the NumberFormatException escape to the entry point.
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "127.0.0.1");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "not-a-port");
        authenticationMgr.replayCreateSecurityIntegration("ldap_broken", properties);
        Config.authentication_chain = new String[] {"native", "ldap_broken"};
        long internalBefore = reasons(AuthFailureReason.INTERNAL);

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        AuthCredentials.ofPassword("ldap_user", "ldap_password", AuthChannel.HTTP, "127.0.0.1")));
        Assertions.assertEquals(AuthFailureReason.INTERNAL, e.getReason());
        Assertions.assertTrue(e.getMessage().contains("ldap_broken"), e.getMessage());
        Assertions.assertEquals(internalBefore + 1, reasons(AuthFailureReason.INTERNAL));
    }

    @Test
    public void testANativeUserWithAnUnknownPluginIsRefused() throws Exception {
        // The stored plugin name is one this FE cannot build a provider for (an image written by a newer
        // version, say). The user is refused outright as a bad credential; it is neither let through nor
        // handed on to the security integrations as if it did not exist.
        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER plugin_user IDENTIFIED BY 'pw'", root));
        UserAuthenticationInfo info = authenticationMgr.getUserAuthenticationInfoByUserIdentity(
                UserIdentity.createAnalyzedUserIdentWithIp("plugin_user", "%"));
        Deencapsulation.setField(info, "authPlugin", "AUTHENTICATION_FROM_A_NEWER_FE");
        registerJwtSi("jwt_si", "jwks.json", ISSUER);
        long badBefore = reasons(AuthFailureReason.BAD_CREDENTIAL);

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                AuthenticationHandler.authenticate(new ConnectContext(),
                        AuthCredentials.ofPassword("plugin_user", "pw", AuthChannel.HTTP, "127.0.0.1")));
        Assertions.assertTrue(e.getMessage().contains("Access denied for user 'plugin_user'"), e.getMessage());
        Assertions.assertEquals(badBefore + 1, reasons(AuthFailureReason.BAD_CREDENTIAL));
    }

    @Test
    public void testChannelLabelTellsTheEntryPointsApart() throws Exception {
        // TC-L2-411-03: the same password login from three entry points lands on three channel
        // values with the same kind. This is what the channel label exists for.
        ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr.createUser((CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER pw_user IDENTIFIED BY 'pw'", root));
        Config.authentication_chain = new String[] {"native"};

        for (AuthChannel channel : new AuthChannel[] {AuthChannel.HTTP, AuthChannel.ARROW_FLIGHT, AuthChannel.THRIFT_LOAD}) {
            String label = channel.name().toLowerCase(java.util.Locale.ROOT);
            long before = successes(label, "plain_password");
            AuthenticationHandler.authenticate(new ConnectContext(),
                    AuthCredentials.ofPassword("pw_user", "pw", channel, "127.0.0.1"));
            Assertions.assertEquals(before + 1, successes(label, "plain_password"), label);
        }
        Assertions.assertEquals(java.util.Set.of("http", "arrow_flight", "thrift_load"),
                java.util.Set.of(AuthCredentials.ofPassword("u", "p", AuthChannel.HTTP, "h").channelLabel(),
                        AuthCredentials.ofPassword("u", "p", AuthChannel.ARROW_FLIGHT, "h").channelLabel(),
                        AuthCredentials.ofPassword("u", "p", AuthChannel.THRIFT_LOAD, "h").channelLabel()));
    }

    @Test
    public void testLabelNamesAndValuesAreTheDocumentedContract() {
        // TC-L1-411-06: alert rules are written against these, so they are spelled out.
        java.util.List<String> keys = MetricRepo.COUNTER_AUTH_SUCCESS.getMetric("http", "bearer_token").getLabels()
                .stream().map(l -> l.getKey()).collect(java.util.stream.Collectors.toList());
        Assertions.assertEquals(java.util.List.of("channel", "kind"), keys);

        java.util.Set<String> kinds = new java.util.HashSet<>();
        for (AuthCredentialKind kind : AuthCredentialKind.values()) {
            // A kind that names the MySQL handshake can only be built on the MySQL channel
            // (AuthCredentials enforces it), so ask for its label there.
            AuthChannel channel = kind.isMysqlProtocolOnly() ? AuthChannel.MYSQL : AuthChannel.HTTP;
            kinds.add(new AuthCredentials("u", new byte[0], kind, channel, "h").kindLabel());
        }
        Assertions.assertEquals(java.util.Set.of("mysql_native_scramble", "mysql_oauth2_client", "gssapi_token",
                "plain_password", "bearer_token"), kinds);
        java.util.Set<String> channels = new java.util.HashSet<>();
        for (AuthChannel channel : AuthChannel.values()) {
            channels.add(new AuthCredentials("u", new byte[0], AuthCredentialKind.PLAIN_PASSWORD, channel, "h")
                    .channelLabel());
        }
        Assertions.assertEquals(java.util.Set.of("mysql", "http", "arrow_flight", "thrift_load", "thrift_gateway"),
                channels);
        Assertions.assertEquals(java.util.Set.of("malformed", "no_matching_si", "signature_fail", "expired",
                "claim_mismatch", "bad_credential", "disabled", "internal"),
                java.util.Arrays.stream(AuthFailureReason.values()).map(AuthFailureReason::label)
                        .collect(java.util.stream.Collectors.toSet()));
    }
}
