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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BearerAuthTest {

    private static final String ISSUER = "http://localhost:38080/realms/master";
    private final String[] requiredIssuer = {ISSUER};
    private final String[] requiredAudience = {"12345"};
    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();

    @Test
    public void testBearerExtractsPrincipalAsEphemeralUser() throws Exception {
        // No declared user on a Bearer request: the provider must derive the user from the
        // principal claim (preferred_username=harbor in the mock token) and return an ephemeral
        // identity.
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        JWTAuthenticationProvider provider =
                new JWTAuthenticationProvider("jwks.json", "preferred_username", requiredIssuer, requiredAudience);
        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);

        UserIdentity uid = provider.authenticate(new ConnectContext().getAccessControlContext(), null,
                AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1"));

        Assertions.assertEquals("harbor", uid.getUser());
        Assertions.assertTrue(uid.isEphemeral());
    }

    @Test
    public void testJwtRejectsBasicCredential() throws Exception {
        // B3: a JWT user must use Bearer, never Basic (stuffing a token into a password slot is an
        // anti-pattern).
        JWTAuthenticationProvider provider =
                new JWTAuthenticationProvider("jwks.json", "preferred_username", requiredIssuer, requiredAudience);
        AuthCredentials basic = AuthCredentials.ofPassword("harbor", "whatever", AuthChannel.HTTP, "127.0.0.1");

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                provider.authenticate(new ConnectContext().getAccessControlContext(),
                        UserIdentity.createEphemeralUserIdent("harbor", "127.0.0.1"), basic));
        Assertions.assertTrue(e.getMessage().contains(AuthCredentialKind.PLAIN_PASSWORD.name()),
                "message should name the rejected kind, was: " + e.getMessage());
    }

    @Test
    public void testSupportsCredentialKindPerProvider() {
        JWTAuthenticationProvider jwt =
                new JWTAuthenticationProvider("jwks.json", "sub", requiredIssuer, requiredAudience);
        // Not MYSQL_NATIVE_SCRAMBLE any more: the MySQL OpenID Connect frame is unwrapped at the entry
        // point and reaches this provider as BEARER_TOKEN, so one shape covers every channel.
        Assertions.assertFalse(jwt.supportsCredentialKind(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE));
        Assertions.assertTrue(jwt.supportsCredentialKind(AuthCredentialKind.BEARER_TOKEN));
        Assertions.assertFalse(jwt.supportsCredentialKind(AuthCredentialKind.PLAIN_PASSWORD));

        PlainPasswordAuthenticationProvider plain =
                new PlainPasswordAuthenticationProvider(com.starrocks.mysql.MysqlPassword.EMPTY_PASSWORD);
        Assertions.assertTrue(plain.supportsCredentialKind(AuthCredentialKind.PLAIN_PASSWORD));
        Assertions.assertFalse(plain.supportsCredentialKind(AuthCredentialKind.BEARER_TOKEN));
    }

    @Test
    public void testAlgNoneIsRejected() throws Exception {
        // Threat T7: an unsigned (alg:none) token must never be accepted.
        MockTokenUtils.MockJwkMgr mockJwkMgr = new MockTokenUtils.MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(ISSUER).subject("8f7f0fa5").audience("12345")
                .claim("preferred_username", "harbor")
                .expirationTime(new Date(System.currentTimeMillis() + 3600_000))
                .build();
        String noneToken = new PlainJWT(claims).serialize();

        Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(noneToken, "harbor", jwkSet, "preferred_username",
                        requiredIssuer, requiredAudience));
    }

    @Test
    public void testEcdsaSignedTokenIsVerified() throws Exception {
        // PR4: EC keys (ES256) must verify, not just RSA. Google/Azure AD default to EC.
        ECKey ecJwk = new ECKeyGenerator(Curve.P_256).keyID("ec-test-1").generate();
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(ISSUER).subject("ec-subject").audience("12345")
                .claim("preferred_username", "ecuser")
                .expirationTime(new Date(System.currentTimeMillis() + 3600_000))
                .build();
        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("ec-test-1").build(), claims);
        signedJWT.sign(new ECDSASigner(ecJwk));
        String token = signedJWT.serialize();

        JWKSet publicSet = new JWKSet(ecJwk.toPublicJWK());
        // Should not throw: the verifier must pick an ECDSA verifier based on the JWK type.
        OpenIdConnectVerifier.verify(token, "ecuser", publicSet, "preferred_username", requiredIssuer, requiredAudience);

        // A tampered EC token (different key) must fail.
        ECKey otherKey = new ECKeyGenerator(Curve.P_256).keyID("ec-test-1").generate();
        JWKSet wrongSet = new JWKSet(otherKey.toPublicJWK());
        Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(token, "ecuser", wrongSet, "preferred_username",
                        requiredIssuer, requiredAudience));
    }

    @Test
    public void testExpiryCheckedBeforePrincipal() throws Exception {
        // PR4 / §5.10 claim-check order: an expired token must be reported as expired, not as a
        // principal mismatch, even when the principal would also not match (RFC 7519 recommends
        // checking expiry early).
        MockTokenUtils.MockJwkMgr mockJwkMgr = new MockTokenUtils.MockJwkMgr();
        JWKSet jwkSet = mockJwkMgr.getJwkSet("jwks.json");
        String expiredToken = mockTokenUtils.generateTestOIDCToken(-60L);

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(expiredToken, "definitely-not-the-principal", jwkSet,
                        "preferred_username", requiredIssuer, requiredAudience));
        Assertions.assertTrue(e.getMessage().contains("expired"),
                "expiry should be reported before principal mismatch, got: " + e.getMessage());
    }

    @Test
    public void testLdapAndOAuth2SupportsCredentialKind() {
        // §5.2 kind-matrix completeness: LDAP accepts password shapes (not Bearer); OAuth2
        // accepts Bearer (not plain password). JWT / PlainPassword are covered by
        // testSupportsCredentialKindPerProvider.
        LDAPAuthProvider ldap = new LDAPAuthProvider("localhost", 389, false,
                null, null, null, null, null, null, null, null);
        // Same reason as JWT: the MySQL clear-password frame arrives stripped, as PLAIN_PASSWORD.
        Assertions.assertFalse(ldap.supportsCredentialKind(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE));
        Assertions.assertTrue(ldap.supportsCredentialKind(AuthCredentialKind.PLAIN_PASSWORD));
        Assertions.assertFalse(ldap.supportsCredentialKind(AuthCredentialKind.BEARER_TOKEN));

        OAuth2Context ctx = new OAuth2Context("http://as", "http://ts", "http://rd", "cid", "secret",
                "jwks.json", "sub", requiredIssuer, requiredAudience, 10L);
        OAuth2AuthenticationProvider oauth2 = new OAuth2AuthenticationProvider(ctx);
        // Both MySQL kinds: a client that speaks the OAuth2 plugin waits for the callback, an
        // ordinary client is let through to the first statement, which carries the URL - the same
        // experience as a native OAuth2 user. What bounds the connection meanwhile is covered in
        // ConnectProcessorTest (every command refused) and ConnectContextTest (closed on timeout).
        Assertions.assertTrue(
                oauth2.supportsCredentialKind(AuthCredentialKind.MYSQL_OAUTH2_CLIENT));
        Assertions.assertTrue(oauth2.supportsCredentialKind(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE));
        Assertions.assertTrue(oauth2.supportsCredentialKind(AuthCredentialKind.BEARER_TOKEN));
        Assertions.assertFalse(oauth2.supportsCredentialKind(AuthCredentialKind.PLAIN_PASSWORD));
    }

    /**
     * A native user whose row names the OAuth2 plugin, connecting with an ordinary MySQL client:
     * the handshake is answered as if it had succeeded and ConnectProcessor turns the first
     * statement into the authorization URL. Pre-existing MySQL behaviour, and the reason this
     * provider still handles MYSQL_NATIVE_SCRAMBLE even though it does not offer that kind to the
     * security integration chain - the native path picks providers from the user row and never
     * consults supportsCredentialKind.
     */
    @Test
    public void testOAuth2LetsAStockMysqlClientThroughToTheFirstStatement() throws Exception {
        OAuth2Context ctx = new OAuth2Context("http://as", "http://ts", "http://rd", "cid", "secret",
                "jwks.json", "sub", requiredIssuer, requiredAudience, 10L);
        OAuth2AuthenticationProvider oauth2 = new OAuth2AuthenticationProvider(ctx);

        UserIdentity declared = UserIdentity.createEphemeralUserIdent("oauth2_user", "%");
        UserIdentity authenticated = oauth2.authenticate(new AccessControlContext(), declared,
                AuthCredentials.ofNativeScramble("oauth2_user", new byte[] {1, 2, 3}, "127.0.0.1"));
        Assertions.assertEquals(declared, authenticated,
                "a stock client must not be made to wait for a token it cannot fetch");
    }

    /**
     * The token names harbor. A native OAuth2 user is looked up by the name the client declared, so
     * a token whose principal is somebody else must be refused for that name: otherwise any valid
     * token from the IdP would open a session as whichever OAuth2 user the client typed. With no
     * declared user (plain Bearer) the principal is the user, as before.
     */
    @Test
    public void testOAuth2BindsABearerTokenToTheDeclaredUser() throws Exception {
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        OAuth2Context ctx = new OAuth2Context("http://as", "http://ts", "http://rd", "cid", "secret",
                "jwks.json", "preferred_username", requiredIssuer, requiredAudience, 10L);
        OAuth2AuthenticationProvider oauth2 = new OAuth2AuthenticationProvider(ctx);
        String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);

        AccessControlContext someoneElse = new AccessControlContext();
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                oauth2.authenticate(someoneElse, UserIdentity.createEphemeralUserIdent("bob", "%"),
                        AuthCredentials.ofBearer(token, AuthChannel.MYSQL, "127.0.0.1")));
        Assertions.assertEquals(AuthFailureReason.CLAIM_MISMATCH, e.getReason(), e.getMessage());
        Assertions.assertNull(someoneElse.getAuthToken(), "a refused token must not be deposited on the connection");

        UserIdentity harbor = UserIdentity.createEphemeralUserIdent("harbor", "%");
        AccessControlContext own = new AccessControlContext();
        Assertions.assertSame(harbor, oauth2.authenticate(own, harbor,
                AuthCredentials.ofBearer(token, AuthChannel.MYSQL, "127.0.0.1")));
        Assertions.assertEquals(token, own.getAuthToken());

        Assertions.assertEquals("harbor", oauth2.authenticate(new AccessControlContext(), null,
                AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1")).getUser());
    }

    /**
     * A token without a kid is legitimate (an IdP with a single key may omit it), but it must be
     * tried against every key of its algorithm, not only the first one the key set happens to list.
     */
    @Test
    public void testATokenWithoutAKidIsTriedAgainstEveryKey() throws Exception {
        RSAKey first = MockTokenUtils.newRsaKey("first");
        RSAKey signer = MockTokenUtils.newRsaKey("second");
        JWKSource<SecurityContext> source = new ImmutableJWKSet<>(new JWKSet(List.of(first.toPublicJWK(), signer.toPublicJWK())));

        JWTClaimsSet claims = new JWTClaimsSet.Builder().issuer(requiredIssuer[0]).audience(requiredAudience[0])
                .claim("preferred_username", "harbor").expirationTime(new Date(System.currentTimeMillis() + 60_000)).build();
        SignedJWT noKid = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).build(), claims);
        noKid.sign(new RSASSASigner(signer));

        Assertions.assertDoesNotThrow(() -> OpenIdConnectVerifier.verify(noKid.serialize(), "harbor", source,
                "preferred_username", requiredIssuer, requiredAudience));

        // ... and a token nobody in the set signed is still refused, after every key was tried.
        SignedJWT stranger = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).build(), claims);
        stranger.sign(new RSASSASigner(MockTokenUtils.newRsaKey("elsewhere")));
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(stranger.serialize(), "harbor", source,
                        "preferred_username", requiredIssuer, requiredAudience));
        Assertions.assertEquals(AuthFailureReason.SIGNATURE_FAIL, e.getReason());
    }

    @Test
    public void testLdapRejectsBearer() {
        // An LDAP user must authenticate with a password, never a Bearer token - the same rule the
        // JWT provider applies in reverse.
        LDAPAuthProvider ldap = new LDAPAuthProvider("localhost", 389, false,
                null, null, null, null, null, null, null, null);
        AuthCredentials bearer = AuthCredentials.ofBearer("a.b.c", AuthChannel.HTTP, "127.0.0.1");
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                ldap.authenticate(new ConnectContext().getAccessControlContext(),
                        UserIdentity.createEphemeralUserIdent("alice", "127.0.0.1"), bearer));
        // Assert on the kind name rather than a phrase, so rewording the message does not break
        // this.
        Assertions.assertTrue(e.getMessage().contains(AuthCredentialKind.BEARER_TOKEN.name()),
                "message should name the rejected kind, was: " + e.getMessage());
    }

    @Test
    public void testOAuth2RejectsBasic() {
        // OAuth2 only accepts a Bearer token; a plain password (HTTP Basic) must be rejected.
        OAuth2Context ctx = new OAuth2Context("http://as", "http://ts", "http://rd", "cid", "secret",
                "jwks.json", "sub", requiredIssuer, requiredAudience, 10L);
        OAuth2AuthenticationProvider oauth2 = new OAuth2AuthenticationProvider(ctx);
        AuthCredentials basic = AuthCredentials.ofPassword("alice", "pwd", AuthChannel.HTTP, "127.0.0.1");
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                oauth2.authenticate(new ConnectContext().getAccessControlContext(),
                        UserIdentity.createEphemeralUserIdent("alice", "127.0.0.1"), basic));
        Assertions.assertTrue(e.getMessage().contains(AuthCredentialKind.PLAIN_PASSWORD.name()),
                "message should name the rejected kind, was: " + e.getMessage());
    }

    @Test
    public void testHmacAlgorithmConfusionIsRejected() throws Exception {
        // Threat T7 / O6: an HS256 token signed with the RSA public key as the HMAC secret (the
        // classic RS256->HS256 algorithm confusion) must be rejected -- HS256 is not in the allowed
        // RSA/EC algorithm set.
        JWKSet jwkSet = new MockTokenUtils.MockJwkMgr().getJwkSet("jwks.json");
        String confusionToken = mockTokenUtils.generateHmacConfusionToken(ISSUER);
        Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(confusionToken, "harbor", jwkSet, "preferred_username",
                        requiredIssuer, requiredAudience));
    }

    /**
     * TC-L1-42-13 (and 42-12 for Kerberos): the whole supportsCredentialKind table from design §5.2,
     * one cell per provider x kind. Adding a provider or a kind without filling in its row here goes
     * red, which is the point.
     */
    @Test
    public void testSupportsCredentialKindMatrix() {
        Map<String, AuthenticationProvider> providers = new LinkedHashMap<>();
        providers.put("plain_password",
                new PlainPasswordAuthenticationProvider(com.starrocks.mysql.MysqlPassword.EMPTY_PASSWORD));
        providers.put("ldap", new LDAPAuthProvider("localhost", 389, false,
                null, null, null, null, null, null, null, null));
        providers.put("jwt", new JWTAuthenticationProvider("jwks.json", "sub", requiredIssuer, requiredAudience));
        providers.put("oauth2", new OAuth2AuthenticationProvider(new OAuth2Context("http://as", "http://ts",
                "http://rd", "cid", "secret", "jwks.json", "sub", requiredIssuer, requiredAudience, 10L)));

        Map<String, Set<AuthCredentialKind>> expected = new LinkedHashMap<>();
        expected.put("plain_password", EnumSet.of(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE,
                AuthCredentialKind.PLAIN_PASSWORD));
        expected.put("ldap", EnumSet.of(AuthCredentialKind.PLAIN_PASSWORD));
        expected.put("jwt", EnumSet.of(AuthCredentialKind.BEARER_TOKEN));
        expected.put("oauth2", EnumSet.of(AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE,
                AuthCredentialKind.MYSQL_OAUTH2_CLIENT, AuthCredentialKind.BEARER_TOKEN));

        Assertions.assertEquals(expected.keySet(), providers.keySet(), "every provider has a row");
        for (Map.Entry<String, AuthenticationProvider> p : providers.entrySet()) {
            for (AuthCredentialKind kind : AuthCredentialKind.values()) {
                Assertions.assertEquals(expected.get(p.getKey()).contains(kind),
                        p.getValue().supportsCredentialKind(kind), p.getKey() + " x " + kind);
            }
        }
    }

    @Test
    public void testJwtProviderRefusesAnythingButAToken() {
        // TC-L1-42-09: the MySQL OpenID Connect frame is unwrapped before it gets here, so the
        // provider has no MySQL branch left - a scramble is simply the wrong kind.
        JWTAuthenticationProvider jwt =
                new JWTAuthenticationProvider("jwks.json", "sub", requiredIssuer, requiredAudience);
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                jwt.authenticate(null, UserIdentity.createEphemeralUserIdent("harbor", "127.0.0.1"),
                        AuthCredentials.ofNativeScramble("harbor", new byte[20], "127.0.0.1")));
        Assertions.assertTrue(e.getMessage().contains("must authenticate via Bearer token"), e.getMessage());
    }

    @Test
    public void testJwkSetAndJwkSourceOverloadsAgree() throws Exception {
        // TC-L1-43-11: the JWKSet overload is the OAuth2 callback's only path and the JWKSource
        // overload is everyone else's. They share one core; this is what notices if they drift.
        JWKSet jwkSet = new MockTokenUtils.MockJwkMgr().getJwkSet("jwks.json");
        JWKSource<SecurityContext> source = new ImmutableJWKSet<>(jwkSet);

        String good = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        Assertions.assertDoesNotThrow(() -> OpenIdConnectVerifier.verify(good, "harbor", jwkSet,
                "preferred_username", requiredIssuer, requiredAudience));
        Assertions.assertDoesNotThrow(() -> OpenIdConnectVerifier.verify(good, "harbor", source,
                "preferred_username", requiredIssuer, requiredAudience));

        String expired = mockTokenUtils.generateTestOIDCToken(-60_000);
        AuthenticationException viaSet = Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(expired, "harbor", jwkSet, "preferred_username",
                        requiredIssuer, requiredAudience));
        AuthenticationException viaSource = Assertions.assertThrows(AuthenticationException.class, () ->
                OpenIdConnectVerifier.verify(expired, "harbor", source, "preferred_username",
                        requiredIssuer, requiredAudience));
        Assertions.assertEquals(AuthFailureReason.EXPIRED, viaSet.getReason());
        Assertions.assertEquals(viaSet.getReason(), viaSource.getReason());
        Assertions.assertEquals(viaSet.getMessage(), viaSource.getMessage());
    }
}
