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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;

public class JWTAuthenticationProviderTest {

    private RSAKey rsaKey;

    @BeforeEach
    public void setUp() throws Exception {
        rsaKey = new RSAKeyGenerator(2048).keyID("test-kid").generate();
        JWKSet jwkSet = new JWKSet(rsaKey.toPublicJWK());

        new MockUp<JwkMgr>() {
            @Mock
            public JWKSet getJwkSet(String jwksUrl) {
                return jwkSet;
            }
        };
    }

    private String buildToken(String subject, String issuer, String audience, Date expiration) throws Exception {
        JWTClaimsSet.Builder claimsBuilder = new JWTClaimsSet.Builder()
                .subject(subject)
                .issuer(issuer);
        if (audience != null) {
            claimsBuilder.audience(audience);
        }
        if (expiration != null) {
            claimsBuilder.expirationTime(expiration);
        }
        SignedJWT signedJWT = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).keyID("test-kid").build(),
                claimsBuilder.build());
        signedJWT.sign(new RSASSASigner(rsaKey));
        return signedJWT.serialize();
    }

    private JWTAuthenticationProvider defaultProvider() {
        return new JWTAuthenticationProvider(
                "jwks.json", "sub", new String[] {"https://issuer.example.com"}, new String[] {"starrocks"});
    }

    @Test
    public void testAuthenticateRawToken_success() throws Exception {
        String token = buildToken("alice", "https://issuer.example.com", "starrocks",
                new Date(System.currentTimeMillis() + 60_000));

        AccessControlContext authContext = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");

        Assertions.assertDoesNotThrow(() -> defaultProvider().authenticateRawToken(authContext, user, token));
        Assertions.assertEquals(token, authContext.getAuthToken());
    }

    @Test
    public void testAuthenticateRawToken_expired() throws Exception {
        String token = buildToken("alice", "https://issuer.example.com", "starrocks",
                new Date(System.currentTimeMillis() - 60_000));

        AccessControlContext authContext = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");

        Assertions.assertThrows(AuthenticationException.class,
                () -> defaultProvider().authenticateRawToken(authContext, user, token));
    }

    @Test
    public void testAuthenticateRawToken_wrongIssuer() throws Exception {
        String token = buildToken("alice", "https://untrusted.example.com", "starrocks",
                new Date(System.currentTimeMillis() + 60_000));

        AccessControlContext authContext = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");

        Assertions.assertThrows(AuthenticationException.class,
                () -> defaultProvider().authenticateRawToken(authContext, user, token));
    }

    @Test
    public void testAuthenticateRawToken_wrongAudience() throws Exception {
        String token = buildToken("alice", "https://issuer.example.com", "other-service",
                new Date(System.currentTimeMillis() + 60_000));

        AccessControlContext authContext = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");

        Assertions.assertThrows(AuthenticationException.class,
                () -> defaultProvider().authenticateRawToken(authContext, user, token));
    }

    @Test
    public void testAuthenticateRawToken_principalMismatch() throws Exception {
        String token = buildToken("bob", "https://issuer.example.com", "starrocks",
                new Date(System.currentTimeMillis() + 60_000));

        AccessControlContext authContext = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");

        Assertions.assertThrows(AuthenticationException.class,
                () -> defaultProvider().authenticateRawToken(authContext, user, token));
    }

    @Test
    public void testAuthenticateRawToken_malformedToken() {
        AccessControlContext authContext = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("alice", "%");

        Assertions.assertThrows(AuthenticationException.class,
                () -> defaultProvider().authenticateRawToken(authContext, user, "not-a-jwt"));
    }

    @Test
    public void testExtractIssuerWithoutVerification() throws Exception {
        String token = buildToken("alice", "https://issuer.example.com", "starrocks",
                new Date(System.currentTimeMillis() + 60_000));

        String issuer = JWTAuthenticationProvider.extractIssuerWithoutVerification(token);
        Assertions.assertEquals("https://issuer.example.com", issuer);
    }

    @Test
    public void testExtractIssuerWithoutVerification_malformedToken() {
        Assertions.assertThrows(AuthenticationException.class,
                () -> JWTAuthenticationProvider.extractIssuerWithoutVerification("not-a-jwt"));
    }

    @Test
    public void testIsIssuerAccepted() {
        JWTAuthenticationProvider provider = defaultProvider();
        Assertions.assertTrue(provider.isIssuerAccepted("https://issuer.example.com"));
        Assertions.assertFalse(provider.isIssuerAccepted("https://untrusted.example.com"));
        Assertions.assertFalse(provider.isIssuerAccepted(null));

        JWTAuthenticationProvider noIssuerProvider =
                new JWTAuthenticationProvider("jwks.json", "sub", new String[0], new String[] {"starrocks"});
        Assertions.assertTrue(noIssuerProvider.isIssuerAccepted("https://any.example.com"));

        JWTAuthenticationProvider nullIssuerProvider = new JWTAuthenticationProvider("jwks.json", "sub", null, null);
        Assertions.assertTrue(nullIssuerProvider.isIssuerAccepted(null));
    }

    @Test
    public void testExtractExpirationTimeWithoutVerification() throws Exception {
        // exp is stored in seconds, so keep the expected value second-aligned
        Date expiration = new Date((System.currentTimeMillis() / 1000 + 60) * 1000);
        String token = buildToken("alice", "https://issuer.example.com", "starrocks", expiration);
        Assertions.assertEquals(expiration, JWTAuthenticationProvider.extractExpirationTimeWithoutVerification(token));

        String noExpToken = buildToken("alice", "https://issuer.example.com", "starrocks", null);
        Assertions.assertNull(JWTAuthenticationProvider.extractExpirationTimeWithoutVerification(noExpToken));

        Assertions.assertThrows(AuthenticationException.class,
                () -> JWTAuthenticationProvider.extractExpirationTimeWithoutVerification("not-a-jwt"));
    }
}
