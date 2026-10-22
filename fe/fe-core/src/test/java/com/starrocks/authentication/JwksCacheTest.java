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

import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.starrocks.common.Config;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * JWKS caching tests (§5.8). Unlike the rest of the auth unit tests, these point a real {@link
 * JwkMgr} at an in-process {@link MockJwksServer} over {@code http://}, so the production {@code
 * JWKSourceBuilder} remote-fetch + cache path is actually exercised (the existing {@code
 * MockTokenUtils.MockJwkMgr} short-circuits to a local file and never caches).
 *
 * Covers TC-L2-44-03 (cache hit), TC-L2-44-04 (kid-miss refresh) and TC-L2-44-07 (http scheme is
 * fetched remotely; per F5 http is allowed, not rejected). Outage-tolerance / rate-limit
 * (TC-L2-44-05/06) are timing-sensitive and tracked separately in the test design doc.
 */
public class JwksCacheTest {

    private static final String ISSUER = "http://localhost:38080/realms/master";
    private final String[] requiredIssuer = {ISSUER};
    private final String[] requiredAudience = {"12345"};
    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();

    private int savedTtl;
    private int savedRefreshAhead;
    private int savedOutageTtl;
    private int savedRateLimit;

    @BeforeEach
    public void setUp() {
        savedTtl = Config.jwks_cache_ttl_seconds;
        savedRefreshAhead = Config.jwks_cache_refresh_ahead_seconds;
        savedOutageTtl = Config.jwks_cache_outage_ttl_seconds;
        savedRateLimit = Config.jwks_cache_refresh_rate_limit_seconds;
    }

    @AfterEach
    public void tearDown() {
        Config.jwks_cache_ttl_seconds = savedTtl;
        Config.jwks_cache_refresh_ahead_seconds = savedRefreshAhead;
        Config.jwks_cache_outage_ttl_seconds = savedOutageTtl;
        Config.jwks_cache_refresh_rate_limit_seconds = savedRateLimit;
    }

    private String publicJwksJson() throws Exception {
        // The public JWKS matching the bundled signer (jwks-private-key.pem / signer-jwks.json).
        return new MockTokenUtils.MockJwkMgr().getJwkSet("jwks.json").toString();
    }

    @Test
    public void testRemoteJwksCachedAcrossAuthentications() throws Exception {
        // TC-L2-44-03 / TC-L2-44-07: an http:// jwks_url is fetched once and reused for repeated
        // authentications (HTTP Bearer authenticates on every request; an uncached fetch would
        // hammer the IdP).
        try (MockJwksServer server = new MockJwksServer(publicJwksJson())) {
            JwkMgr jwkMgr = new JwkMgr();
            JWKSource<SecurityContext> source = jwkMgr.getJwkSource(server.url());
            String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);

            for (int i = 0; i < 5; i++) {
                OpenIdConnectVerifier.verify(token, "harbor", source, "preferred_username",
                        requiredIssuer, requiredAudience);
            }

            Assertions.assertEquals(1, server.fetchCount(),
                    "5 authentications must only fetch JWKS once (cache hit)");
        }
    }

    @Test
    public void testKidMissTriggersRefresh() throws Exception {
        // TC-L2-44-04: a token whose kid is not in the cached set forces a refresh fetch (key
        // rotation), and the rotated key then verifies. Disable the refresh rate limiter so the
        // miss-driven refresh is not throttled within the test window.
        Config.jwks_cache_refresh_rate_limit_seconds = 0;
        try (MockJwksServer server = new MockJwksServer(publicJwksJson())) {
            JwkMgr jwkMgr = new JwkMgr();
            JWKSource<SecurityContext> source = jwkMgr.getJwkSource(server.url());

            // First key (bundled) -> first fetch.
            String tokenA = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
            OpenIdConnectVerifier.verify(tokenA, "harbor", source, "preferred_username",
                    requiredIssuer, requiredAudience);
            Assertions.assertEquals(1, server.fetchCount());

            // Rotate to a brand-new key with a new kid; present a token signed by it.
            RSAKey keyB = MockTokenUtils.newRsaKey("kid-rotated-B");
            server.setBody(mockTokenUtils.publicJwksJson(keyB));
            String tokenB = mockTokenUtils.signRs256(keyB, ISSUER, 3600 * 1000);

            OpenIdConnectVerifier.verify(tokenB, "harbor", source, "preferred_username",
                    requiredIssuer, requiredAudience);
            Assertions.assertTrue(server.fetchCount() >= 2,
                    "an unknown kid must trigger a JWKS refresh fetch");
        }
    }

    private static long fetches(String result) {
        return MetricRepo.COUNTER_JWKS_REFRESH.getMetric(result).getValue();
    }

    @Test
    public void testJwksFetchMetrics() throws Exception {
        // TC-L2-411-05: every JWKS fetch is counted by result and timed, whatever triggered it.
        Config.jwks_cache_refresh_rate_limit_seconds = 0;
        boolean savedInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try (MockJwksServer server = new MockJwksServer(publicJwksJson())) {
            long success0 = fetches("success");
            long failure0 = fetches("failure");
            long samples0 = MetricRepo.HISTO_JWKS_FETCH_LATENCY_MS.getCount();

            JwkMgr jwkMgr = new JwkMgr();
            JWKSource<SecurityContext> source = jwkMgr.getJwkSource(server.url());
            OpenIdConnectVerifier.verify(mockTokenUtils.generateTestOIDCToken(3600 * 1000), "harbor", source,
                    "preferred_username", requiredIssuer, requiredAudience);
            Assertions.assertEquals(success0 + 1, fetches("success"), "the initial load is one successful fetch");
            Assertions.assertEquals(failure0, fetches("failure"));

            // The IdP goes down. A token with an unknown kid forces a refresh, which fails; the
            // outage-tolerant layer answers from the stale keys, so the outcome for the token is
            // still a rejection - but the fetch has to be counted as the failure it was.
            server.setMode(MockJwksServer.Mode.FAIL_503);
            RSAKey keyB = MockTokenUtils.newRsaKey("kid-unknown-during-outage");
            String tokenB = mockTokenUtils.signRs256(keyB, ISSUER, 3600 * 1000);
            Assertions.assertThrows(AuthenticationException.class, () ->
                    OpenIdConnectVerifier.verify(tokenB, "harbor", source, "preferred_username",
                            requiredIssuer, requiredAudience));
            Assertions.assertTrue(fetches("failure") > failure0, "a 503 from the IdP is a failed fetch");
            Assertions.assertEquals(success0 + 1, fetches("success"), "a fetch answered from stale keys is not a success");

            // One latency sample per HTTP request the server saw - retries included, nothing else.
            Assertions.assertEquals(samples0 + server.requestCount(),
                    MetricRepo.HISTO_JWKS_FETCH_LATENCY_MS.getCount());
        } finally {
            MetricRepo.hasInit = savedInit;
        }
    }

    @Test
    public void testAnUnreachableIdpIsATransientFailureNotARejectedCredential() throws Exception {
        // No key set was ever fetched and the IdP answers 503: the token holder did nothing wrong,
        // so the failure must be marked transient - the handler then neither remembers the token
        // as rejected nor refuses it again once the IdP is back.
        Config.jwks_cache_refresh_rate_limit_seconds = 0;
        try (MockJwksServer server = new MockJwksServer(publicJwksJson())) {
            server.setMode(MockJwksServer.Mode.FAIL_503);
            JwkMgr jwkMgr = new JwkMgr();
            JWKSource<SecurityContext> source = jwkMgr.getJwkSource(server.url());
            String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);

            AuthenticationException e = Assertions.assertThrows(AuthenticationException.class, () ->
                    OpenIdConnectVerifier.verify(token, "harbor", source, "preferred_username",
                            requiredIssuer, requiredAudience));
            Assertions.assertTrue(e.isTransientFailure(), e.getMessage());
            Assertions.assertEquals(AuthFailureReason.INTERNAL, e.getReason());

            server.setMode(MockJwksServer.Mode.SERVE);
            Assertions.assertDoesNotThrow(() -> OpenIdConnectVerifier.verify(token, "harbor", source,
                    "preferred_username", requiredIssuer, requiredAudience), "the same token verifies once the IdP is back");
        }
    }

    @Test
    @org.junit.jupiter.api.Tag("timing")
    public void testOutageToleranceKeepsVerifyingWithStaleKeys() throws Exception {
        // TC-L2-44-05: the cache TTL has passed and the IdP is down. A token signed by a key that was
        // in the last good fetch must still verify: the outage window serves the stale set. Timing
        // test: Nimbus insists on TTL >= refresh-ahead + its 15s refresh timeout, so the shortest
        // TTL that builds is 16s and the test waits it out.
        Config.jwks_cache_ttl_seconds = 16;
        Config.jwks_cache_refresh_ahead_seconds = 0;
        Config.jwks_cache_refresh_rate_limit_seconds = 0;
        try (MockJwksServer server = new MockJwksServer(publicJwksJson())) {
            JwkMgr jwkMgr = new JwkMgr();
            JWKSource<SecurityContext> source = jwkMgr.getJwkSource(server.url());
            String token = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
            OpenIdConnectVerifier.verify(token, "harbor", source, "preferred_username", requiredIssuer, requiredAudience);
            Assertions.assertEquals(1, server.fetchCount());

            Thread.sleep(16_500);                     // past the TTL
            server.setMode(MockJwksServer.Mode.FAIL_503);
            Assertions.assertDoesNotThrow(() -> OpenIdConnectVerifier.verify(token, "harbor", source,
                    "preferred_username", requiredIssuer, requiredAudience),
                    "an IdP outage must not log out every Bearer user while the outage window lasts");
        }
    }

    @Test
    public void testRateLimitStopsUnknownKidsFromHammeringTheIdp() throws Exception {
        // TC-L2-44-06: a flood of tokens with unknown kids (an attacker, or a misconfigured client) must
        // not turn into a fetch per token. The first miss refreshes; within the rate-limit window the
        // next miss is answered from what is cached.
        Config.jwks_cache_refresh_rate_limit_seconds = 30;
        try (MockJwksServer server = new MockJwksServer(publicJwksJson())) {
            JwkMgr jwkMgr = new JwkMgr();
            JWKSource<SecurityContext> source = jwkMgr.getJwkSource(server.url());
            OpenIdConnectVerifier.verify(mockTokenUtils.generateTestOIDCToken(3600 * 1000), "harbor", source,
                    "preferred_username", requiredIssuer, requiredAudience);
            int after = server.fetchCount();

            for (int i = 0; i < 3; i++) {
                String unknown = mockTokenUtils.generateTokenSignedByUnknownKey(ISSUER, 3600 * 1000);
                Assertions.assertThrows(AuthenticationException.class, () -> OpenIdConnectVerifier.verify(unknown,
                        "harbor", source, "preferred_username", requiredIssuer, requiredAudience));
            }
            Assertions.assertTrue(server.fetchCount() <= after + 1,
                    "three unknown kids inside the rate-limit window must cause at most one refresh, saw "
                            + (server.fetchCount() - after));
        }
    }

    @Test
    public void testEditingTheLocalKeyFileTakesEffectWithoutRestart() throws Exception {
        // TC-L1-44-08 / AC-18: a jwks_url naming a conf/ file is cached by its mtime. Replace the file
        // and the next authentication reads the new key; the old key no longer verifies.
        File dir = java.nio.file.Files.createTempDirectory("jwks-local").toFile();
        File keyFile = new File(dir, "local-jwks.json");
        JwkMgr jwkMgr = new JwkMgr() {
            @Override
            protected File localKeyFile(String jwksUrl) {
                return new File(dir, jwksUrl);
            }
        };
        RSAKey keyA = MockTokenUtils.newRsaKey("kid-A");
        RSAKey keyB = MockTokenUtils.newRsaKey("kid-B");
        java.nio.file.Files.writeString(keyFile.toPath(), mockTokenUtils.publicJwksJson(keyA));
        String tokenA = mockTokenUtils.signRs256(keyA, ISSUER, 3600 * 1000);
        String tokenB = mockTokenUtils.signRs256(keyB, ISSUER, 3600 * 1000);

        OpenIdConnectVerifier.verify(tokenA, "harbor", jwkMgr.getJwkSource("local-jwks.json"),
                "preferred_username", requiredIssuer, requiredAudience);
        Assertions.assertThrows(AuthenticationException.class, () -> OpenIdConnectVerifier.verify(tokenB, "harbor",
                jwkMgr.getJwkSource("local-jwks.json"), "preferred_username", requiredIssuer, requiredAudience));

        // The operator replaces the key. A filesystem may only track mtime to the second; move it by hand.
        java.nio.file.Files.writeString(keyFile.toPath(), mockTokenUtils.publicJwksJson(keyB));
        Assertions.assertTrue(keyFile.setLastModified(keyFile.lastModified() + 2000));

        OpenIdConnectVerifier.verify(tokenB, "harbor", jwkMgr.getJwkSource("local-jwks.json"),
                "preferred_username", requiredIssuer, requiredAudience);
        Assertions.assertThrows(AuthenticationException.class, () -> OpenIdConnectVerifier.verify(tokenA, "harbor",
                jwkMgr.getJwkSource("local-jwks.json"), "preferred_username", requiredIssuer, requiredAudience),
                "the replaced key must stop working once the file changed");
    }
}
