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

import brave.http.HttpTracing;
import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.authentication.JwkMgr;
import com.starrocks.authentication.MockTokenUtils;
import com.starrocks.authentication.OpenIdConnectVerifier;
import com.starrocks.server.GlobalStateMgr;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class JWTTokenProviderTest {

    private static final Logger LOG = LoggerFactory.getLogger(JWTTokenProviderTest.class);
    private MockWebServer mockWebServer;
    private JWTTokenProvider tokenProvider;
    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();

    @BeforeAll
    public static void beforeAll() throws Exception {
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
    }

    @AfterAll
    public static void afterAll() {
        GlobalStateMgr.getCurrentState().setJwkMgr(null);
    }

    @BeforeEach
    public void setUp() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        tokenProvider = JWTTokenProvider.newBuilder()
                .url(mockWebServer.url("/").toString())
                .clientID("test-client")
                .clientSecret("test-secret")
                .audience("test-audience")
                .scope("test-scope")
                .jwksUrl("jwks.json")
                .principalField("sub")
                .issuer("test-issuer")
                .httpTracing(HttpTracing.create(brave.Tracing.newBuilder().build()))
                .connectionParams(10, 10)
                .build();
    }

    @Disabled("This is an integration test that requires real credentials and network access.")
    @Test
    public void testRealTokenGeneration() throws Exception {
        JwkMgr originalJwkMgr = GlobalStateMgr.getCurrentState().getJwkMgr();
        GlobalStateMgr.getCurrentState().setJwkMgr(new JwkMgr());
        try {
            File propertiesFile = new File(ClassLoader.getSystemClassLoader().getResource("bot.properties").getFile());
            assumeTrue(propertiesFile.exists(), "Skipping test: bot.properties file not found");

            Properties properties = new Properties();
            properties.load(new FileReader(propertiesFile));
            JWTTokenProvider realTokenProvider = JWTTokenProvider.newBuilder()
                    .url(properties.getProperty("iam.token.issuer.url"))
                    .clientID(properties.getProperty("service.client.ias.client.id"))
                    .clientSecret(properties.getProperty("service.client.ias.client.secret"))
                    .scope(properties.getProperty("service.client.scope"))
                    .audience(properties.getProperty("service.client.audience"))
                    .jwksUrl(properties.getProperty("iam.token.jwks.url"))
                    .principalField("sub")
                    .issuer(properties.getProperty("iam.token.issuer.url"))
                    .httpTracing(HttpTracing.create(brave.Tracing.newBuilder().build()))
                    .connectionParams(20, 20)
                    .build();

            String token = realTokenProvider.getToken();
            assertNotNull(token);
            LOG.info("Successfully generated real token: {}", token);

            // Validate the token
            JWKSet jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(
                    properties.getProperty("iam.token.jwks.url"));
            OpenIdConnectVerifier.verify(token, properties.getProperty("service.client.test.user", "2701007084"),
                    jwkSet, "sub",
                    new String[] {properties.getProperty("iam.token.issuer.url")},
                    new String[] {properties.getProperty("service.client.audience")});
        } finally {
            GlobalStateMgr.getCurrentState().setJwkMgr(originalJwkMgr);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockWebServer.shutdown();
    }

    @Test
    public void testClientCredentials() throws Exception {
        String expectedToken = mockTokenUtils.generateTestOIDCToken(3600);
        String tokenJson = "{\"id_token\":\"" + expectedToken + "\"}";
        mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));
        String token = tokenProvider.getToken();
        assertNotNull(token);
        assertEquals(expectedToken, token);
        assertEquals(1, mockWebServer.getRequestCount());

        // Call again to verify caching
        String cachedToken = tokenProvider.getToken();
        assertNotNull(cachedToken);
        assertEquals(expectedToken, cachedToken);
        assertEquals(1, mockWebServer.getRequestCount());
    }

    @Test
    public void testTokenRefresh() throws Exception {
        // 1. get a token with 2 second expiration
        String expectedToken = mockTokenUtils.generateTestOIDCToken(2000); // 2 seconds
        String tokenJson = "{\"id_token\":\"" + expectedToken + "\"}";
        mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));
        String token = tokenProvider.getToken();
        assertNotNull(token);
        assertEquals(expectedToken, token);
        assertEquals(1, mockWebServer.getRequestCount());

        // 2. sleep >2 seconds to make the token expire
        Thread.sleep(2100);

        // 3. get token again, should trigger refresh
        String newExpectedToken = mockTokenUtils.generateTestOIDCToken(3600);
        String newTokenJson = "{\"id_token\":\"" + newExpectedToken + "\"}";
        mockWebServer.enqueue(new MockResponse().setBody(newTokenJson).setResponseCode(200));
        String refreshedToken = tokenProvider.getToken();
        assertNotNull(refreshedToken);
        assertEquals(newExpectedToken, refreshedToken);
        assertEquals(2, mockWebServer.getRequestCount());
    }

    @Test
    public void testTokenProactiveRefresh() throws Exception {
        // 1. get a token with 5 second expiration
        String expectedToken = mockTokenUtils.generateTestOIDCToken(5000); // 5 seconds
        String tokenJson = "{\"id_token\":\"" + expectedToken + "\"}";
        mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));
        String token = tokenProvider.getToken();
        assertNotNull(token);
        assertEquals(expectedToken, token);
        assertEquals(1, mockWebServer.getRequestCount());

        // 2. sleep 4.1 seconds — token is not expired, but past 80% of lifetime
        Thread.sleep(4100);

        // 3. get token again — should trigger proactive refresh
        String newExpectedToken = mockTokenUtils.generateTestOIDCToken(3600);
        String newTokenJson = "{\"id_token\":\"" + newExpectedToken + "\"}";
        mockWebServer.enqueue(new MockResponse().setBody(newTokenJson).setResponseCode(200));
        String refreshedToken = tokenProvider.getToken();
        assertNotNull(refreshedToken);
        assertEquals(newExpectedToken, refreshedToken);
        assertEquals(2, mockWebServer.getRequestCount());
    }

    @Test
    public void testExpiresInFallbackWhenIdTokenIsOpaqueToken() throws Exception {
        // Opaque (non-JWT) id_token: expiry must come from expires_in
        String opaqueToken = "opaque-id-token-value";
        // expires_in = 2 seconds → token should be refreshed after ~2s
        String tokenJson = "{\"id_token\":\"" + opaqueToken + "\",\"expires_in\":2}";
        mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));
        String token = tokenProvider.getToken();
        assertNotNull(token);
        assertEquals(opaqueToken, token);
        assertEquals(1, mockWebServer.getRequestCount());

        // Still within 2s window — should use cache
        assertEquals(opaqueToken, tokenProvider.getToken());
        assertEquals(1, mockWebServer.getRequestCount());

        // After expiry, should refresh
        Thread.sleep(2100);
        String newOpaqueToken = "opaque-id-token-refreshed";
        String newTokenJson = "{\"id_token\":\"" + newOpaqueToken + "\",\"expires_in\":3600}";
        mockWebServer.enqueue(new MockResponse().setBody(newTokenJson).setResponseCode(200));
        String refreshedToken = tokenProvider.getToken();
        assertEquals(newOpaqueToken, refreshedToken);
        assertEquals(2, mockWebServer.getRequestCount());
    }

    @Test
    public void testBuilderValidation() {
        // url is mandatory
        Exception e = assertThrows(IllegalArgumentException.class, () -> {
            JWTTokenProvider.newBuilder()
                    .clientID("id")
                    .clientSecret("secret")
                    .jwksUrl("url")
                    .build();
        });
        assertEquals("url is mandatory", e.getMessage());

        // clientID is mandatory
        e = assertThrows(IllegalArgumentException.class, () -> {
            JWTTokenProvider.newBuilder()
                    .url("url")
                    .clientSecret("secret")
                    .jwksUrl("url")
                    .build();
        });
        assertEquals("clientID is mandatory", e.getMessage());

        // clientSecret is mandatory
        e = assertThrows(IllegalArgumentException.class, () -> {
            JWTTokenProvider.newBuilder()
                    .url("url")
                    .clientID("id")
                    .jwksUrl("url")
                    .build();
        });
        assertEquals("clientSecret is mandatory", e.getMessage());

        // jwksUrl is mandatory
        e = assertThrows(IllegalArgumentException.class, () -> {
            JWTTokenProvider.newBuilder()
                    .url("url")
                    .clientID("id")
                    .clientSecret("secret")
                    .build();
        });
        assertEquals("jwksUrl is mandatory", e.getMessage());
    }

    @Test
    public void testIamErrorThrowsAuthenticationException() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500).setBody("Internal Server Error"));

        assertThrows(AuthenticationException.class, () -> tokenProvider.getToken());
        assertEquals(1, mockWebServer.getRequestCount());
    }

    @Test
    public void testIam4xxThrowsAuthenticationException() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\":\"invalid_client\"}"));

        assertThrows(AuthenticationException.class, () -> tokenProvider.getToken());
    }

    @Test
    public void testRequestContainsAudienceScopeIssuer() throws Exception {
        String expectedToken = mockTokenUtils.generateTestOIDCToken(3600);
        String tokenJson = "{\"id_token\":\"" + expectedToken + "\",\"access_token\":\"unused\"}";
        mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));

        tokenProvider.getToken();

        RecordedRequest request = mockWebServer.takeRequest();
        String body = request.getBody().readUtf8();
        assertNotNull(request.getHeader("Authorization"), "Basic auth header must be present");
        assertEquals("POST", request.getMethod());
        assert body.contains("grant_type=client_credentials");
        assert body.contains("audience=test-audience");
        assert body.contains("scope=test-scope");
        assert body.contains("issuer=test-issuer");
    }

    @Test
    public void testTokenWithExpiresInFallback() throws Exception {
        // Plain string (not a real JWT) forces the expires_in fallback path for TTL
        String plainToken = "not-a-jwt-token";
        String tokenJson = "{\"id_token\":\"" + plainToken + "\",\"expires_in\":7200}";
        mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));

        String token = tokenProvider.getToken();
        assertEquals(plainToken, token);

        // Second call must reuse cached token — no second IAM request
        String cached = tokenProvider.getToken();
        assertEquals(plainToken, cached);
        assertEquals(1, mockWebServer.getRequestCount());
    }

    @Test
    public void testConcurrentAccessResultsInSingleIamCall() throws Exception {
        String expectedToken = mockTokenUtils.generateTestOIDCToken(3600);
        String tokenJson = "{\"id_token\":\"" + expectedToken + "\",\"access_token\":\"unused\"}";
        for (int i = 0; i < 10; i++) {
            mockWebServer.enqueue(new MockResponse().setBody(tokenJson).setResponseCode(200));
        }

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                startLatch.await();
                return tokenProvider.getToken();
            }));
        }

        startLatch.countDown();
        for (Future<String> f : futures) {
            assertEquals(expectedToken, f.get());
        }
        executor.shutdown();

        assertEquals(1, mockWebServer.getRequestCount());
    }
}