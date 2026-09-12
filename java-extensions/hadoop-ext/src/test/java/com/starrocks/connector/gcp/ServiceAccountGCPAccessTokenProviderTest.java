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

package com.starrocks.connector.gcp;

import com.google.cloud.hadoop.fs.gcs.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceAccountGCPAccessTokenProviderTest {

    private static final String EMAIL = "test@project.iam.gserviceaccount.com";
    private static final String KEY_ID = "key-id-123";

    private static String pemPrivateKey;
    private static HttpServer tokenServer;
    private static String tokenServerUrl;
    private static final AtomicInteger TOKEN_REQUESTS = new AtomicInteger();

    @BeforeAll
    public static void setUp() throws IOException, NoSuchAlgorithmException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        pemPrivateKey = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
                        .encodeToString(keyPair.getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n";

        // Stand-in for https://oauth2.googleapis.com/token: accepts the signed JWT grant and returns a token.
        tokenServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        tokenServer.createContext("/token", exchange -> {
            int n = TOKEN_REQUESTS.incrementAndGet();
            byte[] body = ("{\"access_token\":\"ya29.token-" + n
                    + "\",\"expires_in\":3600,\"token_type\":\"Bearer\"}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        tokenServer.start();
        tokenServerUrl = "http://127.0.0.1:" + tokenServer.getAddress().getPort() + "/token";
    }

    @AfterAll
    public static void tearDown() {
        if (tokenServer != null) {
            tokenServer.stop(0);
        }
    }

    private static Configuration serviceAccountConf(String privateKey) {
        Configuration conf = new Configuration();
        conf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
        conf.set("fs.gs.auth.access.token.provider", ServiceAccountGCPAccessTokenProvider.class.getName());
        conf.set(ServiceAccountGCPAccessTokenProvider.SERVICE_ACCOUNT_EMAIL_KEY, EMAIL);
        conf.set(ServiceAccountGCPAccessTokenProvider.SERVICE_ACCOUNT_PRIVATE_KEY_ID_KEY, KEY_ID);
        conf.set(ServiceAccountGCPAccessTokenProvider.SERVICE_ACCOUNT_PRIVATE_KEY_KEY, privateKey);
        conf.set(ServiceAccountGCPAccessTokenProvider.TOKEN_SERVER_URL_KEY, tokenServerUrl);
        return conf;
    }

    @Test
    public void testConnectorInitializesCredentialsWithoutKeyfile() throws IOException {
        // Drive the real gcs-connector credential factory, i.e. the code path Broker Load hits when the
        // GoogleHadoopFileSystem is created. Before this provider existed this threw
        // NullPointerException from FileInputStream(null) because no JSON keyfile path was configured.
        Configuration conf = serviceAccountConf(pemPrivateKey);
        Assertions.assertNull(conf.get("fs.gs.auth.service.account.json.keyfile"));

        GoogleCredentials credentials = HadoopCredentialsConfiguration.getCredentials(conf, "fs.gs");

        Assertions.assertNotNull(credentials);
        Assertions.assertNotNull(credentials.getAccessToken());
        Assertions.assertTrue(credentials.getAccessToken().getTokenValue().startsWith("ya29.token-"));
        Assertions.assertTrue(credentials.getAccessToken().getExpirationTime().toInstant().isAfter(Instant.now()));
    }

    @Test
    public void testProviderMintsAndCachesToken() throws IOException {
        ServiceAccountGCPAccessTokenProvider provider = new ServiceAccountGCPAccessTokenProvider();
        provider.setConf(serviceAccountConf(pemPrivateKey));
        Assertions.assertEquals(AccessTokenProvider.AccessTokenType.GENERIC, provider.getAccessTokenType());

        int before = TOKEN_REQUESTS.get();
        AccessTokenProvider.AccessToken first = provider.getAccessToken();
        AccessTokenProvider.AccessToken second = provider.getAccessToken();
        Assertions.assertEquals(before + 1, TOKEN_REQUESTS.get(), "valid token should be served from cache");
        Assertions.assertEquals(first.getToken(), second.getToken());
        Assertions.assertTrue(first.getExpirationTime().isAfter(Instant.now()));

        provider.refresh();
        Assertions.assertEquals(before + 2, TOKEN_REQUESTS.get(), "refresh() must always fetch a new token");
        Assertions.assertNotEquals(first.getToken(), provider.getAccessToken().getToken());
    }

    @Test
    public void testAcceptsEscapedNewlinesInPrivateKey() {
        // Keys pasted out of a JSON keyfile commonly carry literal "\n" sequences.
        String escaped = pemPrivateKey.replace("\n", "\\n");
        ServiceAccountGCPAccessTokenProvider provider = new ServiceAccountGCPAccessTokenProvider();
        provider.setConf(serviceAccountConf(escaped));
        Assertions.assertNotNull(provider.getAccessToken().getToken());
    }

    @Test
    public void testMissingCredentialsFailWithDescriptiveError() {
        Configuration conf = serviceAccountConf(pemPrivateKey);
        conf.unset(ServiceAccountGCPAccessTokenProvider.SERVICE_ACCOUNT_PRIVATE_KEY_KEY);
        ServiceAccountGCPAccessTokenProvider provider = new ServiceAccountGCPAccessTokenProvider();
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.setConf(conf));
        Assertions.assertTrue(e.getMessage().contains(ServiceAccountGCPAccessTokenProvider.SERVICE_ACCOUNT_PRIVATE_KEY_KEY),
                e.getMessage());
    }

    @Test
    public void testMalformedPrivateKeyDoesNotLeakKeyMaterial() {
        String bogus = "-----BEGIN PRIVATE KEY-----\nbm90LWEta2V5\n-----END PRIVATE KEY-----";
        ServiceAccountGCPAccessTokenProvider provider = new ServiceAccountGCPAccessTokenProvider();
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.setConf(serviceAccountConf(bogus)));
        Assertions.assertTrue(e.getMessage().contains(EMAIL), e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("bm90LWEta2V5"), e.getMessage());
    }

    @Test
    public void testMissingExpirationGetsDefaultLifetime() {
        Instant now = Instant.parse("2026-09-04T00:00:00Z");
        Instant explicit = Instant.parse("2026-09-04T01:00:00Z");
        Assertions.assertEquals(explicit,
                ServiceAccountGCPAccessTokenProvider.expirationOrDefault(Date.from(explicit), now));
        Instant defaulted = ServiceAccountGCPAccessTokenProvider.expirationOrDefault(null, now);
        Assertions.assertTrue(defaulted.isAfter(now), "token without expiration must still expire");
        Assertions.assertTrue(defaulted.isBefore(now.plusSeconds(3600)),
                "assumed lifetime must not exceed Google's default 1h token lifetime");
    }

    @Test
    public void testUnconfiguredProviderFails() {
        ServiceAccountGCPAccessTokenProvider provider = new ServiceAccountGCPAccessTokenProvider();
        Assertions.assertThrows(IOException.class, provider::refresh);
        Assertions.assertThrows(UncheckedIOException.class, provider::getAccessToken);
    }
}
