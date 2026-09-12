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

// gcs-connector ships google-auth-library relocated inside its shaded jar. hadoop-ext is compiled
// against, and only ever runs next to, that shaded jar (FE lib/ and BE lib/jni-packages/), so the
// relocated class is the one that is guaranteed to be on the classpath.
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;

/**
 * {@link AccessTokenProvider} that mints GCS access tokens from an inline service-account key
 * (email + private key id + PEM private key) supplied through the Hadoop {@link Configuration}.
 *
 * <p>gcs-connector 3.x removed the inline {@code fs.gs.auth.service.account.{email,private.key.id,private.key}}
 * credentials it accepted in 2.x; its {@code SERVICE_ACCOUNT_JSON_KEYFILE} mode only reads a JSON keyfile from
 * a local path. StarRocks users pass the key inline via {@code gcp.gcs.service_account_*} properties, so this
 * provider is plugged in through {@code fs.gs.auth.type=ACCESS_TOKEN_PROVIDER} to keep that interface working
 * without materialising a keyfile on every FE/BE node.
 */
public class ServiceAccountGCPAccessTokenProvider implements AccessTokenProvider {
    private static final Logger LOG = LogManager.getLogger(ServiceAccountGCPAccessTokenProvider.class);

    // Same key names gcs-connector 2.x used for inline service-account credentials; 3.x ignores them,
    // which is why this provider reads them itself.
    public static final String SERVICE_ACCOUNT_EMAIL_KEY = "fs.gs.auth.service.account.email";
    public static final String SERVICE_ACCOUNT_PRIVATE_KEY_ID_KEY = "fs.gs.auth.service.account.private.key.id";
    public static final String SERVICE_ACCOUNT_PRIVATE_KEY_KEY = "fs.gs.auth.service.account.private.key";
    // Honoured by gcs-connector for its own credential types; honoured here too so tests (or private
    // token endpoints) can redirect the OAuth2 exchange.
    public static final String TOKEN_SERVER_URL_KEY = "fs.gs.token.server.url";

    private static final String CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
    // Refresh a little before the token actually expires so in-flight requests never carry a stale token.
    private static final long REFRESH_MARGIN_MS = 60_000L;
    // Lifetime assumed when the token response carries no expiration. google-auth requires expires_in for
    // service account tokens, so this is defensive; it is deliberately shorter than Google's 1h default.
    private static final long DEFAULT_TOKEN_LIFETIME_MS = 30 * 60_000L;

    private Configuration config;
    private ServiceAccountCredentials credentials;
    private String serviceAccountEmail;
    private AccessToken cachedToken;

    public ServiceAccountGCPAccessTokenProvider() {
        // Instantiated reflectively by gcs-connector; configuration arrives through setConf().
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
        this.credentials = null;
        this.cachedToken = null;
        if (config == null) {
            return;
        }
        String email = config.get(SERVICE_ACCOUNT_EMAIL_KEY, "");
        String privateKeyId = config.get(SERVICE_ACCOUNT_PRIVATE_KEY_ID_KEY, "");
        String privateKey = config.get(SERVICE_ACCOUNT_PRIVATE_KEY_KEY, "");
        if (email.isEmpty() || privateKeyId.isEmpty() || privateKey.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "GCS service account credentials are incomplete: %s%s%s must all be set when using %s",
                    email.isEmpty() ? SERVICE_ACCOUNT_EMAIL_KEY + " " : "",
                    privateKeyId.isEmpty() ? SERVICE_ACCOUNT_PRIVATE_KEY_ID_KEY + " " : "",
                    privateKey.isEmpty() ? SERVICE_ACCOUNT_PRIVATE_KEY_KEY + " " : "",
                    getClass().getName()));
        }
        this.serviceAccountEmail = email;
        try {
            ServiceAccountCredentials.Builder builder = ServiceAccountCredentials.newBuilder()
                    .setClientEmail(email)
                    .setPrivateKeyId(privateKeyId)
                    .setPrivateKeyString(normalizePrivateKey(privateKey))
                    .setScopes(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
            String tokenServerUrl = config.get(TOKEN_SERVER_URL_KEY, "");
            if (!tokenServerUrl.isEmpty()) {
                builder.setTokenServerUri(URI.create(tokenServerUrl));
            }
            this.credentials = builder.build();
        } catch (IOException | IllegalArgumentException e) {
            // Never echo the key material back; the message only names which account failed.
            throw new IllegalArgumentException(
                    "Invalid GCS service account private key for " + email + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    public AccessTokenType getAccessTokenType() {
        return AccessTokenType.GENERIC;
    }

    @Override
    public synchronized AccessToken getAccessToken() {
        if (cachedToken == null || isExpiringSoon(cachedToken)) {
            try {
                refresh();
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to obtain GCS access token for service account " + serviceAccountEmail, e);
            }
        }
        return cachedToken;
    }

    @Override
    public synchronized void refresh() throws IOException {
        if (credentials == null) {
            throw new IOException(getClass().getName() + " is not configured; setConf() must be called with "
                    + SERVICE_ACCOUNT_EMAIL_KEY + ", " + SERVICE_ACCOUNT_PRIVATE_KEY_ID_KEY + " and "
                    + SERVICE_ACCOUNT_PRIVATE_KEY_KEY);
        }
        com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.AccessToken token =
                credentials.refreshAccessToken();
        cachedToken = new AccessToken(token.getTokenValue(),
                expirationOrDefault(token.getExpirationTime(), Instant.now()));
        LOG.debug("Refreshed GCS access token for service account {}", serviceAccountEmail);
    }

    /**
     * Always attach a concrete expiration: a null one would make this provider cache the token forever, and
     * gcs-connector's OAuth2Credentials likewise treats a token without expiration as never needing refresh.
     */
    static Instant expirationOrDefault(Date expiration, Instant now) {
        return expiration == null ? now.plusMillis(DEFAULT_TOKEN_LIFETIME_MS) : expiration.toInstant();
    }

    private static boolean isExpiringSoon(AccessToken token) {
        Instant expiration = token.getExpirationTime();
        return expiration == null
                || Instant.now().plusMillis(REFRESH_MARGIN_MS).isAfter(expiration);
    }

    /**
     * Keys copied out of a JSON keyfile frequently arrive with literal {@code \n} escape sequences instead of
     * real line breaks; the PEM reader needs real ones.
     */
    static String normalizePrivateKey(String privateKey) {
        return privateKey.replace("\\n", "\n").trim();
    }
}
