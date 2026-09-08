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
import brave.okhttp3.TracingInterceptor;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.authentication.http.BasicAuthInterceptor;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class JWTTokenProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(JWTTokenProvider.class);

    private static final String GRANT_TYPE = "grant_type";
    private static final String SCOPE = "scope";
    private static final String AUDIENCE = "audience";
    private static final String TOKEN_ENDPOINT = "oauth2/token";
    private static final String ISSUER = "issuer";

    // token exchange grant type value
    private static final String CLI_CRN_GRANT_TYPE_VAL = "client_credentials";

    // available across threads
    private volatile CachedToken cachedToken;
    private final Lock refreshLock = new ReentrantLock();

    // private Cache<String, TokenSet> tokenCache;
    private final OkHttpClient client;
    private final Gson gson;
    private final String url;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String audience;
    private final long connectionTimeoutSec;
    private final long readTimeoutSec;
    private final HttpTracing httpTracing;
    private final String jwksUrl;
    private final String principalField;
    private final String issuer;

    enum Policy {
        ROOT,
        STATIC_CLIENT
    }

    private JWTTokenProvider(
            String url, 
            String clientId,
            String clientSecret, 
            String scope,
            String audience,
            long connectionTimeoutSec,
            long readTimeoutSec,
            HttpTracing httpTracing,
            String jwksUrl,
            String principalField,
            String issuer) {
        this.url = url;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.audience = audience;
        this.connectionTimeoutSec = connectionTimeoutSec;
        this.readTimeoutSec = readTimeoutSec;
        this.httpTracing = httpTracing;
        this.jwksUrl = jwksUrl;
        this.principalField = principalField;
        this.issuer = issuer;

        // tokenCache = CacheBuilder.newBuilder().maximumSize(this.maxSize)
        //         .expireAfterWrite(this.ttl, TimeUnit.MINUTES).build();
        client = buildClient(httpTracing);
        gson = (new GsonBuilder()).create();
    }

    private OkHttpClient buildClient(HttpTracing httpTracing) {
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
                .connectTimeout(connectionTimeoutSec, TimeUnit.SECONDS)
                .readTimeout(readTimeoutSec, TimeUnit.SECONDS)
                .addInterceptor(new BasicAuthInterceptor(clientId, clientSecret));
        if (httpTracing != null && httpTracing.tracing() != null) {
            okHttpClientBuilder.addInterceptor(TracingInterceptor.create(httpTracing));
        }
        return okHttpClientBuilder.build();
    }

    public String getToken() throws AuthenticationException {
        LOGGER.debug("JWTTokenProvider: getting token");
        try {
            CachedToken token = cachedToken;

            // Fast path (no lock)
            if (token != null && !token.isCloseToExpiry()) {
                LOGGER.debug("Serving cached token: expires_at={}, ttl_remaining_seconds={}",
                        token.expiresAt(),
                        (token.expiresAt().getTime() - System.currentTimeMillis()) / 1000);
                return token.token().getIdToken();
            }

            refreshLock.lock();
            try {
                // Double check, after the lock
                token = cachedToken;
                if (token == null || token.isCloseToExpiry()) {
                    LOGGER.info("Fetching new token from IAS (reason: {})",
                            token == null ? "no cached token" : "token close to expiry");
                    cachedToken = token = requestToken();
                }
                return token.token().getIdToken();

            } finally {
                refreshLock.unlock();
            }
        } catch (Exception e) {
            throw new AuthenticationException("Failed to get token", e);
        }
    }

    private Request clientCredentialsRequest() {
        FormBody.Builder builder = new FormBody.Builder();
        builder.addEncoded(GRANT_TYPE, CLI_CRN_GRANT_TYPE_VAL);
        if (isNotEmpty(audience)) {
            builder.addEncoded(AUDIENCE, audience);
        }
        if (isNotEmpty(scope)) {
            builder.addEncoded(SCOPE, scope);
        }
        if (isNotEmpty(issuer)) {
            builder.addEncoded(ISSUER, issuer);
        }
        LOGGER.info("Building token request: url={}, grant_type={}, audience={}, scope={}, issuer={}",
                HttpUrl.parse(url).newBuilder().addPathSegments(TOKEN_ENDPOINT).build(),
                CLI_CRN_GRANT_TYPE_VAL,
                audience != null ? audience : "<not set>",
                scope != null ? scope : "<not set>",
                issuer != null ? issuer : "<not set>");
        LOGGER.info("Reproduce with: curl -X POST {} -u '{}:****' -d 'grant_type=client_credentials{}{}{}'",
                HttpUrl.parse(url).newBuilder().addPathSegments(TOKEN_ENDPOINT).build(),
                clientId,
                isNotEmpty(scope) ? "&scope=" + scope : "",
                isNotEmpty(audience) ? "&audience=" + audience : "",
                isNotEmpty(issuer) ? "&issuer=" + issuer : "");
        return buildRequest(builder.build());
    }

    private Request buildRequest(RequestBody body) {
        Request request = new Request.Builder()
                .url(HttpUrl.parse(url).newBuilder().addPathSegments(TOKEN_ENDPOINT).build())
                .post(body)
                .build();
        return request;
    }

    private CachedToken requestToken() throws AuthenticationException {
        Request request = clientCredentialsRequest();
        LOGGER.info("Sending token request to: {}", request.url());
        try (Response response = client.newCall(request).execute()) {
            LOGGER.info("Token response: code={}", response.code());
            if (response.isSuccessful() && response.body() != null) {
                String tokenJson = response.body().string();
                TokenSet tokenSet = gson.fromJson(tokenJson, TokenSet.class);
                String idToken = tokenSet.getIdToken();
                LOGGER.info("Token response: expires_in={}, id_token_prefix={}, id_token_length={}",
                        tokenSet.getExpiresIn(),
                        idToken != null ? idToken.substring(0, Math.min(6, idToken.length())) : "null",
                        idToken != null ? idToken.length() : 0);
                return buildCachedToken(tokenSet);
            } else {
                String errorBody = response.body() != null ? response.body().string() : "null";
                String errorMessage = String.format("error getting token for request: %s and response: code: %d, body: %s",
                        request, response.code(), errorBody);
                LOGGER.error(errorMessage);
                throw new AuthenticationException(errorMessage);
            }
        } catch (IOException e) {
            LOGGER.error("error getting token for request: {}", request, e);
            throw new AuthenticationException("error getting token", e);
        }
    }

    private CachedToken buildCachedToken(TokenSet tokenSet) throws AuthenticationException {
        String idToken = tokenSet.getIdToken();
        if (!isNotEmpty(idToken)) {
            throw new AuthenticationException("Token response missing id_token");
        }

        Date exp = null;
        Date iat = null;

        // Primary: parse expiry from the id_token JWT claims
        try {
            JWTClaimsSet claims = SignedJWT.parse(idToken).getJWTClaimsSet();
            exp = claims.getExpirationTime();
            iat = claims.getIssueTime();
            LOGGER.info("Token JWT claims: sub={}, iss={}", claims.getSubject(), claims.getIssuer());
        } catch (java.text.ParseException pe) {
            LOGGER.debug("id_token is not a parseable JWT; will use expires_in: {}", pe.getMessage());
        }

        // Fallback: use expires_in from the OAuth2 response
        if (exp == null && tokenSet.getExpiresIn() > 0) {
            iat = new Date(System.currentTimeMillis());
            exp = new Date(iat.getTime() + tokenSet.getExpiresIn() * 1000L);
        }

        // Last resort: default 1-hour TTL to avoid caching a token indefinitely
        if (exp == null) {
            LOGGER.warn("Token has no expiry information (no exp claim, no expires_in); defaulting to 1-hour TTL");
            iat = new Date(System.currentTimeMillis());
            exp = new Date(iat.getTime() + 3600_000L);
        }

        LOGGER.info("Cached token: iat={}, exp={}, ttl_seconds={}",
                iat, exp, (exp.getTime() - (iat != null ? iat.getTime() : System.currentTimeMillis())) / 1000);
        return new CachedToken(tokenSet, exp, iat);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String url;
        private String clientID;
        private String clientSecret;
        private String scope;
        private String audience;
        private long connectionTimeoutSec = 30;
        private long readTimeoutSec = 30;
        private HttpTracing httpTracing;
        private String jwksUrl;
        private String principalField;
        private String issuer;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder clientID(String clientID) {
            this.clientID = clientID;
            return this;
        }

        public Builder clientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        public Builder scope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder audience(String audience) {
            this.audience = audience;
            return this;
        }

        public Builder connectionParams(long connectionTimeoutSec, long readTimeoutSec) {
            this.connectionTimeoutSec = connectionTimeoutSec;
            this.readTimeoutSec = readTimeoutSec;
            return this;
        }

        public Builder jwksUrl(String jwksUrl) {
            this.jwksUrl = jwksUrl;
            return this;
        }

        public Builder principalField(String principalField) {
            this.principalField = principalField;
            return this;
        }

        public Builder issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Builder httpTracing(HttpTracing httpTracing) {
            this.httpTracing = httpTracing;
            return this;
        }

        Builder() {
        }

        public JWTTokenProvider build() {
            Preconditions.checkArgument(isNotEmpty(url), "url is mandatory");
            Preconditions.checkArgument(isNotEmpty(clientID), "clientID is mandatory");
            Preconditions.checkArgument(isNotEmpty(clientSecret), "clientSecret is mandatory");
            Preconditions.checkArgument(isNotEmpty(jwksUrl), "jwksUrl is mandatory");
            return new JWTTokenProvider(url, clientID, clientSecret, scope, audience,
                    connectionTimeoutSec, readTimeoutSec, httpTracing, jwksUrl,
                    principalField, issuer);
        }
    }

    private record CachedToken(TokenSet token, Date expiresAt, Date issueTime) {
        boolean isExpired() {
            if (expiresAt == null) {
                return false;
            }
            return System.currentTimeMillis() > expiresAt.getTime();
        }

        public boolean isCloseToExpiry() {
            if (expiresAt == null) {
                return false;
            }
            if (issueTime != null) {
                long lifetime = expiresAt.getTime() - issueTime.getTime();
                long age = System.currentTimeMillis() - issueTime.getTime();
                if (age > lifetime * 0.8) {
                    LOGGER.debug("cached token is close to expiry");
                    return true;
                }
            }
            return isExpired();
        }
    }
}
