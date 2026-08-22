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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Authenticates a StarRocks clear-password handshake against IOMETE identity. */
public class IometeAuthenticationProvider implements AuthenticationProvider {
    private static final Logger LOG = LogManager.getLogger(IometeAuthenticationProvider.class);
    private static final String DEFAULT_TOKEN_PATH = "/api/v1/identity/auth/token";
    private static final String DEFAULT_GRANT_TYPE = "password";
    private static final String DEFAULT_CLIENT_ID = "starrocks";

    private final URI tokenUri;
    private final String grantType;
    private final String clientId;
    private final Duration requestTimeout;
    private final HttpClient httpClient;

    public IometeAuthenticationProvider(String baseUrl, String tokenPath, String grantType, String clientId,
                                        Duration connectTimeout, Duration requestTimeout) {
        this.tokenUri = URI.create(joinPath(baseUrl, tokenPath));
        this.grantType = grantType;
        this.clientId = clientId;
        this.requestTimeout = requestTimeout;
        this.httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    public IometeAuthenticationProvider(String baseUrl) {
        this(baseUrl, DEFAULT_TOKEN_PATH, DEFAULT_GRANT_TYPE, DEFAULT_CLIENT_ID,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Override
    public void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        String password = decodeClearPassword(authResponse);
        String form = "username=" + encode(userIdentity.getUser())
                + "&password=" + encode(password)
                + "&grant_type=" + encode(grantType)
                + "&client_id=" + encode(clientId);
        HttpRequest request = HttpRequest.newBuilder(tokenUri)
                .timeout(requestTimeout)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
                .build();

        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new AuthenticationException("IOMETE authentication failed");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AuthenticationException("IOMETE authentication was interrupted");
        } catch (IOException | RuntimeException e) {
            LOG.warn("IOMETE authentication request failed for user {}: {}", userIdentity.getUser(), e.getMessage());
            throw new AuthenticationException("IOMETE authentication service is unavailable");
        }
    }

    static String decodeClearPassword(byte[] authResponse) throws AuthenticationException {
        if (authResponse == null || authResponse.length == 0) {
            throw new AuthenticationException("empty password is not allowed for IOMETE authentication");
        }
        int length = authResponse.length;
        if (authResponse[length - 1] == 0) {
            length--;
        }
        if (length == 0) {
            throw new AuthenticationException("empty password is not allowed for IOMETE authentication");
        }
        return new String(authResponse, 0, length, StandardCharsets.UTF_8);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String joinPath(String baseUrl, String path) {
        String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        String suffix = path.startsWith("/") ? path : "/" + path;
        return base + suffix;
    }
}
