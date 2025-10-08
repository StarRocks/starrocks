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

package com.starrocks.connector.iceberg.rest;

import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.Security.JWT;
import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.Security.NONE;
import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.Security.OAUTH2;
import static com.starrocks.connector.iceberg.rest.OAuth2SecurityConfig.TOKEN_REFRESH_ENABLED;

public class OAuth2SecurityConfig {

    private IcebergRESTCatalog.Security security = NONE;
    private String credential;
    private String scope;
    private String token;
    private URI serverUri;
    private String audience;
    private boolean tokenRefreshEnabled = OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT;

    // The type of security to use (default: NONE). OAUTH2 requires either a token or credential. Example: OAUTH2
    public static String SECURITY = "security";
    public static String SECURITY_OAUTH2 = "oauth2";
    // The credential to exchange for a token in the OAuth2 client credentials flow with the server
    public static String OAUTH2_CREDENTIAL = "oauth2.credential";
    // The scope which will be used for interactions with the server
    public static String OAUTH2_SCOPE = "oauth2.scope";
    // The Bearer token which will be used for interactions with the server
    public static String OAUTH2_TOKEN = "oauth2.token";
    // The endpoint to retrieve access token from OAuth2 Server
    public static String SERVER_URI = "oauth2.server-uri";
    public static String OAUTH2_AUDIENCE = "oauth2.audience";
    public static String TOKEN_REFRESH_ENABLED = "oauth2.token-refresh-enabled";

    public IcebergRESTCatalog.Security getSecurity() {
        return security;
    }

    public OAuth2SecurityConfig setSecurity(IcebergRESTCatalog.Security security) {
        this.security = security;
        return this;
    }

    public Optional<String> getCredential() {
        return Optional.ofNullable(credential);
    }

    public OAuth2SecurityConfig setCredential(String credential) {
        this.credential = credential;
        return this;
    }

    public Optional<String> getScope() {
        return Optional.ofNullable(scope);
    }

    public OAuth2SecurityConfig setScope(String scope) {
        this.scope = scope;
        return this;
    }

    public Optional<String> getToken() {
        return Optional.ofNullable(token);
    }

    public OAuth2SecurityConfig setToken(String token) {
        this.token = token;
        return this;
    }

    public Optional<URI> getServerUri() {
        return Optional.ofNullable(serverUri);
    }

    public OAuth2SecurityConfig setServerUri(URI serverUri) {
        this.serverUri = serverUri;
        return this;
    }

    public Optional<String> getAudience() {
        return Optional.ofNullable(audience);
    }

    public OAuth2SecurityConfig setAudience(String audience) {
        this.audience = audience;
        return this;
    }

    // OAuth2 requires a credential or token
    public boolean credentialOrTokenPresent() {
        return credential != null || token != null;
    }

    // Scope is applicable only when using credential
    public boolean scopePresentOnlyWithCredential() {
        return !(token != null && scope != null);
    }

    public Optional<Boolean> isTokenRefreshEnabled() {
        return Optional.of(tokenRefreshEnabled);
    }

    public OAuth2SecurityConfig setTokenRefreshEnabled(Boolean tokenRefreshEnabled) {
        this.tokenRefreshEnabled = tokenRefreshEnabled;
        return this;
    }
}

class OAuth2SecurityConfigBuilder {
    public static OAuth2SecurityConfig build(final Map<String, String> properties) {
        String strSecurity = properties.get(OAuth2SecurityConfig.SECURITY);

        if (strSecurity == null) {
            strSecurity = NONE.name();
        }

        OAuth2SecurityConfig config = new OAuth2SecurityConfig();
        if (strSecurity.equalsIgnoreCase(NONE.name())) {
            return new OAuth2SecurityConfig();
        } else if (strSecurity.equalsIgnoreCase(JWT.name())) {
            config.setSecurity(JWT);
        } else if (strSecurity.equalsIgnoreCase(OAUTH2.name())) {
            config = config.setSecurity(OAUTH2);
        }

        config.setCredential(properties.get(OAuth2SecurityConfig.OAUTH2_CREDENTIAL))
                .setScope(properties.get(OAuth2SecurityConfig.OAUTH2_SCOPE))
                .setToken(properties.get(OAuth2SecurityConfig.OAUTH2_TOKEN))
                .setAudience(properties.get(OAuth2SecurityConfig.OAUTH2_AUDIENCE))
                .setTokenRefreshEnabled(Boolean.valueOf(properties.getOrDefault(TOKEN_REFRESH_ENABLED,
                        String.valueOf(OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT))));

        String serverUri = properties.get(OAuth2SecurityConfig.SERVER_URI);
        if (serverUri != null) {
            config = config.setServerUri(URI.create(serverUri));
        }

        // check OAuth2SecurityConfig is valid
        if (!config.credentialOrTokenPresent() || !config.scopePresentOnlyWithCredential()) {
            return new OAuth2SecurityConfig();
        }

        return config;
    }
}
