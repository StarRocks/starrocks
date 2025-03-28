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

import java.net.URI;
import java.util.Map;
import java.util.Optional;

public class OAuth2SecurityConfig {
    private SecurityEnum security = SecurityEnum.NONE;
    private String credential;
    private String scope;
    private String token;
    private URI serverUri;

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

    public SecurityEnum getSecurity() {
        return security;
    }

    public OAuth2SecurityConfig setSecurity(SecurityEnum security) {
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

    // OAuth2 requires a credential or token
    public boolean credentialOrTokenPresent() {
        return credential != null || token != null;
    }

    // Scope is applicable only when using credential
    public boolean scopePresentOnlyWithCredential() {
        return !(token != null && scope != null);
    }
}

class OAuth2SecurityConfigBuilder {
    public static OAuth2SecurityConfig build(final Map<String, String> properties) {
        SecurityEnum security = SecurityEnum.NONE;
        String strSecurity = properties.get(OAuth2SecurityConfig.SECURITY);
        if (strSecurity != null && strSecurity.equalsIgnoreCase(OAuth2SecurityConfig.SECURITY_OAUTH2)) {
            security = SecurityEnum.OAUTH2;
        }

        if (security == SecurityEnum.NONE) {
            return new OAuth2SecurityConfig();
        }

        OAuth2SecurityConfig config = new OAuth2SecurityConfig();
        config = config.setSecurity(SecurityEnum.OAUTH2)
                .setCredential(properties.get(OAuth2SecurityConfig.OAUTH2_CREDENTIAL))
                .setScope(properties.get(OAuth2SecurityConfig.OAUTH2_SCOPE))
                .setToken(properties.get(OAuth2SecurityConfig.OAUTH2_TOKEN));
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

