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

import com.google.re2j.Pattern;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OAuth2SecurityIntegration extends SecurityIntegration {
    final Set<String> requiredProperties = new HashSet<>(Arrays.asList(
            SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
            OAuth2AuthenticationProvider.OAUTH2_TOKEN_SERVER_URL,
            OAuth2AuthenticationProvider.OAUTH2_REDIRECT_URL,
            OAuth2AuthenticationProvider.OAUTH2_CLIENT_ID,
            OAuth2AuthenticationProvider.OAUTH2_CLIENT_SECRET,
            OAuth2AuthenticationProvider.OAUTH2_JWKS_URL,
            OAuth2AuthenticationProvider.OAUTH2_PRINCIPAL_FIELD));

    private static final Pattern COMMA_SPLIT = Pattern.compile("\\s*,\\s*");

    public OAuth2SecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
        String authServerUrl = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_AUTH_SERVER_URL,
                Config.oauth2_auth_server_url);
        String tokenServerUrl = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_TOKEN_SERVER_URL,
                Config.oauth2_token_server_url);
        String oauthRedirectUrl = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_REDIRECT_URL,
                Config.oauth2_redirect_url);
        String clientId = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_CLIENT_ID,
                Config.oauth2_client_id);
        String clientSecret = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_CLIENT_SECRET,
                Config.oauth2_client_secret);
        String jwksUrl = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_JWKS_URL,
                Config.oauth2_jwks_url);
        String principalField = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_PRINCIPAL_FIELD,
                Config.oauth2_principal_field);
        String commaSeparatedIssuer = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_ISSUER,
                Config.oauth2_required_issuer);
        String[] requiredIssuers = commaSeparatedIssuer == null ?
                new String[0] : COMMA_SPLIT.split(commaSeparatedIssuer);
        String commaSeperatedAudiences = propertyMap.getOrDefault(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_AUDIENCE,
                Config.oauth2_required_audience);
        String[] requiredAudiences = commaSeperatedAudiences == null ?
                new String[0] : COMMA_SPLIT.split(commaSeperatedAudiences.trim());

        Long connectWaitTimeout;
        if (propertyMap.containsKey(OAuth2AuthenticationProvider.OAUTH2_CONNECT_WAIT_TIMEOUT)) {
            connectWaitTimeout = Long.valueOf(propertyMap.get(OAuth2AuthenticationProvider.OAUTH2_CONNECT_WAIT_TIMEOUT));
        } else {
            connectWaitTimeout = Config.oauth2_connect_wait_timeout;
        }

        return new OAuth2AuthenticationProvider(new OAuth2Context(
                authServerUrl,
                tokenServerUrl,
                oauthRedirectUrl,
                clientId,
                clientSecret,
                jwksUrl,
                principalField,
                requiredIssuers,
                requiredAudiences,
                connectWaitTimeout));
    }

    @Override
    public void checkProperty() {
        requiredProperties.forEach(s -> {
            if (!propertyMap.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });

        validateIntegerProp(propertyMap, OAuth2AuthenticationProvider.OAUTH2_CONNECT_WAIT_TIMEOUT, 0, Integer.MAX_VALUE);
    }

    private void validateIntegerProp(Map<String, String> propertyMap, String key, int min, int max)
            throws SemanticException {
        if (propertyMap.containsKey(key)) {
            String val = propertyMap.get(key);
            try {
                int intVal = Integer.parseInt(val);
                if (intVal < min || intVal > max) {
                    throw new NumberFormatException("current value of '" +
                            key + "' is invalid, value: " + intVal +
                            ", should be in range [" + min + ", " + max + "]");
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("invalid '" +
                        key + "' property value: " + val + ", error: " + e.getMessage(), e);
            }
        }
    }
}
