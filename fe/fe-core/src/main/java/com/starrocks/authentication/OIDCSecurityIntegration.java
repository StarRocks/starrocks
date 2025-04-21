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
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OIDCSecurityIntegration extends SecurityIntegration {

    public static final Set<String> REQUIRED_PROPERTIES = new HashSet<>(Arrays.asList(
            SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
            OpenIdConnectAuthenticationProvider.OIDC_JWKS_URL,
            OpenIdConnectAuthenticationProvider.OIDC_PRINCIPAL_FIELD));

    private static final Pattern COMMA_SPLIT = Pattern.compile("\\s*,\\s*");

    public OIDCSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
        String jwksUrl = propertyMap.get(OpenIdConnectAuthenticationProvider.OIDC_JWKS_URL);
        String principalFiled = propertyMap.get(OpenIdConnectAuthenticationProvider.OIDC_PRINCIPAL_FIELD);
        String commaSeparatedIssuer = propertyMap.get(OpenIdConnectAuthenticationProvider.OIDC_REQUIRED_ISSUER);
        String[] requireIssuer = commaSeparatedIssuer == null ?
                new String[0] : COMMA_SPLIT.split(commaSeparatedIssuer);
        String commaSeperatedRequireAudiences = propertyMap.get(OpenIdConnectAuthenticationProvider.OIDC_REQUIRED_AUDIENCE);
        String[] requireAudiences = commaSeperatedRequireAudiences == null ?
                new String[0] : COMMA_SPLIT.split(commaSeperatedRequireAudiences.trim());
        return new OpenIdConnectAuthenticationProvider(jwksUrl, principalFiled, requireIssuer, requireAudiences);
    }

    @Override
    public void checkProperty() throws SemanticException {
        REQUIRED_PROPERTIES.forEach(s -> {
            if (!propertyMap.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });
    }
}
