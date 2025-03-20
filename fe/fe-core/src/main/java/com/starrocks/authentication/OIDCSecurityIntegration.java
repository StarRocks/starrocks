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

import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OIDCSecurityIntegration extends SecurityIntegration {
    public static final String OIDC_JWKS_URL = "oidc_jwks_url";
    public static final String OIDC_PRINCIPAL_FIELD = "oidc_principal_field";
    public static final String OIDC_REQUIRED_ISSUER = "oidc_required_issuer";
    public static final String OIDC_REQUIRED_AUDIENCE = "oidc_required_audience";

    public static final Set<String> REQUIRED_PROPERTIES = new HashSet<>(Arrays.asList(
            SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
            OIDCSecurityIntegration.OIDC_JWKS_URL,
            OIDCSecurityIntegration.OIDC_PRINCIPAL_FIELD));

    public OIDCSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() throws AuthenticationException {
        String jwksUrl = propertyMap.get(OIDC_JWKS_URL);
        String principalFiled = propertyMap.get(OIDC_PRINCIPAL_FIELD);
        String requireIssuer = propertyMap.get(OIDC_REQUIRED_ISSUER);
        String requireAudience = propertyMap.get(OIDC_REQUIRED_AUDIENCE);
        return new OpenIdConnectAuthenticationProvider(jwksUrl, principalFiled, requireIssuer, requireAudience);
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
